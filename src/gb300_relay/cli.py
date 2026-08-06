from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import tempfile
import uuid
from contextlib import suppress
from dataclasses import asdict
from pathlib import Path

import uvicorn

from . import __version__
from .client import RelayClient
from .config import AppConfig, GatewayConfig, load_config
from .errors import ConditionalWriteFailed, RelayError
from .gateway import create_app
from .gc import GarbageCollector
from .logging_utils import configure_logging
from .metrics import RelayMetrics
from .storage import S3ObjectStore, sha256_file
from .worker import RelayWorker


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gb300-relay",
        description="S3-backed OpenAI-compatible relay for OSMO and GB300",
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--log-level", default=os.getenv("GB300_RELAY_LOG_LEVEL", "INFO"))
    parser.add_argument("--plain-logs", action="store_true")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def with_config(name: str, help_text: str) -> argparse.ArgumentParser:
        child = subparsers.add_parser(name, help=help_text)
        child.add_argument("--config", required=True, type=Path)
        return child

    gateway = with_config("gateway", "run the OpenAI-compatible OSMO gateway")
    gateway.add_argument("--host")
    gateway.add_argument("--port", type=int)

    worker = with_config("worker", "run a GB300 queue worker")
    worker.add_argument("--once", action="store_true", help="drain the current queue and exit")

    doctor = with_config("doctor", "validate credentials, bucket, conditionals, and s5cmd")
    doctor.add_argument("--skip-transfer", action="store_true")

    submit = with_config("submit", "submit one OpenAI JSON request directly to S3")
    submit.add_argument("--request", required=True, type=Path)
    submit.add_argument("--target", required=True)
    submit.add_argument("--endpoint", default="/v1/chat/completions")
    submit.add_argument("--timeout", type=float, default=900)
    submit.add_argument("--idempotency-key")

    wait = with_config("wait", "wait for and print one relay result")
    wait.add_argument("--job-id", required=True)
    wait.add_argument("--timeout", type=float, default=900)
    wait.add_argument("--keep", action="store_true", help="do not delete acknowledged objects")

    status = with_config("status", "show relay job status")
    status.add_argument("--job-id", required=True)

    cleanup = with_config("cleanup", "delete one exact acknowledged job")
    cleanup.add_argument("--job-id", required=True)
    cleanup.add_argument("--target", required=True)

    gc = with_config("gc", "collect old acknowledged jobs and stale worker heartbeats")
    gc.add_argument(
        "--apply",
        action="store_true",
        help="perform deletion; without this flag only report candidates",
    )
    return parser


def _client(config: AppConfig, store: S3ObjectStore) -> RelayClient:
    gateway = config.gateway or GatewayConfig(require_healthy_worker=False)
    return RelayClient(
        store,
        prefix=config.s3.prefix,
        client_id=gateway.client_id,
        media_policy=gateway.media,
        poll_interval_seconds=gateway.poll_interval_seconds,
        compact_protocol=gateway.compact_protocol,
        compact_manifest_max_bytes=gateway.compact_manifest_max_bytes,
    )


def _run_gateway(config: AppConfig, host: str | None, port: int | None) -> int:
    if config.gateway is None:
        raise RelayError("config has no [gateway] section")
    effective_host = host or config.gateway.host
    if (
        effective_host not in {"127.0.0.1", "::1", "localhost"}
        and not config.gateway.auth_token_env
    ):
        raise RelayError(
            "refusing unauthenticated non-loopback gateway; set gateway.auth_token_env"
        )
    metrics = RelayMetrics("gateway")
    if config.metrics.enabled:
        metrics.start_server(config.metrics.host, config.metrics.port)
    store = S3ObjectStore(config.s3)
    client = _client(config, store)
    app = create_app(store, client, config.gateway, prefix=config.s3.prefix, metrics=metrics)
    try:
        uvicorn.run(
            app,
            host=effective_host,
            port=port or config.gateway.port,
            log_config=None,
        )
    finally:
        store.close()
    return 0


async def _run_worker_async(config: AppConfig, once: bool) -> int:
    if config.worker is None:
        raise RelayError("config has no [worker] section")
    metrics = RelayMetrics("worker")
    if config.metrics.enabled:
        metrics.start_server(config.metrics.host, config.metrics.port)
    store = S3ObjectStore(config.s3)
    worker = RelayWorker(
        store,
        prefix=config.s3.prefix,
        config=config.worker,
        metrics=metrics,
    )
    if once:
        try:
            outcomes = await worker.run_until_idle()
            print(
                json.dumps(
                    [
                        {
                            "job_id": item.job_id,
                            "handled": item.handled,
                            "status": item.status.value if item.status else None,
                        }
                        for item in outcomes
                    ],
                    separators=(",", ":"),
                )
            )
            return 0
        finally:
            await worker.upstream.close()
            store.close()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(signum, worker.stop)
    try:
        await worker.run_forever()
    finally:
        store.close()
    return 0


def _doctor(config: AppConfig, *, skip_transfer: bool) -> int:
    store = S3ObjectStore(config.s3)
    probe = f"{config.s3.prefix}/doctor/{uuid.uuid4().hex}"
    try:
        store.ping()
        print("bucket=ok")
        control_key = f"{probe}/conditional"
        store.put_bytes(control_key, b"one", if_absent=True)
        try:
            store.put_bytes(control_key, b"two", if_absent=True)
        except ConditionalWriteFailed:
            print("conditional_create=ok")
        else:
            raise RelayError("object store did not enforce immutable conditional create")
        if not skip_transfer:
            with tempfile.TemporaryDirectory(prefix="gb300-relay-doctor-") as directory:
                source = Path(directory) / "source.bin"
                destination = Path(directory) / "destination.bin"
                source.write_bytes(os.urandom(1024 * 1024 + 17))
                data_key = f"{probe}/transfer.bin"
                store.upload_file(source, data_key, force_s5cmd=True)
                store.download_file(data_key, destination, force_s5cmd=True)
                if sha256_file(source) != sha256_file(destination):
                    raise RelayError("s5cmd round-trip digest mismatch")
                print("s5cmd_round_trip=ok")
        return 0
    finally:
        removed = store.delete_prefix(probe + "/")
        print(f"cleanup_objects={removed}")
        store.close()


def _submit(config: AppConfig, args: argparse.Namespace) -> int:
    body = json.loads(args.request.read_text(encoding="utf-8"))
    if not isinstance(body, dict):
        raise RelayError("request JSON must be an object")
    with S3ObjectStore(config.s3) as store:
        handle = _client(config, store).submit(
            endpoint=args.endpoint,
            body=body,
            target=args.target,
            timeout_seconds=args.timeout,
            idempotency_key=args.idempotency_key,
        )
    print(handle.model_dump_json())
    return 0


def _wait(config: AppConfig, args: argparse.Namespace) -> int:
    with S3ObjectStore(config.s3) as store:
        completed = _client(config, store).wait(
            args.job_id,
            timeout_seconds=args.timeout,
            cleanup=not args.keep,
        )
    sys.stdout.buffer.write(completed.body)
    if not completed.body.endswith(b"\n"):
        sys.stdout.buffer.write(b"\n")
    return 0 if 200 <= completed.metadata.http_status < 300 else 1


def _status(config: AppConfig, args: argparse.Namespace) -> int:
    with S3ObjectStore(config.s3) as store:
        status = _client(config, store).status(args.job_id)
    done = status.done.model_dump(mode="json") if status.done else None
    if done and done.get("response"):
        done["response"]["body_base64"] = "<omitted>"
    print(
        json.dumps(
            {
                "job_id": status.job_id,
                "state": status.state,
                "done": done,
            },
            default=str,
            separators=(",", ":"),
        )
    )
    return 0


def _cleanup(config: AppConfig, args: argparse.Namespace) -> int:
    with S3ObjectStore(config.s3) as store:
        removed = _client(config, store).cleanup(target=args.target, job_id=args.job_id)
    print(json.dumps({"job_id": args.job_id, "removed_objects": removed}, separators=(",", ":")))
    return 0


def _gc(config: AppConfig, args: argparse.Namespace) -> int:
    with S3ObjectStore(config.s3) as store:
        report = GarbageCollector(
            store,
            prefix=config.s3.prefix,
            config=config.retention,
        ).collect(apply=args.apply)
    print(json.dumps(asdict(report), separators=(",", ":")))
    return 1 if report.errors else 0


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    configure_logging(args.log_level, json_logs=not args.plain_logs)
    try:
        config = load_config(args.config)
        if args.command == "gateway":
            return _run_gateway(config, args.host, args.port)
        if args.command == "worker":
            return asyncio.run(_run_worker_async(config, args.once))
        if args.command == "doctor":
            return _doctor(config, skip_transfer=args.skip_transfer)
        if args.command == "submit":
            return _submit(config, args)
        if args.command == "wait":
            return _wait(config, args)
        if args.command == "status":
            return _status(config, args)
        if args.command == "cleanup":
            return _cleanup(config, args)
        if args.command == "gc":
            return _gc(config, args)
        raise RelayError(f"unsupported command: {args.command}")
    except (RelayError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
