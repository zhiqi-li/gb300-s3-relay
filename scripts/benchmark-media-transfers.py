#!/usr/bin/env python3
"""A/B worker-global versus per-request image/video download concurrency."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import statistics
import time
import uuid
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from urllib.parse import unquote, urlparse

from gb300_relay.client import RelayClient
from gb300_relay.config import AppConfig, MediaPolicy, load_config
from gb300_relay.protocol import JobHandle, JobStatus
from gb300_relay.storage import S3ObjectStore
from gb300_relay.upstream import UpstreamResponse, UpstreamStream
from gb300_relay.worker import RelayWorker


class MediaEchoUpstream:
    """Validate restored media locally without adding model latency."""

    def __init__(self, *, image_size: int, video_size: int) -> None:
        self.image_size = image_size
        self.video_size = video_size
        self.started: dict[int, float] = {}

    async def request(self, endpoint, body, forwarded_headers):
        del endpoint, forwarded_headers
        content = body["messages"][0]["content"]
        image_url = content[1]["image_url"]["url"]
        video_url = content[2]["video_url"]["url"]
        if not image_url.startswith("data:image/"):
            raise ValueError("worker did not restore the image as a data URI")
        image_data = base64.b64decode(image_url.split(",", 1)[1], validate=True)
        if len(image_data) != self.image_size:
            raise ValueError("worker restored an image with the wrong size")
        parsed = urlparse(video_url)
        if parsed.scheme == "file":
            video_size = Path(unquote(parsed.path)).stat().st_size
        elif video_url.startswith("data:video/"):
            video_size = len(base64.b64decode(video_url.split(",", 1)[1], validate=True))
        else:
            raise ValueError("worker did not restore the video as a file or data URI")
        if video_size != self.video_size:
            raise ValueError("worker restored a video with the wrong size")
        request_index = int(body["relay_benchmark_index"])
        self.started[request_index] = time.perf_counter()
        payload = json.dumps(
            {
                "id": f"media-echo-{request_index}",
                "object": "chat.completion",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "media-ok"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
            },
            separators=(",", ":"),
        ).encode()
        return UpstreamResponse(200, {}, "application/json", payload)

    @asynccontextmanager
    async def stream(self, endpoint, body, forwarded_headers):
        del endpoint, body, forwarded_headers
        raise NotImplementedError
        yield UpstreamStream(200, {}, "text/event-stream", None)  # pragma: no cover

    async def close(self) -> None:
        return None


def request_body(args: argparse.Namespace, index: int) -> dict:
    body = {
        "model": args.model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": args.prompt},
                    {
                        "type": "image_url",
                        "image_url": {"url": args.image.resolve().as_uri()},
                    },
                    {
                        "type": "video_url",
                        "video_url": {"url": args.video.resolve().as_uri()},
                    },
                ],
            }
        ],
        "temperature": 0,
        "max_tokens": args.max_tokens,
        "stream": False,
        "chat_template_kwargs": {"enable_thinking": False},
    }
    if args.upstream == "echo":
        body["relay_benchmark_index"] = index
    return body


async def run_mode(
    app_config: AppConfig,
    args: argparse.Namespace,
    *,
    mode: str,
    round_index: int,
) -> dict:
    if app_config.worker is None:
        raise ValueError("benchmark config must contain a [worker] section")
    suffix = uuid.uuid4().hex[:10]
    target = f"media-{args.host_label}-{mode}-{suffix}"
    work_dir = Path(f"/tmp/{target}")
    worker_config = app_config.worker.model_copy(
        update={
            "target": target,
            "worker_id": f"worker-{target}",
            "work_dir": work_dir,
            "max_concurrency": args.requests,
            "max_heavy_concurrency": args.requests,
            "asset_transfer_concurrency": args.transfer_concurrency,
            "asset_transfer_scope": mode,
            "asset_fairness_quantum_bytes": args.fairness_quantum_bytes,
            "poll_interval_seconds": args.poll_interval,
            "poll_jitter_seconds": 0,
            "scan_legacy_ready": False,
            "lease_seconds": 300,
            "lease_heartbeat_seconds": 30,
            "job_timeout_seconds": args.timeout,
            "max_attempts": 1,
        }
    )
    policy = MediaPolicy(
        allow_file_urls=True,
        allowed_file_roots=tuple({args.image.parent.resolve(), args.video.parent.resolve()}),
    )
    worker_store = S3ObjectStore(app_config.s3)
    client_stores = [S3ObjectStore(app_config.s3) for _ in range(args.producer_groups)]
    clients = [
        RelayClient(
            store,
            prefix=app_config.s3.prefix,
            client_id=f"media-bench-{index}",
            producer_group=f"osmo-media-{index}",
            media_policy=policy,
            poll_interval_seconds=args.poll_interval,
            upload_concurrency=2,
            compact_protocol=True,
        )
        for index, store in enumerate(client_stores)
    ]
    echo = MediaEchoUpstream(
        image_size=args.image.stat().st_size,
        video_size=args.video.stat().st_size,
    )
    worker = RelayWorker(
        worker_store,
        prefix=app_config.s3.prefix,
        config=worker_config,
        upstream=echo if args.upstream == "echo" else None,
    )
    handles: list[tuple[RelayClient, JobHandle]] = []

    async def submit_one(index: int) -> float:
        client = clients[index % args.producer_groups]
        started = time.perf_counter()
        handle = await asyncio.to_thread(
            client.submit,
            endpoint="/v1/chat/completions",
            body=request_body(args, index),
            target=target,
            timeout_seconds=args.timeout,
            producer_group=f"osmo-media-{index % args.producer_groups}",
        )
        handles.append((client, handle))
        return time.perf_counter() - started

    try:
        submit_started = time.perf_counter()
        submit_latencies = await asyncio.gather(
            *(submit_one(index) for index in range(args.requests))
        )
        submit_wall = time.perf_counter() - submit_started

        worker_started = time.perf_counter()
        outcomes = await worker.run_until_idle()
        worker_wall = time.perf_counter() - worker_started
        completed = await asyncio.gather(
            *(
                asyncio.to_thread(
                    client.wait,
                    handle,
                    timeout_seconds=args.timeout,
                    acknowledge=False,
                    cleanup=False,
                )
                for client, handle in handles
            )
        )
        statuses = [item.metadata.status for item in completed]
        completion_tokens = 0
        failures = []
        for item in completed:
            try:
                payload = json.loads(item.body)
            except (UnicodeDecodeError, json.JSONDecodeError):
                payload = {}
            completion_tokens += int(payload.get("usage", {}).get("completion_tokens", 0))
            if item.metadata.status != JobStatus.SUCCEEDED:
                failures.append(
                    {
                        "http_status": item.metadata.http_status,
                        "type": item.metadata.failure.type
                        if item.metadata.failure is not None
                        else None,
                        "body": item.body.decode(errors="replace")[:500],
                    }
                )
        media_ready = [started - worker_started for started in echo.started.values()]
        bytes_per_request = args.image.stat().st_size + args.video.stat().st_size
        total_bytes = bytes_per_request * args.requests
        result = {
            "round": round_index,
            "mode": mode,
            "upstream": args.upstream,
            "requests": args.requests,
            "producer_groups": args.producer_groups,
            "transfer_concurrency": args.transfer_concurrency,
            "media_delivery": worker_config.media_delivery,
            "asset_bytes_per_request": bytes_per_request,
            "submit_wall_seconds": round(submit_wall, 4),
            "submit_p50_seconds": round(statistics.median(submit_latencies), 4),
            "worker_wall_seconds": round(worker_wall, 4),
            "effective_download_mib_s": round(total_bytes / worker_wall / 1024**2, 2),
            "handled": sum(item.handled for item in outcomes),
            "succeeded": sum(status == JobStatus.SUCCEEDED for status in statuses),
            "completion_tokens": completion_tokens,
            "failures": failures,
        }
        if media_ready:
            result.update(
                {
                    "media_ready_p50_seconds": round(statistics.median(media_ready), 4),
                    "media_ready_max_seconds": round(max(media_ready), 4),
                }
            )
        return result
    finally:
        await asyncio.gather(
            *(
                asyncio.to_thread(
                    client.cleanup,
                    target=handle.target,
                    job_id=handle.job_id,
                    producer_group=handle.producer_group,
                )
                for client, handle in handles
            ),
            return_exceptions=True,
        )
        try:
            worker_store.delete_prefix(worker.layout.worker_prefix(target))
            worker_store.delete_prefix(worker.layout.grouped_ready_prefix(target))
            worker_store.delete_prefix(worker.layout.target_ready_prefix(target))
        finally:
            await worker.upstream.close()
            worker_store.close()
            for store in client_stores:
                store.close()
            with suppress(OSError):
                work_dir.rmdir()


async def async_main(args: argparse.Namespace) -> None:
    app_config = load_config(args.config)
    results = []
    for round_index in range(1, args.rounds + 1):
        order = args.order if round_index % 2 else tuple(reversed(args.order))
        for mode in order:
            result = await run_mode(
                app_config,
                args,
                mode=mode,
                round_index=round_index,
            )
            results.append(result)
            print(json.dumps(result, separators=(",", ":")), flush=True)
    print(
        json.dumps(
            {"host_label": args.host_label, "results": results},
            separators=(",", ":"),
        ),
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--host-label", required=True)
    parser.add_argument("--image", required=True, type=Path)
    parser.add_argument("--video", required=True, type=Path)
    parser.add_argument("--model", default="Qwen3.6-35B-A3B")
    parser.add_argument("--prompt", default="Describe the image and summarize the video.")
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--requests", type=int, default=8)
    parser.add_argument("--producer-groups", type=int, default=4)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--transfer-concurrency", type=int, default=4)
    parser.add_argument("--fairness-quantum-bytes", type=int, default=16 * 1024**2)
    parser.add_argument("--upstream", choices=("echo", "model"), default="echo")
    parser.add_argument(
        "--order",
        nargs="+",
        choices=("request", "worker"),
        default=("request", "worker"),
    )
    parser.add_argument("--poll-interval", type=float, default=0.05)
    parser.add_argument("--timeout", type=float, default=300)
    args = parser.parse_args()
    if not args.image.is_file() or not args.video.is_file():
        parser.error("--image and --video must name readable files")
    if args.requests < 1 or args.producer_groups < 1 or args.transfer_concurrency < 1:
        parser.error("requests, producer-groups, and transfer-concurrency must be positive")
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
