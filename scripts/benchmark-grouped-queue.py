#!/usr/bin/env python3
"""Benchmark legacy and producer-grouped S3 queues against one model host."""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
import uuid
from contextlib import suppress
from pathlib import Path

from gb300_relay.client import RelayClient
from gb300_relay.config import AppConfig, load_config
from gb300_relay.protocol import JobHandle
from gb300_relay.storage import S3ObjectStore
from gb300_relay.worker import RelayWorker


def request_body(args: argparse.Namespace) -> dict:
    return {
        "model": args.model,
        "messages": [{"role": "user", "content": args.prompt}],
        "temperature": 0,
        "max_tokens": args.max_tokens,
        "ignore_eos": True,
        "stream": False,
        "chat_template_kwargs": {"enable_thinking": False},
    }


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
    target = f"bench-{args.host_label}-{mode}-{suffix}"
    worker_config = app_config.worker.model_copy(
        update={
            "target": target,
            "worker_id": f"worker-{target}",
            "work_dir": Path(f"/tmp/{target}"),
            "max_concurrency": args.requests,
            "max_heavy_concurrency": args.requests,
            "poll_interval_seconds": args.poll_interval,
            "poll_jitter_seconds": 0,
            "scan_legacy_ready": mode == "legacy",
            "lease_seconds": 60,
            "lease_heartbeat_seconds": 10,
            "job_timeout_seconds": args.timeout,
            "max_attempts": 1,
        }
    )
    worker_store = S3ObjectStore(app_config.s3)
    client_stores = [S3ObjectStore(app_config.s3) for _ in range(args.producer_groups)]
    clients = [
        RelayClient(
            store,
            prefix=app_config.s3.prefix,
            client_id=f"bench-client-{index}",
            producer_group=f"osmo-node-{index}" if mode == "grouped" else None,
            poll_interval_seconds=args.poll_interval,
            compact_protocol=True,
            compact_manifest_max_bytes=1024**2,
        )
        for index, store in enumerate(client_stores)
    ]
    worker = RelayWorker(worker_store, prefix=app_config.s3.prefix, config=worker_config)
    worker_task = asyncio.create_task(worker.run_forever())
    handles: list[tuple[RelayClient, JobHandle]] = []
    cleaned = False

    async def one(index: int) -> dict:
        producer_index = index % args.producer_groups
        client = clients[producer_index]
        producer_group = f"osmo-node-{producer_index}" if mode == "grouped" else None
        started = time.perf_counter()
        handle = await asyncio.to_thread(
            client.submit,
            endpoint="/v1/chat/completions",
            body=request_body(args),
            target=target,
            timeout_seconds=args.timeout,
            producer_group=producer_group,
        )
        submitted = time.perf_counter()
        handles.append((client, handle))
        completed = await asyncio.to_thread(
            client.wait,
            handle,
            timeout_seconds=args.timeout,
            acknowledge=False,
            cleanup=False,
        )
        finished = time.perf_counter()
        payload = json.loads(completed.body)
        return {
            "total_seconds": finished - started,
            "submit_seconds": submitted - started,
            "tokens": int(payload.get("usage", {}).get("completion_tokens", 0)),
            "finished": finished,
        }

    await asyncio.sleep(0.15)
    batch_started = time.perf_counter()
    try:
        results = await asyncio.gather(*(one(index) for index in range(args.requests)))
        wall = max(item["finished"] for item in results) - batch_started
        cleanup_results = await asyncio.gather(
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
        cleaned = True
        totals = [item["total_seconds"] for item in results]
        submits = [item["submit_seconds"] for item in results]
        tokens = sum(item["tokens"] for item in results)
        return {
            "round": round_index,
            "mode": mode,
            "requests": args.requests,
            "tokens": tokens,
            "wall_seconds": round(wall, 4),
            "output_tps": round(tokens / wall, 2) if wall else 0,
            "request_p50_seconds": round(statistics.median(totals), 4),
            "request_max_seconds": round(max(totals), 4),
            "submit_p50_seconds": round(statistics.median(submits), 4),
            "cleanup_errors": sum(isinstance(item, Exception) for item in cleanup_results),
        }
    finally:
        worker.stop()
        await worker_task
        if handles and not cleaned:
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
            worker_store.close()
            for store in client_stores:
                store.close()
            with suppress(OSError):
                worker_config.work_dir.rmdir()


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
    parser.add_argument("--model", default="Qwen3.6-35B-A3B")
    parser.add_argument(
        "--prompt",
        default="Classify this sample. Return only the label.",
    )
    parser.add_argument("--max-tokens", type=int, default=256)
    parser.add_argument("--requests", type=int, default=8)
    parser.add_argument("--producer-groups", type=int, default=4)
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument(
        "--order",
        nargs=2,
        choices=("legacy", "grouped"),
        default=("legacy", "grouped"),
    )
    parser.add_argument("--poll-interval", type=float, default=0.05)
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    if args.requests < 1 or args.producer_groups < 1:
        parser.error("requests and producer-groups must be positive")
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
