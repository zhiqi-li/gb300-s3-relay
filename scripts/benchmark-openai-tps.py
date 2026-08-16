#!/usr/bin/env python3
"""Measure fixed-length OpenAI completion throughput and relay target distribution."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import statistics
import time
from collections import Counter
from dataclasses import asdict, dataclass

import httpx


@dataclass(frozen=True, slots=True)
class CaseResult:
    concurrency: int
    requests: int
    successes: int
    completion_tokens: int
    wall_seconds: float
    aggregate_output_tps: float
    latency_p50_seconds: float
    latency_p95_seconds: float
    targets: dict[str, int]


def parse_matrix(value: str) -> tuple[int, ...]:
    values = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    if not values or any(item < 1 for item in values):
        raise argparse.ArgumentTypeError("concurrency must be a comma-separated list of integers")
    return values


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def request_body(model: str, output_tokens: int, index: int) -> dict[str, object]:
    return {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": (
                    "Write a numbered list of concise facts about distributed systems. "
                    f"Continue until stopped. Benchmark request {index}."
                ),
            }
        ],
        "temperature": 0,
        "max_tokens": output_tokens,
        "min_tokens": output_tokens,
        "ignore_eos": True,
        "chat_template_kwargs": {"enable_thinking": False},
    }


async def run_case(
    client: httpx.AsyncClient,
    *,
    url: str,
    model: str,
    output_tokens: int,
    concurrency: int,
    requests_per_worker: int,
    target: str | None,
) -> CaseResult:
    request_count = max(concurrency, concurrency * requests_per_worker)
    queue: asyncio.Queue[int] = asyncio.Queue()
    for index in range(request_count):
        queue.put_nowait(index)
    samples: list[tuple[int, int, float, str]] = []
    headers = {"x-gb300-target": target} if target else None

    async def worker() -> None:
        while True:
            try:
                index = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            started = time.perf_counter()
            response = await client.post(
                url,
                json=request_body(model, output_tokens, index),
                headers=headers,
            )
            elapsed = time.perf_counter() - started
            completion_tokens = 0
            if response.status_code == 200:
                completion_tokens = int(
                    response.json().get("usage", {}).get("completion_tokens", 0)
                )
            samples.append(
                (
                    response.status_code,
                    completion_tokens,
                    elapsed,
                    response.headers.get("x-relay-target", target or "direct"),
                )
            )
            queue.task_done()

    wall_started = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(concurrency)))
    wall = time.perf_counter() - wall_started
    successes = sum(status == 200 and tokens == output_tokens for status, tokens, _, _ in samples)
    total_tokens = sum(tokens for _, tokens, _, _ in samples)
    latencies = [latency for _, _, latency, _ in samples]
    return CaseResult(
        concurrency=concurrency,
        requests=request_count,
        successes=successes,
        completion_tokens=total_tokens,
        wall_seconds=round(wall, 3),
        aggregate_output_tps=round(total_tokens / wall, 2),
        latency_p50_seconds=round(statistics.median(latencies), 3),
        latency_p95_seconds=round(percentile(latencies, 0.95), 3),
        targets=dict(sorted(Counter(target for *_, target in samples).items())),
    )


async def async_main(args: argparse.Namespace) -> int:
    url = args.base_url.rstrip("/") + "/v1/chat/completions"
    limits = httpx.Limits(
        max_connections=max(args.concurrency) * 2,
        max_keepalive_connections=max(args.concurrency) * 2,
    )
    timeout = httpx.Timeout(args.timeout, connect=10.0)
    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        for warmup in range(args.warmup):
            response = await client.post(
                url,
                json=request_body(args.model, args.output_tokens, -warmup - 1),
                headers={"x-gb300-target": args.target} if args.target else None,
            )
            response.raise_for_status()
        for concurrency in args.concurrency:
            result = await run_case(
                client,
                url=url,
                model=args.model,
                output_tokens=args.output_tokens,
                concurrency=concurrency,
                requests_per_worker=args.requests_per_worker,
                target=args.target,
            )
            print(json.dumps(asdict(result), separators=(",", ":")), flush=True)
            if result.successes != result.requests:
                return 1
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--base-url", default="http://127.0.0.1:18080")
    result.add_argument("--model", default="Qwen/Qwen3.8-27B-FP8")
    result.add_argument("--concurrency", type=parse_matrix, default=parse_matrix("1,2,4,8,16,32"))
    result.add_argument("--output-tokens", type=int, default=256)
    result.add_argument("--requests-per-worker", type=int, default=2)
    result.add_argument("--warmup", type=int, default=1)
    result.add_argument("--timeout", type=float, default=900)
    result.add_argument("--target", help="set x-gb300-target for a forced-target run")
    return result


def main() -> int:
    args = parser().parse_args()
    if args.output_tokens < 1 or args.requests_per_worker < 1 or args.warmup < 0:
        raise SystemExit("output tokens and requests per worker must be positive")
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
