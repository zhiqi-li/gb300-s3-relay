#!/usr/bin/env python3
"""Run end-to-end checks through an OpenAI-compatible relay gateway."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import mimetypes
import os
import time
import uuid
from pathlib import Path
from typing import Any

from openai import AsyncOpenAI


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default=os.getenv("OPENAI_BASE_URL", "http://127.0.0.1:8080/v1"),
    )
    parser.add_argument("--model", required=True)
    parser.add_argument("--target", action="append", required=True)
    parser.add_argument(
        "--mode",
        choices=("all", "text", "idempotency", "stream", "multimodal"),
        default="all",
    )
    parser.add_argument("--image", type=Path)
    parser.add_argument("--video", type=Path)
    parser.add_argument("--max-tokens", type=int, default=256)
    parser.add_argument(
        "--disable-thinking",
        action="store_true",
        help="Pass the common SGLang/Qwen chat-template option that disables reasoning.",
    )
    parser.add_argument("--timeout", type=float, default=300.0)
    return parser.parse_args()


def as_data_url(path: Path) -> str:
    media_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:{media_type};base64,{encoded}"


def target_headers(target: str, **extra: str) -> dict[str, str]:
    return {"x-gb300-target": target, **extra}


def model_options(args: argparse.Namespace) -> dict[str, Any]:
    if not args.disable_thinking:
        return {}
    return {"extra_body": {"chat_template_kwargs": {"enable_thinking": False}}}


async def text_checks(client: AsyncOpenAI, args: argparse.Namespace) -> dict[str, Any]:
    async def call(target: str) -> dict[str, Any]:
        started = time.monotonic()
        response = await client.chat.completions.create(
            model=args.model,
            messages=[{"role": "user", "content": "Reply with exactly: relay-ok"}],
            max_tokens=args.max_tokens,
            extra_headers=target_headers(target),
            **model_options(args),
        )
        content = response.choices[0].message.content or ""
        return {
            "target": target,
            "nonempty": bool(content),
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }

    return {"requests": await asyncio.gather(*(call(target) for target in args.target))}


async def idempotency_check(client: AsyncOpenAI, args: argparse.Namespace) -> dict[str, Any]:
    key = f"smoke-{uuid.uuid4()}"
    kwargs: dict[str, Any] = {
        "model": args.model,
        "messages": [{"role": "user", "content": "Reply with exactly: idempotent-ok"}],
        "max_tokens": args.max_tokens,
        "extra_headers": target_headers(args.target[0], **{"idempotency-key": key}),
        **model_options(args),
    }
    first = await client.chat.completions.create(**kwargs)
    second = await client.chat.completions.create(**kwargs)
    first_content = first.choices[0].message.content or ""
    second_content = second.choices[0].message.content or ""
    return {
        "target": args.target[0],
        "same_response_id": first.id == second.id,
        "same_content": first_content == second_content,
        "nonempty": bool(first_content),
    }


async def stream_check(client: AsyncOpenAI, args: argparse.Namespace) -> dict[str, Any]:
    started = time.monotonic()
    first_event: float | None = None
    chunks = 0
    characters = 0
    stream = await client.chat.completions.create(
        model=args.model,
        messages=[
            {
                "role": "user",
                "content": "Write the integers from one through twenty, separated by commas.",
            }
        ],
        max_tokens=args.max_tokens,
        stream=True,
        extra_headers=target_headers(args.target[-1]),
        **model_options(args),
    )
    async for event in stream:
        if first_event is None:
            first_event = time.monotonic()
        chunks += 1
        if event.choices and event.choices[0].delta.content:
            characters += len(event.choices[0].delta.content)
    finished = time.monotonic()
    return {
        "target": args.target[-1],
        "chunks": chunks,
        "characters": characters,
        "first_event_seconds": round((first_event or finished) - started, 3),
        "total_seconds": round(finished - started, 3),
    }


async def multimodal_check(client: AsyncOpenAI, args: argparse.Namespace) -> dict[str, Any]:
    if not args.image and not args.video:
        raise SystemExit("--image or --video is required for multimodal mode")
    content: list[dict[str, Any]] = [
        {
            "type": "text",
            "text": "Briefly describe the provided media.",
        }
    ]
    if args.image:
        content.append(
            {
                "type": "image_url",
                "image_url": {"url": as_data_url(args.image)},
            }
        )
    if args.video:
        content.append(
            {
                "type": "video_url",
                "video_url": {"url": as_data_url(args.video)},
            }
        )
    response = await client.chat.completions.create(
        model=args.model,
        messages=[
            {
                "role": "user",
                "content": content,
            }
        ],
        max_tokens=args.max_tokens,
        extra_headers=target_headers(args.target[0]),
        **model_options(args),
    )
    content = response.choices[0].message.content or ""
    return {"target": args.target[0], "nonempty": bool(content), "characters": len(content)}


async def run() -> None:
    args = parse_args()
    client = AsyncOpenAI(
        base_url=args.base_url,
        api_key=os.getenv("OPENAI_API_KEY", "relay-smoke-test"),
        timeout=args.timeout,
        max_retries=0,
    )
    checks: dict[str, Any] = {}
    try:
        if args.mode in {"all", "text"}:
            checks["text"] = await text_checks(client, args)
        if args.mode in {"all", "idempotency"}:
            checks["idempotency"] = await idempotency_check(client, args)
        if args.mode in {"all", "stream"}:
            checks["stream"] = await stream_check(client, args)
        if args.mode in {"all", "multimodal"}:
            checks["multimodal"] = await multimodal_check(client, args)
    finally:
        await client.close()
    print(json.dumps(checks, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(run())
