#!/usr/bin/env python3
"""Call the local GB300 relay with the official OpenAI Python SDK."""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Any

from openai import APIConnectionError, APIStatusError, OpenAI


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default=os.getenv("OPENAI_BASE_URL", "http://127.0.0.1:8080/v1"),
        help="Relay URL (default: OPENAI_BASE_URL or http://127.0.0.1:8080/v1)",
    )
    parser.add_argument(
        "--model",
        default=os.getenv("OPENAI_MODEL"),
        help="Model ID (default: OPENAI_MODEL or the first model advertised by the relay)",
    )
    parser.add_argument(
        "--target",
        default=os.getenv("GB300_TARGET"),
        help="Optional worker target such as gb300-1; omit to load-balance",
    )
    parser.add_argument(
        "--prompt",
        default="Reply with one short sentence confirming that the GB300 relay works.",
    )
    parser.add_argument("--max-tokens", type=int, default=128)
    parser.add_argument("--stream", action="store_true")
    parser.add_argument(
        "--disable-thinking",
        action="store_true",
        help="Pass the common Qwen/SGLang option that disables reasoning output",
    )
    parser.add_argument(
        "--relay-timeout",
        type=float,
        default=300.0,
        help="End-to-end relay timeout in seconds",
    )
    parser.add_argument(
        "--idempotency-key",
        help="Optional stable key for a retryable logical request",
    )
    return parser.parse_args()


def choose_model(client: OpenAI, requested: str | None) -> str:
    if requested:
        return requested
    model_ids = sorted(model.id for model in client.models.list().data)
    if not model_ids:
        raise RuntimeError(
            "the relay advertised no models; pass --model or configure worker.models"
        )
    selected = model_ids[0]
    print(f"auto-selected model: {selected}", file=sys.stderr)
    return selected


def request_options(args: argparse.Namespace, model: str) -> dict[str, Any]:
    headers = {"x-relay-timeout-seconds": str(args.relay_timeout)}
    if args.target:
        headers["x-gb300-target"] = args.target
    if args.idempotency_key:
        headers["idempotency-key"] = args.idempotency_key

    options: dict[str, Any] = {
        "model": model,
        "messages": [{"role": "user", "content": args.prompt}],
        "max_tokens": args.max_tokens,
        "extra_headers": headers,
    }
    if args.disable_thinking:
        options["extra_body"] = {"chat_template_kwargs": {"enable_thinking": False}}
    return options


def run() -> int:
    args = parse_args()
    if args.relay_timeout <= 0:
        raise SystemExit("--relay-timeout must be positive")

    client = OpenAI(
        base_url=args.base_url,
        api_key=os.getenv("OPENAI_API_KEY", "relay-local"),
        timeout=args.relay_timeout + 15,
        max_retries=0,
    )
    try:
        model = choose_model(client, args.model)
        options = request_options(args, model)
        route = args.target or "automatic"
        print(
            f"gateway={args.base_url} model={model} target={route} stream={args.stream}",
            file=sys.stderr,
        )
        started = time.monotonic()

        if args.stream:
            chunks = 0
            for event in client.chat.completions.create(stream=True, **options):
                text = event.choices[0].delta.content if event.choices else None
                if text:
                    print(text, end="", flush=True)
                    chunks += 1
            print(flush=True)
            print(
                f"completed in {time.monotonic() - started:.3f}s ({chunks} text chunks)",
                file=sys.stderr,
            )
            return 0

        raw = client.chat.completions.with_raw_response.create(**options)
        completion = raw.parse()
        content = completion.choices[0].message.content or ""
        print(content, flush=True)
        print(
            "completed in "
            f"{time.monotonic() - started:.3f}s "
            f"job={raw.headers.get('x-relay-job-id', '-')} "
            f"target={raw.headers.get('x-relay-target', '-')}",
            file=sys.stderr,
        )
        return 0
    except APIConnectionError as exc:
        print(
            f"cannot connect to {args.base_url}: {exc}. Start the local relay gateway first.",
            file=sys.stderr,
        )
        return 2
    except APIStatusError as exc:
        request_id = exc.response.headers.get("x-request-id", "-")
        print(
            f"relay returned HTTP {exc.status_code} (request_id={request_id}): {exc.message}",
            file=sys.stderr,
        )
        return 3
    except RuntimeError as exc:
        print(f"demo configuration error: {exc}", file=sys.stderr)
        return 4
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(run())
