from __future__ import annotations

import os
from typing import Any


def OpenAI(*, base_url: str | None = None, api_key: str | None = None, **kwargs: Any):
    """Create the official synchronous OpenAI client pointed at the relay.

    Install the optional dependency with ``pip install gb300-s3-relay[openai]``.
    Existing applications usually need no code change at all: setting
    ``OPENAI_BASE_URL=http://127.0.0.1:8080/v1`` is enough.
    """

    try:
        from openai import OpenAI as SDKClient
    except ImportError as exc:
        raise RuntimeError("install gb300-s3-relay[openai] to use this helper") from exc
    return SDKClient(
        base_url=base_url or os.getenv("GB300_RELAY_BASE_URL", "http://127.0.0.1:8080/v1"),
        api_key=api_key or os.getenv("GB300_RELAY_API_KEY", "relay-local"),
        **kwargs,
    )


def AsyncOpenAI(*, base_url: str | None = None, api_key: str | None = None, **kwargs: Any):
    """Create the official async OpenAI client pointed at the relay."""

    try:
        from openai import AsyncOpenAI as SDKClient
    except ImportError as exc:
        raise RuntimeError("install gb300-s3-relay[openai] to use this helper") from exc
    return SDKClient(
        base_url=base_url or os.getenv("GB300_RELAY_BASE_URL", "http://127.0.0.1:8080/v1"),
        api_key=api_key or os.getenv("GB300_RELAY_API_KEY", "relay-local"),
        **kwargs,
    )
