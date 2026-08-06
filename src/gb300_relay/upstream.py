from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import httpx

from .config import WorkerConfig
from .errors import UpstreamError

_FORWARDED_RESPONSE_HEADERS = frozenset(
    {
        "content-type",
        "openai-organization",
        "openai-processing-ms",
        "openai-version",
        "retry-after",
        "x-request-id",
    }
)


@dataclass(frozen=True, slots=True)
class UpstreamResponse:
    status_code: int
    headers: dict[str, str]
    content_type: str
    body: bytes


@dataclass(slots=True)
class UpstreamStream:
    status_code: int
    headers: dict[str, str]
    content_type: str
    chunks: AsyncIterator[bytes]


def _retryable_status(status_code: int) -> bool:
    return status_code in {408, 409, 425, 429} or 500 <= status_code <= 599


class OpenAIUpstream:
    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        headers: dict[str, str] = {}
        if config.upstream_api_key_env:
            value = os.environ.get(config.upstream_api_key_env)
            if value:
                headers["authorization"] = f"Bearer {value}"
        self._client = httpx.AsyncClient(
            base_url=config.upstream_base_url,
            headers=headers,
            timeout=httpx.Timeout(config.job_timeout_seconds, connect=15),
            trust_env=False,
        )

    async def close(self) -> None:
        await self._client.aclose()

    @staticmethod
    def _headers(response: httpx.Response) -> dict[str, str]:
        return {
            key.lower(): value
            for key, value in response.headers.items()
            if key.lower() in _FORWARDED_RESPONSE_HEADERS
        }

    async def request(
        self,
        endpoint: str,
        body: dict[str, Any],
        forwarded_headers: dict[str, str],
    ) -> UpstreamResponse:
        try:
            async with self._client.stream(
                "POST", endpoint, json=body, headers=forwarded_headers
            ) as response:
                chunks: list[bytes] = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > self.config.max_response_bytes:
                        raise UpstreamError(
                            "upstream response exceeded configured size limit",
                            status_code=502,
                            retryable=False,
                        )
                    chunks.append(chunk)
                content = b"".join(chunks)
        except UpstreamError:
            raise
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise UpstreamError(str(exc), retryable=True) from exc
        except httpx.HTTPError as exc:
            raise UpstreamError(str(exc), retryable=False) from exc
        return UpstreamResponse(
            status_code=response.status_code,
            headers=self._headers(response),
            content_type=response.headers.get("content-type", "application/json"),
            body=content,
        )

    @asynccontextmanager
    async def stream(
        self,
        endpoint: str,
        body: dict[str, Any],
        forwarded_headers: dict[str, str],
    ):
        try:
            async with self._client.stream(
                "POST", endpoint, json=body, headers=forwarded_headers
            ) as response:

                async def chunks() -> AsyncIterator[bytes]:
                    size = 0
                    async for chunk in response.aiter_raw():
                        size += len(chunk)
                        if size > self.config.max_response_bytes:
                            raise UpstreamError(
                                "upstream stream exceeded configured size limit",
                                status_code=502,
                                retryable=False,
                            )
                        yield chunk

                yield UpstreamStream(
                    status_code=response.status_code,
                    headers=self._headers(response),
                    content_type=response.headers.get("content-type", "text/event-stream"),
                    chunks=chunks(),
                )
        except UpstreamError:
            raise
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise UpstreamError(str(exc), retryable=True) from exc
        except httpx.HTTPError as exc:
            raise UpstreamError(str(exc), retryable=False) from exc

    @staticmethod
    def failure_from_response(response: UpstreamResponse) -> UpstreamError | None:
        if 200 <= response.status_code < 300:
            return None
        return UpstreamError(
            f"upstream returned HTTP {response.status_code}",
            status_code=response.status_code,
            retryable=_retryable_status(response.status_code),
            response_body=response.body,
        )
