from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path

import httpx

from gb300_relay import AsyncOpenAI
from gb300_relay.client import RelayClient
from gb300_relay.config import GatewayConfig, MediaPolicy, WorkerConfig
from gb300_relay.gateway import create_app
from gb300_relay.storage import MemoryObjectStore
from gb300_relay.upstream import UpstreamResponse, UpstreamStream
from gb300_relay.worker import RelayWorker


class SdkUpstream:
    async def request(self, endpoint, body, forwarded_headers):
        del endpoint, forwarded_headers
        payload = {
            "id": "chatcmpl-sdk-test",
            "object": "chat.completion",
            "created": 1_700_000_000,
            "model": body["model"],
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "official-sdk-ok"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }
        return UpstreamResponse(200, {}, "application/json", json.dumps(payload).encode())

    @asynccontextmanager
    async def stream(self, endpoint, body, forwarded_headers):
        del endpoint, body, forwarded_headers

        async def chunks():
            yield b"data: [DONE]\n\n"

        yield UpstreamStream(200, {}, "text/event-stream", chunks())

    async def close(self):
        return None


class OfficialOpenAISdkTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_openai_chat_completions_is_drop_in(self) -> None:
        store = MemoryObjectStore()
        relay = RelayClient(
            store,
            prefix="relay/v1",
            media_policy=MediaPolicy(),
            poll_interval_seconds=0.001,
        )
        app = create_app(
            store,
            relay,
            GatewayConfig(
                targets=("gb300-1",),
                require_healthy_worker=False,
                poll_interval_seconds=0.001,
            ),
            prefix="relay/v1",
        )
        with tempfile.TemporaryDirectory() as directory:
            worker = RelayWorker(
                store,
                prefix="relay/v1",
                config=WorkerConfig(
                    target="gb300-1",
                    worker_id="worker-sdk",
                    work_dir=Path(directory),
                    lease_seconds=30,
                    lease_heartbeat_seconds=5,
                ),
                upstream=SdkUpstream(),
            )
            http_client = httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://relay",
            )
            sdk = AsyncOpenAI(
                base_url="http://relay/v1",
                api_key="local-relay",
                http_client=http_client,
                max_retries=0,
            )
            try:
                request = asyncio.create_task(
                    sdk.chat.completions.create(
                        model="vlm",
                        messages=[{"role": "user", "content": "hello"}],
                    )
                )
                for _ in range(1_000):
                    if await asyncio.to_thread(worker.ready_jobs):
                        break
                    await asyncio.sleep(0.001)
                else:
                    self.fail("official SDK request did not reach the relay queue")
                await worker.run_until_idle()
                completion = await request
            finally:
                await sdk.close()

        self.assertEqual(completion.id, "chatcmpl-sdk-test")
        self.assertEqual(completion.choices[0].message.content, "official-sdk-ok")
        self.assertEqual(completion.usage.total_tokens, 2)


if __name__ == "__main__":
    unittest.main()
