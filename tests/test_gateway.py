from __future__ import annotations

import asyncio
import json
import tempfile
import threading
import time
import unittest
from contextlib import asynccontextmanager
from pathlib import Path

import httpx

from gb300_relay.client import RelayClient
from gb300_relay.config import GatewayConfig, MediaPolicy, WorkerConfig
from gb300_relay.gateway import TargetSelector, create_app
from gb300_relay.layout import ObjectLayout
from gb300_relay.protocol import WorkerHeartbeat, canonical_json_bytes
from gb300_relay.storage import MemoryObjectStore
from gb300_relay.upstream import UpstreamResponse, UpstreamStream
from gb300_relay.worker import RelayWorker


class EchoUpstream:
    def __init__(self) -> None:
        self.calls = 0

    async def request(self, endpoint, body, forwarded_headers):
        del forwarded_headers
        self.calls += 1
        payload = {
            "id": "chatcmpl-relay",
            "object": "chat.completion",
            "model": body.get("model"),
            "endpoint": endpoint,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "relay-ok"},
                    "finish_reason": "stop",
                }
            ],
        }
        return UpstreamResponse(
            200,
            {"x-request-id": "upstream-request"},
            "application/json",
            json.dumps(payload).encode(),
        )

    @asynccontextmanager
    async def stream(self, endpoint, body, forwarded_headers):
        del endpoint, body, forwarded_headers

        async def chunks():
            yield b'data: {"choices":[{"delta":{"content":"relay-ok"}}]}\n\n'
            yield b"data: [DONE]\n\n"

        yield UpstreamStream(200, {}, "text/event-stream", chunks())

    async def close(self):
        return None


class BlockingRefreshStore(MemoryObjectStore):
    def __init__(self) -> None:
        super().__init__()
        self.worker_lists = 0
        self.refresh_entered = threading.Event()
        self.release_refresh = threading.Event()

    def list(self, prefix):
        if prefix.endswith("/workers/"):
            self.worker_lists += 1
            if self.worker_lists > 1:
                self.refresh_entered.set()
                self.release_refresh.wait(timeout=2)
        return super().list(prefix)


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.store = MemoryObjectStore()
        self.relay = RelayClient(
            self.store,
            prefix="relay/v1",
            media_policy=MediaPolicy(),
            poll_interval_seconds=0.001,
        )
        self.config = GatewayConfig(
            targets=("gb300-1",),
            require_healthy_worker=False,
            cleanup_on_success=True,
            cleanup_idempotent_on_success=False,
            poll_interval_seconds=0.001,
            default_timeout_seconds=5,
        )
        self.app = create_app(
            self.store,
            self.relay,
            self.config,
            prefix="relay/v1",
        )

    async def _drain_after_submit(self, worker: RelayWorker) -> None:
        for _ in range(1_000):
            if await asyncio.to_thread(worker.ready_jobs):
                await worker.run_until_idle()
                return
            await asyncio.sleep(0.001)
        self.fail("gateway did not publish a ready job")

    async def test_openai_wire_compatible_request_and_idempotent_retry(self) -> None:
        upstream = EchoUpstream()
        with tempfile.TemporaryDirectory() as directory:
            worker = RelayWorker(
                self.store,
                prefix="relay/v1",
                config=WorkerConfig(
                    target="gb300-1",
                    worker_id="worker-1",
                    work_dir=Path(directory),
                    lease_seconds=30,
                    lease_heartbeat_seconds=5,
                ),
                upstream=upstream,
            )
            transport = httpx.ASGITransport(app=self.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://relay") as http:
                body = {
                    "model": "vlm",
                    "messages": [{"role": "user", "content": "hello"}],
                }
                first_task = asyncio.create_task(
                    http.post(
                        "/v1/chat/completions",
                        headers={
                            "authorization": "Bearer ignored-local-token",
                            "idempotency-key": "same-logical-call",
                        },
                        json=body,
                    )
                )
                await self._drain_after_submit(worker)
                first = await first_task
                second = await http.post(
                    "/v1/chat/completions",
                    headers={"idempotency-key": "same-logical-call"},
                    json=body,
                )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.json()["choices"][0]["message"]["content"], "relay-ok")
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.headers["x-relay-job-id"], second.headers["x-relay-job-id"])
        self.assertEqual(first.headers["x-relay-producer-group"], self.config.producer_group)
        self.assertEqual(upstream.calls, 1)
        self.assertTrue(self.store.list("relay/v1/results/"))

    async def test_normal_success_is_deleted_after_delivery(self) -> None:
        upstream = EchoUpstream()
        with tempfile.TemporaryDirectory() as directory:
            worker = RelayWorker(
                self.store,
                prefix="relay/v1",
                config=WorkerConfig(
                    target="gb300-1",
                    worker_id="worker-1",
                    work_dir=Path(directory),
                    lease_seconds=30,
                    lease_heartbeat_seconds=5,
                ),
                upstream=upstream,
            )
            transport = httpx.ASGITransport(app=self.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://relay") as http:
                response_task = asyncio.create_task(
                    http.post(
                        "/v1/chat/completions",
                        headers={"x-gb300-producer-group": "osmo-node-9"},
                        json={"model": "vlm", "messages": []},
                    )
                )
                await self._drain_after_submit(worker)
                response = await response_task

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["x-relay-producer-group"], "osmo-node-9")
        self.assertFalse(self.store.list("relay/v1/requests/"))
        self.assertFalse(self.store.list("relay/v1/queue/"))
        self.assertFalse(self.store.list("relay/v1/results/"))

    async def test_rejects_non_object_json(self) -> None:
        transport = httpx.ASGITransport(app=self.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://relay") as http:
            response = await http.post("/v1/chat/completions", json=["not", "an", "object"])
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["type"], "invalid_request_error")

    async def test_rejects_unsafe_producer_group(self) -> None:
        transport = httpx.ASGITransport(app=self.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://relay") as http:
            response = await http.post(
                "/v1/chat/completions",
                headers={"x-gb300-producer-group": "../another-node"},
                json={"model": "vlm", "messages": []},
            )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["type"], "invalid_request_error")

    async def test_stale_worker_cache_refresh_does_not_block_selection(self) -> None:
        store = BlockingRefreshStore()
        layout = ObjectLayout("relay/v1")
        heartbeat = WorkerHeartbeat(
            target="gb300-1", worker_id="worker-1", max_concurrency=64, inflight=0
        )
        store.put_bytes(
            layout.worker_heartbeat("gb300-1", "worker-1"),
            canonical_json_bytes(heartbeat.model_dump(mode="json")),
        )
        selector = TargetSelector(
            store,
            layout,
            GatewayConfig(targets=("gb300-1",)),
            cache_seconds=0,
        )
        self.assertEqual(selector.select(), "gb300-1")
        started = time.monotonic()
        try:
            self.assertEqual(selector.select(), "gb300-1")
            self.assertLess(time.monotonic() - started, 0.2)
            self.assertTrue(store.refresh_entered.wait(timeout=1))
        finally:
            store.release_refresh.set()

    async def test_burst_selection_reserves_local_capacity(self) -> None:
        layout = ObjectLayout("relay/v1")
        for target in ("gb300-1", "gb300-2"):
            heartbeat = WorkerHeartbeat(
                target=target,
                worker_id=f"{target}-worker",
                max_concurrency=64,
                inflight=0,
            )
            self.store.put_bytes(
                layout.worker_heartbeat(target, heartbeat.worker_id),
                canonical_json_bytes(heartbeat.model_dump(mode="json")),
            )
        selector = TargetSelector(
            self.store,
            layout,
            GatewayConfig(targets=("gb300-1", "gb300-2")),
        )

        selected = [selector.select(model="vlm") for _ in range(32)]

        self.assertEqual(selected.count("gb300-1"), 16)
        self.assertEqual(selected.count("gb300-2"), 16)
        for target in selected:
            selector.release(target)


if __name__ == "__main__":
    unittest.main()
