from __future__ import annotations

import asyncio
import base64
import json
import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path

from gb300_relay.client import RelayClient
from gb300_relay.config import MediaPolicy, WorkerConfig
from gb300_relay.protocol import JobStatus
from gb300_relay.storage import MemoryObjectStore
from gb300_relay.upstream import UpstreamResponse, UpstreamStream
from gb300_relay.worker import RelayWorker


class FakeUpstream:
    def __init__(self, *, delay: float = 0, failures: int = 0) -> None:
        self.delay = delay
        self.failures = failures
        self.calls = 0
        self.active = 0
        self.max_active = 0
        self.bodies: list[dict] = []

    async def request(self, endpoint, body, forwarded_headers):
        del forwarded_headers
        self.calls += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.bodies.append(body)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            if self.calls <= self.failures:
                return UpstreamResponse(503, {}, "application/json", b'{"error":"retry"}')
            data = json.dumps(
                {
                    "id": f"response-{self.calls}",
                    "object": "chat.completion",
                    "endpoint": endpoint,
                }
            ).encode()
            return UpstreamResponse(200, {"x-request-id": "upstream-1"}, "application/json", data)
        finally:
            self.active -= 1

    @asynccontextmanager
    async def stream(self, endpoint, body, forwarded_headers):
        del endpoint, body, forwarded_headers

        async def chunks():
            yield b'data: {"delta":"one"}\n\n'
            yield b'data: {"delta":"two"}\n\ndata: [DONE]\n\n'

        yield UpstreamStream(200, {}, "text/event-stream", chunks())

    async def close(self):
        return None


class DelayedStreamUpstream(FakeUpstream):
    @asynccontextmanager
    async def stream(self, endpoint, body, forwarded_headers):
        del endpoint, body, forwarded_headers

        async def chunks():
            yield b'data: {"delta":"first"}\n\n'
            await asyncio.sleep(0.1)
            yield b"data: [DONE]\n\n"

        yield UpstreamStream(200, {}, "text/event-stream", chunks())


def client(store: MemoryObjectStore) -> RelayClient:
    return RelayClient(
        store,
        prefix="relay/v1",
        media_policy=MediaPolicy(),
        poll_interval_seconds=0.001,
    )


class ClientWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_compact_protocol_uses_ready_and_done_as_complete_commits(self) -> None:
        store = MemoryObjectStore()
        relay = RelayClient(
            store,
            prefix="relay/v1",
            media_policy=MediaPolicy(),
            poll_interval_seconds=0.001,
            compact_protocol=True,
        )
        handle = relay.submit(
            endpoint="/v1/chat/completions",
            body={"model": "m", "messages": [{"role": "user", "content": "short"}]},
            target="gb300-1",
            timeout_seconds=30,
        )
        self.assertFalse(store.list(f"relay/v1/requests/gb300-1/{handle.job_id}/manifest.json"))
        with tempfile.TemporaryDirectory() as directory:
            worker = RelayWorker(
                store,
                prefix="relay/v1",
                config=WorkerConfig(
                    target="gb300-1",
                    worker_id="worker-1",
                    work_dir=Path(directory),
                    lease_seconds=30,
                    lease_heartbeat_seconds=5,
                ),
                upstream=FakeUpstream(),
            )
            outcome = await worker.process(handle.job_id)
        self.assertEqual(outcome.status, JobStatus.SUCCEEDED)
        result_keys = [item.key for item in store.list(f"relay/v1/results/{handle.job_id}/")]
        self.assertEqual(result_keys, [f"relay/v1/results/{handle.job_id}/DONE.json"])
        completed = relay.wait(handle, timeout_seconds=1)
        self.assertEqual(completed.metadata.status, JobStatus.SUCCEEDED)
        self.assertEqual(json.loads(completed.body)["object"], "chat.completion")

    async def test_multimodal_round_trip(self) -> None:
        store = MemoryObjectStore()
        relay = client(store)
        image = base64.b64encode(b"image").decode()
        video = base64.b64encode(b"video").decode()
        body = {
            "model": "vlm",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "describe"},
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/png;base64,{image}"},
                        },
                        {
                            "type": "video_url",
                            "video_url": {"url": f"data:video/mp4;base64,{video}"},
                        },
                    ],
                }
            ],
        }
        handle = relay.submit(
            endpoint="/v1/chat/completions", body=body, target="gb300-1", timeout_seconds=30
        )
        upstream = FakeUpstream()
        with tempfile.TemporaryDirectory() as directory:
            worker = RelayWorker(
                store,
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
            outcomes = await worker.run_until_idle()
        self.assertEqual(outcomes[0].status, JobStatus.SUCCEEDED)
        forwarded = upstream.bodies[0]["messages"][0]["content"]
        self.assertTrue(forwarded[1]["image_url"]["url"].startswith("data:image/png;base64,"))
        self.assertTrue(forwarded[2]["video_url"]["url"].startswith("file://"))
        completed = relay.wait(handle, timeout_seconds=1, cleanup=True)
        self.assertEqual(completed.metadata.http_status, 200)
        self.assertEqual(json.loads(completed.body)["object"], "chat.completion")
        self.assertFalse(store.list("relay/v1/requests/"))

    async def test_worker_concurrency_is_bounded(self) -> None:
        store = MemoryObjectStore()
        relay = client(store)
        for index in range(10):
            relay.submit(
                endpoint="/v1/chat/completions",
                body={"model": "m", "messages": [{"role": "user", "content": str(index)}]},
                target="gb300-1",
                timeout_seconds=30,
            )
        upstream = FakeUpstream(delay=0.02)
        with tempfile.TemporaryDirectory() as directory:
            worker = RelayWorker(
                store,
                prefix="relay/v1",
                config=WorkerConfig(
                    target="gb300-1",
                    worker_id="worker-1",
                    work_dir=Path(directory),
                    max_concurrency=4,
                    lease_seconds=30,
                    lease_heartbeat_seconds=5,
                ),
                upstream=upstream,
            )
            outcomes = await worker.run_until_idle()
        self.assertEqual(len(outcomes), 10)
        self.assertGreaterEqual(upstream.max_active, 2)
        self.assertLessEqual(upstream.max_active, 4)

    async def test_heavy_request_concurrency_has_a_separate_bound(self) -> None:
        store = MemoryObjectStore()
        relay = client(store)
        for index in range(8):
            relay.submit(
                endpoint="/v1/chat/completions",
                body={"model": "m", "messages": [{"role": "user", "content": str(index)}]},
                target="gb300-1",
                timeout_seconds=30,
            )
        upstream = FakeUpstream(delay=0.02)
        with tempfile.TemporaryDirectory() as directory:
            worker = RelayWorker(
                store,
                prefix="relay/v1",
                config=WorkerConfig(
                    target="gb300-1",
                    worker_id="worker-1",
                    work_dir=Path(directory),
                    max_concurrency=8,
                    max_heavy_concurrency=2,
                    heavy_request_threshold_bytes=1,
                    lease_seconds=30,
                    lease_heartbeat_seconds=5,
                ),
                upstream=upstream,
            )
            outcomes = await worker.run_until_idle()
        self.assertEqual(len(outcomes), 8)
        self.assertEqual(upstream.max_active, 2)

    async def test_two_workers_do_not_process_same_job(self) -> None:
        store = MemoryObjectStore()
        handle = client(store).submit(
            endpoint="/v1/chat/completions",
            body={"model": "m", "messages": []},
            target="gb300-1",
            timeout_seconds=30,
        )
        with (
            tempfile.TemporaryDirectory() as first_dir,
            tempfile.TemporaryDirectory() as second_dir,
        ):
            first_upstream = FakeUpstream(delay=0.02)
            second_upstream = FakeUpstream(delay=0.02)
            common = dict(target="gb300-1", lease_seconds=30, lease_heartbeat_seconds=5)
            first = RelayWorker(
                store,
                prefix="relay/v1",
                config=WorkerConfig(worker_id="worker-1", work_dir=Path(first_dir), **common),
                upstream=first_upstream,
            )
            second = RelayWorker(
                store,
                prefix="relay/v1",
                config=WorkerConfig(worker_id="worker-2", work_dir=Path(second_dir), **common),
                upstream=second_upstream,
            )
            outcomes = await asyncio.gather(
                first.process(handle.job_id), second.process(handle.job_id)
            )
        self.assertEqual(sum(item.handled for item in outcomes), 1)
        self.assertEqual(first_upstream.calls + second_upstream.calls, 1)

    async def test_retries_retryable_upstream_status(self) -> None:
        store = MemoryObjectStore()
        relay = client(store)
        handle = relay.submit(
            endpoint="/v1/chat/completions",
            body={"model": "m", "messages": []},
            target="gb300-1",
            timeout_seconds=30,
        )
        upstream = FakeUpstream(failures=2)
        with tempfile.TemporaryDirectory() as directory:
            worker = RelayWorker(
                store,
                prefix="relay/v1",
                config=WorkerConfig(
                    target="gb300-1",
                    worker_id="worker-1",
                    work_dir=Path(directory),
                    max_attempts=3,
                    retry_base_seconds=0,
                    lease_seconds=30,
                    lease_heartbeat_seconds=5,
                ),
                upstream=upstream,
            )
            await worker.process(handle.job_id)
        self.assertEqual(upstream.calls, 3)
        self.assertEqual(relay.wait(handle, timeout_seconds=1).metadata.status, JobStatus.SUCCEEDED)

    async def test_stream_round_trip(self) -> None:
        store = MemoryObjectStore()
        relay = client(store)
        handle = relay.submit(
            endpoint="/v1/chat/completions",
            body={"model": "m", "messages": [], "stream": True},
            target="gb300-1",
            timeout_seconds=30,
            stream=True,
        )
        with tempfile.TemporaryDirectory() as directory:
            worker = RelayWorker(
                store,
                prefix="relay/v1",
                config=WorkerConfig(
                    target="gb300-1",
                    worker_id="worker-1",
                    work_dir=Path(directory),
                    stream_chunk_bytes=1024,
                    lease_seconds=30,
                    lease_heartbeat_seconds=5,
                ),
                upstream=FakeUpstream(),
            )
            await worker.process(handle.job_id)
        chunks = [chunk async for chunk in relay.aiter_stream(handle, timeout_seconds=1)]
        self.assertIn(b'data: {"delta":"one"}', b"".join(chunks))
        self.assertTrue(b"".join(chunks).endswith(b"data: [DONE]\n\n"))

    async def test_stream_cleanup_precedes_terminal_event_delivery(self) -> None:
        store = MemoryObjectStore()
        relay = client(store)
        handle = relay.submit(
            endpoint="/v1/chat/completions",
            body={"model": "m", "messages": [], "stream": True},
            target="gb300-1",
            timeout_seconds=30,
            stream=True,
        )
        with tempfile.TemporaryDirectory() as directory:
            worker = RelayWorker(
                store,
                prefix="relay/v1",
                config=WorkerConfig(
                    target="gb300-1",
                    worker_id="worker-1",
                    work_dir=Path(directory),
                    stream_chunk_bytes=1024,
                    lease_seconds=30,
                    lease_heartbeat_seconds=5,
                ),
                upstream=FakeUpstream(),
            )
            await worker.process(handle.job_id)

        stream = relay.aiter_stream(handle, timeout_seconds=1, cleanup=True)
        async for chunk in stream:
            if b"data: [DONE]" in chunk:
                break
        await stream.aclose()

        self.assertFalse(store.list("relay/v1/requests/"))
        self.assertFalse(store.list("relay/v1/results/"))
        self.assertFalse(store.list("relay/v1/streams/"))

    async def test_stream_publishes_first_chunk_before_upstream_finishes(self) -> None:
        store = MemoryObjectStore()
        relay = client(store)
        handle = relay.submit(
            endpoint="/v1/chat/completions",
            body={"model": "m", "messages": [], "stream": True},
            target="gb300-1",
            timeout_seconds=30,
            stream=True,
        )
        with tempfile.TemporaryDirectory() as directory:
            worker = RelayWorker(
                store,
                prefix="relay/v1",
                config=WorkerConfig(
                    target="gb300-1",
                    worker_id="worker-1",
                    work_dir=Path(directory),
                    stream_chunk_bytes=1024,
                    stream_flush_interval_seconds=0.01,
                    lease_seconds=30,
                    lease_heartbeat_seconds=5,
                ),
                upstream=DelayedStreamUpstream(),
            )
            processing = asyncio.create_task(worker.process(handle.job_id))
            for _ in range(100):
                if store.list("relay/v1/streams/"):
                    break
                await asyncio.sleep(0.001)
            self.assertTrue(store.list("relay/v1/streams/"))
            self.assertFalse(processing.done())
            await processing


if __name__ == "__main__":
    unittest.main()
