from __future__ import annotations

import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

from gb300_relay.client import RelayClient
from gb300_relay.config import MediaPolicy, RetentionConfig, WorkerConfig
from gb300_relay.errors import StorageError
from gb300_relay.gc import GarbageCollector
from gb300_relay.layout import ObjectLayout
from gb300_relay.protocol import JobStatus, ReadyMarker, canonical_json_bytes, sha256_bytes
from gb300_relay.storage import MemoryObjectStore
from gb300_relay.upstream import UpstreamResponse
from gb300_relay.worker import RelayWorker


class OneShotUpstream:
    async def request(self, endpoint, body, forwarded_headers):
        del endpoint, body, forwarded_headers
        return UpstreamResponse(200, {}, "application/json", b'{"ok":true}')

    def stream(self, endpoint, body, forwarded_headers):
        raise AssertionError("stream was not expected")

    async def close(self):
        return None


class FailReadyOnceStore(MemoryObjectStore):
    def __init__(self) -> None:
        super().__init__()
        self.failed = False

    def put_bytes(self, key, data, *, content_type="application/octet-stream", if_absent=False):
        if key.endswith("/READY.json") and not self.failed:
            self.failed = True
            raise StorageError("injected READY failure")
        return super().put_bytes(
            key,
            data,
            content_type=content_type,
            if_absent=if_absent,
        )


class ReadyRaceStore(MemoryObjectStore):
    def __init__(self) -> None:
        super().__init__()
        self.barrier = threading.Barrier(2)

    def put_bytes(self, key, data, *, content_type="application/octet-stream", if_absent=False):
        if key.endswith("/READY.json") and if_absent:
            self.barrier.wait(timeout=2)
        return super().put_bytes(
            key,
            data,
            content_type=content_type,
            if_absent=if_absent,
        )


def make_client(store: MemoryObjectStore) -> RelayClient:
    return RelayClient(
        store,
        prefix="relay/v1",
        media_policy=MediaPolicy(),
        poll_interval_seconds=0.001,
    )


class RecoveryAndGcTests(unittest.IsolatedAsyncioTestCase):
    async def test_compact_idempotent_submission_race_converges(self) -> None:
        store = ReadyRaceStore()
        relay = RelayClient(
            store,
            prefix="relay/v1",
            media_policy=MediaPolicy(),
            compact_protocol=True,
        )
        arguments = {
            "endpoint": "/v1/chat/completions",
            "body": {"model": "m", "messages": []},
            "target": "gb300-1",
            "timeout_seconds": 30,
            "idempotency_key": "compact-race",
        }
        with ThreadPoolExecutor(max_workers=2) as executor:
            handles = list(executor.map(lambda _: relay.submit(**arguments), range(2)))
        self.assertEqual(handles[0].job_id, handles[1].job_id)
        self.assertEqual(handles[0].submitted_at, handles[1].submitted_at)

    async def test_gc_collects_compact_terminal_jobs(self) -> None:
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
            body={"model": "m", "messages": []},
            target="gb300-1",
            timeout_seconds=30,
        )
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
                upstream=OneShotUpstream(),
            )
            await worker.process(handle.job_id)
        relay.wait(handle, timeout_seconds=1, cleanup=False)
        collector = GarbageCollector(
            store,
            prefix="relay/v1",
            config=RetentionConfig(succeeded_seconds=60),
        )
        report = collector.collect(
            apply=True, now=datetime.now(UTC) + timedelta(seconds=61)
        )
        self.assertEqual(report.terminal_jobs_removed, 1)
        self.assertEqual(report.errors, ())

    async def test_idempotent_retry_repairs_partial_submission(self) -> None:
        store = FailReadyOnceStore()
        relay = make_client(store)
        arguments = {
            "endpoint": "/v1/chat/completions",
            "body": {"model": "m", "messages": []},
            "target": "gb300-1",
            "timeout_seconds": 30,
            "idempotency_key": "repair-me",
        }
        with self.assertRaises(StorageError):
            relay.submit(**arguments)
        handle = relay.submit(**arguments)
        self.assertIsNotNone(store.head(ObjectLayout("relay/v1").ready("gb300-1", handle.job_id)))

    async def test_malformed_manifest_becomes_terminal_failure(self) -> None:
        store = MemoryObjectStore()
        layout = ObjectLayout("relay/v1")
        job_id = "job-malformed"
        manifest = b'{"not":"a relay request"}'
        store.put_bytes(layout.manifest("gb300-1", job_id), manifest)
        ready = ReadyMarker(
            job_id=job_id,
            target="gb300-1",
            manifest_sha256=sha256_bytes(manifest),
        )
        store.put_bytes(
            layout.ready("gb300-1", job_id),
            canonical_json_bytes(ready.model_dump(mode="json")),
        )
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
                upstream=OneShotUpstream(),
            )
            outcome = await worker.process(job_id)
        self.assertEqual(outcome.status, JobStatus.FAILED)
        completed = make_client(store).wait(job_id, timeout_seconds=1)
        self.assertEqual(completed.metadata.http_status, 400)

    async def test_gc_is_dry_run_by_default_and_requires_ack(self) -> None:
        store = MemoryObjectStore()
        relay = make_client(store)
        handle = relay.submit(
            endpoint="/v1/chat/completions",
            body={"model": "m", "messages": []},
            target="gb300-1",
            timeout_seconds=30,
        )
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
                upstream=OneShotUpstream(),
            )
            await worker.process(handle.job_id)
        relay.wait(handle, timeout_seconds=1, cleanup=False)
        collector = GarbageCollector(
            store,
            prefix="relay/v1",
            config=RetentionConfig(succeeded_seconds=60),
        )
        future = datetime.now(UTC) + timedelta(seconds=61)
        preview = collector.collect(now=future)
        self.assertEqual(preview.terminal_candidates, 1)
        self.assertTrue(store.list("relay/v1/results/"))
        applied = collector.collect(apply=True, now=future)
        self.assertEqual(applied.terminal_jobs_removed, 1)
        self.assertFalse(store.list("relay/v1/results/"))


if __name__ == "__main__":
    unittest.main()
