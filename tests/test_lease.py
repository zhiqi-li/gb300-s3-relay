from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from gb300_relay.errors import LeaseLostError
from gb300_relay.layout import ObjectLayout
from gb300_relay.lease import LeaseManager
from gb300_relay.storage import MemoryObjectStore


class CountingStore(MemoryObjectStore):
    def __init__(self) -> None:
        super().__init__()
        self.gets = 0
        self.lists = 0

    def get_bytes(self, key, *, max_bytes=None):
        self.gets += 1
        return super().get_bytes(key, max_bytes=max_bytes)

    def list(self, prefix):
        self.lists += 1
        return super().list(prefix)


class LeaseTests(unittest.TestCase):
    def test_owner_fencing_uses_one_listing_for_an_acquired_immutable_claim(self) -> None:
        store = CountingStore()
        leases = LeaseManager(store, ObjectLayout("relay/v1"), clock_skew_grace_seconds=0)
        token = leases.acquire(
            target="gb300-1", job_id="job-1", worker_id="worker-1", lease_seconds=30
        )
        assert token is not None
        store.gets = 0
        store.lists = 0
        leases.assert_owner(token)
        self.assertEqual(store.lists, 1)
        self.assertEqual(store.gets, 0)

    def test_only_one_worker_wins_a_generation(self) -> None:
        store = MemoryObjectStore()
        leases = LeaseManager(store, ObjectLayout("relay/v1"), clock_skew_grace_seconds=0)
        first = leases.acquire(
            target="gb300-1", job_id="job-1", worker_id="worker-1", lease_seconds=30
        )
        second = leases.acquire(
            target="gb300-1", job_id="job-1", worker_id="worker-2", lease_seconds=30
        )
        self.assertIsNotNone(first)
        self.assertIsNone(second)

    def test_expired_lease_uses_new_immutable_generation(self) -> None:
        store = MemoryObjectStore()
        leases = LeaseManager(store, ObjectLayout("relay/v1"), clock_skew_grace_seconds=0)
        first = leases.acquire(
            target="gb300-1", job_id="job-1", worker_id="worker-1", lease_seconds=10
        )
        self.assertIsNotNone(first)
        future = datetime.now(UTC) + timedelta(seconds=11)
        second = leases.acquire(
            target="gb300-1",
            job_id="job-1",
            worker_id="worker-2",
            lease_seconds=10,
            now=future,
        )
        self.assertIsNotNone(second)
        assert first is not None and second is not None
        self.assertEqual(second.generation, first.generation + 1)
        with self.assertRaises(LeaseLostError):
            leases.assert_owner(first, now=future)


if __name__ == "__main__":
    unittest.main()
