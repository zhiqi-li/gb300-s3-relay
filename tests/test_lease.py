from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from gb300_relay.errors import LeaseLostError
from gb300_relay.layout import ObjectLayout
from gb300_relay.lease import LeaseManager
from gb300_relay.storage import MemoryObjectStore


class LeaseTests(unittest.TestCase):
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
