from __future__ import annotations

import unittest
from datetime import timedelta

from pydantic import ValidationError

from gb300_relay.layout import ObjectLayout
from gb300_relay.protocol import AssetDescriptor, Modality, RelayRequest, utc_now


class ProtocolTests(unittest.TestCase):
    def test_rejects_credential_headers(self) -> None:
        now = utc_now()
        with self.assertRaises(ValidationError):
            RelayRequest(
                job_id="job-1",
                target="gb300-1",
                endpoint="/v1/chat/completions",
                created_at=now,
                expires_at=now + timedelta(minutes=1),
                trace_id="trace-1",
                body={},
                forwarded_headers={"Authorization": "Bearer secret"},
            )

    def test_asset_filename_must_be_safe(self) -> None:
        with self.assertRaises(ValidationError):
            AssetDescriptor(
                asset_id="asset-1",
                modality=Modality.IMAGE,
                media_type="image/png",
                filename="../secret",
                object_name="assets/a.png",
                sha256="0" * 64,
                size_bytes=1,
            )

    def test_layout_round_trip_for_ready_key(self) -> None:
        layout = ObjectLayout("relay/v1")
        key = layout.ready("gb300-1", "job-123")
        self.assertEqual(layout.parse_ready_key(key, "gb300-1"), "job-123")
        self.assertIsNone(layout.parse_ready_key(key, "gb300-2"))

    def test_layout_round_trip_for_grouped_ready_key(self) -> None:
        layout = ObjectLayout("relay/v1")
        key = layout.grouped_ready("gb300-1", "osmo-node-17", "job-123")
        self.assertEqual(
            key,
            "relay/v1/queue/gb300-1/osmo-node-17/job-123.json",
        )
        self.assertEqual(
            layout.parse_grouped_ready_key(key, "gb300-1"),
            ("osmo-node-17", "job-123"),
        )
        self.assertIsNone(layout.parse_grouped_ready_key(key, "gb300-2"))


if __name__ == "__main__":
    unittest.main()
