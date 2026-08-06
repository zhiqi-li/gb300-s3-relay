from __future__ import annotations

import os
import tempfile
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

from gb300_relay.config import S3Config
from gb300_relay.storage import S3ObjectStore, S5CmdRunner


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, *, Bucket, Key, Body, **kwargs):
        del Bucket, kwargs
        self.objects[Key] = Body.read() if hasattr(Body, "read") else bytes(Body)
        return {"ETag": "etag"}

    def get_object(self, *, Bucket, Key):
        del Bucket
        return {"Body": BytesIO(self.objects[Key]), "ContentLength": len(self.objects[Key])}


class FakeS5Cmd:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def copy(self, source: str, destination: str) -> None:
        self.calls.append((source, destination))


class S5CmdEnvironmentTests(unittest.TestCase):
    def test_explicit_profile_isolated_from_ambient_credentials(self) -> None:
        config = S3Config(
            bucket="relay-test",
            endpoint_url="https://objects.example.test",
            profile="relay-profile",
            credentials_file=Path("/secure/relay-credentials"),
        )
        ambient = {
            "AWS_ACCESS_KEY_ID": "wrong-access",
            "AWS_SECRET_ACCESS_KEY": "wrong-secret",
            "AWS_SESSION_TOKEN": "wrong-token",
            "AWS_PROFILE": "wrong-profile",
            "HTTPS_PROXY": "http://broken-proxy.invalid",
        }
        with patch.dict(os.environ, ambient, clear=True):
            environment = S5CmdRunner(config)._environment()
        self.assertNotIn("AWS_ACCESS_KEY_ID", environment)
        self.assertNotIn("AWS_SECRET_ACCESS_KEY", environment)
        self.assertNotIn("AWS_SESSION_TOKEN", environment)
        self.assertNotIn("HTTPS_PROXY", environment)
        self.assertEqual(environment["AWS_PROFILE"], "relay-profile")
        self.assertEqual(environment["AWS_SHARED_CREDENTIALS_FILE"], "/secure/relay-credentials")
        self.assertEqual(environment["S3_ENDPOINT_URL"], "https://objects.example.test")

    def test_small_file_transfer_uses_native_s3_without_spawning_s5cmd(self) -> None:
        config = S3Config(
            bucket="relay-test",
            endpoint_url="https://objects.example.test",
            native_transfer_max_bytes=1024,
        )
        store = object.__new__(S3ObjectStore)
        store.config = config
        store.bucket = config.bucket
        store._client = FakeS3Client()
        store._s5cmd = FakeS5Cmd()
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            destination = Path(directory) / "destination"
            source.write_bytes(b"small-response")
            store.upload_file(source, "small")
            store.download_file("small", destination, expected_size_bytes=14)
            self.assertEqual(destination.read_bytes(), b"small-response")
        self.assertEqual(store._s5cmd.calls, [])

    def test_large_file_upload_keeps_s5cmd_data_path(self) -> None:
        config = S3Config(
            bucket="relay-test",
            endpoint_url="https://objects.example.test",
            native_transfer_max_bytes=4,
        )
        store = object.__new__(S3ObjectStore)
        store.config = config
        store.bucket = config.bucket
        store._client = FakeS3Client()
        store._s5cmd = FakeS5Cmd()
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            source.write_bytes(b"large-response")
            store.upload_file(source, "large")
        self.assertEqual(len(store._s5cmd.calls), 1)


if __name__ == "__main__":
    unittest.main()
