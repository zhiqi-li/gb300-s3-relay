from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from gb300_relay.config import S3Config
from gb300_relay.storage import S5CmdRunner


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


if __name__ == "__main__":
    unittest.main()
