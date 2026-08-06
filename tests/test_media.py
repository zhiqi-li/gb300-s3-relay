from __future__ import annotations

import base64
import tempfile
import unittest
from pathlib import Path

from gb300_relay.config import MediaPolicy
from gb300_relay.errors import InvalidRequestError
from gb300_relay.media import MediaMaterializer, restore_asset_references
from gb300_relay.protocol import Modality


class MediaTests(unittest.TestCase):
    def test_materializes_and_restores_image_and_video(self) -> None:
        image = b"\x89PNG\r\n\x1a\nimage"
        video = b"\x00\x00\x00\x18ftypmp42video"
        body = {
            "model": "vlm",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "describe"},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": "data:image/png;base64," + base64.b64encode(image).decode()
                            },
                        },
                        {
                            "type": "video_url",
                            "video_url": {
                                "url": "data:video/mp4;base64," + base64.b64encode(video).decode()
                            },
                        },
                    ],
                }
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            result = MediaMaterializer(MediaPolicy()).materialize(body, Path(directory))
            self.assertEqual(
                [item.modality for item in result.descriptors], [Modality.IMAGE, Modality.VIDEO]
            )
            restored = restore_asset_references(
                result.body,
                result.descriptors,
                result.paths,
                delivery="auto",
                inline_image_max_bytes=1024,
            )
            content = restored["messages"][0]["content"]
            self.assertTrue(content[1]["image_url"]["url"].startswith("data:image/png;base64,"))
            self.assertTrue(content[2]["video_url"]["url"].startswith("file://"))

    def test_deduplicates_identical_assets(self) -> None:
        encoded = base64.b64encode(b"same").decode()
        body = {
            "messages": [
                {
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/png;base64,{encoded}"},
                        },
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/png;base64,{encoded}"},
                        },
                    ]
                }
            ]
        }
        with tempfile.TemporaryDirectory() as directory:
            result = MediaMaterializer(MediaPolicy()).materialize(body, Path(directory))
        self.assertEqual(len(result.descriptors), 1)

    def test_file_urls_are_denied_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "image.png"
            path.write_bytes(b"image")
            body = {"image_url": {"url": path.resolve().as_uri()}}
            with self.assertRaises(InvalidRequestError):
                MediaMaterializer(MediaPolicy()).materialize(body, Path(directory) / "out")

    def test_enforces_inline_limit(self) -> None:
        encoded = base64.b64encode(b"too-large").decode()
        body = {"image_url": {"url": f"data:image/png;base64,{encoded}"}}
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(InvalidRequestError):
            MediaMaterializer(MediaPolicy(max_inline_data_uri_bytes=2)).materialize(
                body, Path(directory)
            )


if __name__ == "__main__":
    unittest.main()
