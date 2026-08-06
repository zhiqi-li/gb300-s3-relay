from __future__ import annotations

import base64
import binascii
import hashlib
import ipaddress
import mimetypes
import re
import shutil
import socket
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import httpx

from .config import MediaPolicy
from .errors import IntegrityError, InvalidRequestError
from .protocol import RELAY_ASSET_PREFIX, AssetDescriptor, Modality
from .storage import sha256_file

_DATA_URI = re.compile(r"^data:([^;,]+)?(;base64)?,(.*)$", re.DOTALL)
_URL_KEYS = frozenset({"url", "image_url", "video_url", "audio_url", "file_url"})


@dataclass(frozen=True, slots=True)
class MaterializedAssets:
    body: dict[str, Any]
    descriptors: tuple[AssetDescriptor, ...]
    paths: dict[str, Path]


def _modality(media_type: str, hint: str = "") -> Modality:
    lowered = f"{media_type} {hint}".lower()
    if media_type.startswith("image/") or "image" in lowered:
        return Modality.IMAGE
    if media_type.startswith("video/") or "video" in lowered:
        return Modality.VIDEO
    if media_type.startswith("audio/") or "audio" in lowered:
        return Modality.AUDIO
    return Modality.FILE


def _extension(media_type: str) -> str:
    extension = mimetypes.guess_extension(media_type.split(";", 1)[0].strip()) or ".bin"
    if not re.fullmatch(r"\.[A-Za-z0-9]{1,10}", extension):
        return ".bin"
    return extension


def _is_public_address(host: str) -> bool:
    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(host, None)}
    except socket.gaierror:
        return False
    if not addresses:
        return False
    for raw in addresses:
        address = ipaddress.ip_address(raw)
        if not address.is_global:
            return False
    return True


class MediaMaterializer:
    def __init__(self, policy: MediaPolicy) -> None:
        self.policy = policy

    def materialize(self, body: dict[str, Any], directory: Path) -> MaterializedAssets:
        directory.mkdir(parents=True, exist_ok=True)
        output = deepcopy(body)
        descriptors: list[AssetDescriptor] = []
        paths: dict[str, Path] = {}
        deduplicated: dict[tuple[str, str], AssetDescriptor] = {}
        total_bytes = 0

        def add_bytes(data: bytes, media_type: str, hint: str) -> str:
            nonlocal total_bytes
            if len(data) > self.policy.max_asset_bytes:
                raise InvalidRequestError(
                    f"media asset is {len(data)} bytes; limit is {self.policy.max_asset_bytes}"
                )
            digest = hashlib.sha256(data).hexdigest()
            key = (digest, media_type)
            existing = deduplicated.get(key)
            if existing is not None:
                return RELAY_ASSET_PREFIX + existing.asset_id
            if len(descriptors) >= self.policy.max_assets:
                raise InvalidRequestError(f"request exceeds {self.policy.max_assets} media assets")
            total_bytes += len(data)
            if total_bytes > self.policy.max_request_bytes:
                raise InvalidRequestError(
                    f"request media is {total_bytes} bytes; "
                    f"limit is {self.policy.max_request_bytes}"
                )
            asset_id = f"asset-{len(descriptors):04d}-{digest[:16]}"
            suffix = _extension(media_type)
            filename = f"{asset_id}{suffix}"
            path = directory / filename
            path.write_bytes(data)
            descriptor = AssetDescriptor(
                asset_id=asset_id,
                modality=_modality(media_type, hint),
                media_type=media_type,
                filename=filename,
                object_name=f"assets/{filename}",
                sha256=digest,
                size_bytes=len(data),
            )
            descriptors.append(descriptor)
            paths[asset_id] = path
            deduplicated[key] = descriptor
            return RELAY_ASSET_PREFIX + asset_id

        def add_file(path: Path, media_type: str, hint: str) -> str:
            nonlocal total_bytes
            try:
                resolved = path.expanduser().resolve(strict=True)
                roots = tuple(
                    root.expanduser().resolve(strict=True)
                    for root in self.policy.allowed_file_roots
                )
            except OSError as exc:
                raise InvalidRequestError(f"unable to read file URL: {path}") from exc
            if not self.policy.allow_file_urls or not any(
                resolved.is_relative_to(root) for root in roots
            ):
                raise InvalidRequestError(f"file URL is outside allowed roots: {resolved}")
            size = resolved.stat().st_size
            if size > self.policy.max_asset_bytes:
                raise InvalidRequestError(
                    f"media asset is {size} bytes; limit is {self.policy.max_asset_bytes}"
                )
            digest = sha256_file(resolved)
            key = (digest, media_type)
            existing = deduplicated.get(key)
            if existing is not None:
                return RELAY_ASSET_PREFIX + existing.asset_id
            if len(descriptors) >= self.policy.max_assets:
                raise InvalidRequestError(f"request exceeds {self.policy.max_assets} media assets")
            total_bytes += size
            if total_bytes > self.policy.max_request_bytes:
                raise InvalidRequestError(
                    f"request media is {total_bytes} bytes; "
                    f"limit is {self.policy.max_request_bytes}"
                )
            asset_id = f"asset-{len(descriptors):04d}-{digest[:16]}"
            suffix = (
                resolved.suffix
                if re.fullmatch(r"\.[A-Za-z0-9]{1,10}", resolved.suffix)
                else _extension(media_type)
            )
            filename = f"{asset_id}{suffix}"
            destination = directory / filename
            shutil.copyfile(resolved, destination)
            descriptor = AssetDescriptor(
                asset_id=asset_id,
                modality=_modality(media_type, hint),
                media_type=media_type,
                filename=filename,
                object_name=f"assets/{filename}",
                sha256=digest,
                size_bytes=size,
            )
            descriptors.append(descriptor)
            paths[asset_id] = destination
            deduplicated[key] = descriptor
            return RELAY_ASSET_PREFIX + asset_id

        def download_http(url: str, hint: str) -> str:
            parsed = urlparse(url)
            host = (parsed.hostname or "").lower()
            allowed = {value.lower() for value in self.policy.allowed_http_hosts}
            if not self.policy.materialize_http_urls:
                return url
            if host not in allowed:
                raise InvalidRequestError(f"remote media host is not allowlisted: {host}")
            if not _is_public_address(host):
                raise InvalidRequestError(
                    f"remote media host does not resolve to public addresses: {host}"
                )
            try:
                with httpx.stream(
                    "GET", url, follow_redirects=False, timeout=60, trust_env=False
                ) as response:
                    response.raise_for_status()
                    media_type = response.headers.get(
                        "content-type", "application/octet-stream"
                    ).split(";", 1)[0]
                    chunks: list[bytes] = []
                    size = 0
                    for chunk in response.iter_bytes():
                        size += len(chunk)
                        if size > self.policy.max_asset_bytes:
                            raise InvalidRequestError("remote media exceeds max_asset_bytes")
                        chunks.append(chunk)
            except httpx.HTTPError as exc:
                raise InvalidRequestError(f"unable to materialize remote media: {exc}") from exc
            return add_bytes(b"".join(chunks), media_type, hint)

        def visit(value: Any, *, key: str = "", hint: str = "") -> Any:
            if isinstance(value, dict):
                local_hint = str(value.get("type", hint))
                return {
                    child_key: visit(child, key=str(child_key), hint=local_hint)
                    for child_key, child in value.items()
                }
            if isinstance(value, list):
                return [visit(item, key=key, hint=hint) for item in value]
            if not isinstance(value, str) or key not in _URL_KEYS:
                return value
            if value.startswith("data:") and self.policy.materialize_data_urls:
                match = _DATA_URI.match(value)
                if match is None or match.group(2) != ";base64":
                    raise InvalidRequestError("only base64 data URLs are supported")
                media_type = match.group(1) or "application/octet-stream"
                try:
                    data = base64.b64decode(match.group(3), validate=True)
                except (ValueError, binascii.Error) as exc:
                    raise InvalidRequestError("invalid base64 media data URL") from exc
                if len(data) > self.policy.max_inline_data_uri_bytes:
                    raise InvalidRequestError("inline data URL exceeds max_inline_data_uri_bytes")
                return add_bytes(data, media_type, hint)
            if value.startswith("file://"):
                parsed = urlparse(value)
                if parsed.netloc not in {"", "localhost"}:
                    raise InvalidRequestError("remote hosts are not allowed in file URLs")
                media_type = mimetypes.guess_type(parsed.path)[0] or "application/octet-stream"
                return add_file(Path(unquote(parsed.path)), media_type, hint)
            if value.startswith(("https://", "http://")):
                return download_http(value, hint)
            return value

        output = visit(output)
        if not isinstance(output, dict):  # Defensive; the public input is typed as a dict.
            raise InvalidRequestError("OpenAI request body must be a JSON object")
        return MaterializedAssets(output, tuple(descriptors), paths)


def restore_asset_references(
    body: dict[str, Any],
    descriptors: tuple[AssetDescriptor, ...],
    paths: dict[str, Path],
    *,
    delivery: str,
    inline_image_max_bytes: int,
) -> dict[str, Any]:
    by_id = {descriptor.asset_id: descriptor for descriptor in descriptors}

    def replacement(asset_id: str) -> str:
        try:
            descriptor = by_id[asset_id]
            path = paths[asset_id]
        except KeyError as exc:
            raise IntegrityError(f"request references unknown asset {asset_id}") from exc
        use_data = delivery == "data_uri" or (
            delivery == "auto"
            and descriptor.modality == Modality.IMAGE
            and descriptor.size_bytes <= inline_image_max_bytes
        )
        if use_data:
            data = path.read_bytes()
            return f"data:{descriptor.media_type};base64,{base64.b64encode(data).decode('ascii')}"
        return path.resolve().as_uri()

    def visit(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: visit(item) for key, item in value.items()}
        if isinstance(value, list):
            return [visit(item) for item in value]
        if isinstance(value, str) and value.startswith(RELAY_ASSET_PREFIX):
            return replacement(value.removeprefix(RELAY_ASSET_PREFIX))
        return value

    restored = visit(deepcopy(body))
    if not isinstance(restored, dict):
        raise IntegrityError("restored request body is not an object")
    return restored
