from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import tempfile
import threading
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from urllib.parse import quote

import boto3
import botocore.session
import httpx
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import BotoCoreError, ClientError

from .config import S3Config
from .errors import (
    ConditionalWriteFailed,
    ConfigurationError,
    IntegrityError,
    ObjectNotFoundError,
    StorageError,
)


@dataclass(frozen=True, slots=True)
class ObjectInfo:
    key: str
    size: int
    etag: str | None = None
    last_modified: datetime | None = None


class ObjectStore(Protocol):
    bucket: str

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        if_absent: bool = False,
    ) -> ObjectInfo: ...

    def get_bytes(self, key: str, *, max_bytes: int | None = None) -> bytes: ...

    def upload_file(self, local_path: Path, key: str) -> ObjectInfo: ...

    def download_file(self, key: str, local_path: Path) -> ObjectInfo: ...

    def head(self, key: str) -> ObjectInfo | None: ...

    def list(self, prefix: str) -> list[ObjectInfo]: ...

    def delete_keys(self, keys: Iterable[str]) -> None: ...

    def delete_prefix(self, prefix: str) -> int: ...

    def ping(self) -> None: ...


def sha256_file(path: Path, *, chunk_bytes: int = 8 * 1024**2) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_bytes):
            digest.update(chunk)
    return digest.hexdigest()


class S3ObjectStore:
    """Boto3 control plane plus s5cmd data plane.

    Small immutable protocol objects use signed S3 calls. Large request and
    response files use s5cmd, which gives substantially better parallel and
    multipart transfer behavior on S3-compatible storage.
    """

    def __init__(self, config: S3Config) -> None:
        self.config = config
        self.bucket = config.bucket
        botocore_session = botocore.session.get_session()
        botocore_session.set_config_variable("region", config.region)
        if config.profile:
            botocore_session.set_config_variable("profile", config.profile)
        if config.credentials_file:
            botocore_session.set_config_variable("credentials_file", str(config.credentials_file))
        self._session = boto3.Session(botocore_session=botocore_session)
        self._client = self._session.client(
            "s3",
            endpoint_url=config.endpoint_url,
            verify=config.verify_tls,
            config=BotocoreConfig(
                connect_timeout=config.connect_timeout_seconds,
                read_timeout=config.operation_timeout_seconds,
                retries={"max_attempts": config.retry_count, "mode": "adaptive"},
                s3={"addressing_style": config.addressing_style},
                max_pool_connections=config.max_pool_connections,
            ),
        )
        self._http = httpx.Client(
            timeout=httpx.Timeout(
                config.operation_timeout_seconds,
                connect=config.connect_timeout_seconds,
            ),
            verify=config.verify_tls,
            trust_env=not config.clear_proxy_env,
        )
        self._s5cmd = S5CmdRunner(config)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> S3ObjectStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _credentials(self):
        credentials = self._session.get_credentials()
        if credentials is None:
            raise ConfigurationError("AWS credentials were not found")
        return credentials.get_frozen_credentials()

    def _path_style_url(self, key: str) -> str:
        return f"{self.config.endpoint_url}/{quote(self.bucket, safe='')}/{quote(key, safe='/~')}"

    def _conditional_put(self, key: str, data: bytes, content_type: str) -> ObjectInfo:
        url = self._path_style_url(key)
        request = AWSRequest(
            method="PUT",
            url=url,
            data=data,
            headers={"content-type": content_type, "if-none-match": "*"},
        )
        S3SigV4Auth(self._credentials(), "s3", self.config.region).add_auth(request)
        prepared = request.prepare()
        try:
            response = self._http.put(
                url,
                headers=dict(prepared.headers.items()),
                content=data,
            )
        except httpx.HTTPError as exc:
            raise StorageError(
                f"conditional PUT failed for s3://{self.bucket}/{key}: {exc}"
            ) from exc
        if response.status_code in {409, 412}:
            raise ConditionalWriteFailed(f"object already exists: s3://{self.bucket}/{key}")
        if response.status_code not in {200, 201, 204}:
            message = response.text[:1_000].replace("\n", " ")
            raise StorageError(
                f"conditional PUT failed for s3://{self.bucket}/{key}: "
                f"HTTP {response.status_code} {message}"
            )
        return ObjectInfo(key=key, size=len(data), etag=response.headers.get("etag"))

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        if_absent: bool = False,
    ) -> ObjectInfo:
        if if_absent:
            return self._conditional_put(key, data, content_type)
        try:
            response = self._client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
            )
        except (BotoCoreError, ClientError) as exc:
            raise StorageError(f"PUT failed for s3://{self.bucket}/{key}: {exc}") from exc
        return ObjectInfo(key=key, size=len(data), etag=response.get("ETag"))

    def get_bytes(self, key: str, *, max_bytes: int | None = None) -> bytes:
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
            size = int(response.get("ContentLength", 0))
            if max_bytes is not None and size > max_bytes:
                response["Body"].close()
                raise IntegrityError(f"object {key} exceeds {max_bytes} bytes")
            data = response["Body"].read(max_bytes + 1 if max_bytes is not None else None)
        except self._client.exceptions.NoSuchKey as exc:
            raise ObjectNotFoundError(f"object not found: s3://{self.bucket}/{key}") from exc
        except (BotoCoreError, ClientError) as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
            if code in {"NoSuchKey", "404", "NotFound"}:
                raise ObjectNotFoundError(f"object not found: s3://{self.bucket}/{key}") from exc
            raise StorageError(f"GET failed for s3://{self.bucket}/{key}: {exc}") from exc
        if max_bytes is not None and len(data) > max_bytes:
            raise IntegrityError(f"object {key} exceeds {max_bytes} bytes")
        return data

    def upload_file(self, local_path: Path, key: str) -> ObjectInfo:
        path = Path(local_path)
        if not path.is_file():
            raise StorageError(f"upload source is not a file: {path}")
        self._s5cmd.copy(str(path), self.uri(key))
        return ObjectInfo(key=key, size=path.stat().st_size)

    def download_file(self, key: str, local_path: Path) -> ObjectInfo:
        destination = Path(local_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            prefix=f".{destination.name}.", suffix=".partial", dir=destination.parent, delete=False
        ) as handle:
            temporary = Path(handle.name)
        try:
            self._s5cmd.copy(self.uri(key), str(temporary))
            size = temporary.stat().st_size
            os.replace(temporary, destination)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        return ObjectInfo(key=key, size=size)

    def head(self, key: str) -> ObjectInfo | None:
        try:
            response = self._client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in {"NoSuchKey", "404", "NotFound"} or status == 404:
                return None
            raise StorageError(f"HEAD failed for s3://{self.bucket}/{key}: {exc}") from exc
        except BotoCoreError as exc:
            raise StorageError(f"HEAD failed for s3://{self.bucket}/{key}: {exc}") from exc
        return ObjectInfo(
            key=key,
            size=int(response.get("ContentLength", 0)),
            etag=response.get("ETag"),
            last_modified=response.get("LastModified"),
        )

    def list(self, prefix: str) -> list[ObjectInfo]:
        objects: list[ObjectInfo] = []
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
            for page in pages:
                for item in page.get("Contents", ()):
                    objects.append(
                        ObjectInfo(
                            key=item["Key"],
                            size=int(item.get("Size", 0)),
                            etag=item.get("ETag"),
                            last_modified=item.get("LastModified"),
                        )
                    )
        except (BotoCoreError, ClientError) as exc:
            raise StorageError(f"LIST failed for s3://{self.bucket}/{prefix}: {exc}") from exc
        return objects

    def delete_keys(self, keys: Iterable[str]) -> None:
        batch: list[dict[str, str]] = []
        for key in keys:
            batch.append({"Key": key})
            if len(batch) == 1_000:
                self._delete_batch(batch)
                batch = []
        if batch:
            self._delete_batch(batch)

    def _delete_batch(self, objects: list[dict[str, str]]) -> None:
        try:
            response = self._client.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": objects, "Quiet": True},
            )
        except (BotoCoreError, ClientError) as exc:
            raise StorageError(f"DELETE failed in bucket {self.bucket}: {exc}") from exc
        errors = response.get("Errors", ())
        if errors:
            summary = ", ".join(f"{item.get('Key')}:{item.get('Code')}" for item in errors[:10])
            raise StorageError(f"DELETE partially failed in bucket {self.bucket}: {summary}")

    def delete_prefix(self, prefix: str) -> int:
        keys = [item.key for item in self.list(prefix)]
        self.delete_keys(keys)
        return len(keys)

    def ping(self) -> None:
        try:
            self._client.head_bucket(Bucket=self.bucket)
        except (BotoCoreError, ClientError) as exc:
            raise StorageError(f"bucket is unavailable: {self.bucket}: {exc}") from exc

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"


class S5CmdRunner:
    def __init__(self, config: S3Config) -> None:
        self.config = config

    def _binary(self) -> str:
        configured = str(self.config.s5cmd_path)
        if "/" in configured:
            if Path(configured).is_file() and os.access(configured, os.X_OK):
                return configured
        else:
            resolved = shutil.which(configured)
            if resolved:
                return resolved
        raise ConfigurationError(f"s5cmd is not executable: {configured}")

    def _environment(self) -> dict[str, str]:
        env = os.environ.copy()
        if self.config.profile or self.config.credentials_file:
            # Explicit relay credentials must win over credentials injected by
            # OSMO, Kubernetes, a login shell, or a parent SDK process. The AWS
            # provider chain otherwise prefers these ambient values to the
            # dedicated shared-credentials file used by the relay.
            for key in (
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "AWS_SECURITY_TOKEN",
                "AWS_ROLE_ARN",
                "AWS_ROLE_SESSION_NAME",
                "AWS_WEB_IDENTITY_TOKEN_FILE",
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                "AWS_CONTAINER_AUTHORIZATION_TOKEN",
                "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
                "AWS_PROFILE",
                "AWS_DEFAULT_PROFILE",
                "AWS_SHARED_CREDENTIALS_FILE",
            ):
                env.pop(key, None)
        env["AWS_REGION"] = self.config.region
        env["AWS_DEFAULT_REGION"] = self.config.region
        env["S3_ENDPOINT_URL"] = self.config.endpoint_url
        env["AWS_EC2_METADATA_DISABLED"] = "true"
        if self.config.profile:
            env["AWS_PROFILE"] = self.config.profile
        if self.config.credentials_file:
            env["AWS_SHARED_CREDENTIALS_FILE"] = str(self.config.credentials_file)
        if self.config.clear_proxy_env:
            for key in ("HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"):
                env.pop(key, None)
        return env

    def copy(self, source: str, destination: str) -> None:
        command = [
            self._binary(),
            "--endpoint-url",
            self.config.endpoint_url,
            "--retry-count",
            str(self.config.retry_count),
        ]
        if not self.config.verify_tls:
            command.append("--no-verify-ssl")
        command.extend(["cp", source, destination])
        try:
            result = subprocess.run(
                command,
                env=self._environment(),
                text=True,
                capture_output=True,
                timeout=self.config.operation_timeout_seconds,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise StorageError(f"s5cmd transfer failed: {source} -> {destination}: {exc}") from exc
        if result.returncode != 0:
            message = (result.stderr or result.stdout).strip()[-2_000:]
            raise StorageError(
                f"s5cmd transfer failed ({result.returncode}): {source} -> {destination}: {message}"
            )


@dataclass(slots=True)
class _MemoryValue:
    data: bytes
    modified: datetime
    etag: str


class MemoryObjectStore:
    """Thread-safe in-memory store used by concurrency and recovery tests."""

    def __init__(self, bucket: str = "memory") -> None:
        self.bucket = bucket
        self._objects: dict[str, _MemoryValue] = {}
        self._lock = threading.RLock()

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        if_absent: bool = False,
    ) -> ObjectInfo:
        del content_type
        with self._lock:
            if if_absent and key in self._objects:
                raise ConditionalWriteFailed(f"object already exists: {key}")
            digest = hashlib.md5(data, usedforsecurity=False).hexdigest()  # noqa: S324
            now = datetime.now(UTC)
            self._objects[key] = _MemoryValue(data=bytes(data), modified=now, etag=digest)
            return ObjectInfo(key=key, size=len(data), etag=digest, last_modified=now)

    def get_bytes(self, key: str, *, max_bytes: int | None = None) -> bytes:
        with self._lock:
            try:
                data = self._objects[key].data
            except KeyError as exc:
                raise ObjectNotFoundError(f"object not found: {key}") from exc
        if max_bytes is not None and len(data) > max_bytes:
            raise IntegrityError(f"object {key} exceeds {max_bytes} bytes")
        return bytes(data)

    def upload_file(self, local_path: Path, key: str) -> ObjectInfo:
        return self.put_bytes(key, Path(local_path).read_bytes())

    def download_file(self, key: str, local_path: Path) -> ObjectInfo:
        data = self.get_bytes(key)
        path = Path(local_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return ObjectInfo(key=key, size=len(data))

    def head(self, key: str) -> ObjectInfo | None:
        with self._lock:
            value = self._objects.get(key)
            if value is None:
                return None
            return ObjectInfo(
                key=key,
                size=len(value.data),
                etag=value.etag,
                last_modified=value.modified,
            )

    def list(self, prefix: str) -> list[ObjectInfo]:
        with self._lock:
            return [
                ObjectInfo(
                    key=key,
                    size=len(value.data),
                    etag=value.etag,
                    last_modified=value.modified,
                )
                for key, value in sorted(self._objects.items())
                if key.startswith(prefix)
            ]

    def delete_keys(self, keys: Iterable[str]) -> None:
        with self._lock:
            for key in keys:
                self._objects.pop(key, None)

    def delete_prefix(self, prefix: str) -> int:
        with self._lock:
            keys = [key for key in self._objects if key.startswith(prefix)]
            for key in keys:
                del self._objects[key]
            return len(keys)

    def ping(self) -> None:
        return None
