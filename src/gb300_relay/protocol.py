from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SCHEMA_VERSION = "1.0"
SAFE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"
SHA256_PATTERN = r"^[a-f0-9]{64}$"
RELAY_ASSET_PREFIX = "relay://asset/"


def utc_now() -> datetime:
    return datetime.now(UTC)


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Modality(StrEnum):
    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    FILE = "file"


class JobStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


class AssetDescriptor(StrictModel):
    asset_id: str = Field(pattern=SAFE_ID_PATTERN)
    modality: Modality
    media_type: str = Field(min_length=3, max_length=255)
    filename: str = Field(min_length=1, max_length=255)
    object_name: str = Field(pattern=r"^assets/[A-Za-z0-9][A-Za-z0-9._-]{0,255}$")
    sha256: str = Field(pattern=SHA256_PATTERN)
    size_bytes: int = Field(ge=0)

    @field_validator("filename")
    @classmethod
    def filename_must_be_a_basename(cls, value: str) -> str:
        if value in {".", ".."} or "/" in value or "\\" in value or "\x00" in value:
            raise ValueError("filename must be a safe basename")
        return value

    @field_validator("media_type")
    @classmethod
    def media_type_must_not_contain_controls(cls, value: str) -> str:
        if any(ord(character) < 32 or ord(character) == 127 for character in value):
            raise ValueError("media_type cannot contain control characters")
        return value


class RelayRequest(StrictModel):
    schema_version: Literal["1.0"] = SCHEMA_VERSION
    job_id: str = Field(pattern=SAFE_ID_PATTERN)
    target: str = Field(pattern=SAFE_ID_PATTERN)
    endpoint: str = Field(pattern=r"^/v1/[A-Za-z0-9_./-]{1,200}$")
    method: Literal["POST"] = "POST"
    created_at: datetime = Field(default_factory=utc_now)
    expires_at: datetime
    trace_id: str = Field(pattern=SAFE_ID_PATTERN)
    idempotency_key_hash: str | None = Field(default=None, pattern=SHA256_PATTERN)
    stream: bool = False
    body: dict[str, Any]
    assets: tuple[AssetDescriptor, ...] = ()
    forwarded_headers: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_request(self) -> RelayRequest:
        if self.expires_at <= self.created_at:
            raise ValueError("expires_at must be later than created_at")
        ids = [asset.asset_id for asset in self.assets]
        if len(ids) != len(set(ids)):
            raise ValueError("asset_id values must be unique")
        lowered = {key.lower() for key in self.forwarded_headers}
        forbidden = {"authorization", "cookie", "proxy-authorization"} & lowered
        if forbidden:
            raise ValueError("credential-bearing headers cannot be forwarded")
        return self

    @property
    def digest(self) -> str:
        return sha256_bytes(canonical_json_bytes(self.model_dump(mode="json")))


class ReadyMarker(StrictModel):
    schema_version: Literal["1.0"] = SCHEMA_VERSION
    job_id: str = Field(pattern=SAFE_ID_PATTERN)
    target: str = Field(pattern=SAFE_ID_PATTERN)
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    manifest_base64: str | None = Field(default=None, max_length=24 * 1024**2)
    compact_response: bool = False
    created_at: datetime = Field(default_factory=utc_now)


class LeaseClaim(StrictModel):
    schema_version: Literal["1.0"] = SCHEMA_VERSION
    job_id: str = Field(pattern=SAFE_ID_PATTERN)
    target: str = Field(pattern=SAFE_ID_PATTERN)
    worker_id: str = Field(pattern=SAFE_ID_PATTERN)
    generation: int = Field(ge=0)
    created_at: datetime = Field(default_factory=utc_now)
    lease_seconds: int = Field(ge=10, le=86_400)

    @property
    def expires_at(self) -> datetime:
        return self.created_at + timedelta(seconds=self.lease_seconds)


class LeaseHeartbeat(StrictModel):
    schema_version: Literal["1.0"] = SCHEMA_VERSION
    job_id: str = Field(pattern=SAFE_ID_PATTERN)
    worker_id: str = Field(pattern=SAFE_ID_PATTERN)
    generation: int = Field(ge=0)
    created_at: datetime = Field(default_factory=utc_now)
    lease_seconds: int = Field(ge=10, le=86_400)

    @property
    def expires_at(self) -> datetime:
        return self.created_at + timedelta(seconds=self.lease_seconds)


class RelayFailure(StrictModel):
    type: str = Field(min_length=1, max_length=120)
    message: str = Field(min_length=1, max_length=8_192)
    retryable: bool = False
    attempt: int = Field(ge=1)


class RelayResponse(StrictModel):
    schema_version: Literal["1.0"] = SCHEMA_VERSION
    job_id: str = Field(pattern=SAFE_ID_PATTERN)
    target: str = Field(pattern=SAFE_ID_PATTERN)
    worker_id: str = Field(pattern=SAFE_ID_PATTERN)
    status: JobStatus
    created_at: datetime
    completed_at: datetime = Field(default_factory=utc_now)
    attempt: int = Field(ge=1)
    http_status: int = Field(ge=100, le=599)
    content_type: str = Field(min_length=1, max_length=255)
    body_object: str | None = Field(default=None, min_length=1, max_length=512)
    body_base64: str | None = Field(default=None, max_length=24 * 1024**2)
    body_sha256: str = Field(pattern=SHA256_PATTERN)
    body_size_bytes: int = Field(ge=0)
    response_headers: dict[str, str] = Field(default_factory=dict)
    failure: RelayFailure | None = None
    stream_chunk_count: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def failure_matches_status(self) -> RelayResponse:
        if self.status == JobStatus.SUCCEEDED and self.failure is not None:
            raise ValueError("successful response cannot include failure")
        if self.status != JobStatus.SUCCEEDED and self.failure is None:
            raise ValueError("non-successful response must include failure")
        if (self.body_object is None) == (self.body_base64 is None):
            raise ValueError("exactly one of body_object or body_base64 is required")
        return self


class DoneMarker(StrictModel):
    schema_version: Literal["1.0"] = SCHEMA_VERSION
    job_id: str = Field(pattern=SAFE_ID_PATTERN)
    status: JobStatus
    response_sha256: str = Field(pattern=SHA256_PATTERN)
    response: RelayResponse | None = None
    completed_at: datetime = Field(default_factory=utc_now)
    stream_chunk_count: int | None = Field(default=None, ge=0)


class AckMarker(StrictModel):
    schema_version: Literal["1.0"] = SCHEMA_VERSION
    job_id: str = Field(pattern=SAFE_ID_PATTERN)
    acknowledged_at: datetime = Field(default_factory=utc_now)
    client_id: str = Field(pattern=SAFE_ID_PATTERN)


class WorkerHeartbeat(StrictModel):
    schema_version: Literal["1.0"] = SCHEMA_VERSION
    target: str = Field(pattern=SAFE_ID_PATTERN)
    worker_id: str = Field(pattern=SAFE_ID_PATTERN)
    updated_at: datetime = Field(default_factory=utc_now)
    max_concurrency: int = Field(ge=1)
    inflight: int = Field(ge=0)
    models: tuple[str, ...] = ()
    modalities: tuple[Modality, ...] = ()
    healthy: bool = True


class JobHandle(StrictModel):
    job_id: str = Field(pattern=SAFE_ID_PATTERN)
    target: str = Field(pattern=SAFE_ID_PATTERN)
    trace_id: str = Field(pattern=SAFE_ID_PATTERN)
    submitted_at: datetime
    expires_at: datetime


_SECRET_KEY_PATTERN = re.compile(r"(?i)(authorization|api[-_]?key|token|secret|password)")


def redact_mapping(value: Any) -> Any:
    """Return a recursively redacted value suitable for structured logs."""

    if isinstance(value, dict):
        return {
            key: "<redacted>" if _SECRET_KEY_PATTERN.search(str(key)) else redact_mapping(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_mapping(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_mapping(item) for item in value)
    return value
