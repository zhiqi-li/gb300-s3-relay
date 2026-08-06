from __future__ import annotations

import os
import socket
import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .errors import ConfigurationError


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class S3Config(ConfigModel):
    bucket: str = Field(min_length=3, max_length=255)
    prefix: str = "gb300-relay/v1"
    endpoint_url: str
    region: str = "us-east-1"
    profile: str | None = None
    credentials_file: Path | None = None
    s5cmd_path: Path = Path("s5cmd")
    addressing_style: Literal["path", "virtual", "auto"] = "path"
    verify_tls: bool = True
    clear_proxy_env: bool = True
    connect_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    operation_timeout_seconds: float = Field(default=300.0, gt=0, le=86_400)
    retry_count: int = Field(default=10, ge=0, le=100)
    max_pool_connections: int = Field(default=128, ge=1, le=4_096)
    native_transfer_max_bytes: int = Field(default=8 * 1024**2, ge=0, le=1024**3)

    @field_validator("prefix")
    @classmethod
    def normalize_prefix(cls, value: str) -> str:
        value = value.strip("/")
        if not value or ".." in value.split("/"):
            raise ValueError("prefix must be a non-empty safe object prefix")
        return value

    @field_validator("endpoint_url")
    @classmethod
    def endpoint_requires_scheme(cls, value: str) -> str:
        if not value.startswith(("https://", "http://")):
            raise ValueError("endpoint_url must include http:// or https://")
        return value.rstrip("/")


class MediaPolicy(ConfigModel):
    max_request_bytes: int = Field(default=8 * 1024**3, ge=1)
    max_asset_bytes: int = Field(default=6 * 1024**3, ge=1)
    max_assets: int = Field(default=64, ge=0, le=10_000)
    max_inline_data_uri_bytes: int = Field(default=512 * 1024**2, ge=1)
    materialize_data_urls: bool = True
    allow_file_urls: bool = False
    allowed_file_roots: tuple[Path, ...] = ()
    materialize_http_urls: bool = False
    allowed_http_hosts: tuple[str, ...] = ()

    @model_validator(mode="after")
    def file_roots_required_when_enabled(self) -> MediaPolicy:
        if self.allow_file_urls and not self.allowed_file_roots:
            raise ValueError("allowed_file_roots is required when allow_file_urls=true")
        return self


class GatewayConfig(ConfigModel):
    host: str = "127.0.0.1"
    port: int = Field(default=8080, ge=1, le=65_535)
    targets: tuple[str, ...] = ("gb300-1", "gb300-2")
    default_timeout_seconds: float = Field(default=900.0, gt=0, le=86_400)
    max_timeout_seconds: float = Field(default=7_200.0, gt=0, le=86_400)
    poll_interval_seconds: float = Field(default=0.5, gt=0, le=60)
    target_heartbeat_ttl_seconds: float = Field(default=45.0, gt=1, le=3_600)
    require_healthy_worker: bool = True
    cleanup_on_success: bool = True
    cleanup_idempotent_on_success: bool = False
    compact_protocol: bool = False
    compact_manifest_max_bytes: int = Field(default=1024**2, ge=0, le=16 * 1024**2)
    thread_pool_workers: int = Field(default=128, ge=4, le=1_024)
    max_json_body_bytes: int = Field(default=1024**3, ge=1, le=16 * 1024**3)
    auth_token_env: str | None = None
    client_id: str = Field(default_factory=lambda: f"gateway-{socket.gethostname()}-{os.getpid()}")
    allowed_endpoints: tuple[str, ...] = (
        "/v1/chat/completions",
        "/v1/responses",
        "/v1/embeddings",
    )
    media: MediaPolicy = Field(default_factory=MediaPolicy)

    @field_validator("targets")
    @classmethod
    def targets_cannot_be_empty(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("at least one target is required")
        if len(value) != len(set(value)):
            raise ValueError("targets must be unique")
        return value


class WorkerConfig(ConfigModel):
    target: str
    worker_id: str = Field(default_factory=lambda: f"worker-{socket.gethostname()}-{os.getpid()}")
    upstream_base_url: str = "http://127.0.0.1:8000"
    upstream_api_key_env: str | None = "GB300_UPSTREAM_API_KEY"
    max_concurrency: int = Field(default=8, ge=1, le=1_024)
    max_heavy_concurrency: int = Field(default=8, ge=1, le=1_024)
    heavy_request_threshold_bytes: int = Field(default=128 * 1024, ge=1, le=1024**3)
    thread_pool_workers: int = Field(default=128, ge=4, le=1_024)
    asset_transfer_concurrency: int = Field(default=8, ge=1, le=128)
    poll_interval_seconds: float = Field(default=0.5, gt=0, le=60)
    poll_jitter_seconds: float = Field(default=0.25, ge=0, le=60)
    lease_seconds: int = Field(default=1_800, ge=30, le=86_400)
    lease_heartbeat_seconds: int = Field(default=60, ge=5, le=3_600)
    worker_heartbeat_seconds: int = Field(default=10, ge=1, le=600)
    job_timeout_seconds: float = Field(default=1_500, gt=0, le=86_400)
    max_attempts: int = Field(default=3, ge=1, le=100)
    retry_base_seconds: float = Field(default=1.0, ge=0, le=300)
    work_dir: Path = Path("/var/tmp/gb300-relay")
    media_delivery: Literal["auto", "data_uri", "file_uri"] = "auto"
    inline_image_max_bytes: int = Field(default=32 * 1024**2, ge=1)
    max_response_bytes: int = Field(default=512 * 1024**2, ge=1)
    compact_response_max_bytes: int = Field(default=1024**2, ge=0, le=16 * 1024**2)
    stream_chunk_bytes: int = Field(default=256 * 1024, ge=1_024, le=64 * 1024**2)
    stream_flush_interval_seconds: float = Field(default=0.5, gt=0, le=60)
    shutdown_grace_seconds: float = Field(default=300.0, ge=0, le=7_200)
    allowed_endpoints: tuple[str, ...] = (
        "/v1/chat/completions",
        "/v1/responses",
        "/v1/embeddings",
    )
    models: tuple[str, ...] = ()

    @field_validator("upstream_base_url")
    @classmethod
    def normalize_base_url(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("upstream_base_url must include a URL scheme")
        return value.rstrip("/")

    @model_validator(mode="after")
    def heartbeat_must_fit_lease(self) -> WorkerConfig:
        if self.lease_heartbeat_seconds * 2 >= self.lease_seconds:
            raise ValueError("lease_heartbeat_seconds must be less than half the lease")
        return self


class MetricsConfig(ConfigModel):
    enabled: bool = True
    host: str = "127.0.0.1"
    port: int = Field(default=9108, ge=1, le=65_535)


class RetentionConfig(ConfigModel):
    succeeded_seconds: int = Field(default=86_400, ge=60, le=365 * 86_400)
    failed_seconds: int = Field(default=7 * 86_400, ge=60, le=365 * 86_400)
    require_acknowledgement: bool = True
    stale_worker_heartbeat_seconds: int = Field(default=3_600, ge=60, le=365 * 86_400)


class AppConfig(ConfigModel):
    s3: S3Config
    gateway: GatewayConfig | None = None
    worker: WorkerConfig | None = None
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    retention: RetentionConfig = Field(default_factory=RetentionConfig)


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path).expanduser().resolve()
    try:
        raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigurationError(f"unable to load config {config_path}: {exc}") from exc
    try:
        config = AppConfig.model_validate(raw)
    except Exception as exc:
        raise ConfigurationError(f"invalid config {config_path}: {exc}") from exc
    s3_updates: dict[str, Path] = {}
    if config.s3.credentials_file and not config.s3.credentials_file.is_absolute():
        s3_updates["credentials_file"] = config_path.parent / config.s3.credentials_file
    if "/" in str(config.s3.s5cmd_path) and not config.s3.s5cmd_path.is_absolute():
        s3_updates["s5cmd_path"] = config_path.parent / config.s3.s5cmd_path
    if s3_updates:
        config = config.model_copy(update={"s3": config.s3.model_copy(update=s3_updates)})
    if config.worker and not config.worker.work_dir.is_absolute():
        config = config.model_copy(
            update={
                "worker": config.worker.model_copy(
                    update={"work_dir": config_path.parent / config.worker.work_dir}
                )
            }
        )
    return config
