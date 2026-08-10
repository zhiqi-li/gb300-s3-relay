from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import re
import tempfile
import time
import uuid
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .config import GatewayConfig, MediaPolicy
from .errors import (
    ConditionalWriteFailed,
    IntegrityError,
    InvalidRequestError,
    ObjectNotFoundError,
    RelayError,
)
from .layout import ObjectLayout
from .media import MediaMaterializer
from .protocol import (
    SAFE_ID_PATTERN,
    AckMarker,
    DoneMarker,
    JobHandle,
    JobStatus,
    ReadyMarker,
    RelayRequest,
    RelayResponse,
    canonical_json_bytes,
    request_fingerprint,
    sha256_bytes,
)
from .storage import ObjectStore, sha256_file

_TERMINAL_SSE_MARKERS = (
    b"data:[DONE]",
    b'"type":"response.completed"',
    b'"type":"response.failed"',
    b'"type":"response.incomplete"',
    b"event:response.completed",
    b"event:response.failed",
    b"event:response.incomplete",
)
_SAFE_PRODUCER_GROUP = re.compile(SAFE_ID_PATTERN)


def _contains_terminal_sse_event(data: bytes) -> bool:
    compact = b"".join(data.split()).lower()
    return any(marker.lower() in compact for marker in _TERMINAL_SSE_MARKERS)


class JobFailedError(RelayError):
    def __init__(self, response: RelayResponse, body: bytes) -> None:
        message = response.failure.message if response.failure else "relay job failed"
        super().__init__(message)
        self.response = response
        self.body = body


@dataclass(frozen=True, slots=True)
class CompletedJob:
    metadata: RelayResponse
    body: bytes


@dataclass(frozen=True, slots=True)
class JobStatusView:
    job_id: str
    state: str
    done: DoneMarker | None = None


class RelayClient:
    def __init__(
        self,
        store: ObjectStore,
        *,
        prefix: str,
        client_id: str = "relay-client",
        producer_group: str | None = None,
        media_policy: MediaPolicy | None = None,
        poll_interval_seconds: float = 0.5,
        upload_concurrency: int = 8,
        compact_protocol: bool = False,
        compact_manifest_max_bytes: int = 1024**2,
    ) -> None:
        self.store = store
        self.layout = ObjectLayout(prefix)
        self.client_id = client_id
        self.producer_group = self._validate_producer_group(producer_group)
        self.media_policy = media_policy or MediaPolicy()
        self.poll_interval_seconds = poll_interval_seconds
        self.upload_concurrency = max(1, upload_concurrency)
        self.compact_protocol = compact_protocol
        self.compact_manifest_max_bytes = max(0, compact_manifest_max_bytes)

    @staticmethod
    def _validate_producer_group(producer_group: str | None) -> str | None:
        if producer_group is not None and not _SAFE_PRODUCER_GROUP.fullmatch(producer_group):
            raise InvalidRequestError(f"unsafe producer_group: {producer_group!r}")
        return producer_group

    def _ready_key(self, target: str, job_id: str, producer_group: str | None) -> str:
        if producer_group is None:
            return self.layout.ready(target, job_id)
        return self.layout.grouped_ready(target, producer_group, job_id)

    def _job_id(
        self, target: str, idempotency_key: str | None, producer_group: str | None
    ) -> tuple[str, str | None]:
        if idempotency_key is None:
            return f"job-{uuid.uuid4().hex}", None
        if not idempotency_key or len(idempotency_key.encode("utf-8")) > 1_024:
            raise InvalidRequestError("idempotency key must be between 1 and 1024 bytes")
        namespace = f"{target}\0{idempotency_key}"
        if producer_group is not None:
            namespace = f"{target}\0{producer_group}\0{idempotency_key}"
        digest = hashlib.sha256(namespace.encode()).hexdigest()
        return f"idem-{digest[:48]}", digest

    @staticmethod
    def _equivalence_payload(request: RelayRequest) -> dict[str, Any]:
        value = request.model_dump(mode="json")
        for field in ("created_at", "expires_at", "trace_id"):
            value.pop(field, None)
        return value

    def _load_request(
        self, target: str, job_id: str, producer_group: str | None
    ) -> RelayRequest | None:
        try:
            ready_data = self.store.get_bytes(
                self._ready_key(target, job_id, producer_group), max_bytes=24 * 1024**2
            )
        except ObjectNotFoundError:
            ready = None
        else:
            ready = ReadyMarker.model_validate_json(ready_data)
        if ready is not None and ready.manifest_base64 is not None:
            manifest_data = self._decode_base64(ready.manifest_base64, "request manifest")
            if sha256_bytes(manifest_data) != ready.manifest_sha256:
                raise IntegrityError(f"manifest digest mismatch for {job_id}")
            return RelayRequest.model_validate_json(manifest_data)
        try:
            manifest_data = self.store.get_bytes(
                self.layout.manifest(target, job_id), max_bytes=16 * 1024**2
            )
        except ObjectNotFoundError:
            return None
        return RelayRequest.model_validate_json(manifest_data)

    @staticmethod
    def _decode_base64(value: str, description: str) -> bytes:
        try:
            return base64.b64decode(value, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise IntegrityError(f"invalid base64 {description}") from exc

    def _get_bytes_if_present(self, key: str, *, max_bytes: int) -> bytes | None:
        """Read an immutable object without issuing repeated missing-object GETs."""

        if self.store.head(key) is None:
            return None
        try:
            return self.store.get_bytes(key, max_bytes=max_bytes)
        except ObjectNotFoundError:
            # A concurrent cleanup or an eventually consistent compatible store can
            # make the object disappear between HEAD and GET. Treat that race as a
            # poll miss instead of failing the job.
            return None

    def _completed_idempotent_handle(self, request: RelayRequest) -> JobHandle | None:
        raw = self._get_bytes_if_present(
            self.layout.done(request.job_id), max_bytes=24 * 1024**2
        )
        if raw is None:
            return None
        done = DoneMarker.model_validate_json(raw)
        if done.request_fingerprint is None:
            return None
        if done.request_fingerprint != request_fingerprint(request):
            raise InvalidRequestError(
                "idempotency key was already used for a different request"
            )
        return JobHandle(
            job_id=request.job_id,
            target=request.target,
            producer_group=done.producer_group or request.producer_group,
            trace_id=done.trace_id or request.trace_id,
            submitted_at=done.request_created_at or request.created_at,
            expires_at=done.request_expires_at or request.expires_at,
        )

    def submit(
        self,
        *,
        endpoint: str,
        body: dict[str, Any],
        target: str,
        timeout_seconds: float = 900,
        idempotency_key: str | None = None,
        producer_group: str | None = None,
        stream: bool | None = None,
        forwarded_headers: dict[str, str] | None = None,
    ) -> JobHandle:
        if timeout_seconds <= 0:
            raise InvalidRequestError("timeout_seconds must be positive")
        effective_group = self._validate_producer_group(
            self.producer_group if producer_group is None else producer_group
        )
        job_id, idempotency_hash = self._job_id(target, idempotency_key, effective_group)
        existing = (
            self._load_request(target, job_id, effective_group)
            if idempotency_key is not None
            else None
        )
        trace_id = f"trace-{uuid.uuid4().hex}"
        now = datetime.now(UTC)
        with tempfile.TemporaryDirectory(prefix=f"gb300-relay-submit-{job_id}-") as raw_directory:
            assets_directory = Path(raw_directory) / "assets"
            materialized = MediaMaterializer(self.media_policy).materialize(body, assets_directory)
            request = RelayRequest(
                job_id=job_id,
                target=target,
                producer_group=effective_group,
                endpoint=endpoint,
                created_at=now,
                expires_at=now + timedelta(seconds=timeout_seconds),
                trace_id=trace_id,
                idempotency_key_hash=idempotency_hash,
                stream=bool(materialized.body.get("stream", False) if stream is None else stream),
                body=materialized.body,
                assets=materialized.descriptors,
                forwarded_headers=forwarded_headers or {},
            )
            if existing is not None:
                if self._equivalence_payload(existing) != self._equivalence_payload(request):
                    raise InvalidRequestError(
                        "idempotency key was already used for a different request"
                    )
                request = existing
            if idempotency_key is not None:
                completed_handle = self._completed_idempotent_handle(request)
                if completed_handle is not None:
                    return completed_handle

            def upload(descriptor) -> None:
                self.store.upload_file(
                    materialized.paths[descriptor.asset_id],
                    self.layout.asset(target, job_id, descriptor.object_name),
                )

            if request.assets:
                with ThreadPoolExecutor(
                    max_workers=min(self.upload_concurrency, len(request.assets))
                ) as executor:
                    list(executor.map(upload, request.assets))

            manifest_data = canonical_json_bytes(request.model_dump(mode="json"))
            manifest_key = self.layout.manifest(target, job_id)
            inline_manifest = (
                self.compact_protocol
                and len(manifest_data) <= self.compact_manifest_max_bytes
            )
            if existing is None and not inline_manifest:
                try:
                    self.store.put_bytes(
                        manifest_key,
                        manifest_data,
                        content_type="application/json",
                        if_absent=True,
                    )
                except ConditionalWriteFailed as exc:
                    winner = self._load_request(target, job_id, effective_group)
                    if winner is None or self._equivalence_payload(
                        winner
                    ) != self._equivalence_payload(request):
                        raise InvalidRequestError(
                            "idempotent submission raced with a different request"
                        ) from exc
                    request = winner
                    manifest_data = canonical_json_bytes(request.model_dump(mode="json"))

            ready = ReadyMarker(
                job_id=request.job_id,
                target=request.target,
                producer_group=request.producer_group,
                manifest_sha256=sha256_bytes(manifest_data),
                manifest_base64=(
                    base64.b64encode(manifest_data).decode("ascii") if inline_manifest else None
                ),
                compact_response=self.compact_protocol,
            )
            try:
                self.store.put_bytes(
                    self._ready_key(target, job_id, effective_group),
                    canonical_json_bytes(ready.model_dump(mode="json", exclude_defaults=True)),
                    content_type="application/json",
                    if_absent=True,
                )
            except ConditionalWriteFailed as exc:
                try:
                    current = ReadyMarker.model_validate_json(
                        self.store.get_bytes(
                            self._ready_key(target, job_id, effective_group),
                            max_bytes=24 * 1024**2,
                        )
                    )
                except ObjectNotFoundError:
                    completed_handle = self._completed_idempotent_handle(request)
                    if completed_handle is not None:
                        return completed_handle
                    raise IntegrityError(
                        "READY marker disappeared during conditional submission"
                    ) from exc
                if current.manifest_sha256 != ready.manifest_sha256:
                    winner = self._load_request(target, job_id, effective_group)
                    if (
                        winner is None
                        or idempotency_key is None
                        or self._equivalence_payload(winner)
                        != self._equivalence_payload(request)
                    ):
                        raise IntegrityError(
                            "READY marker does not match the request manifest"
                        ) from exc
                    request = winner
            if idempotency_key is not None and effective_group is not None:
                completed_handle = self._completed_idempotent_handle(request)
                if completed_handle is not None:
                    self.store.delete_keys(
                        (self._ready_key(target, job_id, effective_group),)
                    )
                    return completed_handle
            return JobHandle(
                job_id=request.job_id,
                target=request.target,
                producer_group=request.producer_group,
                trace_id=request.trace_id,
                submitted_at=request.created_at,
                expires_at=request.expires_at,
            )

    def status(self, job_id: str) -> JobStatusView:
        key = self.layout.done(job_id)
        try:
            raw = self.store.get_bytes(key, max_bytes=24 * 1024**2)
        except ObjectNotFoundError:
            return JobStatusView(job_id=job_id, state="PENDING")
        done = DoneMarker.model_validate_json(raw)
        return JobStatusView(job_id=job_id, state=done.status.value, done=done)

    def _read_completed(
        self, job_id: str, directory: Path, *, done_data: bytes | None = None
    ) -> CompletedJob:
        if done_data is None:
            done_data = self.store.get_bytes(
                self.layout.done(job_id), max_bytes=24 * 1024**2
            )
        done = DoneMarker.model_validate_json(done_data)
        if done.response is not None:
            metadata = done.response
            metadata_data = canonical_json_bytes(metadata.model_dump(mode="json"))
            if sha256_bytes(metadata_data) != done.response_sha256:
                raise IntegrityError(f"response metadata digest mismatch for {job_id}")
            if metadata.body_base64 is None:
                raise IntegrityError(f"compact response has no inline body for {job_id}")
            body = self._decode_base64(metadata.body_base64, "response body")
            if len(body) != metadata.body_size_bytes:
                raise IntegrityError(f"response body size mismatch for {job_id}")
            if sha256_bytes(body) != metadata.body_sha256:
                raise IntegrityError(f"response body digest mismatch for {job_id}")
            return CompletedJob(metadata=metadata, body=body)
        metadata_data = self.store.get_bytes(
            self.layout.response_metadata(job_id), max_bytes=4 * 1024**2
        )
        if sha256_bytes(metadata_data) != done.response_sha256:
            raise IntegrityError(f"response metadata digest mismatch for {job_id}")
        metadata = RelayResponse.model_validate_json(metadata_data)
        if metadata.body_object is None:
            raise IntegrityError(f"response body object is missing for {job_id}")
        body_path = directory / "response.body"
        self.store.download_file(
            metadata.body_object,
            body_path,
            expected_size_bytes=metadata.body_size_bytes,
        )
        if body_path.stat().st_size != metadata.body_size_bytes:
            raise IntegrityError(f"response body size mismatch for {job_id}")
        if sha256_file(body_path) != metadata.body_sha256:
            raise IntegrityError(f"response body digest mismatch for {job_id}")
        return CompletedJob(metadata=metadata, body=body_path.read_bytes())

    def wait(
        self,
        handle: JobHandle | str,
        *,
        timeout_seconds: float | None = None,
        cleanup: bool = False,
        raise_on_failure: bool = False,
        acknowledge: bool = True,
    ) -> CompletedJob:
        job_id = handle.job_id if isinstance(handle, JobHandle) else handle
        target = handle.target if isinstance(handle, JobHandle) else None
        producer_group = handle.producer_group if isinstance(handle, JobHandle) else None
        deadline = time.monotonic() + (
            timeout_seconds
            if timeout_seconds is not None
            else max(0.0, (handle.expires_at - datetime.now(UTC)).total_seconds())
            if isinstance(handle, JobHandle)
            else 900.0
        )
        while True:
            done_data = self._get_bytes_if_present(
                self.layout.done(job_id), max_bytes=24 * 1024**2
            )
            if done_data is not None:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"timed out waiting for relay job {job_id}")
            time.sleep(self.poll_interval_seconds)
        with tempfile.TemporaryDirectory(prefix=f"gb300-relay-result-{job_id}-") as directory:
            completed = self._read_completed(job_id, Path(directory), done_data=done_data)
        if target is None:
            target = completed.metadata.target
        if acknowledge:
            self.acknowledge(job_id)
        if cleanup:
            self.cleanup(target=target, job_id=job_id, producer_group=producer_group)
        if raise_on_failure and completed.metadata.status != JobStatus.SUCCEEDED:
            raise JobFailedError(completed.metadata, completed.body)
        return completed

    async def aiter_stream(
        self,
        handle: JobHandle,
        *,
        timeout_seconds: float | None = None,
        cleanup: bool = False,
    ) -> AsyncIterator[bytes]:
        deadline = time.monotonic() + (
            timeout_seconds
            if timeout_seconds is not None
            else max(0.0, (handle.expires_at - datetime.now(UTC)).total_seconds())
        )
        next_sequence = 0
        done: DoneMarker | None = None
        terminal_probe = b""
        held_terminal_chunks: list[bytes] = []
        terminal_seen = False
        while True:
            if done is None:
                done_key = self.layout.done(handle.job_id)
                raw = await asyncio.to_thread(
                    self._get_bytes_if_present, done_key, max_bytes=24 * 1024**2
                )
                if raw is not None:
                    done = DoneMarker.model_validate_json(raw)
            objects = await asyncio.to_thread(
                self.store.list, self.layout.stream_prefix(handle.job_id)
            )
            chunks: dict[int, str] = {}
            for item in objects:
                leaf = item.key.rsplit("/", 1)[-1]
                if not leaf.endswith(".sse") or not leaf[:-4].isdigit():
                    continue
                chunks[int(leaf[:-4])] = item.key
            while next_sequence in chunks:
                data = await asyncio.to_thread(self.store.get_bytes, chunks[next_sequence])
                next_sequence += 1
                terminal_probe = (terminal_probe + data)[-8192:]
                terminal_seen = terminal_seen or _contains_terminal_sse_event(terminal_probe)
                is_known_final = (
                    done is not None
                    and done.stream_chunk_count is not None
                    and next_sequence >= done.stream_chunk_count
                )
                if terminal_seen or is_known_final:
                    held_terminal_chunks.append(data)
                else:
                    yield data
            if done is not None:
                expected = done.stream_chunk_count or 0
                if next_sequence >= expected:
                    await asyncio.to_thread(self.acknowledge, handle.job_id)
                    if cleanup and done.status == JobStatus.SUCCEEDED:
                        await asyncio.to_thread(
                            self.cleanup,
                            target=handle.target,
                            job_id=handle.job_id,
                            producer_group=handle.producer_group,
                        )
                    for data in held_terminal_chunks:
                        yield data
                    break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"timed out streaming relay job {handle.job_id}")
            await asyncio.sleep(self.poll_interval_seconds)

    def acknowledge(self, job_id: str) -> None:
        marker = AckMarker(job_id=job_id, client_id=self.client_id)
        try:
            self.store.put_bytes(
                self.layout.ack(job_id),
                canonical_json_bytes(marker.model_dump(mode="json")),
                content_type="application/json",
                if_absent=True,
            )
        except ConditionalWriteFailed:
            return

    def cancel(self, job_id: str) -> None:
        payload = canonical_json_bytes(
            {
                "schema_version": "1.0",
                "job_id": job_id,
                "client_id": self.client_id,
                "cancelled_at": datetime.now(UTC).isoformat(),
            }
        )
        try:
            self.store.put_bytes(
                self.layout.cancel(job_id),
                payload,
                content_type="application/json",
                if_absent=True,
            )
        except ConditionalWriteFailed:
            return

    def cleanup(
        self, *, target: str, job_id: str, producer_group: str | None = None
    ) -> int:
        effective_group = self.producer_group if producer_group is None else producer_group
        prefixes = self.layout.job_cleanup_prefixes(target, job_id)
        with ThreadPoolExecutor(max_workers=len(prefixes)) as executor:
            groups = list(executor.map(self.store.list, prefixes))
        keys = [item.key for group in groups for item in group]
        if effective_group is not None:
            grouped_ready = self.layout.grouped_ready(target, effective_group, job_id)
            if self.store.head(grouped_ready) is not None:
                keys.append(grouped_ready)
        self.store.delete_keys(keys)
        self.store.delete_keys(
            (
                self.layout.ack(job_id),
                self.layout.cancel(job_id),
                self.layout.deadletter(target, job_id),
            )
        )
        return len(keys)


def client_from_gateway_config(
    store: ObjectStore, prefix: str, config: GatewayConfig
) -> RelayClient:
    return RelayClient(
        store,
        prefix=prefix,
        client_id=config.client_id,
        producer_group=config.producer_group,
        media_policy=config.media,
        poll_interval_seconds=config.poll_interval_seconds,
        compact_protocol=config.compact_protocol,
        compact_manifest_max_bytes=config.compact_manifest_max_bytes,
    )
