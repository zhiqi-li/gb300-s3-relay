from __future__ import annotations

import asyncio
import base64
import binascii
import logging
import random
import tempfile
import time
from contextlib import nullcontext, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from pydantic import ValidationError

from .config import WorkerConfig
from .errors import (
    ConditionalWriteFailed,
    IntegrityError,
    InvalidRequestError,
    LeaseLostError,
    ObjectNotFoundError,
    UpstreamError,
)
from .layout import ObjectLayout
from .lease import LeaseManager, LeaseToken
from .logging_utils import log_event
from .media import restore_asset_references
from .metrics import RelayMetrics
from .protocol import (
    DoneMarker,
    JobStatus,
    Modality,
    ReadyMarker,
    RelayFailure,
    RelayRequest,
    RelayResponse,
    WorkerHeartbeat,
    canonical_json_bytes,
    sha256_bytes,
)
from .storage import ObjectStore, sha256_file
from .upstream import OpenAIUpstream, UpstreamResponse

LOGGER = logging.getLogger(__name__)


class Upstream(Protocol):
    async def request(
        self,
        endpoint: str,
        body: dict[str, Any],
        forwarded_headers: dict[str, str],
    ) -> UpstreamResponse: ...

    def stream(
        self,
        endpoint: str,
        body: dict[str, Any],
        forwarded_headers: dict[str, str],
    ): ...

    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ProcessOutcome:
    job_id: str
    handled: bool
    status: JobStatus | None = None


class RelayWorker:
    def __init__(
        self,
        store: ObjectStore,
        *,
        prefix: str,
        config: WorkerConfig,
        upstream: Upstream | None = None,
        metrics: RelayMetrics | None = None,
    ) -> None:
        self.store = store
        self.layout = ObjectLayout(prefix)
        self.config = config
        self.upstream = upstream or OpenAIUpstream(config)
        self.metrics = metrics or RelayMetrics("worker")
        self.leases = LeaseManager(store, self.layout)
        self._heavy_slots = asyncio.Semaphore(
            min(config.max_heavy_concurrency, config.max_concurrency)
        )
        self._inflight: dict[str, asyncio.Task[ProcessOutcome]] = {}
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    def ready_jobs(self) -> list[str]:
        objects = self.store.list(self.layout.target_ready_prefix(self.config.target))
        jobs = {
            job_id
            for item in objects
            if (job_id := self.layout.parse_ready_key(item.key, self.config.target)) is not None
        }
        return sorted(jobs)

    def _is_cancelled(self, job_id: str) -> bool:
        return self.store.head(self.layout.cancel(job_id)) is not None

    def _is_ready(self, job_id: str) -> bool:
        return self.store.head(self.layout.ready(self.config.target, job_id)) is not None

    def _is_done(self, job_id: str) -> bool:
        return self.store.head(self.layout.done(job_id)) is not None

    async def run_forever(self) -> None:
        heartbeat = asyncio.create_task(self._worker_heartbeat_loop(), name="worker-heartbeat")
        try:
            while not self._stop.is_set():
                self._reap_tasks()
                available = self.config.max_concurrency - len(self._inflight)
                if available > 0:
                    try:
                        jobs = await asyncio.to_thread(self.ready_jobs)
                    except Exception:
                        self.metrics.poll_errors.labels("worker", self.config.target).inc()
                        log_event(
                            LOGGER,
                            logging.ERROR,
                            "ready_poll_failed",
                            target=self.config.target,
                            exc_info=True,
                        )
                        jobs = []
                    if jobs:
                        done_flags = await asyncio.gather(
                            *(asyncio.to_thread(self._is_done, job) for job in jobs),
                            return_exceptions=True,
                        )
                        jobs = [
                            job for job, done in zip(jobs, done_flags, strict=True) if done is False
                        ]
                    for job_id in jobs:
                        if available <= 0:
                            break
                        if job_id in self._inflight:
                            continue
                        task = asyncio.create_task(
                            self.process(job_id, discovered=True), name=f"job:{job_id}"
                        )
                        self._inflight[job_id] = task
                        available -= 1
                self.metrics.inflight.labels("worker", self.config.target).set(len(self._inflight))
                delay = self.config.poll_interval_seconds + random.uniform(
                    0, self.config.poll_jitter_seconds
                )
                with suppress(TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            if self._inflight:
                done, pending = await asyncio.wait(
                    self._inflight.values(), timeout=self.config.shutdown_grace_seconds
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
            await self.upstream.close()

    async def run_until_idle(self) -> list[ProcessOutcome]:
        """Process the current queue, including jobs discovered after a batch."""

        outcomes: list[ProcessOutcome] = []
        while True:
            candidates = await asyncio.to_thread(self.ready_jobs)
            done_flags = await asyncio.gather(
                *(asyncio.to_thread(self._is_done, job) for job in candidates)
            )
            jobs = [job for job, is_done in zip(candidates, done_flags, strict=True) if not is_done]
            if not jobs:
                return outcomes
            for offset in range(0, len(jobs), self.config.max_concurrency):
                batch = jobs[offset : offset + self.config.max_concurrency]
                outcomes.extend(
                    await asyncio.gather(
                        *(self.process(job, discovered=True) for job in batch)
                    )
                )

    def _reap_tasks(self) -> None:
        for job_id, task in list(self._inflight.items()):
            if not task.done():
                continue
            del self._inflight[job_id]
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                log_event(
                    LOGGER,
                    logging.ERROR,
                    "job_task_crashed",
                    job_id=job_id,
                    target=self.config.target,
                    exc_info=True,
                )

    async def _worker_heartbeat_loop(self) -> None:
        while True:
            marker = WorkerHeartbeat(
                target=self.config.target,
                worker_id=self.config.worker_id,
                max_concurrency=self.config.max_concurrency,
                inflight=len(self._inflight),
                models=self.config.models,
                modalities=(Modality.IMAGE, Modality.VIDEO, Modality.AUDIO, Modality.FILE),
            )
            try:
                await asyncio.to_thread(
                    self.store.put_bytes,
                    self.layout.worker_heartbeat(self.config.target, self.config.worker_id),
                    canonical_json_bytes(marker.model_dump(mode="json")),
                    content_type="application/json",
                )
            except Exception:
                log_event(
                    LOGGER,
                    logging.ERROR,
                    "worker_heartbeat_failed",
                    target=self.config.target,
                    worker_id=self.config.worker_id,
                    exc_info=True,
                )
            await asyncio.sleep(self.config.worker_heartbeat_seconds)

    async def _lease_heartbeat_loop(self, token: LeaseToken, lost: asyncio.Event) -> None:
        while True:
            try:
                await asyncio.sleep(self.config.lease_heartbeat_seconds)
                await asyncio.to_thread(self.leases.heartbeat, token)
            except LeaseLostError:
                lost.set()
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                # A transient object-store failure is not proof that ownership was
                # lost. Keep retrying; assert_owner fences publication later.
                log_event(
                    LOGGER,
                    logging.WARNING,
                    "lease_heartbeat_failed",
                    job_id=token.job_id,
                    target=token.target,
                    generation=token.generation,
                    exc_info=True,
                )

    async def _download_request(
        self, job_id: str, directory: Path
    ) -> tuple[ReadyMarker, RelayRequest, dict[str, Path]]:
        ready_data = await asyncio.to_thread(
            self.store.get_bytes,
            self.layout.ready(self.config.target, job_id),
            max_bytes=24 * 1024**2,
        )
        ready = ReadyMarker.model_validate_json(ready_data)
        if ready.manifest_base64 is not None:
            try:
                manifest_data = base64.b64decode(ready.manifest_base64, validate=True)
            except (ValueError, binascii.Error) as exc:
                raise IntegrityError(f"invalid base64 manifest for {job_id}") from exc
        else:
            manifest_data = await asyncio.to_thread(
                self.store.get_bytes,
                self.layout.manifest(self.config.target, job_id),
                max_bytes=16 * 1024**2,
            )
        if sha256_bytes(manifest_data) != ready.manifest_sha256:
            raise IntegrityError(f"manifest digest mismatch for {job_id}")
        request = RelayRequest.model_validate_json(manifest_data)
        if request.job_id != job_id or request.target != self.config.target:
            raise IntegrityError(f"manifest identity mismatch for {job_id}")
        if request.endpoint not in self.config.allowed_endpoints:
            raise InvalidRequestError(f"endpoint is not allowed: {request.endpoint}")
        assets_directory = directory / "assets"
        assets_directory.mkdir(parents=True, exist_ok=True)
        asset_paths: dict[str, Path] = {}
        semaphore = asyncio.Semaphore(self.config.asset_transfer_concurrency)

        async def download(descriptor) -> None:
            path = assets_directory / descriptor.filename
            async with semaphore:
                await asyncio.to_thread(
                    self.store.download_file,
                    self.layout.asset(self.config.target, request.job_id, descriptor.object_name),
                    path,
                    expected_size_bytes=descriptor.size_bytes,
                )
            if path.stat().st_size != descriptor.size_bytes:
                raise IntegrityError(f"asset size mismatch: {descriptor.asset_id}")
            digest = await asyncio.to_thread(sha256_file, path)
            if digest != descriptor.sha256:
                raise IntegrityError(f"asset digest mismatch: {descriptor.asset_id}")
            asset_paths[descriptor.asset_id] = path

        await asyncio.gather(*(download(descriptor) for descriptor in request.assets))
        return ready, request, asset_paths

    async def process(self, job_id: str, *, discovered: bool = False) -> ProcessOutcome:
        if not discovered:
            if not await asyncio.to_thread(self._is_ready, job_id):
                return ProcessOutcome(job_id, handled=False)
            if await asyncio.to_thread(self._is_done, job_id):
                return ProcessOutcome(job_id, handled=False)
        token = await asyncio.to_thread(
            self.leases.acquire,
            target=self.config.target,
            job_id=job_id,
            worker_id=self.config.worker_id,
            lease_seconds=self.config.lease_seconds,
        )
        if token is None:
            self.metrics.claim_contention.labels("worker", self.config.target).inc()
            return ProcessOutcome(job_id, handled=False)
        # A client can acknowledge and clean a result after queue discovery but
        # before this coroutine acquires its claim. Recheck both markers so a stale
        # READY listing cannot resurrect an already delivered request.
        if not await asyncio.to_thread(self._is_ready, job_id):
            return ProcessOutcome(job_id, handled=False)
        if await asyncio.to_thread(self._is_done, job_id):
            return ProcessOutcome(job_id, handled=False)
        started = time.monotonic()
        lost = asyncio.Event()
        heartbeat = asyncio.create_task(self._lease_heartbeat_loop(token, lost))
        status = JobStatus.FAILED
        endpoint = "unknown"
        try:
            self.config.work_dir.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(
                prefix=f"{job_id}-", dir=self.config.work_dir
            ) as raw_directory:
                directory = Path(raw_directory)
                ready, request, assets = await self._download_request(job_id, directory)
                endpoint = request.endpoint
                if request.expires_at <= datetime.now(UTC):
                    status = await self._publish_error(
                        request,
                        token,
                        directory,
                        error_type="expired",
                        message="request expired before execution",
                        http_status=408,
                        job_status=JobStatus.EXPIRED,
                        attempt=1,
                        compact_response=ready.compact_response,
                    )
                    return ProcessOutcome(job_id, handled=True, status=status)
                if await asyncio.to_thread(self._is_cancelled, job_id):
                    status = await self._publish_error(
                        request,
                        token,
                        directory,
                        error_type="cancelled",
                        message="request was cancelled",
                        http_status=499,
                        job_status=JobStatus.CANCELLED,
                        attempt=1,
                        compact_response=ready.compact_response,
                    )
                    return ProcessOutcome(job_id, handled=True, status=status)
                is_heavy = bool(request.assets) or len(
                    canonical_json_bytes(request.body)
                ) >= self.config.heavy_request_threshold_bytes
                limiter = self._heavy_slots if is_heavy else nullcontext()
                async with limiter:
                    body = restore_asset_references(
                        request.body,
                        request.assets,
                        assets,
                        delivery=self.config.media_delivery,
                        inline_image_max_bytes=self.config.inline_image_max_bytes,
                    )
                    if request.stream:
                        status = await self._process_stream(
                            request,
                            body,
                            token,
                            directory,
                            lost,
                            compact_response=ready.compact_response,
                        )
                    else:
                        status = await self._process_regular(
                            request,
                            body,
                            token,
                            directory,
                            lost,
                            compact_response=ready.compact_response,
                        )
                return ProcessOutcome(job_id, handled=True, status=status)
        except (IntegrityError, InvalidRequestError, ObjectNotFoundError, ValidationError) as exc:
            request = locals().get("request")
            if not isinstance(request, RelayRequest):
                now = datetime.now(UTC)
                request = RelayRequest(
                    job_id=job_id,
                    target=self.config.target,
                    endpoint="/v1/chat/completions",
                    created_at=now,
                    expires_at=now + timedelta(minutes=5),
                    trace_id=f"invalid-{job_id}"[:128],
                    body={},
                )
            with tempfile.TemporaryDirectory(
                prefix=f"{job_id}-error-", dir=self.config.work_dir
            ) as error_directory:
                status = await self._publish_error(
                    request,
                    token,
                    Path(error_directory),
                    error_type="invalid_request",
                    message=str(exc),
                    http_status=400,
                    attempt=1,
                    compact_response=bool(
                        isinstance(locals().get("ready"), ReadyMarker)
                        and locals()["ready"].compact_response
                    ),
                )
                return ProcessOutcome(job_id, handled=True, status=status)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            elapsed = time.monotonic() - started
            outcome = status.value.lower()
            self.metrics.requests.labels("worker", self.config.target, endpoint, outcome).inc()
            self.metrics.latency.labels("worker", self.config.target, endpoint).observe(elapsed)
            log_event(
                LOGGER,
                logging.INFO,
                "job_finished",
                job_id=job_id,
                target=self.config.target,
                status=status.value,
                duration_seconds=round(elapsed, 6),
            )

    async def _process_regular(
        self,
        request: RelayRequest,
        body: dict[str, Any],
        token: LeaseToken,
        directory: Path,
        lost: asyncio.Event,
        *,
        compact_response: bool,
    ) -> JobStatus:
        last_error: UpstreamError | None = None
        for attempt in range(1, self.config.max_attempts + 1):
            if lost.is_set():
                raise LeaseLostError(f"lease lost while processing {request.job_id}")
            if await asyncio.to_thread(self._is_cancelled, request.job_id):
                return await self._publish_error(
                    request,
                    token,
                    directory,
                    error_type="cancelled",
                    message="request was cancelled",
                    http_status=499,
                    job_status=JobStatus.CANCELLED,
                    attempt=attempt,
                    compact_response=compact_response,
                )
            try:
                async with asyncio.timeout(self.config.job_timeout_seconds):
                    response = await self.upstream.request(
                        request.endpoint, body, request.forwarded_headers
                    )
                if 200 <= response.status_code < 300:
                    await self._publish_response(
                        request,
                        token,
                        directory,
                        body=response.body,
                        http_status=response.status_code,
                        content_type=response.content_type,
                        headers=response.headers,
                        status=JobStatus.SUCCEEDED,
                        attempt=attempt,
                        compact_response=compact_response,
                    )
                    return JobStatus.SUCCEEDED
                error = UpstreamError(
                    f"upstream returned HTTP {response.status_code}",
                    status_code=response.status_code,
                    retryable=response.status_code in {408, 409, 425, 429}
                    or response.status_code >= 500,
                    response_body=response.body,
                )
                if not error.retryable or attempt == self.config.max_attempts:
                    await self._publish_response(
                        request,
                        token,
                        directory,
                        body=response.body,
                        http_status=response.status_code,
                        content_type=response.content_type,
                        headers=response.headers,
                        status=JobStatus.FAILED,
                        attempt=attempt,
                        failure=RelayFailure(
                            type="upstream_http_error",
                            message=str(error),
                            retryable=error.retryable,
                            attempt=attempt,
                        ),
                        compact_response=compact_response,
                    )
                    return JobStatus.FAILED
                last_error = error
            except TimeoutError:
                last_error = UpstreamError("upstream request timed out", retryable=True)
            except UpstreamError as exc:
                last_error = exc
                if not exc.retryable:
                    break
            if attempt < self.config.max_attempts:
                await asyncio.sleep(self.config.retry_base_seconds * (2 ** (attempt - 1)))
        assert last_error is not None
        return await self._publish_error(
            request,
            token,
            directory,
            error_type="upstream_unavailable",
            message=str(last_error),
            http_status=last_error.status_code or 502,
            retryable=last_error.retryable,
            attempt=self.config.max_attempts,
            body=last_error.response_body,
            compact_response=compact_response,
        )

    async def _process_stream(
        self,
        request: RelayRequest,
        body: dict[str, Any],
        token: LeaseToken,
        directory: Path,
        lost: asyncio.Event,
        *,
        compact_response: bool,
    ) -> JobStatus:
        body = dict(body)
        body["stream"] = True
        response_path = directory / "response.body"
        sequence = 0
        buffer = bytearray()
        last_flush = time.monotonic() - self.config.stream_flush_interval_seconds
        http_status = 200
        content_type = "text/event-stream"
        headers: dict[str, str] = {}
        try:
            async with asyncio.timeout(self.config.job_timeout_seconds):
                async with self.upstream.stream(
                    request.endpoint, body, request.forwarded_headers
                ) as response:
                    http_status = response.status_code
                    content_type = response.content_type
                    headers = response.headers
                    if not 200 <= http_status < 300:
                        error_body = bytearray()
                        async for chunk in response.chunks:
                            error_body.extend(chunk)
                        raise UpstreamError(
                            f"upstream returned HTTP {http_status}",
                            status_code=http_status,
                            retryable=http_status in {408, 409, 425, 429} or http_status >= 500,
                            response_body=bytes(error_body),
                        )
                    with response_path.open("wb") as output:
                        async for chunk in response.chunks:
                            if lost.is_set():
                                raise LeaseLostError(f"lease lost while streaming {request.job_id}")
                            output.write(chunk)
                            buffer.extend(chunk)
                            while len(buffer) >= self.config.stream_chunk_bytes:
                                await self._publish_stream_chunk(
                                    request.job_id,
                                    sequence,
                                    bytes(buffer[: self.config.stream_chunk_bytes]),
                                )
                                sequence += 1
                                del buffer[: self.config.stream_chunk_bytes]
                                last_flush = time.monotonic()
                            if (
                                buffer
                                and time.monotonic() - last_flush
                                >= self.config.stream_flush_interval_seconds
                            ):
                                await self._publish_stream_chunk(
                                    request.job_id, sequence, bytes(buffer)
                                )
                                sequence += 1
                                buffer.clear()
                                last_flush = time.monotonic()
                        if buffer:
                            await self._publish_stream_chunk(
                                request.job_id, sequence, bytes(buffer)
                            )
                            sequence += 1
                            buffer.clear()
            await self._publish_response_file(
                request,
                token,
                response_path,
                http_status=http_status,
                content_type=content_type,
                headers=headers,
                status=JobStatus.SUCCEEDED,
                attempt=1,
                stream_chunk_count=sequence,
                compact_response=compact_response,
            )
            return JobStatus.SUCCEEDED
        except (TimeoutError, UpstreamError) as exc:
            message = "upstream stream timed out" if isinstance(exc, TimeoutError) else str(exc)
            raw = getattr(exc, "response_body", None) or canonical_json_bytes(
                {"error": {"message": message, "type": "relay_stream_error"}}
            )
            sse = b"data: " + raw.replace(b"\n", b" ") + b"\n\ndata: [DONE]\n\n"
            await self._publish_stream_chunk(request.job_id, sequence, sse)
            sequence += 1
            response_path.write_bytes(sse)
            await self._publish_response_file(
                request,
                token,
                response_path,
                http_status=getattr(exc, "status_code", None) or 502,
                content_type="text/event-stream",
                headers={},
                status=JobStatus.FAILED,
                attempt=1,
                stream_chunk_count=sequence,
                failure=RelayFailure(
                    type="upstream_stream_error",
                    message=message[:8_192],
                    retryable=getattr(exc, "retryable", False),
                    attempt=1,
                ),
                compact_response=compact_response,
            )
            return JobStatus.FAILED

    async def _publish_stream_chunk(self, job_id: str, sequence: int, data: bytes) -> None:
        key = self.layout.stream_chunk(job_id, sequence)
        try:
            await asyncio.to_thread(
                self.store.put_bytes,
                key,
                data,
                content_type="text/event-stream",
                if_absent=True,
            )
        except ConditionalWriteFailed as exc:
            existing = await asyncio.to_thread(self.store.get_bytes, key)
            if existing != data:
                raise IntegrityError(f"stream chunk collision for {job_id}:{sequence}") from exc

    async def _publish_error(
        self,
        request: RelayRequest,
        token: LeaseToken,
        directory: Path,
        *,
        error_type: str,
        message: str,
        http_status: int,
        attempt: int,
        retryable: bool = False,
        job_status: JobStatus = JobStatus.FAILED,
        body: bytes | None = None,
        compact_response: bool = False,
    ) -> JobStatus:
        failure = RelayFailure(
            type=error_type,
            message=message[:8_192] or error_type,
            retryable=retryable,
            attempt=attempt,
        )
        response_body = body or canonical_json_bytes(
            {
                "error": {
                    "message": failure.message,
                    "type": failure.type,
                    "code": failure.type,
                }
            }
        )
        await self._publish_response(
            request,
            token,
            directory,
            body=response_body,
            http_status=max(100, min(599, http_status)),
            content_type="application/json",
            headers={},
            status=job_status,
            attempt=attempt,
            failure=failure,
            compact_response=compact_response,
        )
        return job_status

    async def _publish_response(
        self,
        request: RelayRequest,
        token: LeaseToken,
        directory: Path,
        *,
        body: bytes,
        http_status: int,
        content_type: str,
        headers: dict[str, str],
        status: JobStatus,
        attempt: int,
        failure: RelayFailure | None = None,
        compact_response: bool = False,
    ) -> None:
        response_path = directory / "response.body"
        response_path.write_bytes(body)
        await self._publish_response_file(
            request,
            token,
            response_path,
            http_status=http_status,
            content_type=content_type,
            headers=headers,
            status=status,
            attempt=attempt,
            failure=failure,
            compact_response=compact_response,
        )

    async def _publish_response_file(
        self,
        request: RelayRequest,
        token: LeaseToken,
        response_path: Path,
        *,
        http_status: int,
        content_type: str,
        headers: dict[str, str],
        status: JobStatus,
        attempt: int,
        stream_chunk_count: int | None = None,
        failure: RelayFailure | None = None,
        compact_response: bool = False,
    ) -> None:
        body_size = response_path.stat().st_size
        body_digest = await asyncio.to_thread(sha256_file, response_path)
        use_compact = (
            compact_response and body_size <= self.config.compact_response_max_bytes
        )
        body_key: str | None = None
        body_base64: str | None = None
        await asyncio.to_thread(self.leases.assert_owner, token)
        if use_compact:
            body_base64 = base64.b64encode(response_path.read_bytes()).decode("ascii")
        else:
            body_key = self.layout.response_body(request.job_id, token.generation)
            await asyncio.to_thread(self.store.upload_file, response_path, body_key)
            # Uploads can take minutes for large responses. Recheck fencing after
            # the transfer before publishing immutable terminal metadata.
            await asyncio.to_thread(self.leases.assert_owner, token)
        metadata = RelayResponse(
            job_id=request.job_id,
            target=request.target,
            worker_id=self.config.worker_id,
            status=status,
            created_at=request.created_at,
            attempt=attempt,
            http_status=http_status,
            content_type=content_type,
            body_object=body_key,
            body_base64=body_base64,
            body_sha256=body_digest,
            body_size_bytes=body_size,
            response_headers=headers,
            failure=failure,
            stream_chunk_count=stream_chunk_count,
        )
        metadata_data = canonical_json_bytes(metadata.model_dump(mode="json"))
        if not use_compact:
            metadata_key = self.layout.response_metadata(request.job_id)
            try:
                await asyncio.to_thread(
                    self.store.put_bytes,
                    metadata_key,
                    metadata_data,
                    content_type="application/json",
                    if_absent=True,
                )
            except ConditionalWriteFailed as exc:
                existing = await asyncio.to_thread(self.store.get_bytes, metadata_key)
                if existing != metadata_data:
                    raise IntegrityError(
                        f"response metadata collision for {request.job_id}"
                    ) from exc
        done = DoneMarker(
            job_id=request.job_id,
            status=status,
            response_sha256=sha256_bytes(metadata_data),
            response=metadata if use_compact else None,
            stream_chunk_count=stream_chunk_count,
        )
        try:
            await asyncio.to_thread(
                self.store.put_bytes,
                self.layout.done(request.job_id),
                canonical_json_bytes(done.model_dump(mode="json", exclude_none=True)),
                content_type="application/json",
                if_absent=True,
            )
        except ConditionalWriteFailed as exc:
            existing = DoneMarker.model_validate_json(
                await asyncio.to_thread(self.store.get_bytes, self.layout.done(request.job_id))
            )
            if existing.response_sha256 != done.response_sha256:
                raise IntegrityError(f"DONE marker collision for {request.job_id}") from exc
        if status != JobStatus.SUCCEEDED:
            with suppress(ConditionalWriteFailed):
                await asyncio.to_thread(
                    self.store.put_bytes,
                    self.layout.deadletter(request.target, request.job_id),
                    metadata_data,
                    content_type="application/json",
                    if_absent=True,
                )
