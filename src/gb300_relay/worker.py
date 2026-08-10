from __future__ import annotations

import asyncio
import base64
import binascii
import logging
import random
import tempfile
import time
from collections import Counter, OrderedDict, defaultdict, deque
from contextlib import asynccontextmanager, nullcontext, suppress
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
    request_fingerprint,
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


@dataclass(frozen=True, slots=True)
class QueuedJob:
    job_id: str
    producer_group: str | None


@dataclass(slots=True)
class _TransferTicket:
    producer_group: str | None
    size_bytes: int
    future: asyncio.Future[None]
    granted: bool = False


class FairTransferLimiter:
    """Global transfer slots allocated with byte-based deficit round-robin."""

    def __init__(self, concurrency: int, quantum_bytes: int) -> None:
        if concurrency < 1 or quantum_bytes < 1:
            raise ValueError("transfer concurrency and quantum must be positive")
        self.concurrency = concurrency
        self.quantum_bytes = quantum_bytes
        self._active = 0
        self._queues: dict[str | None, deque[_TransferTicket]] = {}
        self._groups: deque[str | None] = deque()
        self._deficits: dict[str | None, int] = {}

    @property
    def active(self) -> int:
        return self._active

    @property
    def pending(self) -> int:
        return sum(len(queue) for queue in self._queues.values())

    async def acquire(self, producer_group: str | None, size_bytes: int) -> None:
        loop = asyncio.get_running_loop()
        ticket = _TransferTicket(
            producer_group=producer_group,
            size_bytes=max(1, size_bytes),
            future=loop.create_future(),
        )
        queue = self._queues.get(producer_group)
        if queue is None:
            queue = deque()
            self._queues[producer_group] = queue
            self._groups.append(producer_group)
            self._deficits[producer_group] = 0
        queue.append(ticket)
        self._dispatch()
        try:
            await ticket.future
        except asyncio.CancelledError:
            if ticket.granted:
                # Cancellation can land after dispatch grants a slot but before
                # the waiting task resumes. Return that slot instead of leaking it.
                self.release()
            else:
                if not ticket.future.done():
                    ticket.future.cancel()
                queue = self._queues.get(producer_group)
                if queue is not None:
                    with suppress(ValueError):
                        queue.remove(ticket)
                self._dispatch()
            raise

    def release(self) -> None:
        if self._active < 1:
            raise RuntimeError("transfer limiter released without an active slot")
        self._active -= 1
        self._dispatch()

    @asynccontextmanager
    async def slot(self, producer_group: str | None, size_bytes: int):
        await self.acquire(producer_group, size_bytes)
        try:
            yield
        finally:
            self.release()

    def _dispatch(self) -> None:
        while self._active < self.concurrency and self._groups:
            producer_group = self._groups.popleft()
            queue = self._queues.get(producer_group)
            if queue is None:
                continue
            while queue and queue[0].future.cancelled():
                queue.popleft()
            if not queue:
                self._queues.pop(producer_group, None)
                self._deficits.pop(producer_group, None)
                continue
            self._deficits[producer_group] += self.quantum_bytes
            ticket = queue[0]
            if ticket.size_bytes > self._deficits[producer_group]:
                self._groups.append(producer_group)
                continue
            queue.popleft()
            self._deficits[producer_group] -= ticket.size_bytes
            self._active += 1
            if queue:
                self._groups.append(producer_group)
            else:
                self._queues.pop(producer_group, None)
                self._deficits.pop(producer_group, None)
            ticket.granted = True
            if not ticket.future.done():
                ticket.future.set_result(None)


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
        self._asset_transfers = FairTransferLimiter(
            config.asset_transfer_concurrency,
            config.asset_fairness_quantum_bytes,
        )
        self._inflight: dict[str, asyncio.Task[ProcessOutcome]] = {}
        self._inflight_groups: dict[str, str | None] = {}
        self._terminal_jobs: OrderedDict[str, None] = OrderedDict()
        self._group_cursor = 0
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    def ready_entries(self) -> list[QueuedJob]:
        """Discover queue markers and interleave producer hardware groups fairly."""

        entries = {
            QueuedJob(job_id=job_id, producer_group=producer_group)
            for item in self.store.list(self.layout.grouped_ready_prefix(self.config.target))
            if (parsed := self.layout.parse_grouped_ready_key(item.key, self.config.target))
            is not None
            for producer_group, job_id in (parsed,)
        }
        if self.config.scan_legacy_ready:
            entries.update(
                QueuedJob(job_id=job_id, producer_group=None)
                for item in self.store.list(self.layout.target_ready_prefix(self.config.target))
                if (job_id := self.layout.parse_ready_key(item.key, self.config.target)) is not None
            )
        return self._fair_order(entries)

    def ready_jobs(self) -> list[str]:
        """Backward-compatible view of currently queued job identifiers."""

        return [entry.job_id for entry in self.ready_entries()]

    def _fair_order(self, entries: set[QueuedJob]) -> list[QueuedJob]:
        grouped: dict[str | None, deque[QueuedJob]] = defaultdict(deque)
        for entry in sorted(
            entries,
            key=lambda item: (item.producer_group or "", item.job_id),
        ):
            grouped[entry.producer_group].append(entry)
        groups = sorted(grouped, key=lambda item: item or "")
        if not groups:
            return []
        offset = self._group_cursor % len(groups)
        groups = groups[offset:] + groups[:offset]
        self._group_cursor = (self._group_cursor + 1) % len(groups)
        ordered: list[QueuedJob] = []
        while groups:
            active: list[str | None] = []
            for producer_group in groups:
                ordered.append(grouped[producer_group].popleft())
                if grouped[producer_group]:
                    active.append(producer_group)
            groups = active
        return ordered

    def _remember_terminal(self, job_id: str) -> None:
        if self.config.terminal_cache_size == 0:
            return
        self._terminal_jobs.pop(job_id, None)
        self._terminal_jobs[job_id] = None
        while len(self._terminal_jobs) > self.config.terminal_cache_size:
            self._terminal_jobs.popitem(last=False)

    def _ready_key(self, job_id: str, producer_group: str | None) -> str:
        if producer_group is None:
            return self.layout.ready(self.config.target, job_id)
        return self.layout.grouped_ready(self.config.target, producer_group, job_id)

    def _is_cancelled(self, job_id: str) -> bool:
        return self.store.head(self.layout.cancel(job_id)) is not None

    def _is_ready(self, job_id: str, producer_group: str | None = None) -> bool:
        return self.store.head(self._ready_key(job_id, producer_group)) is not None

    def _is_done(self, job_id: str) -> bool:
        return self.store.head(self.layout.done(job_id)) is not None

    async def _pending_entries(self, entries: list[QueuedJob]) -> list[QueuedJob]:
        candidates = [
            entry
            for entry in entries
            if entry.job_id not in self._inflight and entry.job_id not in self._terminal_jobs
        ]
        if not candidates:
            return []
        done_flags = await asyncio.gather(
            *(asyncio.to_thread(self._is_done, entry.job_id) for entry in candidates),
            return_exceptions=True,
        )
        pending: list[QueuedJob] = []
        for entry, done in zip(candidates, done_flags, strict=True):
            if done is True:
                self._remember_terminal(entry.job_id)
            elif done is False:
                pending.append(entry)
            else:
                self.metrics.poll_errors.labels("worker", self.config.target).inc()
        return pending

    def _select_entries(self, entries: list[QueuedJob], available: int) -> list[QueuedJob]:
        if available <= 0:
            return []
        limit = self.config.max_concurrency_per_producer or self.config.max_concurrency
        limit = min(limit, self.config.max_concurrency)
        group_counts: Counter[str | None] = Counter(self._inflight_groups.values())
        selected: list[QueuedJob] = []
        for entry in entries:
            if len(selected) >= available:
                break
            if group_counts[entry.producer_group] >= limit:
                continue
            selected.append(entry)
            group_counts[entry.producer_group] += 1
        return selected

    async def run_forever(self) -> None:
        heartbeat = asyncio.create_task(self._worker_heartbeat_loop(), name="worker-heartbeat")
        try:
            while not self._stop.is_set():
                self._reap_tasks()
                available = self.config.max_concurrency - len(self._inflight)
                if available > 0:
                    try:
                        entries = await asyncio.to_thread(self.ready_entries)
                    except Exception:
                        self.metrics.poll_errors.labels("worker", self.config.target).inc()
                        log_event(
                            LOGGER,
                            logging.ERROR,
                            "ready_poll_failed",
                            target=self.config.target,
                            exc_info=True,
                        )
                        entries = []
                    pending = await self._pending_entries(entries)
                    for entry in self._select_entries(pending, available):
                        task = asyncio.create_task(
                            self.process(
                                entry.job_id,
                                producer_group=entry.producer_group,
                                discovered=True,
                            ),
                            name=f"job:{entry.job_id}",
                        )
                        self._inflight[entry.job_id] = task
                        self._inflight_groups[entry.job_id] = entry.producer_group
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
            entries = await asyncio.to_thread(self.ready_entries)
            pending = await self._pending_entries(entries)
            batch = self._select_entries(pending, self.config.max_concurrency)
            if not batch:
                return outcomes
            outcomes.extend(
                await asyncio.gather(
                    *(
                        self.process(
                            entry.job_id,
                            producer_group=entry.producer_group,
                            discovered=True,
                        )
                        for entry in batch
                    )
                )
            )

    def _reap_tasks(self) -> None:
        for job_id, task in list(self._inflight.items()):
            if not task.done():
                continue
            del self._inflight[job_id]
            self._inflight_groups.pop(job_id, None)
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

    async def _load_request(
        self, job_id: str, producer_group: str | None
    ) -> tuple[ReadyMarker, RelayRequest]:
        ready_data = await asyncio.to_thread(
            self.store.get_bytes,
            self._ready_key(job_id, producer_group),
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
        if (
            ready.job_id != job_id
            or ready.target != self.config.target
            or ready.producer_group != producer_group
            or request.job_id != job_id
            or request.target != self.config.target
            or request.producer_group != producer_group
        ):
            raise IntegrityError(f"manifest identity mismatch for {job_id}")
        if request.endpoint not in self.config.allowed_endpoints:
            raise InvalidRequestError(f"endpoint is not allowed: {request.endpoint}")
        return ready, request

    async def _download_assets(self, request: RelayRequest, directory: Path) -> dict[str, Path]:
        assets_directory = directory / "assets"
        assets_directory.mkdir(parents=True, exist_ok=True)
        asset_paths: dict[str, Path] = {}
        request_semaphore = asyncio.Semaphore(self.config.asset_transfer_concurrency)

        async def download(descriptor) -> None:
            path = assets_directory / descriptor.filename
            if self.config.asset_transfer_scope == "worker":
                transfer_slot = self._asset_transfers.slot(
                    request.producer_group,
                    descriptor.size_bytes,
                )
            else:
                transfer_slot = request_semaphore
            async with transfer_slot:
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
        return asset_paths

    async def process(
        self,
        job_id: str,
        *,
        producer_group: str | None = None,
        discovered: bool = False,
    ) -> ProcessOutcome:
        # LIST results can briefly outlive a concurrently deleted READY marker.
        # Always confirm the commit marker before creating a lease generation.
        if not await asyncio.to_thread(self._is_ready, job_id, producer_group):
            return ProcessOutcome(job_id, handled=False)
        if not discovered and await asyncio.to_thread(self._is_done, job_id):
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
        if not await asyncio.to_thread(self._is_ready, job_id, producer_group):
            await asyncio.to_thread(
                self.store.delete_prefix,
                self.layout.claim_generation_prefix(token.target, token.job_id, token.generation),
            )
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
                ready, request = await self._load_request(job_id, producer_group)
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
                if request.stream and await asyncio.to_thread(self._is_cancelled, job_id):
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
                is_heavy = (
                    bool(request.assets)
                    or len(canonical_json_bytes(request.body))
                    >= self.config.heavy_request_threshold_bytes
                )
                limiter = self._heavy_slots if is_heavy else nullcontext()
                async with limiter:
                    assets = await self._download_assets(request, directory)
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
                    producer_group=producer_group,
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
                producer_group=producer_group,
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
        use_compact = compact_response and body_size <= self.config.compact_response_max_bytes
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
            producer_group=request.producer_group,
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
            producer_group=request.producer_group,
            request_fingerprint=request_fingerprint(request),
            request_created_at=request.created_at,
            request_expires_at=request.expires_at,
            trace_id=request.trace_id,
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
        self._remember_terminal(request.job_id)
        if status != JobStatus.SUCCEEDED:
            with suppress(ConditionalWriteFailed):
                await asyncio.to_thread(
                    self.store.put_bytes,
                    self.layout.deadletter(request.target, request.job_id),
                    metadata_data,
                    content_type="application/json",
                    if_absent=True,
                )
        if request.producer_group is not None:
            try:
                await asyncio.to_thread(
                    self.store.delete_keys,
                    (
                        self.layout.grouped_ready(
                            request.target,
                            request.producer_group,
                            request.job_id,
                        ),
                    ),
                )
            except Exception:
                log_event(
                    LOGGER,
                    logging.WARNING,
                    "queue_marker_cleanup_failed",
                    job_id=request.job_id,
                    target=request.target,
                    producer_group=request.producer_group,
                    exc_info=True,
                )
