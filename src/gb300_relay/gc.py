from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from .client import RelayClient
from .config import RetentionConfig
from .layout import ObjectLayout
from .protocol import DoneMarker, JobStatus, RelayResponse
from .storage import ObjectStore


@dataclass(frozen=True, slots=True)
class GarbageCollectionReport:
    terminal_candidates: int
    terminal_jobs_removed: int
    objects_removed: int
    stale_worker_heartbeats: int
    errors: tuple[str, ...]


class GarbageCollector:
    """Conservative cleanup for acknowledged terminal jobs and stale heartbeats."""

    def __init__(
        self,
        store: ObjectStore,
        *,
        prefix: str,
        config: RetentionConfig,
        client_id: str = "relay-gc",
    ) -> None:
        self.store = store
        self.layout = ObjectLayout(prefix)
        self.config = config
        self.client = RelayClient(store, prefix=prefix, client_id=client_id)

    def collect(
        self, *, apply: bool = False, now: datetime | None = None
    ) -> GarbageCollectionReport:
        now = now or datetime.now(UTC)
        candidates: list[tuple[str, str]] = []
        errors: list[str] = []
        for item in self.store.list(self.layout.results_prefix()):
            job_id = self.layout.parse_done_key(item.key)
            if job_id is None:
                continue
            try:
                done = DoneMarker.model_validate_json(
                    self.store.get_bytes(item.key, max_bytes=24 * 1024**2)
                )
                metadata = done.response
                if metadata is None:
                    metadata = RelayResponse.model_validate_json(
                        self.store.get_bytes(
                            self.layout.response_metadata(job_id), max_bytes=4 * 1024**2
                        )
                    )
            except Exception as exc:
                errors.append(f"{job_id}: {exc}")
                continue
            completed_at = item.last_modified or done.completed_at
            age = max(0.0, (now - completed_at).total_seconds())
            threshold = (
                self.config.succeeded_seconds
                if done.status == JobStatus.SUCCEEDED
                else self.config.failed_seconds
            )
            if age < threshold:
                continue
            if (
                self.config.require_acknowledgement
                and self.store.head(self.layout.ack(job_id)) is None
            ):
                continue
            candidates.append((metadata.target, job_id))

        removed_jobs = 0
        removed_objects = 0
        if apply:
            for target, job_id in candidates:
                try:
                    removed_objects += self.client.cleanup(target=target, job_id=job_id)
                    removed_jobs += 1
                except Exception as exc:
                    errors.append(f"{job_id}: {exc}")

        stale_heartbeats = []
        for item in self.store.list(self.layout.worker_prefix()):
            observed_at = item.last_modified
            if observed_at is None:
                continue
            if (now - observed_at).total_seconds() >= self.config.stale_worker_heartbeat_seconds:
                stale_heartbeats.append(item.key)
        if apply and stale_heartbeats:
            self.store.delete_keys(stale_heartbeats)

        return GarbageCollectionReport(
            terminal_candidates=len(candidates),
            terminal_jobs_removed=removed_jobs,
            objects_removed=removed_objects,
            stale_worker_heartbeats=len(stale_heartbeats),
            errors=tuple(errors),
        )
