from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from .errors import ConditionalWriteFailed, LeaseLostError
from .layout import ObjectLayout
from .protocol import LeaseClaim, LeaseHeartbeat, canonical_json_bytes
from .storage import ObjectInfo, ObjectStore


@dataclass(frozen=True, slots=True)
class LeaseToken:
    job_id: str
    target: str
    worker_id: str
    generation: int
    lease_seconds: int


@dataclass(frozen=True, slots=True)
class LeaseState:
    claim: LeaseClaim
    last_server_activity: datetime

    def is_expired(self, now: datetime, *, grace_seconds: float = 0.0) -> bool:
        return now >= self.last_server_activity + timedelta(
            seconds=self.claim.lease_seconds + grace_seconds
        )


class LeaseManager:
    """Distributed lease using only immutable conditional S3 writes.

    The target object store supports ``If-None-Match`` but not conditional
    overwrite. Each takeover therefore creates a new immutable generation.
    Heartbeats are immutable timestamped objects beneath that generation.
    The highest live generation is the sole owner.
    """

    def __init__(
        self,
        store: ObjectStore,
        layout: ObjectLayout,
        *,
        clock_skew_grace_seconds: float = 5.0,
    ) -> None:
        self.store = store
        self.layout = layout
        self.clock_skew_grace_seconds = clock_skew_grace_seconds

    def _states(self, target: str, job_id: str) -> dict[int, LeaseState]:
        objects = self.store.list(self.layout.claim_job_prefix(target, job_id))
        claims: dict[int, tuple[LeaseClaim, ObjectInfo]] = {}
        for item in objects:
            generation = self.layout.parse_claim_generation(item.key, target, job_id)
            if generation is None:
                continue
            if item.key == self.layout.claim(target, job_id, generation):
                try:
                    claim = LeaseClaim.model_validate_json(
                        self.store.get_bytes(item.key, max_bytes=64 * 1024)
                    )
                except Exception:
                    # A malformed immutable claim is treated as occupied. It cannot be
                    # safely ignored because that could enable duplicate execution.
                    continue
                claims[generation] = (claim, item)
        activity: dict[int, datetime] = {}
        for item in objects:
            generation = self.layout.parse_claim_generation(item.key, target, job_id)
            claim_entry = claims.get(generation) if generation is not None else None
            if claim_entry is None:
                continue
            claim, _ = claim_entry
            heartbeat_prefix = (
                self.layout.claim_generation_prefix(target, job_id, generation) + "heartbeats/"
            )
            if not item.key.startswith(heartbeat_prefix) or not item.key.endswith(".json"):
                continue
            try:
                heartbeat = LeaseHeartbeat.model_validate_json(
                    self.store.get_bytes(item.key, max_bytes=64 * 1024)
                )
            except Exception:
                continue
            if (
                heartbeat.job_id != claim.job_id
                or heartbeat.worker_id != claim.worker_id
                or heartbeat.generation != claim.generation
                or heartbeat.lease_seconds != claim.lease_seconds
            ):
                continue
            modified = item.last_modified or heartbeat.created_at
            prior = activity.get(generation)
            if prior is None or modified > prior:
                activity[generation] = modified
        states: dict[int, LeaseState] = {}
        for generation, (claim, info) in claims.items():
            modified = activity.get(generation) or info.last_modified or claim.created_at
            states[generation] = LeaseState(claim=claim, last_server_activity=modified)
        return states

    def acquire(
        self,
        *,
        target: str,
        job_id: str,
        worker_id: str,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> LeaseToken | None:
        now = now or datetime.now(UTC)
        states = self._states(target, job_id)
        if states:
            highest = max(states)
            state = states[highest]
            if not state.is_expired(now, grace_seconds=self.clock_skew_grace_seconds):
                if state.claim.worker_id == worker_id:
                    return LeaseToken(job_id, target, worker_id, highest, state.claim.lease_seconds)
                return None
            generation = highest + 1
        else:
            generation = 0
        claim = LeaseClaim(
            job_id=job_id,
            target=target,
            worker_id=worker_id,
            generation=generation,
            lease_seconds=lease_seconds,
        )
        try:
            self.store.put_bytes(
                self.layout.claim(target, job_id, generation),
                canonical_json_bytes(claim.model_dump(mode="json")),
                content_type="application/json",
                if_absent=True,
            )
        except ConditionalWriteFailed:
            return None
        return LeaseToken(job_id, target, worker_id, generation, lease_seconds)

    def heartbeat(self, token: LeaseToken) -> None:
        states = self._states(token.target, token.job_id)
        if not states or max(states) != token.generation:
            raise LeaseLostError(f"lease generation was superseded for {token.job_id}")
        state = states[token.generation]
        if state.claim.worker_id != token.worker_id:
            raise LeaseLostError(f"lease owner changed for {token.job_id}")
        heartbeat = LeaseHeartbeat(
            job_id=token.job_id,
            worker_id=token.worker_id,
            generation=token.generation,
            lease_seconds=token.lease_seconds,
        )
        key = self.layout.claim_heartbeat(
            token.target,
            token.job_id,
            token.generation,
            time.time_ns(),
            token.worker_id,
        )
        self.store.put_bytes(
            key,
            canonical_json_bytes(heartbeat.model_dump(mode="json")),
            content_type="application/json",
            if_absent=True,
        )
        states = self._states(token.target, token.job_id)
        if not states or max(states) != token.generation:
            raise LeaseLostError(f"lease generation was superseded for {token.job_id}")

    def assert_owner(self, token: LeaseToken, *, now: datetime | None = None) -> None:
        now = now or datetime.now(UTC)
        objects = self.store.list(self.layout.claim_job_prefix(token.target, token.job_id))
        generations = {
            generation
            for item in objects
            if (
                generation := self.layout.parse_claim_generation(
                    item.key, token.target, token.job_id
                )
            )
            is not None
        }
        if not generations or max(generations) != token.generation:
            raise LeaseLostError(f"worker no longer owns {token.job_id}")
        claim_key = self.layout.claim(token.target, token.job_id, token.generation)
        if not any(item.key == claim_key for item in objects):
            raise LeaseLostError(f"worker no longer owns {token.job_id}")
        activity = [
            item.last_modified
            for item in objects
            if self.layout.parse_claim_generation(item.key, token.target, token.job_id)
            == token.generation
            and item.last_modified is not None
        ]
        if not activity:
            # Stores used by tests or adapters may not expose server timestamps.
            # Fall back to the fully validated representation in that case.
            states = self._states(token.target, token.job_id)
            state = states.get(token.generation)
            if state is None or state.claim.worker_id != token.worker_id:
                raise LeaseLostError(f"worker no longer owns {token.job_id}")
            last_activity = state.last_server_activity
        else:
            last_activity = max(activity)
        if now >= last_activity + timedelta(
            seconds=token.lease_seconds + self.clock_skew_grace_seconds
        ):
            raise LeaseLostError(f"lease expired for {token.job_id}")
