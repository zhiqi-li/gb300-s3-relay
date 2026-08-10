from __future__ import annotations

import re
from dataclasses import dataclass

from .errors import InvalidRequestError

_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,255}$")


def _segment(value: str, name: str) -> str:
    if not _SAFE_SEGMENT.fullmatch(value):
        raise InvalidRequestError(f"unsafe {name}: {value!r}")
    return value


@dataclass(frozen=True, slots=True)
class ObjectLayout:
    """Canonical object names for one relay namespace."""

    prefix: str

    def __post_init__(self) -> None:
        normalized = self.prefix.strip("/")
        if not normalized or any(part in {"", ".", ".."} for part in normalized.split("/")):
            raise InvalidRequestError("unsafe relay prefix")
        object.__setattr__(self, "prefix", normalized)

    def request_prefix(self, target: str, job_id: str) -> str:
        return f"{self.prefix}/requests/{_segment(target, 'target')}/{_segment(job_id, 'job_id')}"

    def manifest(self, target: str, job_id: str) -> str:
        return f"{self.request_prefix(target, job_id)}/manifest.json"

    def asset(self, target: str, job_id: str, object_name: str) -> str:
        if not object_name.startswith("assets/"):
            raise InvalidRequestError("asset object_name must start with assets/")
        tail = object_name.removeprefix("assets/")
        return f"{self.request_prefix(target, job_id)}/assets/{_segment(tail, 'asset name')}"

    def ready(self, target: str, job_id: str) -> str:
        return f"{self.request_prefix(target, job_id)}/READY.json"

    def grouped_ready(self, target: str, producer_group: str, job_id: str) -> str:
        """A shallow, producer-partitioned request commit marker."""

        return (
            f"{self.grouped_ready_prefix(target, producer_group)}"
            f"{_segment(job_id, 'job_id')}.json"
        )

    def grouped_ready_prefix(self, target: str, producer_group: str | None = None) -> str:
        prefix = f"{self.prefix}/queue/{_segment(target, 'target')}/"
        if producer_group is None:
            return prefix
        return f"{prefix}{_segment(producer_group, 'producer_group')}/"

    def target_ready_prefix(self, target: str) -> str:
        return f"{self.prefix}/requests/{_segment(target, 'target')}/"

    def result_prefix(self, job_id: str) -> str:
        return f"{self.prefix}/results/{_segment(job_id, 'job_id')}"

    def results_prefix(self) -> str:
        return f"{self.prefix}/results/"

    def response_body(self, job_id: str, generation: int | None = None) -> str:
        suffix = "response.body" if generation is None else f"response-g{generation:08d}.body"
        return f"{self.result_prefix(job_id)}/{suffix}"

    def response_metadata(self, job_id: str) -> str:
        return f"{self.result_prefix(job_id)}/response.json"

    def done(self, job_id: str) -> str:
        return f"{self.result_prefix(job_id)}/DONE.json"

    def stream_prefix(self, job_id: str) -> str:
        return f"{self.prefix}/streams/{_segment(job_id, 'job_id')}/"

    def stream_chunk(self, job_id: str, sequence: int) -> str:
        if sequence < 0:
            raise InvalidRequestError("stream sequence cannot be negative")
        return f"{self.stream_prefix(job_id)}{sequence:012d}.sse"

    def cancel(self, job_id: str) -> str:
        return f"{self.prefix}/cancellations/{_segment(job_id, 'job_id')}.json"

    def deadletter(self, target: str, job_id: str) -> str:
        return (
            f"{self.prefix}/deadletter/{_segment(target, 'target')}/"
            f"{_segment(job_id, 'job_id')}.json"
        )

    def ack(self, job_id: str) -> str:
        return f"{self.prefix}/acks/{_segment(job_id, 'job_id')}.json"

    def claim_job_prefix(self, target: str, job_id: str) -> str:
        return f"{self.prefix}/claims/{_segment(target, 'target')}/{_segment(job_id, 'job_id')}/"

    def claim_generation_prefix(self, target: str, job_id: str, generation: int) -> str:
        if generation < 0:
            raise InvalidRequestError("claim generation cannot be negative")
        return f"{self.claim_job_prefix(target, job_id)}{generation:08d}/"

    def claim(self, target: str, job_id: str, generation: int) -> str:
        return f"{self.claim_generation_prefix(target, job_id, generation)}claim.json"

    def claim_heartbeat(
        self,
        target: str,
        job_id: str,
        generation: int,
        timestamp_ns: int,
        worker_id: str,
    ) -> str:
        if timestamp_ns < 0:
            raise InvalidRequestError("heartbeat timestamp cannot be negative")
        return (
            f"{self.claim_generation_prefix(target, job_id, generation)}heartbeats/"
            f"{timestamp_ns:020d}-{_segment(worker_id, 'worker_id')}.json"
        )

    def worker_heartbeat(self, target: str, worker_id: str) -> str:
        return (
            f"{self.prefix}/workers/{_segment(target, 'target')}/"
            f"{_segment(worker_id, 'worker_id')}.json"
        )

    def worker_prefix(self, target: str | None = None) -> str:
        if target is None:
            return f"{self.prefix}/workers/"
        return f"{self.prefix}/workers/{_segment(target, 'target')}/"

    def job_cleanup_prefixes(self, target: str, job_id: str) -> tuple[str, ...]:
        return (
            self.request_prefix(target, job_id) + "/",
            self.result_prefix(job_id) + "/",
            self.stream_prefix(job_id),
            self.claim_job_prefix(target, job_id),
        )

    def parse_ready_key(self, key: str, target: str) -> str | None:
        prefix = self.target_ready_prefix(target)
        if not key.startswith(prefix) or not key.endswith("/READY.json"):
            return None
        middle = key[len(prefix) : -len("/READY.json")]
        if "/" in middle or not _SAFE_SEGMENT.fullmatch(middle):
            return None
        return middle

    def parse_grouped_ready_key(self, key: str, target: str) -> tuple[str, str] | None:
        prefix = self.grouped_ready_prefix(target)
        if not key.startswith(prefix) or not key.endswith(".json"):
            return None
        tail = key[len(prefix) : -len(".json")]
        parts = tail.split("/")
        if len(parts) != 2 or any(not _SAFE_SEGMENT.fullmatch(part) for part in parts):
            return None
        return parts[0], parts[1]

    def parse_claim_generation(self, key: str, target: str, job_id: str) -> int | None:
        prefix = self.claim_job_prefix(target, job_id)
        if not key.startswith(prefix):
            return None
        tail = key[len(prefix) :]
        first = tail.split("/", 1)[0]
        if len(first) != 8 or not first.isdigit():
            return None
        return int(first)

    def parse_done_key(self, key: str) -> str | None:
        prefix = self.results_prefix()
        if not key.startswith(prefix) or not key.endswith("/DONE.json"):
            return None
        middle = key[len(prefix) : -len("/DONE.json")]
        if "/" in middle or not _SAFE_SEGMENT.fullmatch(middle):
            return None
        return middle
