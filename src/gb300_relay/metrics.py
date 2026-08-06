from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server


class RelayMetrics:
    def __init__(self, component: str) -> None:
        self.component = component
        self.registry = CollectorRegistry(auto_describe=True)
        labels = ("component", "target", "endpoint")
        self.requests = Counter(
            "gb300_relay_requests_total",
            "Relay requests observed",
            (*labels, "outcome"),
            registry=self.registry,
        )
        self.latency = Histogram(
            "gb300_relay_request_duration_seconds",
            "End-to-end or worker processing latency",
            labels,
            registry=self.registry,
            buckets=(0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 180, 600, 1800),
        )
        self.inflight = Gauge(
            "gb300_relay_inflight",
            "Currently in-flight relay jobs",
            ("component", "target"),
            registry=self.registry,
        )
        self.bytes = Counter(
            "gb300_relay_bytes_total",
            "Payload bytes transferred by direction",
            ("component", "target", "direction"),
            registry=self.registry,
        )
        self.claim_contention = Counter(
            "gb300_relay_claim_contention_total",
            "Jobs skipped because another worker owns the lease",
            ("component", "target"),
            registry=self.registry,
        )
        self.poll_errors = Counter(
            "gb300_relay_poll_errors_total",
            "Object-store polling failures",
            ("component", "target"),
            registry=self.registry,
        )

    def start_server(self, host: str, port: int) -> None:
        start_http_server(port, addr=host, registry=self.registry)
