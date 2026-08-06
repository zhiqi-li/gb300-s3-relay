from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.background import BackgroundTask

from .client import RelayClient
from .config import GatewayConfig
from .errors import ConfigurationError, InvalidRequestError, RelayError
from .layout import ObjectLayout
from .logging_utils import log_event
from .metrics import RelayMetrics
from .protocol import JobStatus, WorkerHeartbeat
from .storage import ObjectStore

LOGGER = logging.getLogger(__name__)
_FORWARDED_REQUEST_HEADERS = frozenset({"openai-organization", "openai-project", "x-request-id"})


def _openai_error(message: str, *, error_type: str, code: str | None = None) -> dict[str, Any]:
    return {
        "error": {
            "message": message,
            "type": error_type,
            "param": None,
            "code": code or error_type,
        }
    }


class NoHealthyWorkerError(RelayError):
    pass


class TargetSelector:
    def __init__(
        self,
        store: ObjectStore,
        layout: ObjectLayout,
        config: GatewayConfig,
        *,
        cache_seconds: float = 2.0,
    ) -> None:
        self.store = store
        self.layout = layout
        self.config = config
        self.cache_seconds = cache_seconds
        self._lock = threading.Lock()
        self._cache_at = 0.0
        self._cache: dict[str, list[WorkerHeartbeat]] = {}
        self._refreshing = False
        self._round_robin = 0

    def _fetch_workers(self) -> dict[str, list[WorkerHeartbeat]]:
        grouped: dict[str, list[WorkerHeartbeat]] = {
            target: [] for target in self.config.targets
        }
        now = datetime.now(UTC)
        for item in self.store.list(self.layout.worker_prefix()):
            try:
                heartbeat = WorkerHeartbeat.model_validate_json(
                    self.store.get_bytes(item.key, max_bytes=64 * 1024)
                )
            except Exception:
                continue
            observed_at = item.last_modified or heartbeat.updated_at
            age = (now - observed_at).total_seconds()
            if (
                heartbeat.target in grouped
                and heartbeat.healthy
                and -30 <= age <= self.config.target_heartbeat_ttl_seconds
            ):
                grouped[heartbeat.target].append(heartbeat)
        return grouped

    def _refresh_in_background(self) -> None:
        try:
            grouped = self._fetch_workers()
        except Exception:
            log_event(LOGGER, logging.WARNING, "worker_cache_refresh_failed", exc_info=True)
        else:
            with self._lock:
                self._cache = grouped
                self._cache_at = time.monotonic()
        finally:
            with self._lock:
                self._refreshing = False

    def _workers(self, *, force: bool = False) -> dict[str, list[WorkerHeartbeat]]:
        now_monotonic = time.monotonic()
        with self._lock:
            is_fresh = now_monotonic - self._cache_at < self.cache_seconds
            if not force and is_fresh:
                return self._cache
            if not force and self._cache_at > 0:
                if not self._refreshing:
                    self._refreshing = True
                    threading.Thread(
                        target=self._refresh_in_background,
                        name="gb300-worker-cache-refresh",
                        daemon=True,
                    ).start()
                return self._cache
        grouped = self._fetch_workers()
        with self._lock:
            self._cache = grouped
            self._cache_at = time.monotonic()
            return grouped

    def select(self, *, requested: str | None = None, model: str | None = None) -> str:
        if requested is not None and requested not in self.config.targets:
            raise InvalidRequestError(f"unknown relay target: {requested}")
        workers = self._workers()
        candidates = (requested,) if requested else self.config.targets
        scored: list[tuple[float, int, str]] = []
        with self._lock:
            rr = self._round_robin
            self._round_robin += 1
        for index, target in enumerate(candidates):
            healthy = [
                item
                for item in workers.get(target, ())
                if not model or not item.models or model in item.models
            ]
            if not healthy:
                if self.config.require_healthy_worker:
                    continue
                scored.append((1.0, (index - rr) % max(1, len(candidates)), target))
                continue
            capacity = sum(item.max_concurrency for item in healthy)
            inflight = sum(item.inflight for item in healthy)
            utilization = inflight / capacity if capacity else 1.0
            scored.append((utilization, (index - rr) % max(1, len(candidates)), target))
        if not scored:
            raise NoHealthyWorkerError("no healthy GB300 worker is available")
        return min(scored)[2]

    def models(self) -> list[str]:
        values = {
            model
            for workers in self._workers().values()
            for heartbeat in workers
            for model in heartbeat.models
        }
        return sorted(values)

    def health(self) -> dict[str, int]:
        return {target: len(workers) for target, workers in self._workers(force=True).items()}


def create_app(
    store: ObjectStore,
    relay_client: RelayClient,
    config: GatewayConfig,
    *,
    prefix: str,
    metrics: RelayMetrics | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(
            max_workers=config.thread_pool_workers,
            thread_name_prefix="gb300-gateway-io",
        )
        loop.set_default_executor(executor)
        app.state.io_executor = executor
        try:
            yield
        finally:
            executor.shutdown(wait=True)

    app = FastAPI(title="GB300 S3 Relay", version="0.2.0", lifespan=lifespan)
    layout = ObjectLayout(prefix)
    selector = TargetSelector(store, layout, config)
    relay_metrics = metrics or RelayMetrics("gateway")
    expected_auth = os.environ.get(config.auth_token_env) if config.auth_token_env else None
    if config.auth_token_env and not expected_auth:
        raise ConfigurationError(
            f"gateway authentication variable is unset: {config.auth_token_env}"
        )

    def finalize_job(target: str, job_id: str, cleanup: bool) -> None:
        try:
            relay_client.acknowledge(job_id)
            if cleanup:
                relay_client.cleanup(target=target, job_id=job_id)
        except Exception:
            log_event(
                LOGGER,
                logging.WARNING,
                "finalize_failed",
                job_id=job_id,
                target=target,
                exc_info=True,
            )

    def authenticate(request: Request) -> Response | None:
        if expected_auth is None:
            return None
        supplied = request.headers.get("authorization", "")
        if not hmac.compare_digest(supplied, f"Bearer {expected_auth}"):
            return JSONResponse(
                _openai_error("invalid API key", error_type="authentication_error"),
                status_code=401,
            )
        return None

    @app.get("/healthz")
    async def healthz() -> Response:
        try:
            await asyncio.to_thread(store.ping)
        except Exception as exc:
            return JSONResponse({"status": "unhealthy", "error": str(exc)}, status_code=503)
        return JSONResponse({"status": "ok"})

    @app.get("/readyz")
    async def readyz() -> Response:
        try:
            health = await asyncio.to_thread(selector.health)
        except Exception as exc:
            return JSONResponse({"status": "unready", "error": str(exc)}, status_code=503)
        ready = any(health.values()) or not config.require_healthy_worker
        return JSONResponse(
            {"status": "ready" if ready else "unready", "workers": health},
            status_code=200 if ready else 503,
        )

    @app.get("/v1/models")
    async def models(request: Request) -> Response:
        denied = authenticate(request)
        if denied is not None:
            return denied
        values = await asyncio.to_thread(selector.models)
        now = int(time.time())
        return JSONResponse(
            {
                "object": "list",
                "data": [
                    {"id": model, "object": "model", "created": now, "owned_by": "gb300-relay"}
                    for model in values
                ],
            }
        )

    @app.post("/v1/{endpoint_path:path}")
    async def openai_proxy(endpoint_path: str, request: Request) -> Response:
        denied = authenticate(request)
        if denied is not None:
            return denied
        endpoint = f"/v1/{endpoint_path}"
        if endpoint not in config.allowed_endpoints:
            return JSONResponse(
                _openai_error(
                    f"endpoint is not enabled: {endpoint}",
                    error_type="invalid_request_error",
                ),
                status_code=404,
            )
        content_length = request.headers.get("content-length")
        maximum_json_bytes = config.max_json_body_bytes
        try:
            declared_length = int(content_length) if content_length else 0
        except ValueError:
            declared_length = maximum_json_bytes + 1
        if declared_length > maximum_json_bytes:
            return JSONResponse(
                _openai_error("request body is too large", error_type="invalid_request_error"),
                status_code=413,
            )
        raw = await request.body()
        if len(raw) > maximum_json_bytes:
            return JSONResponse(
                _openai_error("request body is too large", error_type="invalid_request_error"),
                status_code=413,
            )
        try:
            body = json.loads(raw)
            if not isinstance(body, dict):
                raise ValueError("body is not an object")
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            return JSONResponse(
                _openai_error(
                    "request body must be a JSON object", error_type="invalid_request_error"
                ),
                status_code=400,
            )
        timeout_header = request.headers.get("x-relay-timeout-seconds")
        try:
            timeout = float(timeout_header) if timeout_header else config.default_timeout_seconds
        except ValueError:
            timeout = -1
        if timeout <= 0 or timeout > config.max_timeout_seconds:
            return JSONResponse(
                _openai_error(
                    f"timeout must be between 0 and {config.max_timeout_seconds} seconds",
                    error_type="invalid_request_error",
                ),
                status_code=400,
            )
        requested_target = request.headers.get("x-gb300-target")
        model = body.get("model") if isinstance(body.get("model"), str) else None
        try:
            target = await asyncio.to_thread(
                selector.select, requested=requested_target, model=model
            )
            forwarded = {
                key: value
                for key, value in request.headers.items()
                if key.lower() in _FORWARDED_REQUEST_HEADERS
            }
            handle = await asyncio.to_thread(
                relay_client.submit,
                endpoint=endpoint,
                body=body,
                target=target,
                timeout_seconds=timeout,
                idempotency_key=request.headers.get("idempotency-key"),
                stream=bool(body.get("stream", False)),
                forwarded_headers=forwarded,
            )
        except NoHealthyWorkerError as exc:
            return JSONResponse(
                _openai_error(str(exc), error_type="service_unavailable"), status_code=503
            )
        except InvalidRequestError as exc:
            return JSONResponse(
                _openai_error(str(exc), error_type="invalid_request_error"), status_code=400
            )
        except Exception as exc:
            log_event(LOGGER, logging.ERROR, "submit_failed", endpoint=endpoint, exc_info=True)
            return JSONResponse(_openai_error(str(exc), error_type="relay_error"), status_code=502)

        started = time.monotonic()
        relay_metrics.requests.labels("gateway", target, endpoint, "submitted").inc()
        relay_metrics.inflight.labels("gateway", target).inc()
        common_headers = {
            "x-relay-job-id": handle.job_id,
            "x-relay-target": target,
            "x-request-id": handle.trace_id,
        }
        if body.get("stream"):
            cleanup_stream = config.cleanup_on_success and (
                not request.headers.get("idempotency-key") or config.cleanup_idempotent_on_success
            )

            async def stream_response():
                outcome = "completed"
                try:
                    async for chunk in relay_client.aiter_stream(
                        handle,
                        timeout_seconds=timeout,
                        cleanup=cleanup_stream,
                    ):
                        yield chunk
                except asyncio.CancelledError:
                    outcome = "disconnected"
                    await asyncio.to_thread(relay_client.cancel, handle.job_id)
                    raise
                except TimeoutError as exc:
                    outcome = "timeout"
                    await asyncio.to_thread(relay_client.cancel, handle.job_id)
                    error = json.dumps(_openai_error(str(exc), error_type="timeout_error"))
                    yield f"data: {error}\n\ndata: [DONE]\n\n".encode()
                except Exception as exc:
                    outcome = "failed"
                    error = json.dumps(_openai_error(str(exc), error_type="relay_error"))
                    yield f"data: {error}\n\ndata: [DONE]\n\n".encode()
                finally:
                    relay_metrics.inflight.labels("gateway", target).dec()
                    relay_metrics.requests.labels("gateway", target, endpoint, outcome).inc()
                    relay_metrics.latency.labels("gateway", target, endpoint).observe(
                        time.monotonic() - started
                    )

            return StreamingResponse(
                stream_response(), media_type="text/event-stream", headers=common_headers
            )
        try:
            completed = await asyncio.to_thread(
                relay_client.wait,
                handle,
                timeout_seconds=timeout,
                cleanup=False,
                acknowledge=False,
            )
        except TimeoutError as exc:
            await asyncio.to_thread(relay_client.cancel, handle.job_id)
            relay_metrics.requests.labels("gateway", target, endpoint, "timeout").inc()
            return JSONResponse(
                _openai_error(str(exc), error_type="timeout_error"),
                status_code=504,
                headers=common_headers,
            )
        except Exception as exc:
            log_event(
                LOGGER,
                logging.ERROR,
                "wait_failed",
                job_id=handle.job_id,
                target=target,
                exc_info=True,
            )
            relay_metrics.requests.labels("gateway", target, endpoint, "failed").inc()
            return JSONResponse(
                _openai_error(str(exc), error_type="relay_error"),
                status_code=502,
                headers=common_headers,
            )
        finally:
            relay_metrics.inflight.labels("gateway", target).dec()
            relay_metrics.latency.labels("gateway", target, endpoint).observe(
                time.monotonic() - started
            )
        response_headers = dict(common_headers)
        response_headers.update(completed.metadata.response_headers)
        cleanup_success = config.cleanup_on_success and (
            not request.headers.get("idempotency-key") or config.cleanup_idempotent_on_success
        )
        cleanup_job = cleanup_success and completed.metadata.status == JobStatus.SUCCEEDED
        outcome = completed.metadata.status.value.lower()
        relay_metrics.requests.labels("gateway", target, endpoint, outcome).inc()
        relay_metrics.bytes.labels("gateway", target, "download").inc(len(completed.body))
        return Response(
            content=completed.body,
            status_code=completed.metadata.http_status,
            media_type=completed.metadata.content_type.split(";", 1)[0],
            headers=response_headers,
            background=BackgroundTask(finalize_job, target, handle.job_id, cleanup_job),
        )

    return app
