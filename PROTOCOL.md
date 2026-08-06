# Relay object protocol v1

Every key is relative to the configured `<prefix>`.

```text
requests/<target>/<job>/assets/*
requests/<target>/<job>/manifest.json
requests/<target>/<job>/READY.json
claims/<target>/<job>/<generation>/claim.json
claims/<target>/<job>/<generation>/heartbeats/*.json
streams/<job>/<sequence>.sse
results/<job>/response-g<generation>.body
results/<job>/response.json
results/<job>/DONE.json
acks/<job>.json
cancellations/<job>.json
deadletter/<target>/<job>.json
workers/<target>/<worker>.json
```

## Visibility and commit markers

A worker consumes only a request with a valid `READY.json`. A client consumes only a response with a valid `DONE.json`. Each marker contains the SHA-256 digest of the preceding JSON object, so visibility of a valid marker represents a complete and verifiable stage commit.

The client uploads assets before the manifest and creates `READY.json` last. The worker uploads the response body and any stream chunks before response metadata and creates `DONE.json` last.

## Claims, takeover, and fencing

The target object store needs conditional create but does not need conditional overwrite. Leases therefore use immutable generations instead of in-place updates:

1. Initial execution attempts generation 0 with `If-None-Match: *`.
2. A live owner appends immutable heartbeat objects under that generation.
3. After server-observed activity exceeds the lease, a contender attempts generation `N + 1`.
4. The worker verifies that it still owns the highest generation immediately before publishing terminal metadata.
5. Response bodies use generation-specific keys so a stale worker cannot overwrite a newer large object.

The object store and the model call cannot form one exactly-once transaction. A process that loses its lease around an upstream call can cause duplicate inference. The public guarantee is at-least-once execution across lease expiry. Clients should provide stable idempotency keys, and upstreams should avoid non-idempotent side effects.

## Integrity and safety

- Manifests and response metadata use canonical JSON and SHA-256 digests.
- Assets and response bodies carry byte length and SHA-256 metadata.
- Downloads are written to a partial path and atomically renamed after transfer.
- Object path segments, filenames, endpoints, and forwarded headers are constrained.
- `Authorization`, `Cookie`, and `Proxy-Authorization` are never written to request manifests.
- Explicit relay credentials override ambient cloud credentials for both boto3 and s5cmd.

## Streaming

Stream objects use fixed-width, monotonically increasing sequence numbers. `DONE.json.stream_chunk_count` declares the final count. A client that observes `DONE.json` still reads every chunk in `[0, count)` before terminating.

The worker publishes the first upstream chunk immediately and then flushes when `stream_chunk_bytes` is reached or `stream_flush_interval_seconds` elapses. If an upstream fails after publishing bytes, the worker appends an OpenAI-style error event and `[DONE]`; it does not restart the stream.

## Cleanup

The gateway acknowledges a terminal response before cleanup. Normal successful jobs can be removed immediately. Idempotent successes and failures are retained according to policy. `gc` defaults to dry-run mode and, unless configured otherwise, only selects acknowledged terminal jobs.
