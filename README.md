# GB300 S3 Relay

An S3-backed, OpenAI-compatible relay for environments where application workloads cannot connect directly to GB300 inference hosts.

The repository contains both sides of the system:

- an OpenAI-compatible HTTP gateway for the client/OSMO side;
- optional `OpenAI` and `AsyncOpenAI` Python client factories;
- a bounded-concurrency worker for each GB300 host;
- text, image, video, audio, and generic file transport;
- idempotency, SHA-256 integrity checks, immutable leases, retries, cancellation, acknowledgement, dead letters, garbage collection, JSON logs, and Prometheus metrics.

```text
existing OpenAI client
        | HTTP /v1/*
        v
client-side gateway -- request + assets --> S3 <-- poll/claim -- GB300 worker
client-side gateway <-- result/SSE chunks -- S3 <-- local HTTP -- model server
```

The client and GB300 hosts do not need network routes to one another. Both sides only need access to the same S3-compatible endpoint.

## Performance modes

The gateway template enables the compact protocol. After assets are uploaded, requests whose
manifest is at most `compact_manifest_max_bytes` commit as one self-contained `READY.json`.
Responses up to the worker's `compact_response_max_bytes` commit as one self-contained
`DONE.json`. Larger payloads transparently retain the full manifest/body/metadata sequence and
use s5cmd where appropriate. This removes process startup and several object-store round trips
from the latency-sensitive text and small-JSON path without reducing large-media capacity.

Files no larger than `native_transfer_max_bytes` use the persistent boto3 connection; larger
files use s5cmd. The storage doctor explicitly forces one s5cmd round trip, so its
`s5cmd_round_trip=ok` result continues to validate both data paths.

Worker admission has two limits. `max_concurrency` controls all in-flight jobs, while
`max_heavy_concurrency` separately bounds requests that contain assets or whose serialized body
exceeds `heavy_request_threshold_bytes`. This permits high short-text concurrency without
allowing a burst of long-context or multimodal requests to exhaust KV cache.
Asset download and SHA-256 work happens inside that heavy-request bound. By default,
`asset_transfer_concurrency` is also one worker-wide limit rather than a fresh limit for every
request. Waiting transfers are admitted across `producer_group` hardware groups using their file
sizes and `asset_fairness_quantum_bytes`, so a producer sending large videos cannot indefinitely
hold back another producer's small images. `asset_transfer_scope = "request"` retains the old
per-request behavior for rollback and A/B testing, but its effective host concurrency can grow to
`max_heavy_concurrency * asset_transfer_concurrency`.
For the common one-image-plus-one-video shape, a useful starting point is
`asset_transfer_concurrency = 2 * max_heavy_concurrency` (the supplied worker templates use
16 and 8 respectively). Lower the global value only when object-store or local disk telemetry
shows saturation.
`thread_pool_workers` sizes the blocking object-store I/O executor independently of model
admission; keep it at least as large as both `max_concurrency` and
`asset_transfer_concurrency` for bursty workloads.

## Quick start

Install the same repository on the client/OSMO side and on each GB300 host:

```bash
git clone <repository-url> gb300-s3-relay
cd gb300-s3-relay
./scripts/bootstrap.sh
```

Keep credentials on the deployment host and never commit them. The example configurations expect a dedicated shared-credentials file such as:

```ini
[gb300-relay]
aws_access_key_id = ...
aws_secret_access_key = ...
```

Copy and edit the files in [`config/`](config). Set the bucket, endpoint, profile, credentials path, model-server URL, and advertised model IDs for your environment.

On each GB300 host, first confirm that its local vLLM, SGLang, or other OpenAI-compatible server is running, then start one worker with a unique target name:

```bash
# GB300 host 1
.venv/bin/gb300-relay doctor --config config/gb300-1.toml
.venv/bin/gb300-relay worker --config config/gb300-1.toml

# GB300 host 2
.venv/bin/gb300-relay doctor --config config/gb300-2.toml
.venv/bin/gb300-relay worker --config config/gb300-2.toml
```

Start the gateway inside the client or OSMO environment:

```bash
unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy
.venv/bin/gb300-relay doctor --config config/osmo-gateway.toml
.venv/bin/gb300-relay gateway --config config/osmo-gateway.toml
```

Existing OpenAI applications normally require only environment changes:

```bash
export OPENAI_BASE_URL=http://127.0.0.1:8080/v1
export OPENAI_API_KEY=relay-local
python existing_app.py
```

### Local SDK demo

With the persistent GB300 workers already running, start one gateway on the local or OSMO
machine. The configuration file must point at the same S3 bucket and prefix as the workers:

```bash
# Terminal 1
.venv/bin/gb300-relay gateway --config /secure/path/osmo-gateway.toml
```

Run the self-contained demo from another terminal. It uses the official OpenAI Python SDK and
prints the model response plus the selected relay target and job ID:

```bash
# Terminal 2
export OPENAI_BASE_URL=http://127.0.0.1:8080/v1
export OPENAI_API_KEY=relay-local

.venv/bin/python examples/local_demo.py --model your-model
```

Omit `--target` to load-balance across healthy workers, or force one GB300 host and enable
streaming:

```bash
.venv/bin/python examples/local_demo.py \
  --model your-model \
  --target gb300-1 \
  --stream
```

If each worker advertises its supported model IDs through `worker.models`, `--model` can be
omitted. Run `examples/local_demo.py --help` for prompt, timeout, idempotency, and Qwen thinking
options. The API key above is only a local placeholder unless gateway authentication is enabled;
when `gateway.auth_token_env` is configured, set `OPENAI_API_KEY` to the matching token.

Or use the package factory explicitly:

```python
from gb300_relay import OpenAI

client = OpenAI()
response = client.chat.completions.create(
    model="your-model",
    messages=[{"role": "user", "content": "Hello"}],
)
print(response.choices[0].message.content)
```

Standard `stream=True` calls are supported. The worker publishes the first upstream chunk immediately, then flushes by size or at a configurable interval; the gateway reconstructs the byte stream as SSE. See [`examples/`](examples) for complete calls.

## Routing, deadlines, and idempotency

The gateway selects a target using the aggregate `inflight / max_concurrency` reported by healthy workers. OpenAI clients can override routing and relay behavior through `extra_headers`:

```python
response = client.chat.completions.create(
    model="your-model",
    messages=[{"role": "user", "content": "Hello"}],
    extra_headers={
        "x-gb300-target": "gb300-2",
        "x-gb300-producer-group": "osmo-node-17",
        "x-relay-timeout-seconds": "1800",
        "idempotency-key": "dataset-row-000042",
    },
)
```

- `x-gb300-target` pins a call to one configured target. Without it, the gateway load-balances across healthy workers.
- `x-gb300-producer-group` identifies the OSMO hardware node. The gateway's stable `producer_group` is used when the header is omitted.
- `x-relay-timeout-seconds` sets the end-to-end deadline.
- `idempotency-key` executes an identical request once within its retention window. Its namespace includes the producer group, so different hardware nodes may reuse the same row ID safely.
- Responses include `x-relay-job-id`, `x-relay-target`, `x-relay-producer-group`, and `x-request-id` headers.

Grouped requests use shallow queue objects at `queue/<target>/<producer-group>/<job-id>.json`.
The worker round-robins non-empty groups before borrowing unused capacity, and
`max_concurrency_per_producer` can add a hard per-group ceiling. Completed grouped queue markers
are removed after `DONE.json` is committed; the terminal request fingerprint preserves
idempotency checks without leaving the active queue to grow indefinitely.

## Images and video

The gateway recursively inspects OpenAI-style `image_url`, `video_url`, `audio_url`, `file_url`, and `url` fields. By default, base64 `data:` URLs are extracted into separate objects rather than embedded in the control manifest.

```python
from base64 import b64encode
from pathlib import Path

from gb300_relay import OpenAI


def data_url(path: str, media_type: str) -> str:
    encoded = b64encode(Path(path).read_bytes()).decode("ascii")
    return f"data:{media_type};base64,{encoded}"


client = OpenAI()
result = client.chat.completions.create(
    model="your-vlm",
    messages=[
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "Summarize the video and describe the image."},
                {
                    "type": "image_url",
                    "image_url": {"url": data_url("frame.png", "image/png")},
                },
                {
                    "type": "video_url",
                    "video_url": {"url": data_url("clip.mp4", "video/mp4")},
                },
            ],
        }
    ],
)
```

For large videos, enable `file://` inputs only for explicit roots using `allow_file_urls` and `allowed_file_roots` in the gateway media policy. The worker defaults to restoring small images as data URLs and videos or large files as worker-local `file://` URLs. The local model server must support the selected delivery format and must be able to read the worker's temporary directory. Run both services under a compatible account or group and without separate private `/tmp` namespaces, or set `media_delivery = "data_uri"` when the model requires inline media. The supplied worker systemd unit keeps the host temporary namespace visible for this reason.

## Lifecycle and delivery guarantees

In the standard path, submission order is assets, `manifest.json`, then the grouped queue marker, and completion order is response body or stream chunks, `response.json`, then `DONE.json`. In the compact path, the manifest is embedded in the grouped queue marker and a small response is embedded in `DONE.json`; those immutable objects remain the respective commit points. Legacy `requests/<target>/<job-id>/READY.json` jobs remain readable while `worker.scan_legacy_ready` is enabled. Contended writes use `If-None-Match: *`, and requests, assets, and responses carry size and SHA-256 metadata.

- During a live lease, only one worker calls the model for a job.
- A new immutable lease generation can take over after worker failure. Execution across lease expiry is therefore **at least once**, not exactly once. Use stable idempotency keys for costly calls and avoid non-idempotent upstream side effects.
- Ordinary successful jobs are acknowledged and deleted after gateway delivery.
- Successful jobs with idempotency keys are retained by default so client retries can reuse the result.
- Failures and dead letters are retained for diagnosis.
- A stream is not restarted after it has published any bytes, avoiding duplicate tokens.

Preview and execute conservative garbage collection:

```bash
.venv/bin/gb300-relay gc --config config/osmo-gateway.toml
.venv/bin/gb300-relay gc --config config/osmo-gateway.toml --apply
```

GC removes only terminal jobs past their retention period and, by default, only after acknowledgement. Configure a bucket lifecycle rule as a final backstop for orphaned, unacknowledged uploads.

## Health and observability

```bash
.venv/bin/gb300-relay doctor --config config/osmo-gateway.toml
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
curl -fsS http://127.0.0.1:9108/metrics
```

Logs are JSON by default. Fields whose names contain `token`, `secret`, `password`, `api-key`, or `authorization` are recursively redacted. The gateway listens on loopback by default. A non-loopback bind is rejected unless `auth_token_env` is configured.

Run a deployment smoke test with the official OpenAI Python SDK:

```bash
.venv/bin/python scripts/smoke-openai.py \
  --base-url http://127.0.0.1:8080/v1 \
  --model your-model \
  --target gb300-1 \
  --target gb300-2
```

Add `--image frame.jpg`, `--video clip.mp4`, or both when running the multimodal check. Select
one check with `--mode text`, `idempotency`, `stream`, or `multimodal`. The script prints only
a machine-readable summary and never prints model content or credentials. For a Qwen deployment
that defaults to reasoning mode, add `--disable-thinking` to exercise visible response content.

## Deployment and integration references

- [`config/osmo-gateway.toml`](config/osmo-gateway.toml): client-side gateway template.
- [`config/gb300-1.toml`](config/gb300-1.toml) and [`config/gb300-2.toml`](config/gb300-2.toml): worker templates.
- [`AGENT_INTEGRATION.md`](AGENT_INTEGRATION.md): deterministic integration contract for deployment agents.
- [`DEPLOYMENT.md`](DEPLOYMENT.md): persistent systemd worker installation and operations.
- [`PROTOCOL.md`](PROTOCOL.md): object layout, commit markers, and failure semantics.
- [`systemd/`](systemd): long-running gateway and worker units.
- [`docker/Dockerfile`](docker/Dockerfile): multi-architecture image definition.

## Development

```bash
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/ruff check .
.venv/bin/python -m build
.venv/bin/python scripts/benchmark-grouped-queue.py \
  --config config/gb300-1.toml --host-label gb300-1 --rounds 2
```

No AWS credentials, model API keys, SSH addresses, or deployment-specific secrets belong in this repository or its logs.
