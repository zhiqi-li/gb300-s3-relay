# Agent integration guide

This document is a deterministic deployment contract for automation agents. It intentionally contains no environment-specific credentials, hostnames, bucket names, or model keys.

## Component placement

| Environment | Component | Required connectivity |
|---|---|---|
| Application or OSMO node | `gb300-relay gateway` | S3 endpoint and local clients |
| Each GB300 host | `gb300-relay worker` | S3 endpoint and its local OpenAI-compatible model server |
| Existing application | Official OpenAI client | Local gateway only |

Do not require direct OSMO-to-GB300 TCP connectivity. Do not expose the GB300 model port for this architecture.

## Inputs an agent must obtain

1. S3-compatible endpoint, bucket, region, profile name, and a credentials-file path.
2. One stable target name per GB300 host.
3. The local model-server base URL on each GB300 host.
4. The environment-variable name containing any model-server API key.
5. Supported model IDs and media delivery mode.
6. Desired worker concurrency, job deadline, lease duration, and retention policy.
7. For `auto` or `file_uri` media delivery, a filesystem namespace and permissions that let
   the local model-server account read the worker's temporary files.
8. One stable producer-group ID per OSMO hardware node, or a trusted caller that sends
   `x-gb300-producer-group` on every request.

Never put credential values into TOML, command arguments, source control, logs, or generated reports. Configurations reference credential files and environment-variable names only.

## Deployment sequence

1. Install Python 3.11 or later and run `scripts/bootstrap.sh` on each host.
2. Copy `config/osmo-gateway.toml` and one worker template into deployment-only paths.
3. Replace every `example` value and set restrictive file permissions on configurations and credentials.
4. Run `gb300-relay doctor --config ...` on every host. Require all of:
   - `bucket=ok`
   - `conditional_create=ok`
   - `s5cmd_round_trip=ok`
   - a nonzero cleanup count for the probe objects
5. Start every worker.
6. Enable `gateway.compact_protocol` and producer-grouped submission only after every target
   runs the matching worker version. Keep `worker.scan_legacy_ready=true` during this rollout.
7. Wait for `/readyz` on the gateway to report at least one healthy worker for each required target.
8. Start or expose the gateway only on the intended interface. Configure `auth_token_env` before any non-loopback bind.
9. Set `OPENAI_BASE_URL=http://<gateway>/v1` and a nonempty `OPENAI_API_KEY` in the application.
10. Perform one forced-target request per worker, one load-balanced request, and one streaming request.
11. If multimodal inference is required, test an actual image and video accepted by the deployed model, not only transport fixtures.
12. After legacy `READY.json` jobs drain, set `worker.scan_legacy_ready=false` to avoid the
    compatibility LIST on every poll.

## Readiness gates

The deployment is ready only when:

- S3 doctor passes on the gateway and every worker host;
- worker heartbeats are younger than `target_heartbeat_ttl_seconds`;
- `/v1/models` contains the expected model IDs;
- a forced request succeeds through every target;
- gateway logs contain no credential material;
- request cleanup or configured retention behaves as expected.

## Existing application integration

Preferred zero-code-change mode:

```bash
export OPENAI_BASE_URL=http://127.0.0.1:8080/v1
export OPENAI_API_KEY=relay-local
exec python application.py
```

Python factory mode:

```python
from gb300_relay import AsyncOpenAI

client = AsyncOpenAI()
```

Per-call controls belong in OpenAI `extra_headers`:

```python
extra_headers = {
    "x-gb300-target": "gb300-1",
    "x-gb300-producer-group": "osmo-node-17",
    "x-relay-timeout-seconds": "1800",
    "idempotency-key": "stable-logical-operation-id",
}
```

## Upgrade procedure

1. Run unit tests, lint, and package build.
2. Upgrade one worker and validate a forced request to that target.
3. Roll the remaining workers.
4. Upgrade the gateway last.
5. Keep `prefix` and protocol major version stable for in-flight jobs.
6. Do not delete an old worker's work directory while it has in-flight requests.

## Failure handling

- A failed application request does not justify stopping an OSMO allocation or the model server.
- A temporary S3 failure should be retried; do not publish a fabricated success.
- Preserve failed response metadata and dead letters for diagnosis.
- Do not clean an unacknowledged job unless an operator explicitly chooses that policy.
- Across lease expiry, assume at-least-once inference and use idempotency for costly operations.
