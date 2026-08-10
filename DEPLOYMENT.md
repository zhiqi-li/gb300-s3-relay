# Persistent worker deployment

This guide installs one GB300 relay worker as a systemd service. Repeat it on every model
host with a unique target and worker ID. Keep environment-specific configurations and all
credentials outside the repository.

## Filesystem layout

```text
/opt/gb300-s3-relay/                 pinned repository checkout
/opt/gb300-s3-relay/.venv/           Python environment
/opt/gb300-s3-relay/.tools/s5cmd     verified data-plane binary
/etc/gb300-relay/<target>.toml       worker configuration
/etc/gb300-relay/aws-credentials     dedicated S3 credentials
/etc/gb300-relay/relay.env           model-server API key
/var/tmp/gb300-relay/                per-request temporary workspace
```

Use a dedicated, non-login `gb300-relay` service account. Recommended permissions are:

| Path | Owner | Mode |
|---|---|---:|
| `/etc/gb300-relay` | `root:gb300-relay` | `0750` |
| `<target>.toml` | `root:gb300-relay` | `0640` |
| `aws-credentials` | `gb300-relay:gb300-relay` | `0600` |
| `relay.env` | `root:root` | `0600` |
| `/var/tmp/gb300-relay` | `gb300-relay:gb300-relay` | `0750` |

## Install the runtime

Clone and pin a reviewed commit in its final location:

```bash
sudo git clone https://github.com/OWNER/gb300-s3-relay.git /opt/gb300-s3-relay
sudo git -C /opt/gb300-s3-relay checkout --detach <reviewed-commit>
```

Create the virtual environment at the final path. Python virtual environments and generated
console scripts are not relocatable, so do not create `.venv` in a staging directory and move
it later.

On a host whose Python includes pip:

```bash
sudo python3 -m venv /opt/gb300-s3-relay/.venv
sudo /opt/gb300-s3-relay/.venv/bin/pip install \
  '/opt/gb300-s3-relay[openai]'
```

On a minimal Python installation without `ensurepip`, use the official pip zipapp:

```bash
sudo python3 -m venv --without-pip /opt/gb300-s3-relay/.venv
sudo install -d -m 0755 /opt/gb300-s3-relay/.tools
sudo curl --fail --location --retry 5 \
  --output /opt/gb300-s3-relay/.tools/pip.pyz \
  https://bootstrap.pypa.io/pip/pip.pyz
sudo /opt/gb300-s3-relay/.venv/bin/python \
  /opt/gb300-s3-relay/.tools/pip.pyz install \
  '/opt/gb300-s3-relay[openai]'
```

Install the pinned, checksum-verified s5cmd release:

```bash
sudo env GB300_RELAY_TOOLS_DIR=/opt/gb300-s3-relay/.tools \
  /opt/gb300-s3-relay/scripts/install-s5cmd.sh
```

## Configure secrets

The S3 credentials file uses the standard shared-credentials format:

```ini
[gb300-relay]
aws_access_key_id = ...
aws_secret_access_key = ...
```

The model key is referenced through the environment-variable name in the TOML file and stored
in the root-only systemd environment file:

```dotenv
GB300_UPSTREAM_API_KEY=...
```

Never place either value in a TOML file, unit file, command argument, source-control commit, or
diagnostic report. After writing the files, apply the ownership and modes from the table above.

For a model server that cannot consume worker-local `file://` paths, including many SGLang
multimodal deployments, set:

```toml
[worker]
media_delivery = "data_uri"
# One worker-wide pool; 16 preserves parallelism for eight image+video requests.
asset_transfer_scope = "worker"
asset_transfer_concurrency = 16
asset_fairness_quantum_bytes = 16777216
```

The worker admits asset transfers across producer hardware groups with byte-weighted fairness.
Keep `asset_transfer_scope = "request"` only as a rolling-back/A-B compatibility switch; under
that legacy mode, every multimodal request receives a separate transfer allowance.

## Validate and start

Run the full storage preflight as the service account before enabling the worker:

```bash
sudo -u gb300-relay /opt/gb300-s3-relay/.venv/bin/gb300-relay doctor \
  --config /etc/gb300-relay/<target>.toml
```

Require `bucket=ok`, `conditional_create=ok`, `s5cmd_round_trip=ok`, and a nonzero cleanup
count. Then install the unit and enable the target instance:

For upgrades that introduce compact protocol support, roll and validate every worker before
enabling `gateway.compact_protocol`. New workers accept both the standard and compact formats;
an older worker cannot read a compact `READY.json`.

Producer-grouped queues use the same worker-first rollout. Keep `worker.scan_legacy_ready=true`
while upgrading workers, then upgrade the gateway so it publishes
`queue/<target>/<producer-group>/<job>.json`. After old `READY.json` jobs have drained, set
`scan_legacy_ready=false` and roll the workers once more to remove the extra legacy LIST.

```bash
sudo install -m 0644 /opt/gb300-s3-relay/systemd/gb300-relay-worker@.service \
  /etc/systemd/system/gb300-relay-worker@.service
sudo systemctl daemon-reload
sudo systemctl enable --now gb300-relay-worker@<target>.service
```

## Operations

```bash
systemctl is-enabled gb300-relay-worker@<target>.service
systemctl is-active gb300-relay-worker@<target>.service
systemctl status gb300-relay-worker@<target>.service
journalctl -u gb300-relay-worker@<target>.service -f
curl -fsS http://127.0.0.1:9108/metrics
```

After a restart, verify that `MainPID` changes, the service returns to `active`, and the gateway
observes a fresh healthy heartbeat for the target. A deployment is complete only after a
forced-target inference succeeds through the shared S3 prefix.

To rotate either credential, atomically replace its protected file and restart the worker. To
upgrade code, pin the new reviewed commit, reinstall the package into the virtual environment
at the same final path, run the doctor, and roll one target at a time.
