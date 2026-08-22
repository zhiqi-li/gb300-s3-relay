#!/usr/bin/env python3
# ruff: noqa: E501
"""Idempotently deploy the optimized model and S3 relay to a YAML-defined fleet."""

from __future__ import annotations

import argparse
import base64
import configparser
import hashlib
import json
import math
import os
import re
import shlex
import sys
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import paramiko
import yaml

ROOT = Path(__file__).resolve().parents[1]
TP1_SAMPLER_PATCH = ROOT / "patches/sglang-tp1-sampler-grammar-sync.patch"
SAFE_SYSTEMD_NAME = re.compile(r"^[A-Za-z0-9_.@-]+$")
SAFE_IMAGE = re.compile(r"^[A-Za-z0-9_./:@+-]+$")
SAFE_SHA256 = re.compile(r"^[0-9a-f]{64}$")
SAFE_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
PRINT_LOCK = threading.Lock()


class DeploymentError(RuntimeError):
    pass


def announce(node: str, message: str) -> None:
    with PRINT_LOCK:
        print(f"{node}: {message}", flush=True)


def read_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise DeploymentError(f"unable to read environment file {path}: {exc}") from exc
    for line_number, raw in enumerate(lines, 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").lstrip()
        if "=" not in line:
            raise DeploymentError(f"{path}:{line_number}: expected NAME=value")
        key, value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise DeploymentError(f"{path}:{line_number}: invalid environment variable name")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        values[key] = value
    return values


def load_spec(path: Path) -> dict[str, Any]:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise DeploymentError(f"unable to load fleet YAML {path}: {exc}") from exc
    if not isinstance(raw, dict) or raw.get("version") != 1:
        raise DeploymentError("fleet YAML must be a mapping with version: 1")
    if not isinstance(raw.get("nodes"), list) or not raw["nodes"]:
        raise DeploymentError("fleet YAML must define at least one node")
    if not isinstance(raw.get("model"), dict):
        raise DeploymentError("fleet YAML must define a model mapping")
    ids: list[str] = []
    for node in raw["nodes"]:
        if not isinstance(node, dict):
            raise DeploymentError("each fleet node must be a mapping")
        node_id = str(node.get("id", ""))
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", node_id):
            raise DeploymentError(f"invalid node id: {node_id!r}")
        if not node.get("host") or not isinstance(node.get("ssh"), dict):
            raise DeploymentError(f"{node_id}: host and ssh mappings are required")
        ssh = node["ssh"]
        if not ssh.get("user") or not ssh.get("password_env") or not ssh.get("host_key_sha256"):
            raise DeploymentError(
                f"{node_id}: ssh.user, ssh.password_env, and ssh.host_key_sha256 are required"
            )
        ids.append(node_id)
    if len(ids) != len(set(ids)):
        raise DeploymentError("fleet node IDs must be unique")
    return raw


def normalize_fingerprint(value: str) -> str:
    return value.removeprefix("SHA256:").rstrip("=")


def redact(text: str, secrets: tuple[str, ...]) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


class PinnedHostKeyPolicy(paramiko.MissingHostKeyPolicy):
    def __init__(self, expected: str) -> None:
        self.expected = normalize_fingerprint(expected)

    def missing_host_key(
        self, client: paramiko.SSHClient, hostname: str, key: paramiko.PKey
    ) -> None:
        actual = base64.b64encode(hashlib.sha256(key.asbytes()).digest()).decode().rstrip("=")
        if actual != self.expected:
            raise DeploymentError(f"{hostname}: SSH host key fingerprint mismatch")
        client.get_host_keys().add(hostname, key.get_name(), key)


class Remote:
    def __init__(self, node: dict[str, Any], password: str, defaults: dict[str, Any]) -> None:
        self.node = node
        self.password = password
        self.secrets = (password,)
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(
            PinnedHostKeyPolicy(str(node["ssh"]["host_key_sha256"]))
        )
        timeout = float(defaults.get("connect_timeout_seconds", 20))
        self.client.connect(
            str(node["host"]),
            port=int(node["ssh"].get("port", defaults.get("port", 22))),
            username=str(node["ssh"]["user"]),
            password=password,
            timeout=timeout,
            banner_timeout=max(30.0, timeout),
            auth_timeout=max(30.0, timeout),
            look_for_keys=False,
            allow_agent=False,
        )
        transport = self.client.get_transport()
        if transport is None:
            raise DeploymentError(f"{node['id']}: SSH transport is unavailable")
        transport.set_keepalive(15)

    def close(self) -> None:
        self.client.close()

    def run(
        self,
        command: str,
        *,
        sudo: bool = False,
        timeout: int = 3600,
        check: bool = True,
    ) -> tuple[int, str]:
        effective = command
        if sudo:
            effective = "sudo -S -p '' bash -lc " + shlex.quote(command)
        transport = self.client.get_transport()
        if transport is None:
            raise DeploymentError(f"{self.node['id']}: SSH transport is unavailable")
        channel = transport.open_session(timeout=timeout)
        channel.settimeout(timeout)
        channel.set_combine_stderr(True)
        channel.exec_command(effective)
        stdin = channel.makefile_stdin("wb")
        stdout = channel.makefile("rb")
        try:
            if sudo:
                stdin.write((self.password + "\n").encode())
                stdin.flush()
                channel.shutdown_write()
            output = stdout.read().decode("utf-8", "replace")
            status = channel.recv_exit_status()
        finally:
            stdin.close()
            stdout.close()
            channel.close()
        combined = redact(output, self.secrets)
        if check and status:
            raise DeploymentError(
                f"{self.node['id']}: remote command failed ({status}): {combined[-5000:]}"
            )
        return status, combined

    def upload_bytes(self, data: bytes, remote_path: str, mode: int = 0o600) -> None:
        sftp = self.client.open_sftp()
        try:
            with sftp.open(remote_path, "wb") as handle:
                handle.write(data)
            sftp.chmod(remote_path, mode)
        finally:
            sftp.close()

    def upload_file(self, local_path: Path, remote_path: str, mode: int = 0o600) -> None:
        sftp = self.client.open_sftp()
        try:
            sftp.put(str(local_path), remote_path)
            sftp.chmod(remote_path, mode)
        finally:
            sftp.close()


def toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def model_values(spec: dict[str, Any]) -> dict[str, Any]:
    model = spec["model"]
    required = ("id", "revision", "image")
    missing = [name for name in required if not model.get(name)]
    if missing:
        raise DeploymentError(f"model is missing required keys: {', '.join(missing)}")
    if not SAFE_IMAGE.fullmatch(str(model["id"])) or not SAFE_IMAGE.fullmatch(str(model["image"])):
        raise DeploymentError("model.id and model.image contain unsafe characters")
    service = str(model.get("service_name", "gb300-qwen38-fp8"))
    container = str(model.get("container_name", service))
    if not SAFE_SYSTEMD_NAME.fullmatch(service) or not SAFE_SYSTEMD_NAME.fullmatch(container):
        raise DeploymentError("model service/container name contains unsafe characters")
    engine = str(model.get("engine", "sglang"))
    if engine != "sglang":
        raise DeploymentError(f"only the sglang model engine is supported, got {engine!r}")
    image_tag = str(model.get("image_tag", "gb300-sglang-qwen38-mrope:latest"))
    if not SAFE_IMAGE.fullmatch(image_tag):
        raise DeploymentError("model.image_tag contains unsafe characters")
    patches = model.get("patches")
    if not isinstance(patches, dict):
        raise DeploymentError("model.patches must define the pinned SGLang fixes")
    for key in (
        "draft_extend_commit",
        "draft_extend_equivalent_commit",
        "fused_kernel_commit",
    ):
        if not SAFE_GIT_COMMIT.fullmatch(str(patches.get(key, ""))):
            raise DeploymentError(f"model.patches.{key} must be a full git commit")
    if not SAFE_SHA256.fullmatch(str(patches.get("fused_kernel_patch_sha256", ""))):
        raise DeploymentError("model.patches.fused_kernel_patch_sha256 must be SHA-256")
    if not SAFE_GIT_COMMIT.fullmatch(str(model.get("image_source_commit", ""))):
        raise DeploymentError("model.image_source_commit must be a full git commit")
    if "@sha256:" not in str(model["image"]):
        raise DeploymentError("model.image must be pinned by digest")
    mtp = model.get("mtp")
    if not isinstance(mtp, dict):
        raise DeploymentError("model.mtp must be a mapping")
    stop_containers = model.get("stop_containers", [])
    if not isinstance(stop_containers, list) or any(
        not SAFE_SYSTEMD_NAME.fullmatch(str(name)) for name in stop_containers
    ):
        raise DeploymentError("model.stop_containers must contain only exact container names")
    return {
        **model,
        "engine": engine,
        "image_tag": image_tag,
        "service_name": service,
        "container_name": container,
        "listen_host": str(model.get("listen_host", "127.0.0.1")),
        "port": int(model.get("port", 8000)),
        "max_model_len": int(model.get("max_model_len", 262_144)),
        "gpu_memory_utilization": float(model.get("gpu_memory_utilization", 0.92)),
        "kv_cache_dtype": str(model.get("kv_cache_dtype", "fp8")),
        "mtp": mtp,
        "stop_containers": [str(name) for name in stop_containers],
    }


def render_sglang_args(spec: dict[str, Any]) -> list[str]:
    model = model_values(spec)
    mtp = model["mtp"]
    multimodal = model.get("multimodal", {})
    limits = json.dumps(
        {
            "image": int(multimodal.get("max_images_per_prompt", 8)),
            "video": int(multimodal.get("max_videos_per_prompt", 2)),
        },
        separators=(",", ":"),
    )
    loader = json.dumps(
        {
            "enable_multithread_load": True,
            "num_threads": int(multimodal.get("model_loader_threads", 64)),
        },
        separators=(",", ":"),
    )
    return [
        "python3",
        "-m",
        "sglang.launch_server",
        "--model-path",
        str(model["id"]),
        "--revision",
        str(model["revision"]),
        "--served-model-name",
        str(model["id"]),
        "--host",
        model["listen_host"],
        "--port",
        str(model["port"]),
        "--tp",
        str(int(model.get("tensor_parallel_size", 1))),
        "--context-length",
        str(model["max_model_len"]),
        "--mem-fraction-static",
        str(model["gpu_memory_utilization"]),
        "--kv-cache-dtype",
        model["kv_cache_dtype"],
        "--max-running-requests",
        str(int(model.get("max_running_requests", 128))),
        "--cuda-graph-max-bs-decode",
        str(int(model.get("cuda_graph_max_bs_decode", 128))),
        "--chunked-prefill-size",
        str(int(model.get("chunked_prefill_size", 16_384))),
        "--tokenizer-worker-num",
        str(int(model.get("tokenizer_worker_num", 6))),
        "--attention-backend",
        str(model.get("attention_backend", "flashinfer")),
        "--reasoning-parser",
        str(model.get("reasoning_parser", "qwen3")),
        "--tool-call-parser",
        str(model.get("tool_call_parser", "qwen3_coder")),
        "--speculative-algorithm",
        str(mtp.get("algorithm", "NEXTN")),
        "--speculative-num-steps",
        str(int(mtp.get("num_steps", 3))),
        "--speculative-eagle-topk",
        str(int(mtp.get("eagle_topk", 1))),
        "--speculative-num-draft-tokens",
        str(int(mtp.get("num_draft_tokens", 4))),
        "--mamba-radix-cache-strategy",
        str(mtp.get("mamba_radix_cache_strategy", "extra_buffer")),
        "--mm-attention-backend",
        str(multimodal.get("attention_backend", "fa4")),
        "--mm-feature-transport",
        str(multimodal.get("feature_transport", "cuda_ipc")),
        "--mm-preprocess-cache-size-mb",
        str(int(multimodal.get("preprocess_cache_size_mb", 8192))),
        "--mm-processor-worker-num",
        str(int(multimodal.get("processor_workers", 4))),
        "--mm-io-worker-num",
        str(int(multimodal.get("io_workers", 8))),
        "--limit-mm-data-per-request",
        limits,
        "--model-loader-extra-config",
        loader,
        "--enable-multimodal",
        "--enable-metrics",
        "--flashinfer-allreduce-fusion-backend",
        str(multimodal.get("allreduce_fusion_backend", "auto")),
        "--trust-remote-code",
    ]


def render_sglang_verify_script() -> bytes:
    return b'''from types import SimpleNamespace

import torch

from sglang.srt.model_executor.forward_batch_info import ForwardBatch


batch_size = 3
draft_tokens = 4
forward_batch = ForwardBatch.__new__(ForwardBatch)
forward_batch.seq_lens = torch.full((batch_size,), 32, dtype=torch.int64)
multimodal_inputs = [
    SimpleNamespace(mrope_position_delta=torch.tensor([[5]], dtype=torch.int64))
    for _ in range(batch_size)
]
batch = SimpleNamespace(multimodal_inputs=multimodal_inputs)
seq_positions = torch.arange(batch_size * draft_tokens, dtype=torch.int64)
forward_batch.compute_spec_mrope_positions(
    SimpleNamespace(device=torch.device("cpu")), batch, seq_positions=seq_positions
)
assert forward_batch.mrope_positions.shape == (3, batch_size * draft_tokens)
expected = (seq_positions.view(batch_size, draft_tokens) + 5).flatten()
for axis in range(3):
    assert torch.equal(forward_batch.mrope_positions[axis], expected)
print("draft_extend_mrope=ok")
'''


def render_sglang_dockerfile(spec: dict[str, Any]) -> bytes:
    model = model_values(spec)
    patches = model["patches"]
    lines = [
        f"FROM {model['image']}",
        "COPY fused-kernel.patch /tmp/fused-kernel.patch",
        "COPY tp1-sampler-grammar-sync.patch /tmp/tp1-sampler-grammar-sync.patch",
        "COPY verify-draft-mrope.py /tmp/verify-draft-mrope.py",
        "RUN cd /sgl-workspace/sglang && "
        "git apply --check /tmp/fused-kernel.patch && "
        "git apply /tmp/fused-kernel.patch && "
        "git apply --check /tmp/tp1-sampler-grammar-sync.patch && "
        "git apply /tmp/tp1-sampler-grammar-sync.patch && "
        "python3 -m py_compile "
        "python/sglang/srt/layers/sampler.py "
        "python/sglang/kernels/ops/attention/fused_qk_rmsnorm_rope_gate.py "
        "python/sglang/srt/models/qwen3_5.py && "
        "python3 /tmp/verify-draft-mrope.py && "
        "rm -f /tmp/fused-kernel.patch /tmp/tp1-sampler-grammar-sync.patch "
        "/tmp/verify-draft-mrope.py",
        "LABEL "
        f'ai.sglang.base.commit="{model["image_source_commit"]}" '
        f'ai.sglang.fix.draft_extend_pr="{patches["draft_extend_pr"]}" '
        f'ai.sglang.fix.draft_extend_commit="{patches["draft_extend_commit"]}" '
        f'ai.sglang.fix.draft_extend_equivalent_commit="{patches["draft_extend_equivalent_commit"]}" '
        f'ai.sglang.fix.fused_kernel_pr="{patches["fused_kernel_pr"]}" '
        f'ai.sglang.fix.fused_kernel_commit="{patches["fused_kernel_commit"]}" '
        'ai.sglang.fix.tp1_sampler_grammar_sync="1"',
        "",
    ]
    return "\n".join(lines).encode()


def fetch_sglang_fused_patch(spec: dict[str, Any]) -> bytes:
    model = model_values(spec)
    patches = model["patches"]
    commit = str(patches["fused_kernel_commit"])
    url = f"https://github.com/sgl-project/sglang/commit/{commit}.patch"
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            data = response.read(8 * 1024 * 1024 + 1)
    except (OSError, urllib.error.URLError) as exc:
        raise DeploymentError(f"unable to download pinned SGLang patch {commit}: {exc}") from exc
    if len(data) > 8 * 1024 * 1024:
        raise DeploymentError(f"SGLang patch {commit} exceeds the 8 MiB limit")
    actual = hashlib.sha256(data).hexdigest()
    expected = str(patches["fused_kernel_patch_sha256"])
    if actual != expected:
        raise DeploymentError(
            f"SGLang patch {commit} checksum mismatch: expected {expected}, got {actual}"
        )
    return data


def render_model_unit(spec: dict[str, Any]) -> bytes:
    model = model_values(spec)
    cache_dir = str(model.get("cache_dir", "/var/lib/gb300-models/huggingface"))
    container_command = shlex.join(render_sglang_args(spec))
    lines = [
        "[Unit]",
        f"Description=GB300 {model['id']} SGLang service",
        "After=docker.service network-online.target",
        "Wants=network-online.target",
        "Requires=docker.service",
        "",
        "[Service]",
        "Type=simple",
        f"ExecStartPre=-/usr/bin/docker rm -f {model['container_name']}",
        "ExecStart=/usr/bin/docker run "
        f"--name {model['container_name']} --pull never --gpus all --network host --ipc host "
        "--ulimit memlock=-1 --ulimit stack=67108864 "
        "-e HF_HOME=/root/.cache/huggingface -e HF_XET_HIGH_PERFORMANCE=1 "
        "-e HF_HUB_DISABLE_TELEMETRY=1 -e SGLANG_USE_CUDA_IPC_TRANSPORT=1 "
        f"-v {cache_dir}:/root/.cache/huggingface "
        f"{model['image_tag']} {container_command}",
        f"ExecStop=-/usr/bin/docker stop --timeout 180 {model['container_name']}",
        "Restart=on-failure",
        "RestartSec=15",
        "TimeoutStartSec=0",
        "TimeoutStopSec=240",
        "KillMode=mixed",
        "LimitNOFILE=1048576",
        "",
        "[Install]",
        "WantedBy=multi-user.target",
        "",
    ]
    return "\n".join(lines).encode()


def relay_values(spec: dict[str, Any]) -> dict[str, Any]:
    relay = spec.get("relay")
    if not isinstance(relay, dict) or not relay.get("enabled", True):
        raise DeploymentError("relay is disabled or missing from the fleet YAML")
    s3 = relay.get("s3")
    if not isinstance(s3, dict):
        raise DeploymentError("relay.s3 must be a mapping")
    for key in ("bucket", "prefix", "endpoint_url", "credentials_file", "profile"):
        if not s3.get(key):
            raise DeploymentError(f"relay.s3.{key} is required")
    return relay


def render_worker_config(spec: dict[str, Any], node_id: str) -> bytes:
    relay = relay_values(spec)
    s3 = relay["s3"]
    worker = relay.get("worker", {})
    model = model_values(spec)
    remote_profile = str(s3.get("remote_profile", "gb300-relay"))
    upstream_base_url = toml_string(f"http://127.0.0.1:{model['port']}")
    lines = [
        "[s3]",
        f"bucket = {toml_string(str(s3['bucket']))}",
        f"prefix = {toml_string(str(s3['prefix']).strip('/'))}",
        f"endpoint_url = {toml_string(str(s3['endpoint_url']).rstrip('/'))}",
        f"region = {toml_string(str(s3.get('region', 'us-east-1')))}",
        f"profile = {toml_string(remote_profile)}",
        'credentials_file = "/etc/gb300-relay/aws-credentials"',
        's5cmd_path = "/opt/gb300-s3-relay/.tools/s5cmd"',
        f"addressing_style = {toml_string(str(s3.get('addressing_style', 'path')))}",
        f"verify_tls = {str(bool(s3.get('verify_tls', True))).lower()}",
        "clear_proxy_env = true",
        f"connect_timeout_seconds = {float(s3.get('connect_timeout_seconds', 10))}",
        f"operation_timeout_seconds = {float(s3.get('operation_timeout_seconds', 900))}",
        f"retry_count = {int(s3.get('retry_count', 10))}",
        f"max_pool_connections = {int(s3.get('max_pool_connections', 128))}",
        f"native_transfer_max_bytes = {int(s3.get('native_transfer_max_bytes', 8 * 1024**2))}",
        "",
        "[worker]",
        f"target = {toml_string(node_id)}",
        f"worker_id = {toml_string(node_id + '-worker-1')}",
        f"upstream_base_url = {upstream_base_url}",
        f"max_concurrency = {int(worker.get('max_concurrency', 64))}",
        f"max_heavy_concurrency = {int(worker.get('max_heavy_concurrency', 8))}",
        "heavy_request_threshold_bytes = 131072",
        f"thread_pool_workers = {int(worker.get('thread_pool_workers', 128))}",
        f"asset_transfer_concurrency = {int(worker.get('asset_transfer_concurrency', 16))}",
        'asset_transfer_scope = "worker"',
        "asset_fairness_quantum_bytes = 16777216",
        f"poll_interval_seconds = {float(worker.get('poll_interval_seconds', 0.05))}",
        f"poll_jitter_seconds = {float(worker.get('poll_jitter_seconds', 0.05))}",
        "scan_legacy_ready = false",
        "terminal_cache_size = 100000",
        "lease_seconds = 1800",
        "lease_heartbeat_seconds = 60",
        f"worker_heartbeat_seconds = {int(worker.get('heartbeat_seconds', 2))}",
        f"job_timeout_seconds = {float(worker.get('job_timeout_seconds', 900))}",
        "max_attempts = 3",
        "retry_base_seconds = 1",
        'work_dir = "/var/tmp/gb300-relay"',
        f"media_delivery = {toml_string(str(worker.get('media_delivery', 'data_uri')))}",
        "inline_image_max_bytes = 33554432",
        "max_response_bytes = 536870912",
        "compact_response_max_bytes = 1048576",
        "stream_chunk_bytes = 262144",
        f"stream_flush_interval_seconds = {float(worker.get('stream_flush_interval_seconds', 0.1))}",
        "shutdown_grace_seconds = 300",
        f"models = [{toml_string(str(model['id']))}]",
        "",
        "[metrics]",
        "enabled = true",
        'host = "127.0.0.1"',
        f"port = {int(worker.get('metrics_port', 9108))}",
        "",
    ]
    return "\n".join(lines).encode()


def render_gateway_config(
    spec: dict[str, Any], nodes: list[dict[str, Any]], credentials_path: Path
) -> str:
    relay = relay_values(spec)
    s3 = relay["s3"]
    gateway = relay.get("gateway", {})
    targets = ", ".join(toml_string(str(node["id"])) for node in nodes)
    profile = str(s3["profile"])
    lines = [
        "[s3]",
        f"bucket = {toml_string(str(s3['bucket']))}",
        f"prefix = {toml_string(str(s3['prefix']).strip('/'))}",
        f"endpoint_url = {toml_string(str(s3['endpoint_url']).rstrip('/'))}",
        f"region = {toml_string(str(s3.get('region', 'us-east-1')))}",
        f"profile = {toml_string(profile)}",
        f"credentials_file = {toml_string(str(credentials_path))}",
        f"addressing_style = {toml_string(str(s3.get('addressing_style', 'path')))}",
        f"verify_tls = {str(bool(s3.get('verify_tls', True))).lower()}",
        "clear_proxy_env = true",
        f"retry_count = {int(s3.get('retry_count', 10))}",
        f"max_pool_connections = {int(s3.get('max_pool_connections', 256))}",
        f"native_transfer_max_bytes = {int(s3.get('native_transfer_max_bytes', 8 * 1024**2))}",
        "",
        "[gateway]",
        f"host = {toml_string(str(gateway.get('host', '127.0.0.1')))}",
        f"port = {int(gateway.get('port', 18080))}",
        f"targets = [{targets}]",
        f"default_timeout_seconds = {float(gateway.get('default_timeout_seconds', 900))}",
        f"max_timeout_seconds = {float(gateway.get('max_timeout_seconds', 1800))}",
        f"poll_interval_seconds = {float(gateway.get('poll_interval_seconds', 0.05))}",
        f"target_heartbeat_ttl_seconds = {float(gateway.get('heartbeat_ttl_seconds', 15))}",
        "require_healthy_worker = true",
        "cleanup_on_success = true",
        "cleanup_idempotent_on_success = false",
        "compact_protocol = true",
        "compact_manifest_max_bytes = 1048576",
        f"thread_pool_workers = {int(gateway.get('thread_pool_workers', 256))}",
        f"producer_group = {toml_string(str(gateway.get('producer_group', 'gb300-fleet-gateway')))}",
        "",
        "[gateway.media]",
        "materialize_data_urls = true",
        "allow_file_urls = false",
        "materialize_http_urls = false",
        "",
        "[metrics]",
        "enabled = false",
        "",
    ]
    return "\n".join(lines)


def read_s3_credentials(spec: dict[str, Any]) -> tuple[bytes, Path]:
    s3 = relay_values(spec)["s3"]
    path = Path(str(s3["credentials_file"])).expanduser().resolve()
    parser = configparser.RawConfigParser()
    if not parser.read(path):
        raise DeploymentError(f"unable to read S3 credentials file {path}")
    profile = str(s3["profile"])
    candidates = (profile, f"profile {profile}")
    section_name = next((name for name in candidates if parser.has_section(name)), None)
    if section_name is None:
        raise DeploymentError(f"S3 profile {profile!r} was not found in {path}")
    section = parser[section_name]
    access = section.get("aws_access_key_id")
    secret = section.get("aws_secret_access_key")
    token = section.get("aws_session_token", fallback=None)
    if not access or not secret:
        raise DeploymentError(f"S3 profile {profile!r} has incomplete credentials")
    remote_profile = str(s3.get("remote_profile", "gb300-relay"))
    lines = [
        f"[{remote_profile}]",
        f"aws_access_key_id = {access}",
        f"aws_secret_access_key = {secret}",
    ]
    if token:
        lines.append(f"aws_session_token = {token}")
    return ("\n".join(lines) + "\n").encode(), path


def make_source_archive() -> Path:
    with tempfile.NamedTemporaryFile(
        prefix="gb300-relay-source-", suffix=".tar.gz", delete=False
    ) as handle:
        archive_path = Path(handle.name)
    includes = ("pyproject.toml", "README.md", "src", "systemd", "scripts/install-s5cmd.sh")
    with tarfile.open(archive_path, "w:gz") as archive:
        for relative in includes:
            source = ROOT / relative
            archive.add(source, arcname=relative, recursive=True)
    return archive_path


def bootstrap_system(remote: Remote, spec: dict[str, Any]) -> None:
    status, _ = remote.run(
        "command -v nvidia-smi >/dev/null && nvidia-smi >/dev/null && "
        "command -v docker >/dev/null && docker info >/dev/null 2>&1 && "
        "command -v nvidia-ctk >/dev/null && "
        "docker info --format '{{json .Runtimes}}' | grep -q nvidia",
        sudo=True,
        check=False,
    )
    if status == 0:
        announce(str(remote.node["id"]), "system runtime already ready")
        return
    runtime = spec.get("runtime", {})
    driver_package = shlex.quote(str(runtime.get("driver_package", "nvidia-driver-595-open")))
    docker_package = shlex.quote(str(runtime.get("docker_package", "docker.io")))
    toolkit_version = runtime.get("nvidia_container_toolkit_version")
    toolkit_package = (
        f"nvidia-container-toolkit={shlex.quote(str(toolkit_version))}"
        if toolkit_version
        else "nvidia-container-toolkit"
    )
    command = f"""
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
packages=()
command -v curl >/dev/null 2>&1 || packages+=(curl)
command -v gpg >/dev/null 2>&1 || packages+=(gnupg)
command -v python3 >/dev/null 2>&1 || packages+=(python3)
command -v docker >/dev/null 2>&1 || packages+=({docker_package})
if ! command -v nvidia-smi >/dev/null 2>&1 || ! nvidia-smi >/dev/null 2>&1; then
  packages+=({driver_package})
fi
if [ "${{#packages[@]}}" -gt 0 ]; then
  apt-get update
  apt-get install -y ca-certificates "${{packages[@]}}"
fi
if ! command -v nvidia-ctk >/dev/null 2>&1; then
  curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | gpg --dearmor --yes -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
  curl -fsSL https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#' > /etc/apt/sources.list.d/nvidia-container-toolkit.list
  apt-get update
  apt-get install -y {toolkit_package}
fi
systemctl enable --now docker
if ! docker info --format '{{{{json .Runtimes}}}}' | grep -q nvidia; then
  nvidia-ctk runtime configure --runtime=docker
  systemctl restart docker
fi
if ! nvidia-smi >/dev/null 2>&1; then
  echo 'NVIDIA driver was installed but is not loaded; reboot this node and rerun deployment' >&2
  exit 42
fi
docker info --format '{{{{json .Runtimes}}}}' | grep -q nvidia
"""
    announce(str(remote.node["id"]), "installing GPU/container runtime")
    remote.run(command, sudo=True, timeout=1800)


def deploy_model(remote: Remote, spec: dict[str, Any], fused_patch: bytes) -> None:
    node_id = str(remote.node["id"])
    model = model_values(spec)
    tag = uuid.uuid4().hex
    build_context = f"/tmp/gb300-sglang-build-{tag}"
    temp_unit = f"/tmp/gb300-model-unit-{tag}.service"
    remote.run(f"install -d -m 0700 {shlex.quote(build_context)}")
    remote.upload_bytes(fused_patch, f"{build_context}/fused-kernel.patch")
    remote.upload_file(
        TP1_SAMPLER_PATCH,
        f"{build_context}/tp1-sampler-grammar-sync.patch",
    )
    remote.upload_bytes(
        render_sglang_verify_script(), f"{build_context}/verify-draft-mrope.py"
    )
    remote.upload_bytes(render_sglang_dockerfile(spec), f"{build_context}/Dockerfile")
    remote.upload_bytes(render_model_unit(spec), temp_unit, 0o644)
    service = f"{model['service_name']}.service"
    cache_dir = str(model.get("cache_dir", "/var/lib/gb300-models/huggingface"))
    timeout = int(model.get("start_timeout_seconds", 3600))
    attempts = max(1, math.ceil(timeout / 5))
    patches = model["patches"]
    fused_commit = str(patches["fused_kernel_commit"])
    announce(node_id, "building pinned SGLang image with mRoPE fixes")
    build_command = f"""
set -euo pipefail
install -d -o root -g root -m 0755 /etc/gb300-model {shlex.quote(cache_dir)}
docker image inspect {shlex.quote(str(model["image"]))} >/dev/null 2>&1 || docker pull {shlex.quote(str(model["image"]))}
if ! docker image inspect --format '{{{{index .Config.Labels "ai.sglang.fix.fused_kernel_commit"}}}}' {shlex.quote(str(model["image_tag"]))} 2>/dev/null | grep -Fxq {shlex.quote(fused_commit)} ||
   ! docker image inspect --format '{{{{index .Config.Labels "ai.sglang.fix.tp1_sampler_grammar_sync"}}}}' {shlex.quote(str(model["image_tag"]))} 2>/dev/null | grep -Fxq 1; then
  docker build --pull=false --tag {shlex.quote(str(model["image_tag"]))} {shlex.quote(build_context)}
fi
rm -rf {shlex.quote(build_context)}
"""
    remote.run(build_command, sudo=True, timeout=3600)

    if model["stop_containers"]:
        announce(node_id, "stopping configured legacy model stack")
        legacy_names = " ".join(shlex.quote(name) for name in model["stop_containers"])
        remote.run(
            "for name in "
            + legacy_names
            + "; do docker container inspect \"$name\" >/dev/null 2>&1 && "
            + "docker stop --timeout 180 \"$name\" || true; done",
            sudo=True,
            timeout=900,
        )

    announce(node_id, "GPU-testing fused mRoPE kernel patch")
    remote.run(f"systemctl stop {shlex.quote(service)}", sudo=True, check=False, timeout=300)
    test_command = (
        "docker run --rm --gpus all --ipc host "
        f"{shlex.quote(str(model['image_tag']))} bash -lc "
        + shlex.quote(
            "cd /sgl-workspace/sglang/test && "
            "python3 registered/kernels/ops/attention/"
            "test_fused_qk_rmsnorm_rope_gate.py"
        )
    )
    test_status, test_output = remote.run(
        test_command, sudo=True, check=False, timeout=1200
    )
    if test_status:
        remote.run(f"systemctl start {shlex.quote(service)}", sudo=True, check=False)
        raise DeploymentError(
            f"{node_id}: fused mRoPE GPU regression failed: {test_output[-5000:]}"
        )

    announce(node_id, "starting SGLang model service")
    install_command = f"""
set -euo pipefail
install -o root -g root -m 0644 {shlex.quote(temp_unit)} /etc/systemd/system/{shlex.quote(service)}
rm -f {shlex.quote(temp_unit)}
systemctl daemon-reload
systemctl enable {shlex.quote(service)}
systemctl restart {shlex.quote(service)}
for attempt in $(seq 1 {attempts}); do
  if curl -fsS http://127.0.0.1:{model["port"]}/health >/dev/null; then
    curl -fsS http://127.0.0.1:{model["port"]}/v1/models | grep -Fq {shlex.quote(str(model["id"]))}
    exit 0
  fi
  if ! systemctl is-active --quiet {shlex.quote(service)}; then
    systemctl status --no-pager {shlex.quote(service)} || true
    journalctl -u {shlex.quote(service)} --no-pager -n 200 || true
    exit 1
  fi
  sleep 5
done
echo 'timed out waiting for the SGLang health endpoint' >&2
journalctl -u {shlex.quote(service)} --no-pager -n 200 || true
exit 1
"""
    remote.run(install_command, sudo=True, timeout=timeout + 600)
    announce(node_id, "SGLang model ready")


def deploy_relay(
    remote: Remote,
    spec: dict[str, Any],
    archive_path: Path,
    credentials: bytes,
    *,
    run_smoke: bool,
) -> None:
    node_id = str(remote.node["id"])
    legacy_targets = remote.node.get("legacy_relay_targets", [])
    if not isinstance(legacy_targets, list):
        raise DeploymentError(f"{node_id}: legacy_relay_targets must be a list")
    legacy_services: list[str] = []
    for target in legacy_targets:
        target_name = str(target)
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", target_name):
            raise DeploymentError(f"{node_id}: invalid legacy relay target: {target_name!r}")
        if target_name != node_id and target_name not in legacy_services:
            legacy_services.append(target_name)
    legacy_disable = "\n".join(
        f"systemctl disable --now {shlex.quote(f'gb300-relay-worker@{target}.service')} || true"
        for target in legacy_services
    )
    tag = uuid.uuid4().hex
    paths = {
        "archive": f"/tmp/gb300-relay-source-{tag}.tar.gz",
        "credentials": f"/tmp/gb300-relay-credentials-{tag}",
        "config": f"/tmp/gb300-relay-config-{tag}.toml",
        "unit": f"/tmp/gb300-relay-unit-{tag}.service",
    }
    remote.upload_file(archive_path, paths["archive"])
    remote.upload_bytes(credentials, paths["credentials"])
    remote.upload_bytes(render_worker_config(spec, node_id), paths["config"])
    remote.upload_bytes(
        (ROOT / "systemd/gb300-relay-worker@.service").read_bytes(), paths["unit"], 0o644
    )
    announce(node_id, "deploying S3 relay worker")
    command = f"""
set -euo pipefail
if ! id -u gb300-relay >/dev/null 2>&1; then
  useradd --system --home-dir /var/lib/gb300-relay --create-home --shell /usr/sbin/nologin gb300-relay
fi
install -d -o root -g gb300-relay -m 0750 /etc/gb300-relay
install -d -o root -g root -m 0755 /opt/gb300-s3-relay
install -d -o gb300-relay -g gb300-relay -m 0750 /var/tmp/gb300-relay
tar -xzf {shlex.quote(paths["archive"])} -C /opt/gb300-s3-relay
if [ ! -x /opt/gb300-s3-relay/.venv/bin/python ]; then
  python3 -m venv --without-pip /opt/gb300-s3-relay/.venv
fi
install -d -o root -g root -m 0755 /opt/gb300-s3-relay/.tools
if ! /opt/gb300-s3-relay/.venv/bin/python -m pip --version >/dev/null 2>&1 && [ ! -f /opt/gb300-s3-relay/.tools/pip.pyz ]; then
  curl --fail --location --retry 5 --silent --show-error --output /opt/gb300-s3-relay/.tools/pip.pyz https://bootstrap.pypa.io/pip/pip.pyz
fi
install_ok=0
for attempt in 1 2 3 4 5; do
  if /opt/gb300-s3-relay/.venv/bin/python -m pip --version >/dev/null 2>&1; then
    installer='/opt/gb300-s3-relay/.venv/bin/python -m pip'
  else
    installer='/opt/gb300-s3-relay/.venv/bin/python /opt/gb300-s3-relay/.tools/pip.pyz'
  fi
  if $installer install --disable-pip-version-check --quiet /opt/gb300-s3-relay; then
    install_ok=1
    break
  fi
  sleep $((attempt * 3))
done
test "$install_ok" -eq 1
if [ ! -x /opt/gb300-s3-relay/.tools/s5cmd ]; then
  env GB300_RELAY_TOOLS_DIR=/opt/gb300-s3-relay/.tools bash /opt/gb300-s3-relay/scripts/install-s5cmd.sh
fi
install -o gb300-relay -g gb300-relay -m 0600 {shlex.quote(paths["credentials"])} /etc/gb300-relay/aws-credentials
install -o root -g gb300-relay -m 0640 {shlex.quote(paths["config"])} /etc/gb300-relay/{node_id}.toml
install -o root -g root -m 0644 {shlex.quote(paths["unit"])} /etc/systemd/system/gb300-relay-worker@.service
rm -f {shlex.quote(paths["archive"])} {shlex.quote(paths["credentials"])} {shlex.quote(paths["config"])} {shlex.quote(paths["unit"])}
systemctl daemon-reload
{legacy_disable}
"""
    remote.run(command, sudo=True, timeout=1800)
    if run_smoke:
        announce(node_id, "validating S3 conditionals and s5cmd transfer")
        remote.run(
            "sudo -u gb300-relay /opt/gb300-s3-relay/.venv/bin/gb300-relay "
            f"--plain-logs doctor --config /etc/gb300-relay/{node_id}.toml",
            sudo=True,
            timeout=900,
        )
    service = f"gb300-relay-worker@{node_id}.service"
    remote.run(
        f"systemctl enable {shlex.quote(service)} && "
        f"systemctl restart {shlex.quote(service)}",
        sudo=True,
    )
    metrics_port = int(spec.get("relay", {}).get("worker", {}).get("metrics_port", 9108))
    ready_command = f"""
set -euo pipefail
service={shlex.quote(service)}
check_ready() {{
  systemctl is-active --quiet "$service"
  main_pid=$(systemctl show --property MainPID --value "$service")
  test "$main_pid" -gt 1
  ss -ltnp 'sport = :{metrics_port}' | grep -Fq "pid=$main_pid,"
  curl -fsS http://127.0.0.1:{metrics_port}/metrics >/dev/null
}}
check_ready
sleep 3
check_ready
"""
    for _ in range(30):
        status, _ = remote.run(
            ready_command,
            sudo=True,
            check=False,
        )
        if status == 0:
            announce(node_id, "relay worker ready")
            return
        time.sleep(2)
    raise DeploymentError(f"{node_id}: relay worker did not become ready")


def preflight(remote: Remote) -> None:
    node_id = str(remote.node["id"])
    status, output = remote.run(
        "uname -m; python3 --version; nvidia-smi --query-gpu=name,memory.total "
        "--format=csv,noheader 2>/dev/null || true",
        check=False,
    )
    if status:
        raise DeploymentError(f"{node_id}: preflight failed: {output[-1000:]}")
    first_line = output.splitlines()[0] if output.splitlines() else "unknown"
    announce(node_id, f"SSH preflight ready ({first_line})")


def selected_nodes(spec: dict[str, Any], requested: list[str]) -> list[dict[str, Any]]:
    enabled = [node for node in spec["nodes"] if node.get("enabled", True)]
    if not requested:
        return enabled
    values = {part.strip() for item in requested for part in item.split(",") if part.strip()}
    known = {str(node["id"]) for node in enabled}
    unknown = values - known
    if unknown:
        raise DeploymentError(f"unknown or disabled nodes: {', '.join(sorted(unknown))}")
    return [node for node in enabled if node["id"] in values]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config/fleet.local.yaml")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument(
        "--node", action="append", default=[], help="node ID; repeat or comma-separate"
    )
    parser.add_argument(
        "--stage",
        action="append",
        choices=("all", "system", "model", "relay"),
        help="deployment stage; default comes from fleet.default_stages",
    )
    parser.add_argument("--skip-smoke", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--gateway-output",
        type=Path,
        default=ROOT / "config/local-fleet-gateway.toml",
    )
    parser.add_argument("--start-gateway", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    spec = load_spec(args.config.expanduser().resolve())
    env = {**os.environ, **read_dotenv(args.env_file.expanduser().resolve())}
    nodes = selected_nodes(spec, args.node)
    if not nodes:
        raise DeploymentError("no enabled nodes were selected")
    configured_stages = spec.get("fleet", {}).get("default_stages", ["system", "model", "relay"])
    stages = set(args.stage or configured_stages)
    if "all" in stages:
        stages = {"system", "model", "relay"}
    for stage in stages:
        if stage not in {"system", "model", "relay"}:
            raise DeploymentError(f"unsupported deployment stage: {stage}")

    passwords: dict[str, str] = {}
    for node in nodes:
        variable = str(node["ssh"]["password_env"])
        if not env.get(variable):
            raise DeploymentError(f"{node['id']}: environment variable {variable} is unset")
        passwords[str(node["id"])] = env[variable]

    credentials: bytes | None = None
    credentials_path: Path | None = None
    if "relay" in stages or args.start_gateway:
        credentials, credentials_path = read_s3_credentials(spec)
    fused_patch = fetch_sglang_fused_patch(spec) if "model" in stages else None

    print(
        "deployment_plan="
        + json.dumps(
            {
                "nodes": [node["id"] for node in nodes],
                "stages": sorted(stages),
                "model": spec["model"]["id"],
                "engine": model_values(spec)["engine"],
                "dry_run": args.dry_run,
            },
            separators=(",", ":"),
        ),
        flush=True,
    )
    if args.dry_run:
        return 0

    archive_path = make_source_archive() if "relay" in stages else None
    defaults = spec.get("ssh_defaults", {})
    parallelism = min(
        len(nodes), max(1, int(spec.get("fleet", {}).get("deployment_parallelism", 2)))
    )

    def deploy_node(node: dict[str, Any]) -> str:
        node_id = str(node["id"])
        remote = Remote(node, passwords[node_id], defaults)
        if credentials is not None:
            remote.secrets += tuple(
                line.split("=", 1)[1].strip()
                for line in credentials.decode("utf-8").splitlines()
                if "=" in line
            )
        try:
            preflight(remote)
            if "system" in stages:
                bootstrap_system(remote, spec)
            if "model" in stages:
                if fused_patch is None:
                    raise DeploymentError("SGLang fused-kernel patch was not prepared")
                deploy_model(remote, spec, fused_patch)
            if "relay" in stages:
                if archive_path is None or credentials is None:
                    raise DeploymentError("relay deployment inputs were not prepared")
                deploy_relay(
                    remote,
                    spec,
                    archive_path,
                    credentials,
                    run_smoke=not args.skip_smoke,
                )
            return node_id
        finally:
            remote.close()

    completed: list[str] = []
    errors: list[str] = []
    try:
        with ThreadPoolExecutor(max_workers=parallelism) as pool:
            futures = {pool.submit(deploy_node, node): str(node["id"]) for node in nodes}
            for future in as_completed(futures):
                node_id = futures[future]
                try:
                    completed.append(future.result())
                except Exception as exc:
                    errors.append(f"{node_id}: {redact(str(exc), tuple(passwords.values()))}")
    finally:
        if archive_path is not None:
            archive_path.unlink(missing_ok=True)
    if errors:
        for error in errors:
            print(f"ERROR {error}", file=sys.stderr)
        return 1

    gateway_output: Path | None = None
    if credentials_path is None and args.start_gateway:
        _, credentials_path = read_s3_credentials(spec)
    if credentials_path is not None:
        gateway_output = args.gateway_output.expanduser().resolve()
        gateway_output.parent.mkdir(parents=True, exist_ok=True)
        gateway_output.write_text(
            render_gateway_config(spec, nodes, credentials_path), encoding="utf-8"
        )
        gateway_output.chmod(0o600)
        print(f"gateway_config={gateway_output}", flush=True)

    print("deployment_ready=" + ",".join(sorted(completed)), flush=True)
    if args.start_gateway:
        if gateway_output is None:
            raise DeploymentError("gateway configuration was not generated")
        executable = ROOT / ".venv/bin/gb300-relay"
        os.execv(
            executable,
            [str(executable), "gateway", "--config", str(gateway_output)],
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except DeploymentError as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
