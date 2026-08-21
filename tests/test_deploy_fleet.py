from __future__ import annotations

import importlib.util
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
MODULE_SPEC = importlib.util.spec_from_file_location(
    "deploy_fleet", ROOT / "scripts/deploy-fleet.py"
)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
deploy_fleet = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(deploy_fleet)


def fleet_spec() -> dict:
    return yaml.safe_load((ROOT / "config/fleet.example.yaml").read_text())


def test_sglang_model_is_digest_and_patch_pinned() -> None:
    model = deploy_fleet.model_values(fleet_spec())

    assert model["engine"] == "sglang"
    assert "@sha256:" in model["image"]
    assert len(model["image_source_commit"]) == 40
    assert len(model["patches"]["draft_extend_commit"]) == 40
    assert len(model["patches"]["draft_extend_equivalent_commit"]) == 40
    assert len(model["patches"]["fused_kernel_commit"]) == 40
    assert len(model["patches"]["fused_kernel_patch_sha256"]) == 64
    assert model["stop_containers"] == []


def test_sglang_args_enable_vision_and_mtp_optimizations() -> None:
    args = deploy_fleet.render_sglang_args(fleet_spec())
    joined = " ".join(args)

    assert "--model-path Qwen/Qwen3.8-27B-FP8" in joined
    assert "--speculative-algorithm NEXTN" in joined
    assert "--speculative-num-steps 3" in joined
    assert "--speculative-num-draft-tokens 4" in joined
    assert "--mamba-radix-cache-strategy extra_buffer" in joined
    assert "--cuda-graph-max-bs-decode 128" in joined
    assert "--attention-backend trtllm_mha" in joined
    assert "--mm-attention-backend fa4" in joined
    assert "--mm-feature-transport cuda_ipc" in joined
    assert "--mm-preprocess-cache-size-mb 8192" in joined
    assert "--enable-multimodal" in args
    assert "--enable-metrics" in args
    assert "--flashinfer-allreduce-fusion-backend auto" in joined
    assert "--enable-flashinfer-allreduce-fusion" not in args


def test_sglang_image_applies_fused_patch_and_records_both_fixes() -> None:
    dockerfile = deploy_fleet.render_sglang_dockerfile(fleet_spec()).decode()

    assert "git apply --check /tmp/fused-kernel.patch" in dockerfile
    assert "python3 /tmp/verify-draft-mrope.py" in dockerfile
    assert 'ai.sglang.fix.draft_extend_pr="34154"' in dockerfile
    assert 'ai.sglang.fix.fused_kernel_pr="35744"' in dockerfile


def test_model_unit_runs_only_the_patched_sglang_image() -> None:
    unit = deploy_fleet.render_model_unit(fleet_spec()).decode()

    assert "SGLang service" in unit
    assert "gb300-sglang-qwen38-mrope:20260821" in unit
    assert "python3 -m sglang.launch_server" in unit
    assert "vllm" not in unit.lower()


def test_bootstrap_does_not_upgrade_a_working_gpu_driver() -> None:
    source = (ROOT / "scripts/deploy-fleet.py").read_text()

    assert "if ! command -v nvidia-smi" in source
    assert "packages+=({driver_package})" in source
    assert "channel.set_combine_stderr(True)" in source


def test_relay_migrates_legacy_units_and_checks_the_new_listener_pid() -> None:
    source = (ROOT / "scripts/deploy-fleet.py").read_text()

    assert "legacy_relay_targets" in source
    assert "systemctl disable --now" in source
    assert "systemctl show --property MainPID" in source
    assert 'grep -Fq "pid=$main_pid,"' in source
