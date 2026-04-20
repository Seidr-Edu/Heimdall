from __future__ import annotations

import copy
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

from heimdall.manifests.pipeline import derive_lidskjalv_defaults
from heimdall.models import (
    STEP_ANDVARI,
    STEP_ANDVARI_V2,
    STEP_ANDVARI_V3,
    STEP_BROKK,
    STEP_EITRI,
    STEP_EITRI_GENERATED,
    STEP_EITRI_GENERATED_V2,
    STEP_EITRI_GENERATED_V3,
    STEP_KVASIR,
    STEP_KVASIR_V2,
    STEP_KVASIR_V3,
    STEP_LIDSKJALV_GENERATED,
    STEP_LIDSKJALV_GENERATED_V2,
    STEP_LIDSKJALV_GENERATED_V3,
    STEP_LIDSKJALV_ORIGINAL,
    STEP_MIMIR,
    STEP_MIMIR_V2,
    STEP_MIMIR_V3,
)
from heimdall.utils import read_json

if TYPE_CHECKING:
    from heimdall.adapters import AdapterContext


ANDVARI_STEPS = (STEP_ANDVARI, STEP_ANDVARI_V2, STEP_ANDVARI_V3)
EITRI_GENERATED_STEPS = (
    STEP_EITRI_GENERATED,
    STEP_EITRI_GENERATED_V2,
    STEP_EITRI_GENERATED_V3,
)
KVASIR_STEPS = (STEP_KVASIR, STEP_KVASIR_V2, STEP_KVASIR_V3)
MIMIR_STEPS = (STEP_MIMIR, STEP_MIMIR_V2, STEP_MIMIR_V3)
LIDSKJALV_GENERATED_STEPS = (
    STEP_LIDSKJALV_GENERATED,
    STEP_LIDSKJALV_GENERATED_V2,
    STEP_LIDSKJALV_GENERATED_V3,
)
KVASIR_BUILTIN_WRITE_SCOPE_IGNORE_PREFIXES = frozenset(
    {
        "./completion/proof/logs",
        "./.mvn_repo",
        "./.m2",
        "./.gradle",
        "./target",
        "./build",
    }
)


def build_step_manifest_payload(
    step: str, context: AdapterContext
) -> dict[str, object]:
    if step == STEP_BROKK:
        return {
            "version": 1,
            "run_id": context.config.run_id,
            "repo_url": context.config.source.repo_url,
            "commit_sha": context.config.source.commit_sha,
        }
    if step in {STEP_EITRI, *EITRI_GENERATED_STEPS}:
        payload: dict[str, object] = {
            "version": 1,
            "run_id": context.config.run_id,
            "source_relpaths": list(context.config.eitri.source_relpaths),
        }
        if context.config.eitri.parser_extension is not None:
            payload["parser_extension"] = context.config.eitri.parser_extension
        if context.config.eitri.writer_extension is not None:
            payload["writer_extension"] = context.config.eitri.writer_extension
        if context.config.eitri.verbose:
            payload["verbose"] = True
        writers: Mapping[str, object] = context.config.eitri.writers
        if step in EITRI_GENERATED_STEPS:
            payload["writers"] = _generated_eitri_writers(writers)
        elif step == STEP_EITRI:
            payload["writers"] = _original_eitri_writers(writers)
        elif writers:
            payload["writers"] = dict(writers)
        return payload
    if step in ANDVARI_STEPS:
        return {
            "version": 1,
            "run_id": context.config.run_id,
            "adapter": "codex",
            "gating_mode": context.config.andvari.gating_mode,
            "max_iter": context.config.andvari.max_iter,
            "max_gate_revisions": context.config.andvari.max_gate_revisions,
            "model_gate_timeout_sec": context.config.andvari.model_gate_timeout_sec,
            "diagram_relpath": "diagram.puml",
        }
    if step in KVASIR_STEPS:
        payload = {
            "version": 1,
            "run_id": context.config.run_id,
            "adapter": "codex",
            "diagram_relpath": "diagram.puml",
            "max_iter": context.config.kvasir.max_iter,
            "runner_timeout_sec": context.config.kvasir.runner_timeout_sec,
        }
        if context.config.kvasir.original_subdir is not None:
            payload["original_subdir"] = context.config.kvasir.original_subdir
        if context.config.kvasir.generated_subdir is not None:
            payload["generated_subdir"] = context.config.kvasir.generated_subdir
        write_scope_ignore_prefixes = _service_manifest_kvasir_write_scope_prefixes(
            context.config.kvasir.write_scope_ignore_prefixes
        )
        if write_scope_ignore_prefixes:
            payload["write_scope_ignore_prefixes"] = write_scope_ignore_prefixes
        return payload
    if step in MIMIR_STEPS:
        snapshot_sources = mimir_snapshot_sources(step, context.run_root)
        candidate_label = _mimir_candidate_label(step)
        candidates = [
            {"label": label, "snapshot_relpath": f"{label}/model_snapshot.json"}
            for label in snapshot_sources
            if label != "original"
        ]
        if not candidates:
            candidates = [
                {
                    "label": candidate_label,
                    "snapshot_relpath": f"{candidate_label}/model_snapshot.json",
                }
            ]
        return {
            "version": 1,
            "run_id": context.config.run_id,
            "mode": "analytics",
            "baseline_label": "original",
            "baseline_snapshot_relpath": "original/model_snapshot.json",
            "candidates": candidates,
        }

    generated = step in LIDSKJALV_GENERATED_STEPS
    defaults = _derive_scan_defaults(context)
    target_config = (
        context.config.lidskjalv.generated
        if generated
        else context.config.lidskjalv.original
    )
    scan_label = _lidskjalv_scan_label(step) if generated else "original"
    payload = {
        "version": 1,
        "run_id": context.config.run_id,
        "scan_label": scan_label,
        "project_key": _lidskjalv_project_key(
            target_config.project_key, defaults["generated_key"], step
        )
        if generated
        else target_config.project_key or defaults["original_key"],
        "project_name": _lidskjalv_project_name(
            target_config.project_name, defaults["generated_name"], step
        )
        if generated
        else target_config.project_name or defaults["original_name"],
        "skip_sonar": context.config.lidskjalv.skip_sonar,
    }
    if target_config.repo_subdir is not None:
        payload["repo_subdir"] = target_config.repo_subdir
    return payload


def build_step_runtime_hints(
    step: str, context: AdapterContext
) -> dict[str, object] | None:
    if step not in KVASIR_STEPS:
        return None

    hints: dict[str, object] = {}
    original = _lidskjalv_build_hint(context.run_root, STEP_LIDSKJALV_ORIGINAL)
    if original:
        hints["original"] = original
    generated = _lidskjalv_build_hint(
        context.run_root, _lidskjalv_generated_step_for_kvasir_step(step)
    )
    if generated:
        hints["generated"] = generated
    return hints or None


def _service_manifest_kvasir_write_scope_prefixes(
    prefixes: tuple[str, ...],
) -> list[str]:
    return [
        prefix
        for prefix in prefixes
        if _normalize_repo_relative_prefix(prefix)
        not in KVASIR_BUILTIN_WRITE_SCOPE_IGNORE_PREFIXES
    ]


def _normalize_repo_relative_prefix(prefix: str) -> str:
    collapsed = "/".join(
        part
        for part in prefix.strip().replace("\\", "/").split("/")
        if part not in ("", ".")
    )
    if not collapsed:
        return "."
    return f"./{collapsed}"


def brokk_source_manifest(run_root: Path) -> Path:
    return run_root / "services" / "brokk" / "run" / "inputs" / "source-manifest.json"


def mimir_snapshot_sources(step: str, run_root: Path) -> dict[str, Path]:
    sources: dict[str, Path] = {}
    original = (
        run_root
        / "services"
        / "eitri"
        / "run"
        / "artifacts"
        / "model"
        / "model_snapshot.json"
    )
    if original.is_file():
        sources["original"] = original

    candidate_label = _mimir_candidate_label(step)
    generated = (
        run_root
        / "services"
        / _eitri_generated_service_for_mimir_step(step)
        / "run"
        / "artifacts"
        / "model"
        / "model_snapshot.json"
    )
    if generated.is_file():
        sources[candidate_label] = generated
    return dict(sorted(sources.items()))


def _derive_scan_defaults(context: AdapterContext) -> dict[str, str]:
    source_manifest_path = brokk_source_manifest(context.run_root)
    repo_url = context.config.source.repo_url
    if source_manifest_path.is_file():
        source_manifest = read_json(source_manifest_path)
        repo_url = str(source_manifest.get("repo_url", repo_url))
    return derive_lidskjalv_defaults(repo_url)


def _lidskjalv_build_hint(run_root: Path, step: str) -> dict[str, object] | None:
    service_dir = _service_dir_for_step(step)
    report_path = (
        run_root / "services" / service_dir / "run" / "outputs" / "run_report.json"
    )
    if not report_path.is_file():
        return None

    try:
        report = read_json(report_path)
    except Exception:
        return None

    scan = report.get("scan")
    if not isinstance(scan, Mapping):
        return None

    hint: dict[str, object] = {}
    for field_name in ("build_tool", "build_jdk", "build_subdir", "java_version_hint"):
        value = _optional_hint_str(scan.get(field_name))
        if value is not None:
            hint[field_name] = value

    if not hint:
        return None

    hint["source"] = step
    return hint


def _branch_suffix(step: str) -> str:
    if step.endswith("-v2"):
        return "v2"
    if step.endswith("-v3"):
        return "v3"
    return ""


def _mimir_candidate_label(step: str) -> str:
    suffix = _branch_suffix(step)
    return "andvari_generated" if not suffix else f"andvari_generated_{suffix}"


def _eitri_generated_service_for_mimir_step(step: str) -> str:
    suffix = _branch_suffix(step)
    return "eitri-generated" if not suffix else f"eitri-generated-{suffix}"


def _lidskjalv_generated_step_for_kvasir_step(step: str) -> str:
    suffix = _branch_suffix(step)
    if suffix == "v2":
        return STEP_LIDSKJALV_GENERATED_V2
    if suffix == "v3":
        return STEP_LIDSKJALV_GENERATED_V3
    return STEP_LIDSKJALV_GENERATED


def _lidskjalv_scan_label(step: str) -> str:
    suffix = _branch_suffix(step)
    return "generated" if not suffix else f"generated-{suffix}"


def _lidskjalv_project_key(configured: str | None, default: str, step: str) -> str:
    suffix = _branch_suffix(step)
    base = configured or default
    return base if not suffix else f"{base}_{suffix}"


def _lidskjalv_project_name(configured: str | None, default: str, step: str) -> str:
    suffix = _branch_suffix(step)
    base = configured or default
    return base if not suffix else f"{base} {suffix}"


def _service_dir_for_step(step: str) -> str:
    return {
        STEP_LIDSKJALV_ORIGINAL: "lidskjalv-original",
        STEP_LIDSKJALV_GENERATED: "lidskjalv-generated",
        STEP_LIDSKJALV_GENERATED_V2: "lidskjalv-generated-v2",
        STEP_LIDSKJALV_GENERATED_V3: "lidskjalv-generated-v3",
    }[step]


def _generated_eitri_writers(configured: Mapping[str, object]) -> dict[str, object]:
    writers: dict[str, object] = copy.deepcopy(dict(configured))
    plantuml = writers.get("plantuml")
    if plantuml is None:
        writers["plantuml"] = {"generateDegradedDiagrams": False}
        return writers
    if isinstance(plantuml, Mapping):
        plantuml_config = copy.deepcopy(dict(plantuml))
        plantuml_config["generateDegradedDiagrams"] = False
        writers["plantuml"] = plantuml_config
    return writers


def _original_eitri_writers(configured: Mapping[str, object]) -> dict[str, object]:
    writers: dict[str, object] = copy.deepcopy(dict(configured))
    plantuml = writers.get("plantuml")
    if plantuml is None:
        writers["plantuml"] = {"generateDegradedDiagrams": True}
        return writers
    if isinstance(plantuml, Mapping):
        plantuml_config = copy.deepcopy(dict(plantuml))
        plantuml_config["generateDegradedDiagrams"] = True
        writers["plantuml"] = plantuml_config
    return writers


def _optional_hint_str(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None
