from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

from heimdall.manifests.pipeline import derive_lidskjalv_defaults
from heimdall.models import (
    STEP_ANDVARI,
    STEP_BROKK,
    STEP_EITRI,
    STEP_EITRI_GENERATED,
    STEP_KVASIR,
    STEP_LIDSKJALV_GENERATED,
    STEP_LIDSKJALV_ORIGINAL,
    STEP_MIMIR,
)
from heimdall.utils import read_json

if TYPE_CHECKING:
    from heimdall.adapters import AdapterContext


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
    if step in {STEP_EITRI, STEP_EITRI_GENERATED}:
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
        if context.config.eitri.writers:
            payload["writers"] = context.config.eitri.writers
        return payload
    if step == STEP_ANDVARI:
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
    if step == STEP_KVASIR:
        payload = {
            "version": 1,
            "run_id": context.config.run_id,
            "adapter": "codex",
            "diagram_relpath": "diagram.puml",
            "max_iter": context.config.kvasir.max_iter,
        }
        if context.config.kvasir.original_subdir is not None:
            payload["original_subdir"] = context.config.kvasir.original_subdir
        if context.config.kvasir.generated_subdir is not None:
            payload["generated_subdir"] = context.config.kvasir.generated_subdir
        if context.config.kvasir.write_scope_ignore_prefixes:
            payload["write_scope_ignore_prefixes"] = list(
                context.config.kvasir.write_scope_ignore_prefixes
            )
        return payload
    if step == STEP_MIMIR:
        snapshot_sources = mimir_snapshot_sources(context.run_root)
        if "original" not in snapshot_sources:
            return {
                "version": 1,
                "run_id": context.config.run_id,
                "mode": "analytics",
                "baseline_label": "original",
                "baseline_snapshot_relpath": "original/model_snapshot.json",
                "candidates": [
                    {
                        "label": "andvari_generated",
                        "snapshot_relpath": "andvari_generated/model_snapshot.json",
                    }
                ],
            }
        candidates = [
            {"label": label, "snapshot_relpath": f"{label}/model_snapshot.json"}
            for label in snapshot_sources
            if label != "original"
        ]
        if not candidates:
            candidates = [
                {
                    "label": "andvari_generated",
                    "snapshot_relpath": "andvari_generated/model_snapshot.json",
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

    generated = step == STEP_LIDSKJALV_GENERATED
    defaults = _derive_scan_defaults(context)
    target_config = (
        context.config.lidskjalv.generated
        if generated
        else context.config.lidskjalv.original
    )
    scan_label = "generated" if generated else "original"
    payload = {
        "version": 1,
        "run_id": context.config.run_id,
        "scan_label": scan_label,
        "project_key": target_config.project_key or defaults[f"{scan_label}_key"],
        "project_name": target_config.project_name or defaults[f"{scan_label}_name"],
        "skip_sonar": context.config.lidskjalv.skip_sonar,
    }
    if target_config.repo_subdir is not None:
        payload["repo_subdir"] = target_config.repo_subdir
    return payload


def build_step_runtime_hints(
    step: str, context: AdapterContext
) -> dict[str, object] | None:
    if step != STEP_KVASIR:
        return None

    hints: dict[str, object] = {}
    original = _lidskjalv_build_hint(context.run_root, generated=False)
    if original:
        hints["original"] = original
    generated = _lidskjalv_build_hint(context.run_root, generated=True)
    if generated:
        hints["generated"] = generated
    return hints or None


def brokk_source_manifest(run_root: Path) -> Path:
    return run_root / "services" / "brokk" / "run" / "inputs" / "source-manifest.json"


def mimir_snapshot_sources(run_root: Path) -> dict[str, Path]:
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

    generated = (
        run_root
        / "services"
        / "eitri-generated"
        / "run"
        / "artifacts"
        / "model"
        / "model_snapshot.json"
    )
    if generated.is_file():
        sources["andvari_generated"] = generated

    services_root = run_root / "services"
    if services_root.is_dir():
        for service_dir in sorted(services_root.iterdir()):
            if not service_dir.is_dir():
                continue
            name = service_dir.name
            if not name.startswith("eitri-") or name == "eitri-generated":
                continue
            snapshot = (
                service_dir / "run" / "artifacts" / "model" / "model_snapshot.json"
            )
            if not snapshot.is_file():
                continue
            label = name.removeprefix("eitri-").replace("-", "_")
            sources.setdefault(label, snapshot)
    return dict(sorted(sources.items()))


def _derive_scan_defaults(context: AdapterContext) -> dict[str, str]:
    source_manifest_path = brokk_source_manifest(context.run_root)
    repo_url = context.config.source.repo_url
    if source_manifest_path.is_file():
        source_manifest = read_json(source_manifest_path)
        repo_url = str(source_manifest.get("repo_url", repo_url))
    return derive_lidskjalv_defaults(repo_url)


def _lidskjalv_build_hint(
    run_root: Path, *, generated: bool
) -> dict[str, object] | None:
    step = STEP_LIDSKJALV_GENERATED if generated else STEP_LIDSKJALV_ORIGINAL
    service_dir = "lidskjalv-generated" if generated else "lidskjalv-original"
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


def _optional_hint_str(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None
