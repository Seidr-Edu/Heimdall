from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from heimdall.manifests.pipeline import derive_lidskjalv_defaults
from heimdall.models import (
    STEP_ANDVARI,
    STEP_BROKK,
    STEP_EITRI,
    STEP_KVASIR,
    STEP_LIDSKJALV_GENERATED,
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
    if step == STEP_EITRI:
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
        "sonar_wait_timeout_sec": context.config.lidskjalv.sonar_wait_timeout_sec,
        "sonar_wait_poll_sec": context.config.lidskjalv.sonar_wait_poll_sec,
    }
    if target_config.repo_subdir is not None:
        payload["repo_subdir"] = target_config.repo_subdir
    return payload


def brokk_source_manifest(run_root: Path) -> Path:
    return run_root / "services" / "brokk" / "run" / "inputs" / "source-manifest.json"


def _derive_scan_defaults(context: AdapterContext) -> dict[str, str]:
    source_manifest_path = brokk_source_manifest(context.run_root)
    repo_url = context.config.source.repo_url
    if source_manifest_path.is_file():
        source_manifest = read_json(source_manifest_path)
        repo_url = str(source_manifest.get("repo_url", repo_url))
    return derive_lidskjalv_defaults(repo_url)
