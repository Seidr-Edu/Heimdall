from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil

from heimdall.manifest import derive_lidskjalv_defaults
from heimdall.models import (
    ALL_STEPS,
    ArtifactRecord,
    DockerMount,
    PipelineConfig,
    ResolvedImages,
    RuntimeConfig,
    STEP_ANDVARI,
    STEP_BROKK,
    STEP_EITRI,
    STEP_KVASIR,
    STEP_LIDSKJALV_GENERATED,
    STEP_LIDSKJALV_ORIGINAL,
    StepDefinition,
    StepPrepared,
)
from heimdall.reporting import load_report
from heimdall.simpleyaml import dumps
from heimdall.utils import ensure_directory, read_json, write_text


@dataclass(frozen=True)
class AdapterContext:
    config: PipelineConfig
    runtime: RuntimeConfig
    run_root: Path
    resolved_images: ResolvedImages


STEP_DEFINITIONS: dict[str, StepDefinition] = {
    STEP_BROKK: StepDefinition(
        name=STEP_BROKK,
        depends_on=(),
        service_dir_name="brokk",
        report_relative_path="outputs/run_report.json",
    ),
    STEP_EITRI: StepDefinition(
        name=STEP_EITRI,
        depends_on=(STEP_BROKK,),
        service_dir_name="eitri",
        report_relative_path="outputs/run_report.json",
    ),
    STEP_LIDSKJALV_ORIGINAL: StepDefinition(
        name=STEP_LIDSKJALV_ORIGINAL,
        depends_on=(STEP_BROKK,),
        service_dir_name="lidskjalv-original",
        report_relative_path="outputs/run_report.json",
    ),
    STEP_ANDVARI: StepDefinition(
        name=STEP_ANDVARI,
        depends_on=(STEP_EITRI, STEP_LIDSKJALV_ORIGINAL),
        service_dir_name="andvari",
        report_relative_path="outputs/run_report.json",
    ),
    STEP_KVASIR: StepDefinition(
        name=STEP_KVASIR,
        depends_on=(STEP_BROKK, STEP_EITRI, STEP_ANDVARI),
        service_dir_name="kvasir",
        report_relative_path="outputs/test_port.json",
    ),
    STEP_LIDSKJALV_GENERATED: StepDefinition(
        name=STEP_LIDSKJALV_GENERATED,
        depends_on=(STEP_ANDVARI,),
        service_dir_name="lidskjalv-generated",
        report_relative_path="outputs/run_report.json",
    ),
}


def step_definitions() -> dict[str, StepDefinition]:
    return dict(STEP_DEFINITIONS)


def topological_steps() -> tuple[str, ...]:
    return ALL_STEPS


def prepare_step(step: str, context: AdapterContext, *, stage_inputs: bool = True) -> StepPrepared:
    definition = STEP_DEFINITIONS[step]
    service_root = context.run_root / "services" / definition.service_dir_name
    config_dir = service_root / "config"
    run_dir = service_root / "run"
    config_path = config_dir / "manifest.yaml"

    ensure_directory(service_root, 0o755)
    ensure_directory(config_dir, 0o755)
    ensure_directory(run_dir, 0o777)

    if step == STEP_BROKK:
        payload = {
            "version": 1,
            "run_id": context.config.run_id,
            "repo_url": context.config.source.repo_url,
            "commit_sha": context.config.source.commit_sha,
        }
        env = {"BROKK_MANIFEST": "/run/config/manifest.yaml"}
        mounts = (
            DockerMount(config_dir, "/run/config", True),
            DockerMount(run_dir, "/run", False),
        )
        image_ref = context.config.images.brokk
        resolved_image_id = context.resolved_images.brokk
    elif step == STEP_EITRI:
        payload = {
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
        env = {"EITRI_MANIFEST": "/run/config/manifest.yaml"}
        mounts = (
            DockerMount(_brokk_original_repo(context.run_root), "/input/repo", True),
            DockerMount(config_dir, "/run/config", True),
            DockerMount(run_dir, "/run", False),
        )
        image_ref = context.config.images.eitri
        resolved_image_id = context.resolved_images.eitri
    elif step == STEP_ANDVARI:
        payload = {
            "version": 1,
            "run_id": context.config.run_id,
            "adapter": "codex",
            "gating_mode": context.config.andvari.gating_mode,
            "max_iter": context.config.andvari.max_iter,
            "max_gate_revisions": context.config.andvari.max_gate_revisions,
            "model_gate_timeout_sec": context.config.andvari.model_gate_timeout_sec,
            "diagram_relpath": "diagram.puml",
        }
        input_model_dir = service_root / "input" / "model"
        ensure_directory(input_model_dir, 0o755)
        if stage_inputs:
            source_diagram = _eitri_diagram(context.run_root)
            destination_diagram = input_model_dir / "diagram.puml"
            shutil.copy2(source_diagram, destination_diagram)
            destination_diagram.chmod(0o644)
        env = {"ANDVARI_MANIFEST": "/run/config/manifest.yaml"}
        mounts = (
            DockerMount(input_model_dir, "/input/model", True),
            DockerMount(config_dir, "/run/config", True),
            DockerMount(run_dir, "/run", False),
            DockerMount(context.runtime.codex_bin_dir, "/opt/provider/bin", True),
            DockerMount(context.runtime.codex_home_dir, "/opt/provider-seed/codex-home", True),
        )
        image_ref = context.config.images.andvari
        resolved_image_id = context.resolved_images.andvari
    elif step == STEP_KVASIR:
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
            payload["write_scope_ignore_prefixes"] = list(context.config.kvasir.write_scope_ignore_prefixes)
        env = {"KVASIR_MANIFEST": "/run/config/manifest.yaml"}
        mounts = (
            DockerMount(_brokk_original_repo(context.run_root), "/input/original-repo", True),
            DockerMount(_andvari_generated_repo(context.run_root), "/input/generated-repo", True),
            DockerMount(_eitri_model_dir(context.run_root), "/input/model", True),
            DockerMount(config_dir, "/run/config", True),
            DockerMount(run_dir, "/run", False),
            DockerMount(context.runtime.codex_bin_dir, "/opt/provider/bin", True),
            DockerMount(context.runtime.codex_home_dir, "/opt/provider-seed/codex-home", True),
        )
        image_ref = context.config.images.kvasir
        resolved_image_id = context.resolved_images.kvasir
    else:
        generated = step == STEP_LIDSKJALV_GENERATED
        defaults = _derive_scan_defaults(context)
        target_config = context.config.lidskjalv.generated if generated else context.config.lidskjalv.original
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
        env = {"LIDSKJALV_MANIFEST": "/run/config/manifest.yaml"}
        if not context.config.lidskjalv.skip_sonar:
            if context.runtime.sonar_host_url is not None:
                env["SONAR_HOST_URL"] = context.runtime.sonar_host_url
            sonar_token = _sonar_token()
            if sonar_token is not None:
                env["SONAR_TOKEN"] = sonar_token
            if context.runtime.sonar_organization is not None:
                env["SONAR_ORGANIZATION"] = context.runtime.sonar_organization
        input_repo = _andvari_generated_repo(context.run_root) if generated else _brokk_original_repo(context.run_root)
        mounts = (
            DockerMount(input_repo, "/input/repo", True),
            DockerMount(config_dir, "/run/config", True),
            DockerMount(run_dir, "/run", False),
        )
        image_ref = context.config.images.lidskjalv
        resolved_image_id = context.resolved_images.lidskjalv

    manifest_text = dumps(payload)
    write_text(config_path, manifest_text)
    report_path = run_dir / definition.report_relative_path
    return StepPrepared(
        definition=definition,
        configured_image_ref=image_ref,
        resolved_image_id=resolved_image_id,
        service_root=service_root,
        run_dir=run_dir,
        config_dir=config_dir,
        config_path=config_path,
        report_path=report_path,
        manifest_payload=payload,
        manifest_text=manifest_text,
        env=env,
        mounts=mounts,
    )


def classify_report(step: str, report_path: Path) -> tuple[str, str | None, dict[str, ArtifactRecord]]:
    report = load_report(report_path)
    report_status = str(report.get("status", "")).strip() or None
    reason = _classify_reason(step, report)
    success = _is_success(step, report)
    if success:
        return "passed", reason, _artifact_records(step, report_path)
    if report_status == "error":
        return "error", reason, _artifact_records(step, report_path)
    return "failed", reason, _artifact_records(step, report_path)


def _is_success(step: str, report: dict[str, object]) -> bool:
    if step == STEP_KVASIR:
        return report.get("status") == "passed" and report.get("behavioral_verdict") == "pass"
    return report.get("status") == "passed"


def _classify_reason(step: str, report: dict[str, object]) -> str | None:
    reason = report.get("reason")
    if reason:
        return str(reason)
    if step == STEP_KVASIR and report.get("status") == "passed" and report.get("behavioral_verdict") != "pass":
        return "behavioral-verdict-not-pass"
    return None


def _artifact_records(step: str, report_path: Path) -> dict[str, ArtifactRecord]:
    run_dir = report_path.parent.parent
    records: dict[str, ArtifactRecord] = {}
    if step == STEP_BROKK:
        source_manifest = run_dir / "inputs" / "source-manifest.json"
        original_repo = run_dir / "artifacts" / "original-repo"
        if source_manifest.exists():
            records["source_manifest"] = ArtifactRecord(owner=step, path=str(source_manifest))
        if original_repo.exists():
            records["original_repo"] = ArtifactRecord(owner=step, path=str(original_repo))
    elif step == STEP_EITRI:
        diagram = run_dir / "artifacts" / "model" / "diagram.puml"
        logs_dir = run_dir / "artifacts" / "model" / "logs"
        if diagram.exists():
            records["model_diagram"] = ArtifactRecord(owner=step, path=str(diagram))
        if logs_dir.exists():
            records["model_logs"] = ArtifactRecord(owner=step, path=str(logs_dir))
    elif step == STEP_ANDVARI:
        generated_repo = run_dir / "artifacts" / "generated-repo"
        logs_dir = run_dir / "artifacts" / "andvari" / "logs"
        report_dir = run_dir / "artifacts" / "andvari" / "report"
        if generated_repo.exists():
            records["generated_repo"] = ArtifactRecord(owner=step, path=str(generated_repo))
        if logs_dir.exists():
            records["andvari_logs"] = ArtifactRecord(owner=step, path=str(logs_dir))
        if report_dir.exists():
            records["andvari_report_dir"] = ArtifactRecord(owner=step, path=str(report_dir))
    elif step == STEP_KVASIR:
        records["kvasir_report"] = ArtifactRecord(owner=step, path=str(report_path))
    elif step == STEP_LIDSKJALV_ORIGINAL:
        records["lidskjalv_original_report"] = ArtifactRecord(owner=step, path=str(report_path))
    elif step == STEP_LIDSKJALV_GENERATED:
        records["lidskjalv_generated_report"] = ArtifactRecord(owner=step, path=str(report_path))
    return records


def upstream_report_dependencies(step: str, run_root: Path) -> dict[str, Path]:
    base = run_root / "services"
    mapping: dict[str, Path] = {}
    for dependency in STEP_DEFINITIONS[step].depends_on:
        service_dir = STEP_DEFINITIONS[dependency].service_dir_name
        rel_path = STEP_DEFINITIONS[dependency].report_relative_path
        mapping[dependency] = base / service_dir / "run" / rel_path
    return mapping


def brokk_source_manifest(run_root: Path) -> Path:
    return run_root / "services" / "brokk" / "run" / "inputs" / "source-manifest.json"


def _derive_scan_defaults(context: AdapterContext) -> dict[str, str]:
    source_manifest_path = brokk_source_manifest(context.run_root)
    repo_url = context.config.source.repo_url
    if source_manifest_path.is_file():
        source_manifest = read_json(source_manifest_path)
        repo_url = str(source_manifest.get("repo_url", repo_url))
    return derive_lidskjalv_defaults(repo_url)


def _brokk_original_repo(run_root: Path) -> Path:
    return run_root / "services" / "brokk" / "run" / "artifacts" / "original-repo"


def _eitri_diagram(run_root: Path) -> Path:
    return run_root / "services" / "eitri" / "run" / "artifacts" / "model" / "diagram.puml"


def _eitri_model_dir(run_root: Path) -> Path:
    return run_root / "services" / "eitri" / "run" / "artifacts" / "model"


def _andvari_generated_repo(run_root: Path) -> Path:
    return run_root / "services" / "andvari" / "run" / "artifacts" / "generated-repo"


def _sonar_token() -> str | None:
    import os

    return os.environ.get("SONAR_TOKEN")
