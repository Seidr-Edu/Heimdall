from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from heimdall.manifests.services import build_step_manifest_payload
from heimdall.models import (
    ALL_STEPS,
    STEP_ANDVARI,
    STEP_BROKK,
    STEP_EITRI,
    STEP_KVASIR,
    STEP_LIDSKJALV_GENERATED,
    STEP_LIDSKJALV_ORIGINAL,
    ArtifactRecord,
    DockerMount,
    PipelineConfig,
    ResolvedImages,
    RuntimeConfig,
    StepDefinition,
    StepPrepared,
    StepStatus,
)
from heimdall.reporting import load_report
from heimdall.simpleyaml import dumps
from heimdall.utils import ensure_directory, write_text


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
        depends_on=(STEP_EITRI,),
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


def prepare_step(
    step: str, context: AdapterContext, *, stage_inputs: bool = True
) -> StepPrepared:
    definition = STEP_DEFINITIONS[step]
    service_root = context.run_root / "services" / definition.service_dir_name
    config_dir = service_root / "config"
    run_dir = service_root / "run"
    config_path = config_dir / "manifest.yaml"

    ensure_directory(service_root, 0o755)
    ensure_directory(config_dir, 0o755)
    ensure_directory(run_dir, 0o777)
    payload: dict[str, object]
    env: dict[str, str]
    mounts: tuple[DockerMount, ...]
    image_ref: str
    resolved_image_id: str
    provider_bin_source: Path | None = None
    provider_bin_dest: Path | None = None
    provider_seed_source: Path | None = None
    provider_seed_dest: Path | None = None

    if step == STEP_BROKK:
        payload = build_step_manifest_payload(step, context)
        env = {"BROKK_MANIFEST": "/run/config/manifest.yaml"}
        mounts = (
            DockerMount(config_dir, "/run/config", True),
            DockerMount(run_dir, "/run", False),
        )
        image_ref = context.config.images.brokk
        resolved_image_id = context.resolved_images.brokk
    elif step == STEP_EITRI:
        payload = build_step_manifest_payload(step, context)
        env = {"EITRI_MANIFEST": "/run/config/manifest.yaml"}
        mounts = (
            DockerMount(_brokk_original_repo(context.run_root), "/input/repo", True),
            DockerMount(config_dir, "/run/config", True),
            DockerMount(run_dir, "/run", False),
        )
        image_ref = context.config.images.eitri
        resolved_image_id = context.resolved_images.eitri
    elif step == STEP_ANDVARI:
        payload = build_step_manifest_payload(step, context)
        input_model_dir = service_root / "input" / "model"
        ensure_directory(input_model_dir, 0o755)
        if stage_inputs:
            source_diagram = _eitri_diagram(context.run_root)
            destination_diagram = input_model_dir / "diagram.puml"
            shutil.copy2(source_diagram, destination_diagram)
            destination_diagram.chmod(0o644)
        provider_bin_dir = service_root / "input" / "provider-bin"
        provider_seed_dir = service_root / "input" / "provider-seed"
        env = {"ANDVARI_MANIFEST": "/run/config/manifest.yaml"}
        mounts = (
            DockerMount(input_model_dir, "/input/model", True),
            DockerMount(config_dir, "/run/config", True),
            DockerMount(run_dir, "/run", False),
            DockerMount(provider_bin_dir, "/opt/provider/bin", True),
            DockerMount(provider_seed_dir, "/opt/provider-seed/codex-home", True),
        )
        image_ref = context.config.images.andvari
        resolved_image_id = context.resolved_images.andvari
        provider_bin_source = context.runtime.codex_bin_dir
        provider_bin_dest = provider_bin_dir
        provider_seed_source = context.runtime.codex_home_dir
        provider_seed_dest = provider_seed_dir
    elif step == STEP_KVASIR:
        payload = build_step_manifest_payload(step, context)
        provider_bin_dir = service_root / "input" / "provider-bin"
        provider_seed_dir = service_root / "input" / "provider-seed"
        env = {"KVASIR_MANIFEST": "/run/config/manifest.yaml"}
        mounts = (
            DockerMount(
                _brokk_original_repo(context.run_root), "/input/original-repo", True
            ),
            DockerMount(
                _andvari_generated_repo(context.run_root), "/input/generated-repo", True
            ),
            DockerMount(_eitri_model_dir(context.run_root), "/input/model", True),
            DockerMount(config_dir, "/run/config", True),
            DockerMount(run_dir, "/run", False),
            DockerMount(provider_bin_dir, "/opt/provider/bin", True),
            DockerMount(provider_seed_dir, "/opt/provider-seed/codex-home", True),
        )
        image_ref = context.config.images.kvasir
        resolved_image_id = context.resolved_images.kvasir
        provider_bin_source = context.runtime.codex_bin_dir
        provider_bin_dest = provider_bin_dir
        provider_seed_source = context.runtime.codex_home_dir
        provider_seed_dest = provider_seed_dir
    else:
        generated = step == STEP_LIDSKJALV_GENERATED
        payload = build_step_manifest_payload(step, context)
        env = {"LIDSKJALV_MANIFEST": "/run/config/manifest.yaml"}
        if not context.config.lidskjalv.skip_sonar:
            if context.runtime.sonar_host_url is not None:
                env["SONAR_HOST_URL"] = context.runtime.sonar_host_url
            sonar_token = _sonar_token()
            if sonar_token is not None:
                env["SONAR_TOKEN"] = sonar_token
            if context.runtime.sonar_organization is not None:
                env["SONAR_ORGANIZATION"] = context.runtime.sonar_organization
        input_repo = (
            _andvari_generated_repo(context.run_root)
            if generated
            else _brokk_original_repo(context.run_root)
        )
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
        provider_bin_source=provider_bin_source,
        provider_bin_dest=provider_bin_dest,
        provider_seed_source=provider_seed_source,
        provider_seed_dest=provider_seed_dest,
    )


def classify_report(
    step: str, report_path: Path
) -> tuple[StepStatus, str | None, dict[str, ArtifactRecord]]:
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
        return (
            report.get("status") == "passed"
            and report.get("behavioral_verdict") == "pass"
        )
    return report.get("status") == "passed"


def _classify_reason(step: str, report: dict[str, object]) -> str | None:
    reason = report.get("reason")
    if reason:
        return str(reason)
    if (
        step == STEP_KVASIR
        and report.get("status") == "passed"
        and report.get("behavioral_verdict") != "pass"
    ):
        return "behavioral-verdict-not-pass"
    return None


def _artifact_records(step: str, report_path: Path) -> dict[str, ArtifactRecord]:
    run_dir = report_path.parent.parent
    records: dict[str, ArtifactRecord] = {}
    if step == STEP_BROKK:
        source_manifest = run_dir / "inputs" / "source-manifest.json"
        original_repo = run_dir / "artifacts" / "original-repo"
        if source_manifest.exists():
            records["source_manifest"] = ArtifactRecord(
                owner=step, path=str(source_manifest)
            )
        if original_repo.exists():
            records["original_repo"] = ArtifactRecord(
                owner=step, path=str(original_repo)
            )
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
            records["generated_repo"] = ArtifactRecord(
                owner=step, path=str(generated_repo)
            )
        if logs_dir.exists():
            records["andvari_logs"] = ArtifactRecord(owner=step, path=str(logs_dir))
        if report_dir.exists():
            records["andvari_report_dir"] = ArtifactRecord(
                owner=step, path=str(report_dir)
            )
    elif step == STEP_KVASIR:
        records["kvasir_report"] = ArtifactRecord(owner=step, path=str(report_path))
    elif step == STEP_LIDSKJALV_ORIGINAL:
        records["lidskjalv_original_report"] = ArtifactRecord(
            owner=step, path=str(report_path)
        )
    elif step == STEP_LIDSKJALV_GENERATED:
        records["lidskjalv_generated_report"] = ArtifactRecord(
            owner=step, path=str(report_path)
        )
    return records


def upstream_report_dependencies(step: str, run_root: Path) -> dict[str, Path]:
    base = run_root / "services"
    mapping: dict[str, Path] = {}
    for dependency in STEP_DEFINITIONS[step].depends_on:
        service_dir = STEP_DEFINITIONS[dependency].service_dir_name
        rel_path = STEP_DEFINITIONS[dependency].report_relative_path
        mapping[dependency] = base / service_dir / "run" / rel_path
    return mapping


def _brokk_original_repo(run_root: Path) -> Path:
    return run_root / "services" / "brokk" / "run" / "artifacts" / "original-repo"


def _eitri_diagram(run_root: Path) -> Path:
    return (
        run_root / "services" / "eitri" / "run" / "artifacts" / "model" / "diagram.puml"
    )


def _eitri_model_dir(run_root: Path) -> Path:
    return run_root / "services" / "eitri" / "run" / "artifacts" / "model"


def _andvari_generated_repo(run_root: Path) -> Path:
    return run_root / "services" / "andvari" / "run" / "artifacts" / "generated-repo"


def _sonar_token() -> str | None:
    import os

    return os.environ.get("SONAR_TOKEN")
