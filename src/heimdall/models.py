from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

PullPolicy = Literal["if-missing", "always", "never"]
StepStatus = Literal["pending", "running", "passed", "failed", "error", "blocked", "skipped"]

STEP_BROKK = "brokk"
STEP_EITRI = "eitri"
STEP_ANDVARI = "andvari"
STEP_KVASIR = "kvasir"
STEP_LIDSKJALV_ORIGINAL = "lidskjalv-original"
STEP_LIDSKJALV_GENERATED = "lidskjalv-generated"

ALL_STEPS = (
    STEP_BROKK,
    STEP_EITRI,
    STEP_LIDSKJALV_ORIGINAL,
    STEP_ANDVARI,
    STEP_KVASIR,
    STEP_LIDSKJALV_GENERATED,
)


@dataclass(frozen=True)
class SourceConfig:
    repo_url: str
    commit_sha: str


@dataclass(frozen=True)
class ImageRefs:
    brokk: str
    eitri: str
    andvari: str
    kvasir: str
    lidskjalv: str


@dataclass(frozen=True)
class ResolvedImages:
    brokk: str
    eitri: str
    andvari: str
    kvasir: str
    lidskjalv: str


@dataclass(frozen=True)
class EitriConfig:
    source_relpaths: tuple[str, ...] = (".",)
    parser_extension: str | None = None
    writer_extension: str | None = None
    verbose: bool = False
    writers: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class AndvariConfig:
    gating_mode: str = "model"
    max_iter: int = 8
    max_gate_revisions: int = 3
    model_gate_timeout_sec: int = 120


@dataclass(frozen=True)
class KvasirConfig:
    original_subdir: str | None = None
    generated_subdir: str | None = None
    max_iter: int = 5
    write_scope_ignore_prefixes: tuple[str, ...] = ()


@dataclass(frozen=True)
class LidskjalvTargetConfig:
    repo_subdir: str | None = None
    project_key: str | None = None
    project_name: str | None = None


@dataclass(frozen=True)
class LidskjalvConfig:
    skip_sonar: bool = False
    sonar_wait_timeout_sec: int = 300
    sonar_wait_poll_sec: int = 5
    original: LidskjalvTargetConfig = field(default_factory=LidskjalvTargetConfig)
    generated: LidskjalvTargetConfig = field(default_factory=LidskjalvTargetConfig)


@dataclass(frozen=True)
class PipelineConfig:
    version: int
    run_id: str
    source: SourceConfig
    images: ImageRefs
    eitri: EitriConfig
    andvari: AndvariConfig
    kvasir: KvasirConfig
    lidskjalv: LidskjalvConfig


@dataclass(frozen=True)
class RuntimeConfig:
    runs_root: Path
    codex_bin_dir: Path
    codex_home_dir: Path
    pull_policy: PullPolicy
    sonar_host_url: str | None
    sonar_token_present: bool
    sonar_organization: str | None
    verbose: bool = False


@dataclass(frozen=True)
class DockerMount:
    host_path: Path
    container_path: str
    read_only: bool


@dataclass(frozen=True)
class StepDefinition:
    name: str
    depends_on: tuple[str, ...]
    service_dir_name: str
    report_relative_path: str


@dataclass(frozen=True)
class StepPrepared:
    definition: StepDefinition
    configured_image_ref: str
    resolved_image_id: str
    service_root: Path
    run_dir: Path
    config_dir: Path
    config_path: Path
    report_path: Path
    manifest_payload: dict[str, object]
    manifest_text: str
    env: dict[str, str]
    mounts: tuple[DockerMount, ...]


@dataclass
class StepState:
    status: StepStatus = "pending"
    reason: str | None = None
    blocked_by: list[str] = field(default_factory=list)
    started_at: str | None = None
    finished_at: str | None = None
    configured_image_ref: str | None = None
    resolved_image_id: str | None = None
    fingerprint: str | None = None
    report_path: str | None = None
    report_status: str | None = None


@dataclass
class ArtifactRecord:
    owner: str
    path: str


@dataclass
class StepResult:
    step: str
    status: StepStatus
    reason: str | None
    report_status: str | None
    report_path: Path | None
    fingerprint: str
    configured_image_ref: str
    resolved_image_id: str
    started_at: str
    finished_at: str
    blocked_by: list[str] = field(default_factory=list)
    artifacts: dict[str, ArtifactRecord] = field(default_factory=dict)
