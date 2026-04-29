from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

PullPolicy = Literal["if-missing", "always", "never"]
StepStatus = Literal[
    "pending", "running", "passed", "failed", "error", "blocked", "skipped"
]
JobStatus = Literal["pending", "running", "passed", "failed", "error"]

STEP_BROKK = "brokk"
STEP_EITRI = "eitri"
STEP_EITRI_GENERATED = "eitri-generated"
STEP_EITRI_GENERATED_V2 = "eitri-generated-v2"
STEP_EITRI_GENERATED_V3 = "eitri-generated-v3"
STEP_ANDVARI = "andvari"
STEP_ANDVARI_V2 = "andvari-v2"
STEP_ANDVARI_V3 = "andvari-v3"
STEP_MIMIR = "mimir"
STEP_MIMIR_V2 = "mimir-v2"
STEP_MIMIR_V3 = "mimir-v3"
STEP_KVASIR = "kvasir"
STEP_KVASIR_V2 = "kvasir-v2"
STEP_KVASIR_V3 = "kvasir-v3"
STEP_LIDSKJALV_ORIGINAL = "lidskjalv-original"
STEP_LIDSKJALV_GENERATED = "lidskjalv-generated"
STEP_LIDSKJALV_GENERATED_V2 = "lidskjalv-generated-v2"
STEP_LIDSKJALV_GENERATED_V3 = "lidskjalv-generated-v3"

ALL_STEPS = (
    STEP_BROKK,
    STEP_EITRI,
    STEP_LIDSKJALV_ORIGINAL,
    STEP_ANDVARI,
    STEP_EITRI_GENERATED,
    STEP_MIMIR,
    STEP_KVASIR,
    STEP_LIDSKJALV_GENERATED,
    STEP_ANDVARI_V2,
    STEP_EITRI_GENERATED_V2,
    STEP_MIMIR_V2,
    STEP_KVASIR_V2,
    STEP_LIDSKJALV_GENERATED_V2,
    STEP_ANDVARI_V3,
    STEP_EITRI_GENERATED_V3,
    STEP_MIMIR_V3,
    STEP_KVASIR_V3,
    STEP_LIDSKJALV_GENERATED_V3,
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
    mimir: str
    kvasir: str
    lidskjalv: str


@dataclass(frozen=True)
class ResolvedImages:
    brokk: str
    eitri: str
    andvari: str
    mimir: str
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
    runner_timeout_sec: int = 7200
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
    codex_host_bin_dir: Path
    codex_home_dir: Path
    pull_policy: PullPolicy
    sonar_host_url: str | None
    sonar_token_present: bool
    sonar_organization: str | None
    verbose: bool = False
    andvari_internal_network_name: str = ""
    andvari_proxy_url: str = ""


@dataclass(frozen=True)
class WorkerConfig:
    queue_root: Path
    runs_root: Path
    codex_bin_dir: Path
    codex_host_bin_dir: Path | None
    codex_home_dir: Path
    pull_policy: PullPolicy
    verbose: bool
    images: ImageRefs
    eitri: EitriConfig
    andvari: AndvariConfig
    kvasir: KvasirConfig
    lidskjalv: LidskjalvConfig
    andvari_internal_network_name: str = ""
    andvari_proxy_url: str = ""


@dataclass(frozen=True)
class QueueRequest:
    repo_url: str
    commit_sha: str
    eitri: dict[str, object] = field(default_factory=dict)
    andvari: dict[str, object] = field(default_factory=dict)
    kvasir: dict[str, object] = field(default_factory=dict)
    lidskjalv: dict[str, object] = field(default_factory=dict)
    version: int = 1


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
    order_after: tuple[str, ...] = ()


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
    provider_bin_source: Path | None = None
    provider_bin_dest: Path | None = None
    provider_seed_source: Path | None = None
    provider_seed_dest: Path | None = None


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
