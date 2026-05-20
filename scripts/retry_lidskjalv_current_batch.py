#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from heimdall.adapters import normalized_report_status  # noqa: E402
from heimdall.images import DockerError, run_container  # noqa: E402
from heimdall.manifests.pipeline import load_pipeline_manifest  # noqa: E402
from heimdall.reporting import load_report  # noqa: E402
from heimdall.simpleyaml import loads  # noqa: E402


DEFAULT_BATCH_START = "20260430T100001Z__ulisesbocchio_jasypt-spring-boot__2243cb80"
DEFAULT_BATCH_END = "20260508T095300Z__stealthcopter_AndroidNetworkTools__a82af8a5"
DEFAULT_OUTPUT_ROOT = Path("/srv/pipeline/retries/lidskjalv-current-batch")
DEFAULT_RUNS_ROOT = Path("/srv/pipeline/runs")
DEFAULT_WORKER_CONFIG = Path("/srv/pipeline/worker.yaml")
LIDSKJALV_STEPS = (
    "lidskjalv-original",
    "lidskjalv-generated",
    "lidskjalv-generated-v2",
    "lidskjalv-generated-v3",
)
SERVICE_REPORT_SUCCESS = "passed"
TARGET_FAILURE_STATUSES = frozenset({"failed", "error"})
REQUIRED_SONAR_ENV_VARS = ("SONAR_HOST_URL", "SONAR_TOKEN", "SONAR_ORGANIZATION")


@dataclass(frozen=True)
class StepSelection:
    run_id: str
    step: str
    source: str
    status: str
    reason: str | None
    project_key: str | None
    run_root: Path


@dataclass(frozen=True)
class ReplayContext:
    selection: StepSelection
    input_repo: Path
    manifest_payload: Mapping[str, object]
    manifest_text: str
    project_key: str
    requested_image_ref: str
    requested_image_source: str
    configured_image_ref: str | None
    timeout_sec: float | None
    service_root: Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Replay failed Lidskjalv submissions for the current experiment batch."
    )
    parser.add_argument(
        "--runs-root",
        type=Path,
        default=DEFAULT_RUNS_ROOT,
        help=f"Pipeline runs root (default: {DEFAULT_RUNS_ROOT})",
    )
    parser.add_argument(
        "--worker-config",
        type=Path,
        default=DEFAULT_WORKER_CONFIG,
        help=f"Worker config used to discover the default runs root (default: {DEFAULT_WORKER_CONFIG})",
    )
    parser.add_argument(
        "--batch-start",
        default=DEFAULT_BATCH_START,
        help=f"First run_id to include (default: {DEFAULT_BATCH_START})",
    )
    parser.add_argument(
        "--batch-end",
        default=DEFAULT_BATCH_END,
        help=f"Last run_id to include (default: {DEFAULT_BATCH_END})",
    )
    parser.add_argument(
        "--run-id",
        action="append",
        dest="run_ids",
        help="Limit retries to a specific run_id. Repeatable.",
    )
    parser.add_argument(
        "--step",
        action="append",
        choices=LIDSKJALV_STEPS,
        dest="steps",
        help="Limit retries to a specific lidskjalv step. Repeatable.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help=f"Stable root for standalone replay outputs (default: {DEFAULT_OUTPUT_ROOT})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List selected retry targets and planned output paths without running Docker.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replay even if a prior standalone retry already submitted successfully.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    runs_root = resolve_runs_root(args.runs_root, args.worker_config)
    selections = discover_targets(
        runs_root=runs_root,
        batch_start=args.batch_start,
        batch_end=args.batch_end,
        run_ids=tuple(args.run_ids or ()),
        steps=tuple(args.steps or LIDSKJALV_STEPS),
    )
    invocation_started_at = timestamp_slug()
    aggregate_entries: list[dict[str, object]] = []

    if not selections:
        print("No failed/error lidskjalv targets matched the requested scope.")
        write_invocation_summary(
            args.output_root,
            invocation_started_at,
            runs_root,
            aggregate_entries,
        )
        return 0

    if args.dry_run:
        for selection in selections:
            stable_dir = stable_step_output_dir(args.output_root, selection)
            print(
                f"{selection.run_id}\t{selection.step}\t{selection.status}\t"
                f"{selection.reason or ''}\t{stable_dir}"
            )
            aggregate_entries.append(
                {
                    "run_id": selection.run_id,
                    "step": selection.step,
                    "selector_source": selection.source,
                    "selected_status": selection.status,
                    "selected_reason": selection.reason,
                    "result": "dry-run",
                    "stable_output_dir": str(stable_dir),
                }
            )
        write_invocation_summary(
            args.output_root,
            invocation_started_at,
            runs_root,
            aggregate_entries,
        )
        return 0

    require_sonar_env()
    overall_success = True

    for selection in selections:
        stable_dir = stable_step_output_dir(args.output_root, selection)
        prior_success = load_prior_success(stable_dir)
        if prior_success is not None and not args.force:
            print(
                f"Skipping {selection.run_id} {selection.step}: prior submission success "
                f"at {prior_success['attempt_dir']}"
            )
            aggregate_entries.append(
                {
                    "run_id": selection.run_id,
                    "step": selection.step,
                    "selector_source": selection.source,
                    "selected_status": selection.status,
                    "selected_reason": selection.reason,
                    "result": "skipped_prior_success",
                    "stable_output_dir": str(stable_dir),
                    "attempt_dir": prior_success["attempt_dir"],
                }
            )
            continue

        attempt_dir = stable_dir / f"attempt-{timestamp_slug()}"
        try:
            context = build_replay_context(selection)
            attempt_entry = execute_replay_attempt(
                attempt_dir=attempt_dir,
                context=context,
                output_root=args.output_root,
            )
        except Exception as exc:  # pragma: no cover - failure capture
            overall_success = False
            error_entry = {
                "run_id": selection.run_id,
                "step": selection.step,
                "selector_source": selection.source,
                "selected_status": selection.status,
                "selected_reason": selection.reason,
                "result": "preflight_failed",
                "stable_output_dir": str(stable_dir),
                "attempt_dir": str(attempt_dir),
                "error": str(exc) or exc.__class__.__name__,
            }
            ensure_directory(attempt_dir)
            write_json(attempt_dir / "summary.json", error_entry)
            print(
                f"Replay preflight failed for {selection.run_id} {selection.step}: "
                f"{error_entry['error']}",
                file=sys.stderr,
            )
            aggregate_entries.append(error_entry)
            continue

        aggregate_entries.append(attempt_entry)
        if attempt_entry["result"] != "submission_success":
            overall_success = False

    write_invocation_summary(
        args.output_root,
        invocation_started_at,
        runs_root,
        aggregate_entries,
    )
    return 0 if overall_success else 1


def resolve_runs_root(explicit_runs_root: Path, worker_config: Path) -> Path:
    if explicit_runs_root != DEFAULT_RUNS_ROOT:
        return explicit_runs_root.resolve()
    if not worker_config.is_file():
        return explicit_runs_root.resolve()
    payload = load_yaml_file(worker_config)
    runs_root = payload.get("runs_root")
    if isinstance(runs_root, str) and runs_root.strip():
        return Path(runs_root).resolve()
    return explicit_runs_root.resolve()


def discover_targets(
    *,
    runs_root: Path,
    batch_start: str,
    batch_end: str,
    run_ids: tuple[str, ...],
    steps: tuple[str, ...],
) -> list[StepSelection]:
    selected_run_ids = set(run_ids)
    results: list[StepSelection] = []
    for run_root in sorted(path for path in runs_root.iterdir() if path.is_dir()):
        run_id = run_root.name
        if selected_run_ids:
            if run_id not in selected_run_ids:
                continue
        elif run_id < batch_start or run_id > batch_end:
            continue
        pipeline_report = load_optional_json(
            run_root / "pipeline" / "outputs" / "run_report.json"
        )
        for step in steps:
            selection = select_step_target(run_root, step, pipeline_report)
            if selection is not None:
                results.append(selection)
    return results


def select_step_target(
    run_root: Path,
    step: str,
    pipeline_report: Mapping[str, object] | None,
) -> StepSelection | None:
    service_report_path = (
        run_root / "services" / step / "run" / "outputs" / "run_report.json"
    )
    if service_report_path.is_file():
        service_report = load_json_file(service_report_path)
        status = str(service_report.get("status") or "").strip()
        if status in TARGET_FAILURE_STATUSES:
            reason = optional_non_empty_str(service_report.get("reason"))
            project_key = optional_non_empty_str(service_report.get("project_key"))
            return StepSelection(
                run_id=run_root.name,
                step=step,
                source="service_report",
                status=status,
                reason=reason,
                project_key=project_key,
                run_root=run_root,
            )
        return None

    pipeline_step = pipeline_step_state(pipeline_report, step)
    status = optional_non_empty_str(pipeline_step.get("status"))
    if status not in TARGET_FAILURE_STATUSES:
        return None
    reason = optional_non_empty_str(pipeline_step.get("reason"))
    project_key = read_manifest_project_key(
        run_root / "services" / step / "config" / "manifest.yaml"
    )
    return StepSelection(
        run_id=run_root.name,
        step=step,
        source="pipeline_report_fallback",
        status=status,
        reason=reason,
        project_key=project_key,
        run_root=run_root,
    )


def pipeline_step_state(
    pipeline_report: Mapping[str, object] | None, step: str
) -> Mapping[str, object]:
    if pipeline_report is None:
        return {}
    steps = pipeline_report.get("steps")
    if not isinstance(steps, Mapping):
        return {}
    entry = steps.get(step)
    return entry if isinstance(entry, Mapping) else {}


def build_replay_context(selection: StepSelection) -> ReplayContext:
    service_root = selection.run_root / "services" / selection.step
    manifest_path = service_root / "config" / "manifest.yaml"
    if not manifest_path.is_file():
        raise RuntimeError(f"Missing stored manifest: {manifest_path}")
    manifest_text = manifest_path.read_text(encoding="utf-8")
    manifest_payload = load_yaml_file(manifest_path)
    project_key = optional_non_empty_str(manifest_payload.get("project_key"))
    if project_key is None:
        raise RuntimeError(f"Manifest is missing project_key: {manifest_path}")
    input_repo = determine_input_repo(selection.run_root, selection.step)
    if not input_repo.exists():
        raise RuntimeError(
            f"Resolved input repo does not exist for {selection.step}: {input_repo}"
        )
    requested_image_ref, requested_image_source, configured_image_ref = (
        resolve_lidskjalv_image(selection.run_root, selection.step)
    )
    timeout_sec = load_lidskjalv_timeout(selection.run_root)
    return ReplayContext(
        selection=selection,
        input_repo=input_repo,
        manifest_payload=manifest_payload,
        manifest_text=manifest_text,
        project_key=project_key,
        requested_image_ref=requested_image_ref,
        requested_image_source=requested_image_source,
        configured_image_ref=configured_image_ref,
        timeout_sec=timeout_sec,
        service_root=service_root,
    )


def determine_input_repo(run_root: Path, step: str) -> Path:
    if step == "lidskjalv-original":
        return run_root / "services" / "brokk" / "run" / "artifacts" / "original-repo"
    suffix = step_suffix(step)
    kvasir_service = "kvasir" if suffix == "" else f"kvasir-{suffix}"
    andvari_service = "andvari" if suffix == "" else f"andvari-{suffix}"
    ported_repo = (
        run_root
        / "services"
        / kvasir_service
        / "run"
        / "artifacts"
        / "ported-tests-repo"
    )
    if ported_repo.exists() and has_valid_kvasir_report(run_root, step):
        return ported_repo
    return (
        run_root / "services" / andvari_service / "run" / "artifacts" / "generated-repo"
    )


def has_valid_kvasir_report(run_root: Path, step: str) -> bool:
    report_path = kvasir_report_path(run_root, step)
    if not report_path.is_file():
        return False
    try:
        report = load_report(report_path)
    except RuntimeError:
        return False
    if not isinstance(report, Mapping):
        return False
    return (
        normalized_report_status(kvasir_step_for_lidskjalv_step(step), report)
        is not None
    )


def kvasir_report_path(run_root: Path, step: str) -> Path:
    kvasir_step = kvasir_step_for_lidskjalv_step(step)
    service = "kvasir" if step_suffix(step) == "" else kvasir_step
    return run_root / "services" / service / "run" / "outputs" / "test_port.json"


def kvasir_step_for_lidskjalv_step(step: str) -> str:
    suffix = step_suffix(step)
    return "kvasir" if suffix == "" else f"kvasir-{suffix}"


def step_suffix(step: str) -> str:
    if step.endswith("-v2"):
        return "v2"
    if step.endswith("-v3"):
        return "v3"
    return ""


def resolve_lidskjalv_image(run_root: Path, step: str) -> tuple[str, str, str | None]:
    state_path = run_root / "pipeline" / "state.json"
    state = load_json_file(state_path)
    steps = state.get("steps")
    if not isinstance(steps, Mapping):
        raise RuntimeError(f"Invalid pipeline state: {state_path}")
    step_state = steps.get(step)
    if not isinstance(step_state, Mapping):
        raise RuntimeError(f"Missing step state for {step}: {state_path}")
    configured = optional_non_empty_str(step_state.get("configured_image_ref"))
    resolved = optional_non_empty_str(step_state.get("resolved_image_id"))
    if resolved is not None:
        return resolved, "resolved_image_id", configured
    if configured is not None:
        return configured, "configured_image_ref", configured
    raise RuntimeError(f"Missing image reference for {step}: {state_path}")


def load_lidskjalv_timeout(run_root: Path) -> float | None:
    manifest_path = run_root / "pipeline" / "manifest.yaml"
    if not manifest_path.is_file():
        return 7200.0
    _manifest_text, config = load_pipeline_manifest(manifest_path)
    timeout_sec = config.lidskjalv.execution_timeout_sec
    if timeout_sec <= 0:
        return None
    return float(timeout_sec)


def execute_replay_attempt(
    attempt_dir: Path,
    context: ReplayContext,
    output_root: Path,
) -> dict[str, object]:
    ensure_directory(attempt_dir)
    config_dir = attempt_dir / "config"
    run_dir = attempt_dir / "run"
    outputs_dir = run_dir / "outputs"
    ensure_directory(config_dir, 0o755)
    # Match Heimdall's step runtime staging so the service user can create outputs.
    ensure_directory(run_dir, 0o777)
    ensure_directory(outputs_dir, 0o777)

    copied_manifest_path = config_dir / "manifest.yaml"
    copied_manifest_path.write_text(context.manifest_text, encoding="utf-8")
    docker_log_path = attempt_dir / "docker.log"

    container_env = {
        "LIDSKJALV_MANIFEST": "/run/config/manifest.yaml",
        "SONAR_HOST_URL": os.environ["SONAR_HOST_URL"],
        "SONAR_TOKEN": os.environ["SONAR_TOKEN"],
        "SONAR_ORGANIZATION": os.environ["SONAR_ORGANIZATION"],
    }
    mounts = [
        (context.input_repo, "/input/repo", True),
        (config_dir, "/run/config", True),
        (run_dir, "/run", False),
    ]

    image_ref_used, image_source_used = run_lidskjalv_container(
        requested_image_ref=context.requested_image_ref,
        requested_image_source=context.requested_image_source,
        configured_image_ref=context.configured_image_ref,
        env=container_env,
        mounts=mounts,
        log_path=docker_log_path,
        timeout_sec=context.timeout_sec,
    )

    retry_report_path = run_dir / "outputs" / "run_report.json"
    retry_report = load_optional_json(retry_report_path)
    summary = summarize_attempt(
        context=context,
        attempt_dir=attempt_dir,
        retry_report=retry_report,
        image_ref_used=image_ref_used,
        image_source_used=image_source_used,
        docker_log_path=docker_log_path,
        output_root=output_root,
    )
    write_json(attempt_dir / "summary.json", summary)
    print(
        f"{summary['result']}: {context.selection.run_id} {context.selection.step} "
        f"project_key={context.project_key}"
    )
    return summary


def run_lidskjalv_container(
    *,
    requested_image_ref: str,
    requested_image_source: str,
    configured_image_ref: str | None,
    env: Mapping[str, str],
    mounts: list[tuple[Path, str, bool]],
    log_path: Path,
    timeout_sec: float | None,
) -> tuple[str, str]:
    try:
        run_container(
            requested_image_ref,
            dict(env),
            mounts,
            output_path=log_path,
            timeout_sec=timeout_sec,
            timeout_reason="lidskjalv-timeout",
        )
        return requested_image_ref, requested_image_source
    except DockerError as exc:
        if (
            requested_image_source != "resolved_image_id"
            or not looks_like_missing_image(exc)
            or configured_image_ref is None
            or configured_image_ref == requested_image_ref
        ):
            raise
    run_container(
        configured_image_ref,
        dict(env),
        mounts,
        output_path=log_path,
        timeout_sec=timeout_sec,
        timeout_reason="lidskjalv-timeout",
    )
    return configured_image_ref, "configured_image_ref_fallback"


def summarize_attempt(
    *,
    context: ReplayContext,
    attempt_dir: Path,
    retry_report: Mapping[str, object] | None,
    image_ref_used: str,
    image_source_used: str,
    docker_log_path: Path,
    output_root: Path,
) -> dict[str, object]:
    sonar_task_id = retry_scan_task_id(retry_report)
    retry_project_key = (
        optional_non_empty_str(retry_report.get("project_key"))
        if retry_report is not None
        else None
    )
    report_status = (
        optional_non_empty_str(retry_report.get("status"))
        if retry_report is not None
        else None
    )
    submission_success = (
        retry_report is not None
        and report_status == SERVICE_REPORT_SUCCESS
        and sonar_task_id is not None
        and retry_project_key == context.project_key
    )
    result = "submission_success" if submission_success else "submission_failed"
    return {
        "run_id": context.selection.run_id,
        "step": context.selection.step,
        "selector_source": context.selection.source,
        "selected_status": context.selection.status,
        "selected_reason": context.selection.reason,
        "result": result,
        "submission_success": submission_success,
        "attempt_dir": str(attempt_dir),
        "stable_output_dir": str(
            stable_step_output_dir(output_root, context.selection)
        ),
        "input_repo": str(context.input_repo),
        "project_key": context.project_key,
        "retry_project_key": retry_project_key,
        "image_ref_requested": context.requested_image_ref,
        "image_source_requested": context.requested_image_source,
        "configured_image_ref": context.configured_image_ref,
        "image_ref_used": image_ref_used,
        "image_source_used": image_source_used,
        "timeout_sec": context.timeout_sec,
        "retry_report_path": str(attempt_dir / "run" / "outputs" / "run_report.json"),
        "retry_report_status": report_status,
        "retry_report_reason": (
            optional_non_empty_str(retry_report.get("reason"))
            if retry_report is not None
            else None
        ),
        "sonar_task_id": sonar_task_id,
        "docker_log_path": str(docker_log_path),
        "finished_at": timestamp_iso(),
    }


def retry_scan_task_id(report: Mapping[str, object] | None) -> str | None:
    if report is None:
        return None
    scan = report.get("scan")
    if not isinstance(scan, Mapping):
        return None
    return optional_non_empty_str(scan.get("sonar_task_id"))


def load_prior_success(stable_dir: Path) -> Mapping[str, object] | None:
    if not stable_dir.is_dir():
        return None
    for summary_path in sorted(stable_dir.glob("attempt-*/summary.json"), reverse=True):
        summary = load_optional_json(summary_path)
        if not isinstance(summary, Mapping):
            continue
        if summary.get("submission_success") is True:
            return summary
    return None


def stable_step_output_dir(output_root: Path, selection: StepSelection) -> Path:
    return output_root / selection.run_id / selection.step


def write_invocation_summary(
    output_root: Path,
    invocation_started_at: str,
    runs_root: Path,
    entries: Sequence[Mapping[str, object]],
) -> None:
    ensure_directory(output_root)
    write_json(
        output_root / f"invocation-{invocation_started_at}.json",
        {
            "invocation_started_at": invocation_started_at,
            "runs_root": str(runs_root),
            "entries": list(entries),
        },
    )


def require_sonar_env() -> None:
    missing = [name for name in REQUIRED_SONAR_ENV_VARS if not os.environ.get(name)]
    if missing:
        raise RuntimeError(
            f"Missing required Sonar environment variable(s): {', '.join(missing)}"
        )


def read_manifest_project_key(path: Path) -> str | None:
    if not path.is_file():
        return None
    payload = load_yaml_file(path)
    return optional_non_empty_str(payload.get("project_key"))


def load_yaml_file(path: Path) -> Mapping[str, object]:
    loaded = loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, Mapping):
        raise RuntimeError(f"Expected mapping YAML document: {path}")
    return loaded


def load_json_file(path: Path) -> Mapping[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise RuntimeError(f"Expected JSON object: {path}")
    return payload


def load_optional_json(path: Path) -> Mapping[str, object] | None:
    if not path.is_file():
        return None
    return load_json_file(path)


def optional_non_empty_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def looks_like_missing_image(exc: Exception) -> bool:
    detail = str(exc).lower()
    return "no such image" in detail or "unable to find image" in detail


def ensure_directory(path: Path, mode: int | None = None) -> None:
    path.mkdir(parents=True, exist_ok=True)
    if mode is not None:
        path.chmod(mode)


def write_json(path: Path, payload: Mapping[str, object]) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def timestamp_slug() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def timestamp_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
