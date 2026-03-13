from __future__ import annotations

import sys
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path

from heimdall import __version__
from heimdall.adapters import (
    STEP_DEFINITIONS,
    AdapterContext,
    classify_report,
    prepare_step,
    step_definitions,
    topological_steps,
    upstream_report_dependencies,
)
from heimdall.images import run_container
from heimdall.manifest import pipeline_to_document, runtime_snapshot
from heimdall.models import (
    PipelineConfig,
    ResolvedImages,
    RuntimeConfig,
    StepResult,
    StepState,
)
from heimdall.reporting import write_artifact_index, write_run_outputs
from heimdall.simpleyaml import dumps
from heimdall.state import StateStore, fingerprint_step, hash_file, load_existing_state
from heimdall.utils import (
    ensure_directory,
    stage_executable_tree,
    stage_readable_tree,
    timestamp_utc,
    write_text,
)


class PreflightError(RuntimeError):
    """Raised when orchestrator preflight fails."""


def run_pipeline(
    *,
    config: PipelineConfig,
    runtime: RuntimeConfig,
    resolved_images: ResolvedImages,
    run_root: Path,
    source_manifest_text: str,
    fresh_run: bool,
) -> Path:
    pipeline_dir = run_root / "pipeline"
    outputs_dir = pipeline_dir / "outputs"
    logs_dir = pipeline_dir / "logs"
    ensure_directory(run_root, 0o755)
    ensure_directory(pipeline_dir, 0o755)
    ensure_directory(outputs_dir, 0o755)
    ensure_directory(logs_dir, 0o755)
    _stage_service_roots(run_root)

    state_path = pipeline_dir / "state.json"
    artifact_index_path = pipeline_dir / "artifact_index.json"
    run_report_path = outputs_dir / "run_report.json"
    summary_path = outputs_dir / "summary.md"
    manifest_copy_path = pipeline_dir / "manifest.yaml"
    resolved_path = pipeline_dir / "resolved.yaml"

    if fresh_run and run_root.exists() and manifest_copy_path.exists():
        raise PreflightError(
            f"Run directory already exists: {run_root}. Use resume instead."
        )

    write_text(manifest_copy_path, source_manifest_text)

    existing_state = load_existing_state(state_path)
    steps = _initial_step_state(existing_state)
    store = StateStore(state_path, steps)
    started_at = timestamp_utc()
    runtime_view = runtime_snapshot(runtime)
    context = AdapterContext(
        config=config,
        runtime=runtime,
        run_root=run_root,
        resolved_images=resolved_images,
    )

    _write_resolved(resolved_path, config, runtime_view, resolved_images, run_root)
    _emit(context.runtime, f"starting run {config.run_id} in {run_root}")
    _run_scheduler(context, store, runtime_view, fresh_run)
    _write_resolved(resolved_path, config, runtime_view, resolved_images, run_root)

    steps_snapshot, artifacts_snapshot = store.snapshot()
    finished_at = timestamp_utc()
    write_artifact_index(artifact_index_path, config.run_id, artifacts_snapshot)
    write_run_outputs(
        run_report_path,
        summary_path,
        config.run_id,
        steps_snapshot,
        artifacts_snapshot,
        started_at,
        finished_at,
    )
    _emit(context.runtime, f"finished run {config.run_id}")
    return run_root


def _run_scheduler(
    context: AdapterContext,
    store: StateStore,
    runtime_view: dict[str, object],
    fresh_run: bool,
) -> None:
    reusable = _compute_reuse_plan(
        context, store.snapshot()[0], runtime_view, fresh_run
    )
    futures: dict[Future[StepResult], str] = {}
    finished: dict[str, StepResult] = {}
    pending = set(topological_steps())

    with ThreadPoolExecutor(max_workers=3) as executor:
        while pending or futures:
            for step in list(pending):
                definition = STEP_DEFINITIONS[step]
                if any(
                    dependency not in finished for dependency in definition.depends_on
                ):
                    continue
                blocked_by = [
                    dependency
                    for dependency in definition.depends_on
                    if finished[dependency].status not in {"passed", "skipped"}
                ]
                if blocked_by:
                    result = _blocked_result(step, context, blocked_by, runtime_view)
                    _emit(
                        context.runtime, f"[{step}] blocked by {', '.join(blocked_by)}"
                    )
                    finished[step] = result
                    pending.remove(step)
                    store.update_step(step, _step_state_from_result(result))
                    continue
                if step in reusable:
                    result = reusable[step]
                    _emit(context.runtime, f"[{step}] reusing passed step")
                    finished[step] = result
                    pending.remove(step)
                    store.update_step(step, _step_state_from_result(result))
                    if result.artifacts:
                        store.add_artifacts(result.artifacts)
                    continue
                future = executor.submit(_execute_step, step, context, runtime_view)
                futures[future] = step
                pending.remove(step)
                store.update_step(
                    step, StepState(status="running", started_at=timestamp_utc())
                )
                _emit(context.runtime, f"[{step}] started")
            if not futures:
                continue
            done, _pending_futures = wait(futures.keys(), return_when=FIRST_COMPLETED)
            for future in done:
                step = futures.pop(future)
                result = future.result()
                finished[step] = result
                store.update_step(step, _step_state_from_result(result))
                if result.artifacts:
                    store.add_artifacts(result.artifacts)
                _emit(
                    context.runtime,
                    f"[{step}] finished status={result.status} reason={result.reason or '-'}",
                )


def _compute_reuse_plan(
    context: AdapterContext,
    existing_state: dict[str, StepState],
    runtime_view: dict[str, object],
    fresh_run: bool,
) -> dict[str, StepResult]:
    if fresh_run:
        return {}
    reusable: dict[str, StepResult] = {}
    invalidated: set[str] = set()
    for step in topological_steps():
        if any(dep in invalidated for dep in STEP_DEFINITIONS[step].depends_on):
            invalidated.add(step)
            continue
        previous = existing_state.get(step)
        if (
            previous is None
            or previous.status != "passed"
            or previous.report_status != "passed"
            or previous.fingerprint is None
            or previous.report_path is None
        ):
            invalidated.add(step)
            continue
        report_path = Path(previous.report_path)
        if not report_path.is_file():
            invalidated.add(step)
            continue
        prepared = prepare_step(step, context)
        upstream_hashes = {
            dep: hash_file(path)
            for dep, path in upstream_report_dependencies(
                step, context.run_root
            ).items()
            if path.is_file()
        }
        current_fingerprint = fingerprint_step(
            orchestrator_version=__version__,
            step=step,
            resolved_image_id=prepared.resolved_image_id,
            manifest_text=prepared.manifest_text,
            upstream_report_hashes=upstream_hashes,
            runtime_snapshot=runtime_view,
        )
        if current_fingerprint != previous.fingerprint:
            invalidated.add(step)
            continue
        status, reason, artifacts = classify_report(step, report_path)
        reusable[step] = StepResult(
            step=step,
            status="skipped",
            reason=reason or "reused-passed-step",
            report_status="passed",
            report_path=report_path,
            fingerprint=current_fingerprint,
            configured_image_ref=prepared.configured_image_ref,
            resolved_image_id=prepared.resolved_image_id,
            started_at=previous.started_at or timestamp_utc(),
            finished_at=previous.finished_at or timestamp_utc(),
            artifacts=artifacts,
        )
    return reusable


def _execute_step(
    step: str, context: AdapterContext, runtime_view: dict[str, object]
) -> StepResult:
    prepared = prepare_step(step, context)
    started_at = timestamp_utc()
    log_path = context.run_root / "pipeline" / "logs" / f"{step}.log"
    write_text(log_path, "")
    upstream_hashes = {
        dep: hash_file(path)
        for dep, path in upstream_report_dependencies(step, context.run_root).items()
        if path.is_file()
    }
    fingerprint = fingerprint_step(
        orchestrator_version=__version__,
        step=step,
        resolved_image_id=prepared.resolved_image_id,
        manifest_text=prepared.manifest_text,
        upstream_report_hashes=upstream_hashes,
        runtime_snapshot=runtime_view,
    )
    if prepared.provider_bin_source is not None and prepared.provider_bin_dest is not None:
        stage_executable_tree(
            prepared.provider_bin_source,
            prepared.provider_bin_dest,
        )
    if (
        prepared.provider_seed_source is not None
        and prepared.provider_seed_dest is not None
    ):
        stage_readable_tree(
            prepared.provider_seed_source,
            prepared.provider_seed_dest,
        )
    run_container(
        prepared.configured_image_ref,
        prepared.env,
        [
            (mount.host_path, mount.container_path, mount.read_only)
            for mount in prepared.mounts
        ],
        stream_output=context.runtime.verbose,
        output_path=log_path,
        log_prefix=step if context.runtime.verbose else None,
    )
    finished_at = timestamp_utc()
    if not prepared.report_path.is_file():
        return StepResult(
            step=step,
            status="error",
            reason="missing-canonical-report",
            report_status=None,
            report_path=None,
            fingerprint=fingerprint,
            configured_image_ref=prepared.configured_image_ref,
            resolved_image_id=prepared.resolved_image_id,
            started_at=started_at,
            finished_at=finished_at,
        )
    status, reason, artifacts = classify_report(step, prepared.report_path)
    return StepResult(
        step=step,
        status=status,
        reason=reason,
        report_status=_read_report_status(prepared.report_path),
        report_path=prepared.report_path,
        fingerprint=fingerprint,
        configured_image_ref=prepared.configured_image_ref,
        resolved_image_id=prepared.resolved_image_id,
        started_at=started_at,
        finished_at=finished_at,
        artifacts=artifacts,
    )


def _read_report_status(path: Path) -> str | None:
    import json

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    raw = payload.get("status")
    return str(raw) if raw is not None else None


def _blocked_result(
    step: str,
    context: AdapterContext,
    blocked_by: list[str],
    runtime_view: dict[str, object],
) -> StepResult:
    prepared = prepare_step(step, context, stage_inputs=False)
    fingerprint = fingerprint_step(
        orchestrator_version=__version__,
        step=step,
        resolved_image_id=prepared.resolved_image_id,
        manifest_text=prepared.manifest_text,
        upstream_report_hashes={
            dep: hash_file(path)
            for dep, path in upstream_report_dependencies(
                step, context.run_root
            ).items()
            if path.is_file()
        },
        runtime_snapshot=runtime_view,
    )
    now = timestamp_utc()
    return StepResult(
        step=step,
        status="blocked",
        reason="blocked-by-upstream",
        report_status=None,
        report_path=prepared.report_path,
        fingerprint=fingerprint,
        configured_image_ref=prepared.configured_image_ref,
        resolved_image_id=prepared.resolved_image_id,
        started_at=now,
        finished_at=now,
        blocked_by=blocked_by,
    )


def _initial_step_state(existing: dict[str, StepState]) -> dict[str, StepState]:
    result: dict[str, StepState] = {}
    for step in topological_steps():
        result[step] = existing.get(step, StepState())
    return result


def _step_state_from_result(result: StepResult) -> StepState:
    return StepState(
        status=result.status,
        reason=result.reason,
        blocked_by=result.blocked_by,
        started_at=result.started_at,
        finished_at=result.finished_at,
        configured_image_ref=result.configured_image_ref,
        resolved_image_id=result.resolved_image_id,
        fingerprint=result.fingerprint,
        report_path=str(result.report_path) if result.report_path is not None else None,
        report_status=result.report_status,
    )


def _stage_service_roots(run_root: Path) -> None:
    services_root = run_root / "services"
    ensure_directory(services_root, 0o755)
    for definition in step_definitions().values():
        service_root = services_root / definition.service_dir_name
        ensure_directory(service_root, 0o755)
        ensure_directory(service_root / "config", 0o755)
        ensure_directory(service_root / "run", 0o777)


def _write_resolved(
    path: Path,
    config: PipelineConfig,
    runtime_view: dict[str, object],
    resolved_images: ResolvedImages,
    run_root: Path,
) -> None:
    document = {
        "schema_version": "heimdall_resolved.v1",
        "run_id": config.run_id,
        "runs_root": str(run_root.parent),
        "pull_policy": runtime_view["pull_policy"],
        "runtime": runtime_view,
        "images": {
            "brokk": {
                "configured_ref": config.images.brokk,
                "resolved_image_id": resolved_images.brokk,
            },
            "eitri": {
                "configured_ref": config.images.eitri,
                "resolved_image_id": resolved_images.eitri,
            },
            "andvari": {
                "configured_ref": config.images.andvari,
                "resolved_image_id": resolved_images.andvari,
            },
            "kvasir": {
                "configured_ref": config.images.kvasir,
                "resolved_image_id": resolved_images.kvasir,
            },
            "lidskjalv": {
                "configured_ref": config.images.lidskjalv,
                "resolved_image_id": resolved_images.lidskjalv,
            },
        },
        "pipeline": pipeline_to_document(config),
    }
    source_manifest_path = (
        run_root / "services" / "brokk" / "run" / "inputs" / "source-manifest.json"
    )
    if source_manifest_path.is_file():
        document["source_manifest"] = str(source_manifest_path)
    write_text(path, dumps(document))


def _emit(runtime: RuntimeConfig, message: str) -> None:
    if runtime.verbose:
        print(f"[heimdall] {message}", file=sys.stderr, flush=True)
