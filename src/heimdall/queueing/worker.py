from __future__ import annotations

import fcntl
import json
import os
import shlex
import subprocess
import sys
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

from heimdall.execution import (
    build_runtime,
    resume_run_root,
    run_pipeline_manifest_path,
)
from heimdall.manifests.pipeline import ManifestValidationError, derive_repo_identity
from heimdall.manifests.queue import (
    build_pipeline_manifest_for_job,
    dump_queue_request,
    load_queue_request,
)
from heimdall.models import JobStatus, QueueRequest, RuntimeConfig, WorkerConfig
from heimdall.simpleyaml import YamlError, dumps, loads
from heimdall.sonar_follow_up import sonar_follow_up_path
from heimdall.utils import (
    compact_run_id,
    ensure_directory,
    read_json,
    timestamp_utc,
    write_text,
)

_JOB_SCHEMA_VERSION = "heimdall_queue_job.v1"


def resolve_worker_config_path(path: Path | None) -> Path:
    if path is not None:
        return path.resolve()
    env_value = os.environ.get("HEIMDALL_WORKER_CONFIG")
    if env_value:
        return Path(env_value).expanduser().resolve()
    return Path("worker.yaml").resolve()


def enqueue_request(
    worker_config: WorkerConfig, request: QueueRequest
) -> dict[str, object]:
    _ensure_queue_layout(worker_config.queue_root)
    job_id = _allocate_job_id(worker_config, request)
    job_dir = _job_dir(worker_config, job_id)
    job_dir.mkdir(parents=False, exist_ok=False)

    write_text(_request_path(worker_config, job_id), dump_queue_request(request))

    submitted_at = timestamp_utc()
    run_dir = worker_config.runs_root / job_id
    document: dict[str, object] = {
        "schema_version": _JOB_SCHEMA_VERSION,
        "job_id": job_id,
        "status": "pending",
        "repo_url": request.repo_url,
        "commit_sha": request.commit_sha,
        "run_id": job_id,
        "run_dir": str(run_dir),
        "submitted_at": submitted_at,
        "started_at": None,
        "finished_at": None,
        "pipeline_status": None,
        "reason": None,
    }
    _write_yaml(_job_path(worker_config, job_id), document)
    (worker_config.queue_root / "pending" / job_id).write_text("", encoding="utf-8")
    return document


def worker_loop(
    worker_config: WorkerConfig,
    *,
    poll_interval_sec: int,
    once: bool,
) -> int:
    _ensure_queue_layout(worker_config.queue_root)
    _emit_worker_log(
        "worker_start",
        queue_root=str(worker_config.queue_root),
        runs_root=str(worker_config.runs_root),
    )
    with _worker_lock(worker_config.queue_root / "worker.lock"):
        while True:
            job_id = _find_oldest_marker(worker_config.queue_root / "running")
            if job_id is not None:
                _emit_worker_log("resume_claimed", job_id=job_id, status="running")
                _reconcile_running_job(worker_config, job_id)
                if once:
                    return 0
                continue

            job_id = _claim_pending_job(worker_config.queue_root)
            if job_id is not None:
                _emit_worker_log("job_claimed", job_id=job_id, status="running")
                _run_pending_job(worker_config, job_id)
                if once:
                    return 0
                continue

            _emit_worker_log("worker_idle", status="idle")
            if once:
                return 0
            time.sleep(poll_interval_sec)


def load_job_status_document(
    worker_config: WorkerConfig, job_id: str
) -> dict[str, object]:
    job_path = _job_path(worker_config, job_id)
    if not job_path.is_file():
        raise RuntimeError(f"Unknown job ID: {job_id}")
    document = _load_yaml_mapping(job_path, "queue job")
    report_path = _run_report_path_from_job(document)
    if report_path is not None and report_path.is_file():
        report = read_json(report_path)
        document["pipeline"] = {
            "status": report.get("status"),
            "reason": report.get("reason"),
            "started_at": report.get("started_at"),
            "finished_at": report.get("finished_at"),
            "steps": report.get("steps"),
        }
        document["report_path"] = str(report_path)
    sonar_path = _sonar_follow_up_path_from_job(document)
    if sonar_path is not None and sonar_path.is_file():
        document["sonar_follow_up"] = read_json(sonar_path)
        document["sonar_follow_up_path"] = str(sonar_path)
    document["request_path"] = str(_request_path(worker_config, job_id))
    document["job_path"] = str(job_path)
    document["pipeline_manifest_path"] = str(
        _pipeline_manifest_path(worker_config, job_id)
    )
    return document


def dump_job_status_document(document: dict[str, object]) -> str:
    return dumps(document)


def submit_remote(
    remote: str,
    remote_worker_config: str,
    request: QueueRequest,
    *,
    remote_cli: str,
) -> subprocess.CompletedProcess[str]:
    command = _build_remote_cli_command(
        remote_cli,
        "enqueue",
        "--worker-config",
        remote_worker_config,
        "--stdin",
    )
    return subprocess.run(
        ["ssh", remote, command],
        input=dump_queue_request(request),
        text=True,
        capture_output=True,
        check=False,
    )


def status_remote(
    remote: str,
    remote_worker_config: str,
    job_id: str,
    *,
    remote_cli: str,
) -> subprocess.CompletedProcess[str]:
    command = _build_remote_cli_command(
        remote_cli,
        "status",
        "--worker-config",
        remote_worker_config,
        job_id,
    )
    return subprocess.run(
        ["ssh", remote, command],
        text=True,
        capture_output=True,
        check=False,
    )


def _ensure_queue_layout(queue_root: Path) -> None:
    ensure_directory(queue_root, 0o755)
    for name in ("jobs", "pending", "running", "finished", "failed"):
        ensure_directory(queue_root / name, 0o755)


def _run_pending_job(worker_config: WorkerConfig, job_id: str) -> None:
    request = load_queue_request(_request_path(worker_config, job_id))
    job = _update_job_record(
        worker_config,
        job_id,
        {
            "status": "running",
            "started_at": timestamp_utc(),
            "reason": None,
            "pipeline_status": None,
            "finished_at": None,
        },
    )
    manifest_text = build_pipeline_manifest_for_job(
        worker_config, request, run_id=str(job["run_id"])
    )
    write_text(_pipeline_manifest_path(worker_config, job_id), manifest_text)
    runtime = _runtime_from_worker_config(worker_config)
    try:
        run_root, _config = run_pipeline_manifest_path(
            _pipeline_manifest_path(worker_config, job_id), runtime
        )
    except Exception as exc:
        _finalize_job_error(worker_config, job_id, str(exc))
        _emit_worker_log("job_failed", job_id=job_id, status="error", reason=str(exc))
        return
    _finalize_job_from_run_root(worker_config, job_id, run_root)


def _reconcile_running_job(worker_config: WorkerConfig, job_id: str) -> None:
    job = _load_yaml_mapping(_job_path(worker_config, job_id), "queue job")
    run_dir = _job_run_dir(job)
    if run_dir is None:
        _finalize_job_error(worker_config, job_id, "missing_run_dir")
        return
    runtime = _runtime_from_worker_config(worker_config)
    report_path = run_dir / "pipeline" / "outputs" / "run_report.json"
    if report_path.is_file():
        _finalize_job_from_run_root(worker_config, job_id, run_dir)
        return
    manifest_path = run_dir / "pipeline" / "manifest.yaml"
    try:
        if manifest_path.is_file():
            resume_run_root(run_dir, runtime)
        else:
            run_dir = _run_from_queue_manifest(worker_config, job_id, runtime, job)
    except Exception as exc:
        _finalize_job_error(worker_config, job_id, str(exc))
        _emit_worker_log("job_failed", job_id=job_id, status="error", reason=str(exc))
        return
    _finalize_job_from_run_root(worker_config, job_id, run_dir)


def _finalize_job_from_run_root(
    worker_config: WorkerConfig, job_id: str, run_root: Path
) -> None:
    report_path = run_root / "pipeline" / "outputs" / "run_report.json"
    if not report_path.is_file():
        _finalize_job_error(worker_config, job_id, "missing_pipeline_report")
        return
    report = read_json(report_path)
    pipeline_status = str(report.get("status", "")).strip() or "error"
    reason_raw = report.get("reason")
    reason = str(reason_raw).strip() if reason_raw not in {None, ""} else None
    finished_at_raw = report.get("finished_at")
    finished_at = (
        str(finished_at_raw).strip()
        if isinstance(finished_at_raw, str) and finished_at_raw.strip()
        else timestamp_utc()
    )
    started_at_raw = report.get("started_at")
    started_at = (
        str(started_at_raw).strip()
        if isinstance(started_at_raw, str) and started_at_raw.strip()
        else None
    )
    job_status: JobStatus
    marker_dir: Path
    if pipeline_status == "passed":
        job_status = "passed"
        marker_dir = worker_config.queue_root / "finished"
    elif pipeline_status == "failed":
        job_status = "failed"
        marker_dir = worker_config.queue_root / "failed"
    else:
        job_status = "error"
        marker_dir = worker_config.queue_root / "failed"

    _update_job_record(
        worker_config,
        job_id,
        {
            "status": job_status,
            "started_at": started_at,
            "finished_at": finished_at,
            "pipeline_status": pipeline_status,
            "reason": reason,
        },
    )
    _move_running_marker(worker_config.queue_root, job_id, marker_dir)
    _emit_worker_log(
        "job_finished",
        job_id=job_id,
        run_id=job_id,
        status=job_status,
        reason=reason,
    )


def _finalize_job_error(worker_config: WorkerConfig, job_id: str, reason: str) -> None:
    _update_job_record(
        worker_config,
        job_id,
        {
            "status": "error",
            "finished_at": timestamp_utc(),
            "pipeline_status": "error",
            "reason": reason,
        },
    )
    _move_running_marker(
        worker_config.queue_root, job_id, worker_config.queue_root / "failed"
    )


def _move_running_marker(queue_root: Path, job_id: str, destination_dir: Path) -> None:
    running_marker = queue_root / "running" / job_id
    if running_marker.exists():
        os.replace(running_marker, destination_dir / job_id)


def _job_dir(worker_config: WorkerConfig, job_id: str) -> Path:
    return worker_config.queue_root / "jobs" / job_id


def _request_path(worker_config: WorkerConfig, job_id: str) -> Path:
    return _job_dir(worker_config, job_id) / "request.yaml"


def _job_path(worker_config: WorkerConfig, job_id: str) -> Path:
    return _job_dir(worker_config, job_id) / "job.yaml"


def _pipeline_manifest_path(worker_config: WorkerConfig, job_id: str) -> Path:
    return _job_dir(worker_config, job_id) / "pipeline.yaml"


def _allocate_job_id(worker_config: WorkerConfig, request: QueueRequest) -> str:
    _display, _repo_name, base_key = derive_repo_identity(request.repo_url)
    base = f"{compact_run_id()}__{base_key}__{request.commit_sha[:8]}"
    suffix = 1
    candidate = base
    jobs_root = worker_config.queue_root / "jobs"
    while (jobs_root / candidate).exists():
        suffix += 1
        candidate = f"{base}__{suffix}"
    return candidate


def _claim_pending_job(queue_root: Path) -> str | None:
    pending_dir = queue_root / "pending"
    running_dir = queue_root / "running"
    for marker in sorted(pending_dir.iterdir(), key=lambda item: item.name):
        if not marker.is_file():
            continue
        target = running_dir / marker.name
        try:
            os.replace(marker, target)
        except FileNotFoundError:
            continue
        return marker.name
    return None


def _find_oldest_marker(directory: Path) -> str | None:
    for marker in sorted(directory.iterdir(), key=lambda item: item.name):
        if marker.is_file():
            return marker.name
    return None


@contextmanager
def _worker_lock(lock_path: Path) -> Iterator[None]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")
    acquired = False
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError as exc:
            raise RuntimeError(
                f"Another worker is already holding {lock_path}"
            ) from exc
        yield
    finally:
        if acquired:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _load_yaml_mapping(path: Path, label: str) -> dict[str, object]:
    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ManifestValidationError(
            f"Failed to read {label}: {path} ({exc})"
        ) from exc
    try:
        loaded = loads(raw_text)
    except YamlError as exc:
        raise ManifestValidationError(f"Invalid {label} YAML: {exc}") from exc
    if not isinstance(loaded, Mapping):
        raise ManifestValidationError(
            f"{label.capitalize()} root must be a mapping/object"
        )
    return dict(loaded)


def _runtime_from_worker_config(worker_config: WorkerConfig) -> RuntimeConfig:
    return build_runtime(
        worker_config.runs_root,
        worker_config.codex_bin_dir,
        worker_config.codex_host_bin_dir,
        worker_config.codex_home_dir,
        worker_config.pull_policy,
        worker_config.verbose,
        andvari_internal_network_name=worker_config.andvari_internal_network_name,
        andvari_proxy_url=worker_config.andvari_proxy_url,
    )


def _run_from_queue_manifest(
    worker_config: WorkerConfig,
    job_id: str,
    runtime: RuntimeConfig,
    job: Mapping[str, object],
) -> Path:
    manifest_file = _pipeline_manifest_path(worker_config, job_id)
    if manifest_file.is_file():
        manifest_text = manifest_file.read_text(encoding="utf-8")
        if manifest_text.strip():
            try:
                run_root, _config = run_pipeline_manifest_path(manifest_file, runtime)
            except (YamlError, ManifestValidationError):
                pass
            else:
                return run_root

    request = load_queue_request(_request_path(worker_config, job_id))
    run_id_raw = job.get("run_id")
    if not isinstance(run_id_raw, str) or not run_id_raw.strip():
        raise RuntimeError("missing_run_id")
    manifest_text = build_pipeline_manifest_for_job(
        worker_config,
        request,
        run_id=run_id_raw,
    )
    write_text(manifest_file, manifest_text)
    run_root, _config = run_pipeline_manifest_path(manifest_file, runtime)
    return run_root


def _write_yaml(path: Path, document: Mapping[str, object]) -> None:
    write_text(path, dumps(dict(document)))


def _load_job_document(worker_config: WorkerConfig, job_id: str) -> dict[str, object]:
    return _load_yaml_mapping(_job_path(worker_config, job_id), "queue job")


def _update_job_record(
    worker_config: WorkerConfig, job_id: str, updates: Mapping[str, object]
) -> dict[str, object]:
    document = _load_job_document(worker_config, job_id)
    for key, value in updates.items():
        document[key] = value
    _write_yaml(_job_path(worker_config, job_id), document)
    return document


def _job_run_dir(document: Mapping[str, object]) -> Path | None:
    raw = document.get("run_dir")
    if not isinstance(raw, str) or not raw.strip():
        return None
    return Path(raw).expanduser().resolve()


def _run_report_path_from_job(document: Mapping[str, object]) -> Path | None:
    run_dir = _job_run_dir(document)
    if run_dir is None:
        return None
    return run_dir / "pipeline" / "outputs" / "run_report.json"


def _sonar_follow_up_path_from_job(document: Mapping[str, object]) -> Path | None:
    run_dir = _job_run_dir(document)
    if run_dir is None:
        return None
    return sonar_follow_up_path(run_dir)


def _emit_worker_log(event: str, **fields: object) -> None:
    payload = {"event": event, **fields}
    print(json.dumps(payload, sort_keys=True), file=sys.stderr, flush=True)


def _build_remote_cli_command(remote_cli: str, *args: str) -> str:
    cli = remote_cli.strip()
    if not cli:
        raise RuntimeError("Remote CLI command must not be empty")
    quoted_args = " ".join(shlex.quote(arg) for arg in args)
    if quoted_args:
        return f"{cli} {quoted_args}"
    return cli
