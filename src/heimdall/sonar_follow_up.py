from __future__ import annotations

import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from heimdall.adapters import STEP_DEFINITIONS
from heimdall.models import (
    STEP_LIDSKJALV_GENERATED,
    STEP_LIDSKJALV_GENERATED_V2,
    STEP_LIDSKJALV_GENERATED_V3,
    STEP_LIDSKJALV_ORIGINAL,
    StepState,
)
from heimdall.reporting import load_report
from heimdall.utils import timestamp_utc, write_text

SONAR_FOLLOW_UP_SCHEMA_VERSION = "heimdall_sonar_follow_up.v1"
SONAR_FOLLOW_UP_POLL_INTERVAL_SEC = 30
SONAR_MEASURE_KEYS = (
    "bugs,vulnerabilities,code_smells,coverage,duplicated_lines_density,"
    "reliability_rating,security_rating,sqale_rating,ncloc,sqale_index"
)
_FOLLOW_UP_TERMINAL_STATUSES = {"complete", "failed", "skipped"}
_FOLLOW_UP_STEPS = (
    STEP_LIDSKJALV_ORIGINAL,
    STEP_LIDSKJALV_GENERATED,
    STEP_LIDSKJALV_GENERATED_V2,
    STEP_LIDSKJALV_GENERATED_V3,
)


def sonar_follow_up_path(run_root: Path) -> Path:
    return run_root / "pipeline" / "outputs" / "sonar_follow_up.json"


def load_sonar_follow_up(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def sync_sonar_follow_up(
    run_root: Path,
    run_id: str,
    steps: Mapping[str, StepState],
) -> dict[str, object]:
    path = sonar_follow_up_path(run_root)
    existing = _load_existing_document(path)
    existing_steps = _mapping(existing.get("steps")) if existing else {}

    step_documents = {
        step: _build_step_document(
            run_root,
            step,
            steps.get(step),
            _mapping(existing_steps.get(step)),
        )
        for step in _FOLLOW_UP_STEPS
    }
    document: dict[str, object] = {
        "schema_version": SONAR_FOLLOW_UP_SCHEMA_VERSION,
        "run_id": run_id,
        "status": _overall_status(step_documents.values()),
        "updated_at": timestamp_utc(),
        "steps": step_documents,
    }
    write_text(path, json.dumps(document, indent=2) + "\n")
    return document


def update_sonar_follow_up(
    path: Path, *, sonar_host_url: str, sonar_token: str
) -> bool:
    document = load_sonar_follow_up(path)
    steps = _mapping(document.get("steps"))
    if not steps:
        return False

    changed = False
    new_steps: dict[str, object] = {}
    for step, raw_entry in steps.items():
        entry = _mapping(raw_entry)
        updated_entry = _refresh_entry(
            entry,
            sonar_host_url=sonar_host_url,
            sonar_token=sonar_token,
        )
        if updated_entry != entry:
            changed = True
        new_steps[step] = updated_entry

    document["steps"] = new_steps
    new_status = _overall_status(new_steps.values())
    if document.get("status") != new_status:
        changed = True
    document["status"] = new_status
    updated_at = timestamp_utc()
    if document.get("updated_at") != updated_at:
        changed = True
    document["updated_at"] = updated_at

    if changed:
        write_text(path, json.dumps(document, indent=2) + "\n")
    return changed


def find_pending_sonar_follow_up_paths(runs_root: Path) -> list[Path]:
    if not runs_root.is_dir():
        return []

    paths: list[Path] = []
    for run_root in sorted(runs_root.iterdir(), key=lambda item: item.name):
        if not run_root.is_dir():
            continue
        path = sonar_follow_up_path(run_root)
        if not path.is_file():
            continue
        try:
            document = load_sonar_follow_up(path)
        except (OSError, json.JSONDecodeError):
            continue
        if str(document.get("status", "")).strip() == "pending":
            paths.append(path)
    return paths


def sonar_worker_loop(runs_root: Path, *, once: bool) -> int:
    runs_root = runs_root.resolve()
    if not runs_root.is_dir():
        raise RuntimeError(f"Runs root does not exist: {runs_root}")

    sonar_host_url = os.environ.get("SONAR_HOST_URL")
    sonar_token = os.environ.get("SONAR_TOKEN")
    missing = []
    if not sonar_host_url:
        missing.append("SONAR_HOST_URL")
    if not sonar_token:
        missing.append("SONAR_TOKEN")
    if missing:
        raise RuntimeError(
            f"Missing Sonar environment variable(s): {', '.join(missing)}"
        )
    assert sonar_host_url is not None
    assert sonar_token is not None

    while True:
        for path in find_pending_sonar_follow_up_paths(runs_root):
            update_sonar_follow_up(
                path,
                sonar_host_url=sonar_host_url,
                sonar_token=sonar_token,
            )
        if once:
            return 0
        time.sleep(SONAR_FOLLOW_UP_POLL_INTERVAL_SEC)


def _load_existing_document(path: Path) -> dict[str, object] | None:
    if not path.is_file():
        return None
    try:
        document = load_sonar_follow_up(path)
    except (OSError, json.JSONDecodeError):
        return None
    return document


def _build_step_document(
    run_root: Path,
    step: str,
    state: StepState | None,
    existing: Mapping[str, object],
) -> dict[str, object]:
    report_path = _step_report_path(run_root, step)
    report = _load_service_report(report_path)
    if report is None:
        return _entry_without_report(step, state)

    scan = _mapping(report.get("scan"))
    report_status = _nullable_str(report.get("status"))
    reason = _nullable_str(report.get("reason"))
    project_key = _nullable_str(report.get("project_key"))
    scan_label = _nullable_str(report.get("scan_label")) or _scan_label_for_step(step)
    sonar_task_id = _nullable_str(scan.get("sonar_task_id"))
    data_status = _nullable_str(scan.get("data_status")) or "unavailable"
    ce_task_status = _nullable_str(scan.get("ce_task_status"))
    quality_gate_status = _nullable_str(scan.get("quality_gate_status"))
    measures = _json_object(scan.get("measures"))

    if data_status == "skipped":
        return _new_entry(
            step=step,
            project_key=project_key,
            scan_label=scan_label,
            sonar_task_id=sonar_task_id,
            status="skipped",
            reason=reason,
            ce_task_status=ce_task_status,
            quality_gate_status=quality_gate_status,
            data_status="skipped",
            measures=measures,
            last_checked_at=None,
            last_error=None,
        )

    if report_status != "passed":
        return _new_entry(
            step=step,
            project_key=project_key,
            scan_label=scan_label,
            sonar_task_id=sonar_task_id,
            status="failed",
            reason=reason or "scan_failed",
            ce_task_status=ce_task_status,
            quality_gate_status=quality_gate_status,
            data_status="failed" if sonar_task_id else "unavailable",
            measures=measures,
            last_checked_at=None,
            last_error=None,
        )

    if sonar_task_id is None:
        return _new_entry(
            step=step,
            project_key=project_key,
            scan_label=scan_label,
            sonar_task_id=None,
            status="failed",
            reason=reason or "missing-sonar-task-id",
            ce_task_status=ce_task_status,
            quality_gate_status=quality_gate_status,
            data_status="unavailable",
            measures=measures,
            last_checked_at=None,
            last_error=None,
        )

    if (
        existing
        and _nullable_str(existing.get("sonar_task_id")) == sonar_task_id
        and _nullable_str(existing.get("status")) in {"pending", "complete", "failed"}
    ):
        entry = dict(existing)
        entry["step"] = step
        entry["project_key"] = project_key
        entry["scan_label"] = scan_label
        entry["sonar_task_id"] = sonar_task_id
        return entry

    return _new_entry(
        step=step,
        project_key=project_key,
        scan_label=scan_label,
        sonar_task_id=sonar_task_id,
        status="pending",
        reason=None,
        ce_task_status=ce_task_status,
        quality_gate_status=quality_gate_status,
        data_status="pending",
        measures=measures,
        last_checked_at=None,
        last_error=None,
    )


def _entry_without_report(step: str, state: StepState | None) -> dict[str, object]:
    reason = state.reason if state is not None else None
    status = "skipped"
    if state is not None and state.status in {"failed", "error"}:
        status = "failed"
    return _new_entry(
        step=step,
        project_key=None,
        scan_label=_scan_label_for_step(step),
        sonar_task_id=None,
        status=status,
        reason=reason,
        ce_task_status=None,
        quality_gate_status=None,
        data_status="unavailable" if status == "failed" else "skipped",
        measures={},
        last_checked_at=None,
        last_error=None,
    )


def _refresh_entry(
    entry: Mapping[str, object],
    *,
    sonar_host_url: str,
    sonar_token: str,
) -> dict[str, object]:
    current = dict(entry)
    if _nullable_str(current.get("status")) in _FOLLOW_UP_TERMINAL_STATUSES:
        return current

    project_key = _nullable_str(current.get("project_key"))
    task_id = _nullable_str(current.get("sonar_task_id"))
    if task_id is None or project_key is None:
        current["status"] = "failed"
        current["reason"] = (
            _nullable_str(current.get("reason")) or "missing-sonar-task-id"
        )
        current["data_status"] = "unavailable"
        current["last_checked_at"] = timestamp_utc()
        current["last_error"] = None
        return current

    try:
        ce_json = _sonar_api_get_json(
            sonar_host_url,
            sonar_token,
            "/api/ce/task",
            {"id": task_id},
        )
    except RuntimeError as exc:
        current["status"] = "pending"
        current["last_checked_at"] = timestamp_utc()
        current["last_error"] = str(exc)
        return current

    ce_status = _nullable_str(_mapping(ce_json.get("task")).get("status")) or "UNKNOWN"
    current["ce_task_status"] = ce_status
    current["last_checked_at"] = timestamp_utc()
    current["last_error"] = None

    if ce_status in {"PENDING", "IN_PROGRESS", "UNKNOWN"}:
        current["status"] = "pending"
        current["reason"] = None
        current["data_status"] = "pending"
        return current

    if ce_status in {"FAILED", "CANCELED"}:
        current["status"] = "failed"
        current["reason"] = "sonar-task-failed"
        current["data_status"] = "failed"
        return current

    try:
        qg_json = _sonar_api_get_json(
            sonar_host_url,
            sonar_token,
            "/api/qualitygates/project_status",
            {"projectKey": project_key},
        )
        measures_raw = _sonar_api_get_json(
            sonar_host_url,
            sonar_token,
            "/api/measures/component",
            {"component": project_key, "metricKeys": SONAR_MEASURE_KEYS},
        )
    except RuntimeError as exc:
        current["status"] = "pending"
        current["data_status"] = "pending"
        current["last_error"] = str(exc)
        return current

    qg_status = _nullable_str(_mapping(qg_json.get("projectStatus")).get("status"))
    measures = _extract_measures(measures_raw)
    current["quality_gate_status"] = qg_status
    current["measures"] = measures
    current["data_status"] = "complete" if measures else "unavailable"

    if qg_status not in {None, "", "NONE", "OK"}:
        current["status"] = "failed"
        current["reason"] = "quality-gate-failed"
        return current

    current["status"] = "complete"
    current["reason"] = None
    return current


def _sonar_api_get_json(
    sonar_host_url: str,
    sonar_token: str,
    path: str,
    params: Mapping[str, str],
) -> dict[str, object]:
    query = urllib.parse.urlencode(params)
    url = f"{sonar_host_url.rstrip('/')}{path}?{query}"
    auth = base64.b64encode(f"{sonar_token}:".encode()).decode("ascii")
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Basic {auth}",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace").strip()
        raise RuntimeError(
            f"Sonar API request failed for {path}: HTTP {exc.code}{f' {detail}' if detail else ''}"
        ) from exc
    except OSError as exc:
        raise RuntimeError(f"Sonar API request failed for {path}: {exc}") from exc

    try:
        loaded = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid Sonar API response for {path}: {exc}") from exc
    if not isinstance(loaded, dict):
        raise RuntimeError(
            f"Invalid Sonar API response for {path}: root must be an object"
        )
    return dict(loaded)


def _extract_measures(payload: Mapping[str, object]) -> dict[str, str]:
    component = _mapping(payload.get("component"))
    raw_measures = component.get("measures")
    if not isinstance(raw_measures, list):
        return {}
    measures: dict[str, str] = {}
    for item in raw_measures:
        if not isinstance(item, Mapping):
            continue
        metric = _nullable_str(item.get("metric"))
        value = _nullable_str(item.get("value"))
        if metric is None or value is None:
            continue
        measures[metric] = value
    return measures


def _step_report_path(run_root: Path, step: str) -> Path:
    definition = STEP_DEFINITIONS[step]
    return (
        run_root
        / "services"
        / definition.service_dir_name
        / "run"
        / definition.report_relative_path
    )


def _load_service_report(path: Path) -> dict[str, object] | None:
    if not path.is_file():
        return None
    try:
        return load_report(path)
    except RuntimeError:
        return None


def _new_entry(
    *,
    step: str,
    project_key: str | None,
    scan_label: str,
    sonar_task_id: str | None,
    status: str,
    reason: str | None,
    ce_task_status: str | None,
    quality_gate_status: str | None,
    data_status: str,
    measures: dict[str, str],
    last_checked_at: str | None,
    last_error: str | None,
) -> dict[str, object]:
    return {
        "step": step,
        "project_key": project_key,
        "scan_label": scan_label,
        "sonar_task_id": sonar_task_id,
        "status": status,
        "reason": reason,
        "ce_task_status": ce_task_status,
        "quality_gate_status": quality_gate_status,
        "data_status": data_status,
        "measures": measures,
        "last_checked_at": last_checked_at,
        "last_error": last_error,
    }


def _overall_status(entries: Any) -> str:
    statuses = [
        _nullable_str(_mapping(entry).get("status")) or "pending" for entry in entries
    ]
    if not statuses:
        return "skipped"
    if any(status == "failed" for status in statuses):
        return "failed"
    if any(status == "pending" for status in statuses):
        return "pending"
    if all(status == "skipped" for status in statuses):
        return "skipped"
    return "complete"


def _scan_label_for_step(step: str) -> str:
    if step == STEP_LIDSKJALV_GENERATED:
        return "generated"
    if step == STEP_LIDSKJALV_GENERATED_V2:
        return "generated-v2"
    if step == STEP_LIDSKJALV_GENERATED_V3:
        return "generated-v3"
    return "original"


def _nullable_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _mapping(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return value
    return {}


def _json_object(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, str] = {}
    for key, raw in value.items():
        if raw is None:
            continue
        result[str(key)] = str(raw)
    return result
