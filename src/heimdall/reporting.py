from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

from heimdall.models import ArtifactRecord, StepState
from heimdall.utils import write_text


def load_report(path: Path) -> dict[str, object]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise RuntimeError(f"Failed to read report: {path} ({exc})") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON report: {path} ({exc})") from exc


def write_artifact_index(
    path: Path, run_id: str, artifacts: dict[str, ArtifactRecord]
) -> None:
    document: dict[str, object] = {
        "schema_version": "heimdall_artifact_index.v1",
        "run_id": run_id,
        "artifacts": {
            key: {"owner": artifact.owner, "path": artifact.path}
            for key, artifact in sorted(artifacts.items())
        },
    }
    write_text(path, json.dumps(document, indent=2) + "\n")


def write_run_outputs(
    report_path: Path,
    summary_path: Path,
    run_id: str,
    steps: dict[str, StepState],
    artifacts: dict[str, ArtifactRecord],
    started_at: str,
    finished_at: str,
) -> None:
    overall_status = _overall_status(steps)
    reason = _overall_reason(steps)
    document = {
        "schema_version": "heimdall_run_report.v1",
        "run_id": run_id,
        "status": overall_status,
        "reason": reason,
        "started_at": started_at,
        "finished_at": finished_at,
        "steps": {
            step: {
                "status": state.status,
                "reason": state.reason,
                "blocked_by": state.blocked_by,
                "started_at": state.started_at,
                "finished_at": state.finished_at,
                "configured_image_ref": state.configured_image_ref,
                "resolved_image_id": state.resolved_image_id,
                "fingerprint": state.fingerprint,
                "report_path": state.report_path,
                "report_status": state.report_status,
            }
            for step, state in steps.items()
        },
        "artifacts": {
            key: {"owner": artifact.owner, "path": artifact.path}
            for key, artifact in sorted(artifacts.items())
        },
    }
    write_text(report_path, json.dumps(document, indent=2) + "\n")
    write_text(summary_path, _render_summary(document))


def _overall_status(steps: dict[str, StepState]) -> str:
    states = [step.status for step in steps.values()]
    if any(status == "error" for status in states):
        return "error"
    if any(status in {"failed", "blocked"} for status in states):
        return "failed"
    if all(status in {"passed", "skipped"} for status in states):
        return "passed"
    return "running"


def _overall_reason(steps: dict[str, StepState]) -> str | None:
    for state in steps.values():
        if state.status in {"failed", "error", "blocked"}:
            return state.reason
    return None


def _render_summary(document: Mapping[str, object]) -> str:
    steps = document["steps"]
    assert isinstance(steps, dict)
    lines = [
        "# Heimdall Run Report",
        "",
        "| Field | Value |",
        "|-------|-------|",
        f"| run_id | {document['run_id']} |",
        f"| status | {document['status']} |",
        f"| reason | {document.get('reason') or ''} |",
        f"| started_at | {document['started_at']} |",
        f"| finished_at | {document['finished_at']} |",
        "",
        "## Steps",
        "",
        "| Step | Status | Report Status | Reason |",
        "|------|--------|---------------|--------|",
    ]
    for step, state in steps.items():
        assert isinstance(state, dict)
        lines.append(
            f"| {step} | {state.get('status', '')} | {state.get('report_status', '') or ''} | {state.get('reason', '') or ''} |"
        )
    lines.append("")
    return "\n".join(lines)
