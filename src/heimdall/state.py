from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from threading import Lock

from heimdall.models import ArtifactRecord, StepState
from heimdall.utils import write_text


def hash_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def hash_file(path: Path) -> str:
    return hash_bytes(path.read_bytes())


def fingerprint_step(
    *,
    orchestrator_version: str,
    step: str,
    resolved_image_id: str,
    manifest_text: str,
    upstream_report_hashes: dict[str, str],
    runtime_snapshot: dict[str, object],
) -> str:
    payload = {
        "schema_version": "heimdall_step_fingerprint.v1",
        "orchestrator_version": orchestrator_version,
        "step": step,
        "resolved_image_id": resolved_image_id,
        "manifest_sha256": hash_bytes(manifest_text.encode("utf-8")),
        "upstream_report_hashes": upstream_report_hashes,
        "runtime_snapshot": runtime_snapshot,
    }
    return hash_bytes(json.dumps(payload, sort_keys=True).encode("utf-8"))


def load_existing_state(path: Path) -> dict[str, StepState]:
    if not path.is_file():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    steps = data.get("steps", {})
    if not isinstance(steps, dict):
        return {}
    result: dict[str, StepState] = {}
    for step, raw in steps.items():
        if not isinstance(raw, dict):
            continue
        result[step] = StepState(
            status=raw.get("status", "pending"),
            reason=raw.get("reason"),
            blocked_by=list(raw.get("blocked_by", [])),
            started_at=raw.get("started_at"),
            finished_at=raw.get("finished_at"),
            configured_image_ref=raw.get("configured_image_ref"),
            resolved_image_id=raw.get("resolved_image_id"),
            fingerprint=raw.get("fingerprint"),
            report_path=raw.get("report_path"),
            report_status=raw.get("report_status"),
        )
    return result


class StateStore:
    def __init__(self, path: Path, steps: dict[str, StepState]) -> None:
        self.path = path
        self.steps = steps
        self.artifacts: dict[str, ArtifactRecord] = {}
        self._lock = Lock()

    def update_step(self, step: str, state: StepState) -> None:
        with self._lock:
            self.steps[step] = state
            self._write_locked()

    def add_artifacts(self, artifacts: dict[str, ArtifactRecord]) -> None:
        with self._lock:
            self.artifacts.update(artifacts)
            self._write_locked()

    def snapshot(self) -> tuple[dict[str, StepState], dict[str, ArtifactRecord]]:
        with self._lock:
            return dict(self.steps), dict(self.artifacts)

    def _write_locked(self) -> None:
        document = {
            "schema_version": "heimdall_state.v1",
            "steps": {step: asdict(state) for step, state in self.steps.items()},
            "artifacts": {
                name: {"owner": artifact.owner, "path": artifact.path}
                for name, artifact in sorted(self.artifacts.items())
            },
        }
        write_text(self.path, json.dumps(document, indent=2) + "\n")
