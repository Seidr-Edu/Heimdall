#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from heimdall.utils import timestamp_utc  # noqa: E402


DEFAULT_OUTPUT_ROOT = Path("/srv/pipeline/retries/sonar-resubmission")
TASK_ID_RE = re.compile(r"/api/ce/task\?id=([A-Za-z0-9_-]+)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Recover successful manual Sonar submissions from sidecar docker logs "
            "when report-task.txt was not persisted."
        )
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Update sidecar summary.json files. Without this, only report changes.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    results = discover_recoveries(args.output_root)
    if args.apply:
        for result in results:
            if result["status"] == "recoverable":
                apply_recovery(
                    Path(str(result["summary_path"])), str(result["sonar_task_id"])
                )
        results = discover_recoveries(args.output_root)

    audit = {
        "schema_version": "heimdall_manual_sonar_recovery.v1",
        "generated_at": timestamp_utc(),
        "mode": "apply" if args.apply else "dry-run",
        "recoverable_count": sum(
            1 for item in results if item["status"] == "recoverable"
        ),
        "already_success_count": sum(
            1 for item in results if item["status"] == "already_success"
        ),
        "true_failure_count": sum(
            1 for item in results if item["status"] == "true_failure"
        ),
        "results": results,
    }
    audit_path = args.output_root / "recovered_manual_submissions.json"
    write_json(audit_path, audit)

    for result in results:
        print(
            f"{result['status']}\t{result.get('run_id') or ''}\t"
            f"{result.get('step') or ''}\t{result.get('sonar_task_id') or ''}\t"
            f"{result['summary_path']}"
        )
    print(f"Wrote audit: {audit_path}")
    if not args.apply and audit["recoverable_count"]:
        print("Run again with --apply to update recoverable sidecar summaries.")
    return 0


def discover_recoveries(output_root: Path) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for summary_path in sorted(output_root.glob("*/*/manual-attempt-*/summary.json")):
        summary = load_json(summary_path)
        docker_log_path = Path(
            str(summary.get("docker_log_path") or summary_path.parent / "docker.log")
        )
        log_text = read_text(docker_log_path)
        task_id = extract_task_id(log_text)
        analysis_successful = "ANALYSIS SUCCESSFUL" in log_text
        submission_success = summary.get("submission_success") is True
        if submission_success and non_empty_str(summary.get("sonar_task_id")):
            status = "already_success"
            task_id = non_empty_str(summary.get("sonar_task_id")) or task_id
        elif analysis_successful and task_id:
            status = "recoverable"
        else:
            status = "true_failure"
        results.append(
            {
                "status": status,
                "run_id": non_empty_str(summary.get("run_id")),
                "step": non_empty_str(summary.get("step")),
                "project_key": non_empty_str(summary.get("project_key")),
                "sonar_task_id": task_id,
                "analysis_successful": analysis_successful,
                "summary_path": str(summary_path),
                "docker_log_path": str(docker_log_path),
                "previous_result": non_empty_str(summary.get("result")),
                "previous_error": non_empty_str(summary.get("error")),
            }
        )
    return results


def apply_recovery(summary_path: Path, sonar_task_id: str) -> None:
    summary = dict(load_json(summary_path))
    summary["result"] = "submission_success"
    summary["submission_success"] = True
    summary["sonar_task_id"] = sonar_task_id
    summary["error"] = None
    summary["recovered_from_docker_log"] = True
    summary["recovered_at"] = timestamp_utc()
    write_json(summary_path, summary)


def extract_task_id(log_text: str) -> str | None:
    matches = TASK_ID_RE.findall(log_text)
    return matches[-1] if matches else None


def load_json(path: Path) -> Mapping[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise RuntimeError(f"Expected JSON object: {path}")
    return payload


def read_text(path: Path) -> str:
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def non_empty_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


if __name__ == "__main__":
    raise SystemExit(main())
