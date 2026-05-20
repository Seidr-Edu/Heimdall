#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from heimdall.models import (  # noqa: E402
    STEP_LIDSKJALV_GENERATED,
    STEP_LIDSKJALV_GENERATED_V2,
    STEP_LIDSKJALV_GENERATED_V3,
    STEP_LIDSKJALV_ORIGINAL,
)
from heimdall.sonar_follow_up import (  # noqa: E402
    SONAR_FOLLOW_UP_SCHEMA_VERSION,
    SONAR_MEASURE_KEYS,
    load_sonar_follow_up,
    update_sonar_follow_up,
)
from heimdall.utils import timestamp_utc  # noqa: E402


DEFAULT_OUTPUT_ROOT = Path("/srv/pipeline/retries/sonar-resubmission")
FOLLOW_UP_DIRNAME = "follow_up"
LIDSKJALV_STEPS = (
    STEP_LIDSKJALV_ORIGINAL,
    STEP_LIDSKJALV_GENERATED,
    STEP_LIDSKJALV_GENERATED_V2,
    STEP_LIDSKJALV_GENERATED_V3,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill SonarCloud metrics for sidecar Sonar resubmissions without "
            "modifying original pipeline run directories."
        )
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--once",
        action="store_true",
        help="Poll once and exit. Without this flag, poll until all entries are terminal.",
    )
    parser.add_argument("--poll-interval-sec", type=float, default=30.0)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    successes = discover_successful_submissions(args.output_root)
    if not successes:
        print("No successful sidecar submissions found.")
        return 0

    if args.dry_run:
        for success in successes:
            print(
                f"{success['run_id']}\t{success['step']}\t"
                f"{success['project_key']}\t{success['sonar_task_id']}"
            )
        print(f"Selected {len(successes)} successful sidecar submission(s).")
        return 0

    sonar_host_url = os.environ.get("SONAR_HOST_URL")
    sonar_token = os.environ.get("SONAR_TOKEN")
    missing = [
        name
        for name, value in {
            "SONAR_HOST_URL": sonar_host_url,
            "SONAR_TOKEN": sonar_token,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(
            f"Missing required Sonar environment variable(s): {', '.join(missing)}"
        )
    assert sonar_host_url is not None
    assert sonar_token is not None

    while True:
        follow_up_paths = sync_sidecar_follow_up(args.output_root, successes)
        for path in follow_up_paths:
            update_sonar_follow_up(
                path,
                sonar_host_url=sonar_host_url,
                sonar_token=sonar_token,
            )
        rows = write_metric_exports(args.output_root, follow_up_paths)
        pending = [row for row in rows if row.get("status") == "pending"]
        print(f"Backfilled {len(rows)} sidecar submission(s); pending={len(pending)}.")
        if args.once or not pending:
            return 0 if not pending else 1
        time.sleep(args.poll_interval_sec)


def discover_successful_submissions(output_root: Path) -> list[dict[str, object]]:
    successes: dict[tuple[str, str], dict[str, object]] = {}
    for summary_path in sorted(output_root.glob("*/*/*attempt-*/summary.json")):
        summary = load_optional_json(summary_path)
        if summary.get("submission_success") is not True:
            continue
        run_id = non_empty_str(summary.get("run_id"))
        step = non_empty_str(summary.get("step"))
        sonar_task_id = non_empty_str(summary.get("sonar_task_id"))
        project_key = non_empty_str(summary.get("project_key"))
        if None in {run_id, step, sonar_task_id, project_key}:
            continue
        assert run_id is not None
        assert step is not None
        key = (run_id, step)
        successes[key] = {
            "run_id": run_id,
            "step": step,
            "scan_label": non_empty_str(summary.get("scan_label"))
            or scan_label_for_step(step),
            "variant": non_empty_str(summary.get("variant")) or variant_for_step(step),
            "project_key": project_key,
            "project_name": non_empty_str(summary.get("project_name")),
            "sonar_task_id": sonar_task_id,
            "attempt_type": non_empty_str(summary.get("attempt_type")),
            "attempt_dir": non_empty_str(summary.get("attempt_dir")),
            "summary_path": str(summary_path),
        }
    return [successes[key] for key in sorted(successes)]


def sync_sidecar_follow_up(
    output_root: Path, successes: Sequence[Mapping[str, object]]
) -> list[Path]:
    by_run: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for success in successes:
        run_id = non_empty_str(success.get("run_id"))
        if run_id is not None:
            by_run[run_id].append(success)

    paths: list[Path] = []
    for run_id, entries in sorted(by_run.items()):
        path = sidecar_follow_up_path(output_root, run_id)
        existing = load_optional_json(path)
        existing_steps = mapping(existing.get("steps"))
        step_docs = {
            non_empty_str(entry.get("step")) or "unknown": build_step_document(
                entry,
                mapping(existing_steps.get(non_empty_str(entry.get("step")) or "")),
            )
            for entry in sorted(entries, key=lambda item: str(item.get("step")))
        }
        document = {
            "schema_version": SONAR_FOLLOW_UP_SCHEMA_VERSION,
            "sidecar_schema_version": "heimdall_sonar_resubmission_follow_up.v1",
            "run_id": run_id,
            "status": overall_status(step_docs.values()),
            "updated_at": timestamp_utc(),
            "steps": step_docs,
        }
        write_json(path, document)
        paths.append(path)
    return paths


def build_step_document(
    success: Mapping[str, object], existing: Mapping[str, object]
) -> dict[str, object]:
    step = non_empty_str(success.get("step")) or "unknown"
    project_key = non_empty_str(success.get("project_key"))
    sonar_task_id = non_empty_str(success.get("sonar_task_id"))
    if (
        existing
        and non_empty_str(existing.get("sonar_task_id")) == sonar_task_id
        and non_empty_str(existing.get("status")) in {"pending", "complete", "failed"}
    ):
        document = dict(existing)
        document.update(
            {
                "step": step,
                "project_key": project_key,
                "scan_label": non_empty_str(success.get("scan_label"))
                or scan_label_for_step(step),
                "sonar_task_id": sonar_task_id,
            }
        )
        return document
    return {
        "step": step,
        "project_key": project_key,
        "scan_label": non_empty_str(success.get("scan_label"))
        or scan_label_for_step(step),
        "sonar_task_id": sonar_task_id,
        "status": "pending",
        "reason": None,
        "ce_task_status": None,
        "quality_gate_status": None,
        "data_status": "pending",
        "measures": {},
        "last_checked_at": None,
        "last_error": None,
        "attempt_type": non_empty_str(success.get("attempt_type")),
        "attempt_dir": non_empty_str(success.get("attempt_dir")),
    }


def write_metric_exports(
    output_root: Path, follow_up_paths: Sequence[Path]
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    metric_keys = [key.strip() for key in SONAR_MEASURE_KEYS.split(",") if key.strip()]
    for path in follow_up_paths:
        document = load_sonar_follow_up(path)
        run_id = non_empty_str(document.get("run_id")) or path.parent.name
        for step, raw_entry in sorted(mapping(document.get("steps")).items()):
            entry = mapping(raw_entry)
            measures = mapping(entry.get("measures"))
            row: dict[str, object] = {
                "run_id": run_id,
                "step": step,
                "scan_label": non_empty_str(entry.get("scan_label")),
                "variant": variant_for_step(step),
                "project_key": non_empty_str(entry.get("project_key")),
                "sonar_task_id": non_empty_str(entry.get("sonar_task_id")),
                "status": non_empty_str(entry.get("status")),
                "reason": non_empty_str(entry.get("reason")),
                "ce_task_status": non_empty_str(entry.get("ce_task_status")),
                "quality_gate_status": non_empty_str(entry.get("quality_gate_status")),
                "data_status": non_empty_str(entry.get("data_status")),
                "last_checked_at": non_empty_str(entry.get("last_checked_at")),
                "last_error": non_empty_str(entry.get("last_error")),
                "follow_up_path": str(path),
            }
            for metric in metric_keys:
                row[metric] = non_empty_str(measures.get(metric))
            rows.append(row)

    write_json(
        output_root / "metrics.json",
        {
            "schema_version": "heimdall_sonar_resubmission_metrics.v1",
            "generated_at": timestamp_utc(),
            "row_count": len(rows),
            "rows": rows,
        },
    )
    write_metrics_csv(output_root / "metrics.csv", rows, metric_keys)
    return rows


def write_metrics_csv(
    path: Path, rows: Sequence[Mapping[str, object]], metric_keys: Sequence[str]
) -> None:
    fieldnames = [
        "run_id",
        "step",
        "scan_label",
        "variant",
        "project_key",
        "sonar_task_id",
        "status",
        "reason",
        "ce_task_status",
        "quality_gate_status",
        "data_status",
        "last_checked_at",
        "last_error",
        *metric_keys,
        "follow_up_path",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def sidecar_follow_up_path(output_root: Path, run_id: str) -> Path:
    return output_root / FOLLOW_UP_DIRNAME / run_id / "sonar_follow_up.json"


def overall_status(entries: Any) -> str:
    statuses = [
        non_empty_str(mapping(entry).get("status")) or "pending" for entry in entries
    ]
    if not statuses:
        return "skipped"
    if any(status == "failed" for status in statuses):
        return "failed"
    if any(status == "pending" for status in statuses):
        return "pending"
    return "complete"


def scan_label_for_step(step: str) -> str:
    if step == STEP_LIDSKJALV_GENERATED:
        return "generated"
    if step == STEP_LIDSKJALV_GENERATED_V2:
        return "generated-v2"
    if step == STEP_LIDSKJALV_GENERATED_V3:
        return "generated-v3"
    return "original"


def variant_for_step(step: str) -> str:
    if step == STEP_LIDSKJALV_GENERATED:
        return "generated"
    if step == STEP_LIDSKJALV_GENERATED_V2:
        return "v2"
    if step == STEP_LIDSKJALV_GENERATED_V3:
        return "v3"
    return "original"


def load_optional_json(path: Path) -> Mapping[str, object]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, Mapping) else {}


def mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def non_empty_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
