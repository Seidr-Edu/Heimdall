#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
SCRIPT_ROOT = Path(__file__).resolve().parent
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from heimdall.sonar_follow_up import (  # noqa: E402
    SONAR_MEASURE_KEYS,
    _extract_measures,
    _sonar_api_get_json,
)
from heimdall.utils import timestamp_utc  # noqa: E402

import export_analysis_bundle as export_bundle  # noqa: E402
import resubmit_missing_sonar as batch_scope  # noqa: E402


DEFAULT_RUNS_ROOT = Path("/srv/pipeline/runs")
DEFAULT_SONAR_SIDECAR_ROOT = Path("/srv/pipeline/retries/sonar-resubmission")
AUDIT_FILENAME = "project_metrics_backfill.json"
AUDIT_CSV_FILENAME = "project_metrics_backfill.csv"


@dataclass(frozen=True)
class ExpectedProject:
    agent: str
    run_id: str
    variant: str
    step: str
    project_key: str


@dataclass(frozen=True)
class FollowUpDestination:
    path: Path
    source: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill Sonar metrics by project key for the current Codex/Claude "
            "experiment batch. This is for cases where Sonar projects exist but "
            "run logs still have missing, pending, failed, or partial metrics."
        )
    )
    parser.add_argument("--runs-root", type=Path, default=DEFAULT_RUNS_ROOT)
    parser.add_argument(
        "--sonar-sidecar-root", type=Path, default=DEFAULT_SONAR_SIDECAR_ROOT
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write refreshed metrics into matching sonar_follow_up.json files.",
    )
    parser.add_argument(
        "--require-all-metrics",
        action="store_true",
        help=(
            "Treat a Sonar project as incomplete unless all configured metrics are "
            "returned. Without this, any returned measure set is accepted."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
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

    result = backfill_project_metrics(
        runs_root=args.runs_root,
        sonar_sidecar_root=args.sonar_sidecar_root,
        sonar_host_url=sonar_host_url,
        sonar_token=sonar_token,
        apply=args.apply,
        require_all_metrics=args.require_all_metrics,
    )
    print(
        f"expected_rows={result['expected_rows']} "
        f"expected_projects={result['expected_projects']} "
        f"queried={result['queried']} "
        f"available={result['available']} "
        f"missing_or_inaccessible={result['missing_or_inaccessible']} "
        f"updated_entries={result['updated_entries']} "
        f"apply={args.apply}"
    )
    print(f"audit={result['audit_path']}")
    print(f"audit_csv={result['audit_csv_path']}")
    return 0 if result["missing_or_inaccessible"] == 0 else 1


def backfill_project_metrics(
    *,
    runs_root: Path,
    sonar_sidecar_root: Path,
    sonar_host_url: str,
    sonar_token: str,
    apply: bool,
    require_all_metrics: bool = False,
) -> dict[str, object]:
    expected = discover_expected_projects(runs_root)
    unique_keys = sorted({item.project_key for item in expected})
    destinations = discover_follow_up_destinations(runs_root, sonar_sidecar_root)
    destinations_by_project = index_destinations_by_project(destinations)

    rows: list[dict[str, object]] = []
    fetched: dict[str, dict[str, object]] = {}
    for project_key in unique_keys:
        project = fetch_project_metrics(
            project_key,
            sonar_host_url=sonar_host_url,
            sonar_token=sonar_token,
            require_all_metrics=require_all_metrics,
        )
        fetched[project_key] = project
        rows.append(
            {
                "project_key": project_key,
                "status": project["status"],
                "quality_gate_status": project.get("quality_gate_status"),
                "metric_count": len(mapping(project.get("measures"))),
                "missing_metrics": ",".join(project.get("missing_metrics", [])),
                "error": project.get("error"),
                "destination_count": len(destinations_by_project.get(project_key, [])),
            }
        )

    updated_entries = 0
    if apply:
        updated_entries = apply_metric_updates(destinations, fetched)

    audit_path = sonar_sidecar_root / AUDIT_FILENAME
    audit_csv_path = sonar_sidecar_root / AUDIT_CSV_FILENAME
    write_json(
        audit_path,
        {
            "schema_version": "heimdall_sonar_project_metrics_backfill.v1",
            "generated_at": timestamp_utc(),
            "applied": apply,
            "expected_rows": len(expected),
            "expected_projects": len(unique_keys),
            "updated_entries": updated_entries,
            "rows": rows,
        },
    )
    write_csv(audit_csv_path, rows)

    available = sum(1 for row in rows if row["status"] == "available")
    missing_or_inaccessible = len(rows) - available
    return {
        "expected_rows": len(expected),
        "expected_projects": len(unique_keys),
        "queried": len(rows),
        "available": available,
        "missing_or_inaccessible": missing_or_inaccessible,
        "updated_entries": updated_entries,
        "audit_path": str(audit_path),
        "audit_csv_path": str(audit_csv_path),
    }


def discover_expected_projects(runs_root: Path) -> list[ExpectedProject]:
    expected: list[ExpectedProject] = []
    for agent, run_id in batch_scope.scoped_run_ids():
        run_root = runs_root / run_id
        if not run_root.is_dir():
            continue
        for variant, steps in export_bundle.VARIANT_STEPS.items():
            step = str(steps["lidskjalv"])
            project_key = project_key_for_step(run_root, step)
            if project_key is None:
                continue
            expected.append(
                ExpectedProject(
                    agent=agent,
                    run_id=run_id,
                    variant=variant,
                    step=step,
                    project_key=project_key,
                )
            )
    return expected


def project_key_for_step(run_root: Path, step: str) -> str | None:
    report = export_bundle.load_json(export_bundle.service_report_path(run_root, step))
    project_key = export_bundle.non_empty_str(report.get("project_key"))
    if project_key is not None:
        return project_key
    project_key = export_bundle.sonar_project_key_from_follow_up(run_root, step)
    if project_key is not None:
        return project_key
    return export_bundle.service_manifest_project_key(run_root, step)


def discover_follow_up_destinations(
    runs_root: Path, sonar_sidecar_root: Path
) -> list[FollowUpDestination]:
    destinations: list[FollowUpDestination] = []
    for _, run_id in batch_scope.scoped_run_ids():
        path = runs_root / run_id / "pipeline" / "outputs" / "sonar_follow_up.json"
        if path.is_file():
            destinations.append(FollowUpDestination(path=path, source="run_follow_up"))
    for path in sorted(
        (sonar_sidecar_root / "follow_up").glob("*/sonar_follow_up.json")
    ):
        destinations.append(FollowUpDestination(path=path, source="sidecar_follow_up"))
    return destinations


def index_destinations_by_project(
    destinations: Sequence[FollowUpDestination],
) -> dict[str, list[FollowUpDestination]]:
    indexed: dict[str, list[FollowUpDestination]] = defaultdict(list)
    for destination in destinations:
        document = load_json(destination.path)
        for raw_entry in mapping(document.get("steps")).values():
            project_key = non_empty_str(mapping(raw_entry).get("project_key"))
            if project_key is not None:
                indexed[project_key].append(destination)
    return indexed


def fetch_project_metrics(
    project_key: str,
    *,
    sonar_host_url: str,
    sonar_token: str,
    require_all_metrics: bool,
) -> dict[str, object]:
    metric_keys = metric_key_list()
    try:
        measures_raw = _sonar_api_get_json(
            sonar_host_url,
            sonar_token,
            "/api/measures/component",
            {"component": project_key, "metricKeys": ",".join(metric_keys)},
        )
        qg_raw = _sonar_api_get_json(
            sonar_host_url,
            sonar_token,
            "/api/qualitygates/project_status",
            {"projectKey": project_key},
        )
    except RuntimeError as exc:
        return {
            "project_key": project_key,
            "status": "missing_or_inaccessible",
            "quality_gate_status": None,
            "measures": {},
            "missing_metrics": metric_keys,
            "error": str(exc),
        }

    measures = _extract_measures(measures_raw)
    missing_metrics = [metric for metric in metric_keys if metric not in measures]
    if require_all_metrics and missing_metrics:
        status = "partial"
    else:
        status = "available" if measures else "missing_or_inaccessible"
    return {
        "project_key": project_key,
        "status": status,
        "quality_gate_status": non_empty_str(
            mapping(qg_raw.get("projectStatus")).get("status")
        ),
        "measures": measures,
        "missing_metrics": missing_metrics,
        "error": None,
    }


def apply_metric_updates(
    destinations: Sequence[FollowUpDestination],
    fetched: Mapping[str, Mapping[str, object]],
) -> int:
    updated_entries = 0
    checked_at = timestamp_utc()
    for destination in destinations:
        document = load_json(destination.path)
        steps = mapping(document.get("steps"))
        if not steps:
            continue
        changed = False
        new_steps: dict[str, object] = {}
        for step, raw_entry in steps.items():
            entry = dict(mapping(raw_entry))
            project_key = non_empty_str(entry.get("project_key"))
            project = fetched.get(project_key or "")
            if project and project.get("status") == "available":
                measures = dict(mapping(project.get("measures")))
                entry["measures"] = measures
                entry["quality_gate_status"] = project.get("quality_gate_status")
                entry["data_status"] = "complete" if measures else "unavailable"
                entry["status"] = "complete" if measures else "pending"
                entry["reason"] = None if measures else entry.get("reason")
                entry["last_checked_at"] = checked_at
                entry["last_error"] = None
                if entry != raw_entry:
                    changed = True
                    updated_entries += 1
            new_steps[str(step)] = entry
        if changed:
            document["steps"] = new_steps
            document["status"] = overall_status(new_steps.values())
            document["updated_at"] = checked_at
            write_json(destination.path, document)
    return updated_entries


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
    if all(status == "skipped" for status in statuses):
        return "skipped"
    return "complete"


def metric_key_list() -> list[str]:
    return [key.strip() for key in SONAR_MEASURE_KEYS.split(",") if key.strip()]


def load_json(path: Path) -> Mapping[str, object]:
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


def write_csv(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    fieldnames = [
        "project_key",
        "status",
        "quality_gate_status",
        "metric_count",
        "missing_metrics",
        "error",
        "destination_count",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


if __name__ == "__main__":
    raise SystemExit(main())
