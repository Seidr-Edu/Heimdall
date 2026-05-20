#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path


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
from heimdall.reporting import load_report  # noqa: E402
from heimdall.sonar_follow_up import (  # noqa: E402
    load_sonar_follow_up,
    sonar_follow_up_path,
    sync_sonar_follow_up,
    update_sonar_follow_up,
)
from heimdall.state import load_existing_state  # noqa: E402

DEFAULT_RUNS_ROOT = Path("/srv/pipeline/runs")
LIDSKJALV_STEPS = (
    STEP_LIDSKJALV_ORIGINAL,
    STEP_LIDSKJALV_GENERATED,
    STEP_LIDSKJALV_GENERATED_V2,
    STEP_LIDSKJALV_GENERATED_V3,
)


@dataclass(frozen=True)
class TargetRun:
    run_root: Path
    selection_reason: str

    @property
    def run_id(self) -> str:
        return self.run_root.name


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create missing Sonar follow-up documents and refresh pending Sonar "
            "metrics for successful Lidskjalv submissions."
        )
    )
    parser.add_argument(
        "--runs-root",
        type=Path,
        default=DEFAULT_RUNS_ROOT,
        help=f"Pipeline runs root (default: {DEFAULT_RUNS_ROOT})",
    )
    parser.add_argument(
        "--run-id",
        action="append",
        dest="run_ids",
        help="Limit processing to a specific run_id. Repeatable.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the runs that would be reconciled without contacting Sonar.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    targets = discover_target_runs(args.runs_root, tuple(args.run_ids or ()))
    if not targets:
        print("No pending or backfillable Sonar follow-up runs found.")
        return 0

    if args.dry_run:
        for target in targets:
            print(f"{target.run_id}\t{target.selection_reason}")
        print(f"Selected {len(targets)} run(s).")
        return 0

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

    for target in targets:
        result = process_target_run(
            target,
            sonar_host_url=sonar_host_url,
            sonar_token=sonar_token,
        )
        print(
            f"{result['run_id']}\t{result['selection_reason']}\t"
            f"created={result['created']}\tchanged={result['changed']}\t"
            f"status={result['status']}"
        )
    print(f"Processed {len(targets)} run(s).")
    return 0


def discover_target_runs(
    runs_root: Path,
    run_ids: tuple[str, ...] = (),
) -> list[TargetRun]:
    if not runs_root.is_dir():
        raise RuntimeError(f"Runs root does not exist: {runs_root}")

    selected_ids = set(run_ids)
    targets: list[TargetRun] = []
    for run_root in sorted(item for item in runs_root.iterdir() if item.is_dir()):
        if selected_ids and run_root.name not in selected_ids:
            continue
        follow_up_path = sonar_follow_up_path(run_root)
        if follow_up_path.is_file():
            try:
                document = load_sonar_follow_up(follow_up_path)
            except Exception:
                if run_has_successful_submission(run_root):
                    targets.append(TargetRun(run_root, "invalid_follow_up"))
                continue
            if str(document.get("status")).strip() == "pending":
                targets.append(TargetRun(run_root, "pending_follow_up"))
            continue
        if run_has_successful_submission(run_root):
            targets.append(TargetRun(run_root, "missing_follow_up"))
    return targets


def process_target_run(
    target: TargetRun,
    *,
    sonar_host_url: str,
    sonar_token: str,
) -> dict[str, object]:
    path = sonar_follow_up_path(target.run_root)
    created = False
    if target.selection_reason != "pending_follow_up" or not path.is_file():
        steps = load_existing_state(target.run_root / "pipeline" / "state.json")
        if not steps:
            raise RuntimeError(
                f"Run is missing pipeline state needed to backfill follow-up: "
                f"{target.run_root}"
            )
        sync_sonar_follow_up(target.run_root, target.run_id, steps)
        created = True

    changed = update_sonar_follow_up(
        path,
        sonar_host_url=sonar_host_url,
        sonar_token=sonar_token,
    )
    document = load_sonar_follow_up(path)
    return {
        "run_id": target.run_id,
        "selection_reason": target.selection_reason,
        "created": created,
        "changed": changed,
        "status": document.get("status"),
    }


def run_has_successful_submission(run_root: Path) -> bool:
    for step in LIDSKJALV_STEPS:
        report_path = (
            run_root / "services" / step / "run" / "outputs" / "run_report.json"
        )
        if not report_path.is_file():
            continue
        try:
            report = load_report(report_path)
        except RuntimeError:
            continue
        if str(report.get("status")).strip() != "passed":
            continue
        scan = report.get("scan")
        if not isinstance(scan, dict):
            continue
        sonar_task_id = str(scan.get("sonar_task_id") or "").strip()
        if sonar_task_id:
            return True
    return False


if __name__ == "__main__":
    raise SystemExit(main())
