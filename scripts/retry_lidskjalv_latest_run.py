#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from heimdall.manifests.queue import load_worker_config  # noqa: E402

import retry_lidskjalv_current_batch as base  # noqa: E402


DEFAULT_OUTPUT_ROOT = Path("/srv/pipeline/retries/lidskjalv-latest-run")
DEFAULT_WORKER_CONFIG = Path("/srv/pipeline/worker.yaml")
DEFAULT_RUNS_ROOT = Path("/srv/pipeline/runs")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Replay failed Lidskjalv submissions for the latest pipeline run."
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
        help=(
            "Worker config used to discover the runs root and current Lidskjalv image "
            f"(default: {DEFAULT_WORKER_CONFIG})"
        ),
    )
    parser.add_argument(
        "--run-id",
        help="Override the target run_id. Defaults to the latest run directory.",
    )
    parser.add_argument(
        "--step",
        action="append",
        choices=base.LIDSKJALV_STEPS,
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
    runs_root = base.resolve_runs_root(args.runs_root, args.worker_config)
    latest_run = resolve_target_run(runs_root, args.run_id)
    worker_image_ref = current_lidskjalv_image(args.worker_config)
    steps = tuple(args.steps or base.LIDSKJALV_STEPS)
    selections = discover_targets_for_run(latest_run, steps)
    invocation_started_at = base.timestamp_slug()
    aggregate_entries: list[dict[str, object]] = []

    if not selections:
        print(f"No failed/error lidskjalv targets matched in run {latest_run.name}.")
        base.write_invocation_summary(
            args.output_root,
            invocation_started_at,
            runs_root,
            aggregate_entries,
        )
        return 0

    if args.dry_run:
        for selection in selections:
            stable_dir = base.stable_step_output_dir(args.output_root, selection)
            print(
                f"{selection.run_id}\t{selection.step}\t{selection.status}\t"
                f"{selection.reason or ''}\t{worker_image_ref}\t{stable_dir}"
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
                    "image_ref_requested": worker_image_ref,
                    "image_source_requested": "worker_config",
                }
            )
        base.write_invocation_summary(
            args.output_root,
            invocation_started_at,
            runs_root,
            aggregate_entries,
        )
        return 0

    base.require_sonar_env()
    overall_success = True

    for selection in selections:
        stable_dir = base.stable_step_output_dir(args.output_root, selection)
        prior_success = base.load_prior_success(stable_dir)
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

        attempt_dir = stable_dir / f"attempt-{base.timestamp_slug()}"
        try:
            context = build_replay_context(selection, worker_image_ref)
            attempt_entry = base.execute_replay_attempt(
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
            base.ensure_directory(attempt_dir)
            base.write_json(attempt_dir / "summary.json", error_entry)
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

    base.write_invocation_summary(
        args.output_root,
        invocation_started_at,
        runs_root,
        aggregate_entries,
    )
    return 0 if overall_success else 1


def resolve_target_run(runs_root: Path, run_id: str | None) -> Path:
    if run_id is not None:
        run_root = runs_root / run_id
        if not run_root.is_dir():
            raise RuntimeError(f"Requested run does not exist: {run_root}")
        return run_root
    candidates = sorted(path for path in runs_root.iterdir() if path.is_dir())
    if not candidates:
        raise RuntimeError(f"No run directories found under {runs_root}")
    return candidates[-1]


def current_lidskjalv_image(worker_config_path: Path) -> str:
    config = load_worker_config(worker_config_path)
    image_ref = config.images.lidskjalv.strip()
    if not image_ref:
        raise RuntimeError(
            f"Worker config does not define images.lidskjalv: {worker_config_path}"
        )
    return image_ref


def discover_targets_for_run(
    run_root: Path,
    steps: tuple[str, ...],
) -> list[base.StepSelection]:
    pipeline_report = base.load_optional_json(
        run_root / "pipeline" / "outputs" / "run_report.json"
    )
    results: list[base.StepSelection] = []
    for step in steps:
        selection = base.select_step_target(run_root, step, pipeline_report)
        if selection is not None:
            results.append(selection)
    return results


def build_replay_context(
    selection: base.StepSelection,
    worker_image_ref: str,
) -> base.ReplayContext:
    context = base.build_replay_context(selection)
    return replace(
        context,
        requested_image_ref=worker_image_ref,
        requested_image_source="worker_config",
        configured_image_ref=worker_image_ref,
    )


if __name__ == "__main__":
    raise SystemExit(main())
