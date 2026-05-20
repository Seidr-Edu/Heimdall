#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
SCRIPT_ROOT = Path(__file__).resolve().parent
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from heimdall.simpleyaml import loads  # noqa: E402
from heimdall.utils import compact_run_id, timestamp_utc  # noqa: E402

import resubmit_missing_sonar as batch_scope  # noqa: E402


DEFAULT_RUNS_ROOT = Path("/srv/pipeline/runs")
DEFAULT_SONAR_SIDECAR_ROOT = Path("/srv/pipeline/retries/sonar-resubmission")
DEFAULT_EXPORT_ROOT = Path("/srv/pipeline/exports")
VARIANTS = ("original", "generated", "v2", "v3")
VARIANT_STEPS = {
    "original": {
        "lidskjalv": "lidskjalv-original",
        "mimir": None,
        "kvasir": None,
    },
    "generated": {
        "lidskjalv": "lidskjalv-generated",
        "mimir": "mimir",
        "kvasir": "kvasir",
    },
    "v2": {
        "lidskjalv": "lidskjalv-generated-v2",
        "mimir": "mimir-v2",
        "kvasir": "kvasir-v2",
    },
    "v3": {
        "lidskjalv": "lidskjalv-generated-v3",
        "mimir": "mimir-v3",
        "kvasir": "kvasir-v3",
    },
}
SONAR_METRICS = (
    "bugs",
    "vulnerabilities",
    "code_smells",
    "security_hotspots",
    "files",
    "classes",
    "functions",
    "statements",
    "ncloc",
    "sqale_index",
    "duplicated_files",
    "duplicated_lines",
    "duplicated_blocks",
    "blocker_violations",
    "critical_violations",
    "major_violations",
    "minor_violations",
    "info_violations",
    "coverage",
    "duplicated_lines_density",
    "reliability_rating",
    "security_rating",
    "sqale_rating",
    "complexity",
    "cognitive_complexity",
    "comment_lines",
    "comment_lines_density",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export Mimir stats, Kvasir reports, and Lidskjalv/Sonar metrics for "
            "the current Codex/Claude experiment batch."
        )
    )
    parser.add_argument("--runs-root", type=Path, default=DEFAULT_RUNS_ROOT)
    parser.add_argument(
        "--sonar-sidecar-root", type=Path, default=DEFAULT_SONAR_SIDECAR_ROOT
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help=(
            "Bundle destination. Defaults to "
            "/srv/pipeline/exports/heimdall-analysis-<timestamp>."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow writing into an existing output directory.",
    )
    parser.add_argument(
        "--include-raw",
        action="store_true",
        help="Also copy raw source reports for provenance/debugging.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = args.output_dir or (
        DEFAULT_EXPORT_ROOT / f"heimdall-analysis-{compact_run_id()}"
    )
    if output_dir.exists() and not args.force:
        raise RuntimeError(f"Output directory already exists: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    bundle = build_bundle(
        runs_root=args.runs_root,
        sonar_sidecar_root=args.sonar_sidecar_root,
        output_dir=output_dir,
        copy_raw=args.include_raw,
    )
    write_bundle(output_dir, bundle)
    print(f"Wrote analysis bundle: {output_dir}")
    print(
        f"runs={len(bundle['runs'])} variants={len(bundle['variants'])} "
        f"sonar_projects={len(bundle['sonar_projects'])}"
    )
    return 0


def build_bundle(
    *,
    runs_root: Path,
    sonar_sidecar_root: Path,
    output_dir: Path,
    copy_raw: bool,
) -> dict[str, list[dict[str, object]]]:
    run_rows: list[dict[str, object]] = []
    variant_rows: list[dict[str, object]] = []
    mimir_rows: list[dict[str, object]] = []
    kvasir_rows: list[dict[str, object]] = []
    sonar_candidates = collect_sonar_candidates(runs_root, sonar_sidecar_root)
    sonar_by_project = choose_sonar_projects(sonar_candidates)

    for agent, run_id in batch_scope.scoped_run_ids():
        run_root = runs_root / run_id
        if not run_root.is_dir():
            continue
        run_row = build_run_row(agent, run_root)
        run_rows.append(run_row)
        for variant in VARIANTS:
            variant_row = build_variant_row(
                agent=agent,
                run_root=run_root,
                variant=variant,
                sonar_by_project=sonar_by_project,
            )
            variant_rows.append(variant_row)
            if mimir_report_exists(run_root, variant):
                mimir_rows.append(build_mimir_row(agent, run_root, variant))
            if kvasir_report_exists(run_root, variant):
                kvasir_rows.append(build_kvasir_row(agent, run_root, variant))
        if copy_raw:
            copy_raw_run_reports(output_dir, agent, run_root)

    if copy_raw:
        copy_raw_sidecar_reports(output_dir, sonar_sidecar_root)

    valid_sonar_project_keys = {
        str(row["project_key"]) for row in variant_rows if row.get("project_key")
    }
    sonar_agent_by_project = sonar_agent_map(variant_rows)
    sonar_project_rows = [
        build_sonar_project_row(key, sonar_by_project[key], sonar_agent_by_project)
        for key in sorted(valid_sonar_project_keys)
        if key in sonar_by_project
    ]
    return {
        "runs": run_rows,
        "variants": variant_rows,
        "mimir": mimir_rows,
        "kvasir": kvasir_rows,
        "sonar_projects": sonar_project_rows,
    }


def build_run_row(agent: str, run_root: Path) -> dict[str, object]:
    manifest = load_yaml(run_root / "pipeline" / "manifest.yaml")
    report = load_json(run_root / "pipeline" / "outputs" / "run_report.json")
    source = mapping(manifest.get("source"))
    return {
        "agent": agent,
        "run_id": run_root.name,
        "repo_slug": repo_slug_from_run_id(run_root.name),
        "repo_url": non_empty_str(source.get("repo_url")),
        "commit_sha": non_empty_str(source.get("commit_sha")),
        "status": non_empty_str(report.get("status")),
    }


def build_variant_row(
    *,
    agent: str,
    run_root: Path,
    variant: str,
    sonar_by_project: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    steps = VARIANT_STEPS[variant]
    lidskjalv_step = str(steps["lidskjalv"])
    mimir_step = steps["mimir"]
    kvasir_step = steps["kvasir"]
    state = load_pipeline_state(run_root)
    if variant == "original":
        andvari_status, andvari_reason = (None, None)
        mimir_status, mimir_reason = (None, None)
        kvasir_status, kvasir_reason = (None, None)
    else:
        andvari_status, andvari_reason = service_status_reason(
            run_root,
            andvari_step_for_variant(variant),
            state,
            "run_report",
        )
        mimir_status, mimir_reason = (
            service_status_reason(run_root, str(mimir_step), state, "run_report")
            if mimir_step is not None
            else (None, None)
        )
        kvasir_status, kvasir_reason = (
            service_status_reason(run_root, str(kvasir_step), state, "kvasir_report")
            if kvasir_step is not None
            else (None, None)
        )
    lidskjalv_status, lidskjalv_reason = service_status_reason(
        run_root,
        lidskjalv_step,
        state,
        "run_report",
    )
    project_key = valid_sonar_project_key(run_root, lidskjalv_step, sonar_by_project)
    return {
        "agent": agent,
        "run_id": run_root.name,
        "repo_slug": repo_slug_from_run_id(run_root.name),
        "variant": variant,
        "andvari_status": andvari_status,
        "andvari_reason": andvari_reason,
        "mimir_status": mimir_status,
        "mimir_reason": mimir_reason,
        "kvasir_status": kvasir_status,
        "kvasir_reason": kvasir_reason,
        "lidskjalv_status": lidskjalv_status,
        "lidskjalv_reason": lidskjalv_reason,
        "project_key": project_key,
    }


def build_mimir_row(agent: str, run_root: Path, variant: str) -> dict[str, object]:
    step = str(VARIANT_STEPS[variant]["mimir"])
    path = service_report_path(run_root, step)
    report = load_json(path)
    comparison = first_comparison(report)
    diff_counts = mapping(comparison.get("diff_counts"))
    component_scores = mapping(comparison.get("component_scores"))
    component_exact = mapping(component_scores.get("exact"))
    component_fuzzy = mapping(component_scores.get("fuzzy"))
    row = {
        "agent": agent,
        "run_id": run_root.name,
        "repo_slug": repo_slug_from_run_id(run_root.name),
        "variant": variant,
        "exact_similarity": comparison.get("exact_similarity"),
        "fuzzy_similarity": comparison.get("fuzzy_similarity"),
        "component_exact_packages": component_exact.get("packages"),
        "component_exact_types": component_exact.get("types"),
        "component_exact_fields": component_exact.get("fields"),
        "component_exact_methods": component_exact.get("methods"),
        "component_exact_relations": component_exact.get("relations"),
        "component_fuzzy_matched_type_coverage": component_fuzzy.get(
            "matched_type_coverage"
        ),
        "component_fuzzy_field_preservation": component_fuzzy.get("field_preservation"),
        "component_fuzzy_method_preservation": component_fuzzy.get(
            "method_preservation"
        ),
        "component_fuzzy_relation_preservation": component_fuzzy.get(
            "relation_preservation"
        ),
        "component_fuzzy_name_package_retention": component_fuzzy.get(
            "name_package_retention"
        ),
    }
    for key in (
        "missing_packages",
        "added_packages",
        "missing_types",
        "added_types",
        "likely_renamed_or_moved_types",
        "missing_fields",
        "added_fields",
        "changed_fields",
        "missing_methods",
        "added_methods",
        "changed_methods",
        "missing_relations",
        "added_relations",
        "changed_relations",
    ):
        row[key] = diff_counts.get(key)
    return row


def build_kvasir_row(agent: str, run_root: Path, variant: str) -> dict[str, object]:
    step = str(VARIANT_STEPS[variant]["kvasir"])
    path = kvasir_report_path(run_root, step)
    report = load_json(path)
    result = mapping(report.get("result"))
    evidence = mapping(report.get("evidence"))
    behavioral = mapping(evidence.get("behavioral"))
    suite_changes = mapping(evidence.get("suite_changes"))
    retention = mapping(evidence.get("retention"))
    diagnostics = mapping(report.get("diagnostics"))
    write_scope = mapping(diagnostics.get("write_scope"))
    porting = mapping(report.get("porting"))
    porting_execution = mapping(porting.get("execution"))
    baselines = mapping(report.get("baselines"))
    original_baseline = mapping(baselines.get("original"))
    generated_baseline = mapping(baselines.get("generated"))
    original_execution = mapping(original_baseline.get("execution"))
    generated_execution = mapping(generated_baseline.get("execution"))
    row = {
        "agent": agent,
        "run_id": run_root.name,
        "repo_slug": repo_slug_from_run_id(run_root.name),
        "variant": variant,
        "status": non_empty_str(result.get("status")),
        "reason": non_empty_str(result.get("reason")),
        "verdict": non_empty_str(result.get("verdict")),
        "verdict_reason": non_empty_str(result.get("verdict_reason")),
        "failure_class": non_empty_str(result.get("failure_class")),
        "original_baseline_status": non_empty_str(original_baseline.get("status")),
        "generated_baseline_status": non_empty_str(generated_baseline.get("status")),
        "porting_status": non_empty_str(porting.get("status")),
        "behavioral_failing_case_count": behavioral.get("failing_case_count"),
        "behavioral_failing_case_unique_count": behavioral.get(
            "failing_case_unique_count"
        ),
        "behavioral_failing_case_occurrence_count": behavioral.get(
            "failing_case_occurrence_count"
        ),
        "suite_added": suite_changes.get("added"),
        "suite_modified": suite_changes.get("modified"),
        "suite_deleted": suite_changes.get("deleted"),
        "suite_total": suite_changes.get("total"),
        "retention_original_snapshot_file_count": retention.get(
            "original_snapshot_file_count"
        ),
        "retention_final_ported_test_file_count": retention.get(
            "final_ported_test_file_count"
        ),
        "retention_retained_original_test_file_count": retention.get(
            "retained_original_test_file_count"
        ),
        "retention_removed_original_test_file_count": retention.get(
            "removed_original_test_file_count"
        ),
        "retention_ratio": retention.get("retention_ratio"),
        "write_scope_violation_count": write_scope.get("violation_count"),
    }
    add_execution_counts(row, "original_baseline", original_execution)
    add_execution_counts(row, "generated_baseline", generated_execution)
    add_execution_counts(row, "porting", porting_execution)
    return row


def andvari_step_for_variant(variant: str) -> str:
    if variant == "v2":
        return "andvari-v2"
    if variant == "v3":
        return "andvari-v3"
    return "andvari"


def load_pipeline_state(run_root: Path) -> Mapping[str, object]:
    payload = load_json(run_root / "pipeline" / "state.json")
    steps = mapping(payload.get("steps"))
    return steps if steps else payload


def service_status_reason(
    run_root: Path,
    step: str,
    state: Mapping[str, object],
    report_kind: str,
) -> tuple[str | None, str | None]:
    report = load_service_status_report(run_root, step, report_kind)
    if report:
        if report_kind == "kvasir_report":
            result = mapping(report.get("result"))
            return non_empty_str(result.get("status")), non_empty_str(
                result.get("reason")
            )
        return non_empty_str(report.get("status")), non_empty_str(report.get("reason"))
    state_entry = mapping(state.get(step))
    return non_empty_str(state_entry.get("status")), non_empty_str(
        state_entry.get("reason")
    )


def load_service_status_report(
    run_root: Path, step: str, report_kind: str
) -> Mapping[str, object]:
    if report_kind == "kvasir_report":
        return load_json(kvasir_report_path(run_root, step))
    return load_json(service_report_path(run_root, step))


def valid_sonar_project_key(
    run_root: Path,
    lidskjalv_step: str,
    sonar_by_project: Mapping[str, Mapping[str, object]],
) -> str | None:
    report = load_json(service_report_path(run_root, lidskjalv_step))
    if not report:
        state = load_pipeline_state(run_root)
        state_status = non_empty_str(mapping(state.get(lidskjalv_step)).get("status"))
        if state_status in {"blocked", "skipped"}:
            return None

    for project_key in candidate_sonar_project_keys(run_root, lidskjalv_step):
        if project_key in sonar_by_project:
            return project_key
    return None


def candidate_sonar_project_keys(run_root: Path, step: str) -> list[str]:
    candidates: list[str] = []
    report = load_json(service_report_path(run_root, step))
    report_key = non_empty_str(report.get("project_key"))
    if report_key is not None:
        candidates.append(report_key)

    document = load_json(run_root / "pipeline" / "outputs" / "sonar_follow_up.json")
    entry = mapping(mapping(document.get("steps")).get(step))
    follow_up_key = non_empty_str(entry.get("project_key"))
    if follow_up_key is not None:
        candidates.append(follow_up_key)

    manifest_key = service_manifest_project_key(run_root, step)
    if manifest_key is not None:
        candidates.append(manifest_key)

    return list(dict.fromkeys(candidates))


def mimir_report_exists(run_root: Path, variant: str) -> bool:
    step = VARIANT_STEPS[variant]["mimir"]
    return step is not None and service_report_path(run_root, str(step)).is_file()


def kvasir_report_exists(run_root: Path, variant: str) -> bool:
    step = VARIANT_STEPS[variant]["kvasir"]
    return step is not None and kvasir_report_path(run_root, str(step)).is_file()


def add_execution_counts(
    row: dict[str, object], prefix: str, execution: Mapping[str, object]
) -> None:
    for key in (
        "tests_discovered",
        "tests_executed",
        "tests_failed",
        "tests_errors",
        "tests_skipped",
    ):
        row[f"{prefix}_{key}"] = execution.get(key)


def collect_sonar_candidates(
    runs_root: Path, sonar_sidecar_root: Path
) -> list[dict[str, object]]:
    candidates: list[dict[str, object]] = []
    for agent, run_id in batch_scope.scoped_run_ids():
        path = runs_root / run_id / "pipeline" / "outputs" / "sonar_follow_up.json"
        candidates.extend(
            read_sonar_follow_up(path, source="run_follow_up", agent=agent)
        )

    for path in sorted(
        (sonar_sidecar_root / "follow_up").glob("*/sonar_follow_up.json")
    ):
        candidates.extend(
            read_sonar_follow_up(path, source="sidecar_follow_up", agent=None)
        )

    return candidates


def read_sonar_follow_up(
    path: Path, *, source: str, agent: str | None
) -> list[dict[str, object]]:
    document = load_json(path)
    if not document:
        return []
    run_id = non_empty_str(document.get("run_id")) or path.parent.name
    rows: list[dict[str, object]] = []
    for step, raw_entry in mapping(document.get("steps")).items():
        entry = mapping(raw_entry)
        project_key = non_empty_str(entry.get("project_key"))
        if not project_key:
            continue
        measures = mapping(entry.get("measures"))
        row = {
            "repo_slug": repo_slug_from_run_id(run_id),
            "variant": variant_for_lidskjalv_step(step),
            "project_key": project_key,
        }
        row.update(
            {metric: non_empty_str(measures.get(metric)) for metric in SONAR_METRICS}
        )
        rows.append(row)
    return rows


def build_sonar_project_row(
    project_key: str,
    sonar: Mapping[str, object],
    agent_by_project: Mapping[str, str | None],
) -> dict[str, object]:
    variant = non_empty_str(sonar.get("variant"))
    row: dict[str, object] = {
        "agent": agent_by_project.get(project_key),
        "project_key": project_key,
        "repo_slug": non_empty_str(sonar.get("repo_slug")),
        "variant": variant,
    }
    row.update({metric: sonar.get(metric) for metric in SONAR_METRICS})
    return row


def sonar_agent_map(
    variant_rows: Sequence[Mapping[str, object]],
) -> dict[str, str | None]:
    agents_by_project: dict[str, set[str]] = {}
    variants_by_project: dict[str, set[str]] = {}
    for row in variant_rows:
        project_key = non_empty_str(row.get("project_key"))
        if project_key is None:
            continue
        agents_by_project.setdefault(project_key, set()).add(str(row.get("agent")))
        variants_by_project.setdefault(project_key, set()).add(str(row.get("variant")))

    result: dict[str, str | None] = {}
    for project_key, agents in agents_by_project.items():
        variants = variants_by_project.get(project_key, set())
        if variants == {"original"}:
            result[project_key] = None
        elif len(agents) == 1:
            result[project_key] = next(iter(agents))
        else:
            result[project_key] = None
    return result


def choose_sonar_projects(
    candidates: Sequence[Mapping[str, object]],
) -> dict[str, dict[str, object]]:
    selected: dict[str, dict[str, object]] = {}
    for candidate in candidates:
        project_key = non_empty_str(candidate.get("project_key"))
        if not project_key:
            continue
        current = selected.get(project_key)
        if current is None or sonar_rank(candidate) > sonar_rank(current):
            selected[project_key] = dict(candidate)
    return selected


def sonar_rank(row: Mapping[str, object]) -> tuple[int, str, str]:
    status = non_empty_str(row.get("status"))
    has_metrics = any(
        non_empty_str(row.get(metric)) is not None for metric in SONAR_METRICS
    )
    has_task = non_empty_str(row.get("sonar_task_id")) is not None
    source = non_empty_str(row.get("source")) or ""
    if status == "complete" and has_metrics:
        score = 5
    elif status == "complete":
        score = 4
    elif has_task and has_metrics:
        score = 3
    elif has_task:
        score = 2
    else:
        score = 1
    return (score, non_empty_str(row.get("last_checked_at")) or "", source)


def write_bundle(
    output_dir: Path, bundle: Mapping[str, Sequence[Mapping[str, object]]]
) -> None:
    tables_dir = output_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    for name, rows in bundle.items():
        write_jsonl(tables_dir / f"{name}.jsonl", rows)
        write_csv(tables_dir / f"{name}.csv", rows)

    manifest = {
        "schema_version": "heimdall_analysis_bundle.v1",
        "generated_at": timestamp_utc(),
        "run_count": len(bundle["runs"]),
        "variant_count": len(bundle["variants"]),
        "sonar_project_count": len(bundle["sonar_projects"]),
        "tables": sorted(bundle),
        "notes": [
            "variants is the primary denormalized analysis table.",
            "sonar_projects is deduplicated by project_key.",
            "Original Sonar project keys are shared across Codex and Claude runs.",
            "variants is the 320-row experiment matrix and carries tool status/reason columns.",
            "Use non-empty variants.project_key to join valid Sonar metrics from sonar_projects.",
            "sonar_projects excludes blocked variants that were accidentally scanned later.",
            "mimir and kvasir contain only real tool report rows.",
            "raw/ is included only when export runs with --include-raw.",
        ],
    }
    write_json(output_dir / "manifest.json", manifest)
    write_readme(output_dir)


def write_readme(output_dir: Path) -> None:
    (output_dir / "README.md").write_text(
        "\n".join(
            [
                "# Heimdall Analysis Bundle",
                "",
                "Primary tables are in `tables/` as both CSV and JSONL.",
                "",
                "- `runs`: one row per pipeline run.",
                "- `variants`: one row per run variant: original, generated, v2, v3; use status/reason columns for coverage.",
                "- `sonar_projects`: one row per valid deduplicated Sonar project key.",
                "- `mimir`: extracted Mimir comparison stats for real Mimir reports only.",
                "- `kvasir`: extracted Kvasir behavioral/test-porting stats for real Kvasir reports only.",
                "",
                "Use non-empty `variants.project_key` to join to `sonar_projects.project_key`.",
                "Empty `variants.project_key` means there is no valid Sonar row for that observation.",
                "Only original Sonar rows intentionally share project keys across Codex and Claude.",
                "Generated, v2, and v3 rows are agent-specific Sonar submissions when project_key is present.",
                "Raw reports are copied under `raw/` only when export runs with `--include-raw`.",
                "",
            ]
        ),
        encoding="utf-8",
    )


def copy_raw_run_reports(output_dir: Path, agent: str, run_root: Path) -> None:
    raw_root = output_dir / "raw" / "runs" / agent / run_root.name
    raw_paths = [
        run_root / "pipeline" / "manifest.yaml",
        run_root / "pipeline" / "outputs" / "run_report.json",
        run_root / "pipeline" / "outputs" / "summary.md",
        run_root / "pipeline" / "outputs" / "sonar_follow_up.json",
    ]
    for variant in VARIANTS:
        steps = VARIANT_STEPS[variant]
        if steps["mimir"] is not None:
            raw_paths.append(service_report_path(run_root, str(steps["mimir"])))
        if steps["kvasir"] is not None:
            raw_paths.append(kvasir_report_path(run_root, str(steps["kvasir"])))
        raw_paths.append(service_report_path(run_root, str(steps["lidskjalv"])))
    copy_existing_files(run_root, raw_root, raw_paths)


def copy_raw_sidecar_reports(output_dir: Path, sidecar_root: Path) -> None:
    if not sidecar_root.is_dir():
        return
    raw_root = output_dir / "raw" / "sidecar" / "sonar-resubmission"
    paths = [
        *sidecar_root.glob("*.json"),
        *sidecar_root.glob("follow_up/*/sonar_follow_up.json"),
        *sidecar_root.glob("*/*/*attempt-*/summary.json"),
    ]
    copy_existing_files(sidecar_root, raw_root, paths)


def copy_existing_files(
    source_root: Path, raw_root: Path, paths: Sequence[Path]
) -> None:
    for path in paths:
        if not path.is_file():
            continue
        destination = raw_root / path.relative_to(source_root)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)


def service_report_path(run_root: Path, service: str) -> Path:
    return run_root / "services" / service / "run" / "outputs" / "run_report.json"


def kvasir_report_path(run_root: Path, service: str) -> Path:
    return run_root / "services" / service / "run" / "outputs" / "test_port.json"


def sonar_project_key_from_follow_up(run_root: Path, step: str) -> str | None:
    path = run_root / "pipeline" / "outputs" / "sonar_follow_up.json"
    document = load_json(path)
    entry = mapping(mapping(document.get("steps")).get(step))
    return non_empty_str(entry.get("project_key"))


def service_manifest_project_key(run_root: Path, step: str) -> str | None:
    manifest = load_yaml(run_root / "services" / step / "config" / "manifest.yaml")
    return non_empty_str(manifest.get("project_key"))


def first_comparison(report: Mapping[str, object]) -> Mapping[str, object]:
    comparisons = mapping(report.get("diagram_comparisons"))
    for value in comparisons.values():
        if isinstance(value, Mapping):
            return value
    return {}


def variant_for_lidskjalv_step(step: str) -> str:
    for variant, steps in VARIANT_STEPS.items():
        if steps["lidskjalv"] == step:
            return variant
    return "unknown"


def sonar_scope_for_variant(variant: str) -> str:
    return "shared_original" if variant == "original" else "agent_specific"


def repo_slug_from_run_id(run_id: str) -> str:
    parts = run_id.split("__")
    if len(parts) < 3:
        return run_id
    return "__".join(parts[1:-1])


def load_json(path: Path) -> Mapping[str, object]:
    if not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, Mapping) else {}


def load_yaml(path: Path) -> Mapping[str, object]:
    if not path.is_file():
        return {}
    payload = loads(path.read_text(encoding="utf-8"))
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


def write_jsonl(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def write_csv(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({str(key) for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


if __name__ == "__main__":
    raise SystemExit(main())
