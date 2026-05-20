from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tests.helpers import write_file


def load_script(name: str):
    path = REPO_ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


EXPORT = load_script("export_analysis_bundle")


class ExportAnalysisBundleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(prefix="analysis-export-tests-")
        self.root = Path(self.tempdir.name)
        self.runs_root = self.root / "runs"
        self.sidecar_root = self.root / "sidecar"
        self.output_dir = self.root / "bundle"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_bundle_dedupes_only_original_project_keys(self) -> None:
        codex_run = "20260501T000000Z__owner_repo__abc123"
        claude_run = "20260502T000000Z__owner_repo__abc123"
        self._write_run(codex_run, agent_suffix="codex")
        self._write_run(claude_run, agent_suffix="claude")

        with mock.patch.object(
            EXPORT.batch_scope,
            "scoped_run_ids",
            return_value=[("codex", codex_run), ("claude", claude_run)],
        ):
            bundle = EXPORT.build_bundle(
                runs_root=self.runs_root,
                sonar_sidecar_root=self.sidecar_root,
                output_dir=self.output_dir,
                copy_raw=True,
            )
            EXPORT.write_bundle(self.output_dir, bundle)

        self.assertEqual(len(bundle["runs"]), 2)
        self.assertNotIn("reason", bundle["runs"][0])
        self.assertEqual(len(bundle["variants"]), 8)
        original_variants = [
            row for row in bundle["variants"] if row["variant"] == "original"
        ]
        generated_variants = [
            row for row in bundle["variants"] if row["variant"] == "generated"
        ]
        self.assertEqual(
            {row["project_key"] for row in original_variants},
            {"owner_repo__original"},
        )
        self.assertEqual(len({row["project_key"] for row in generated_variants}), 2)
        self.assertNotIn("sonar_scope", original_variants[0])
        self.assertEqual(
            sum(
                1
                for row in bundle["sonar_projects"]
                if row["project_key"] == "owner_repo__original"
            ),
            1,
        )
        self.assertTrue((self.output_dir / "tables" / "variants.csv").is_file())
        runs_header = (
            (self.output_dir / "tables" / "runs.csv")
            .read_text(encoding="utf-8")
            .splitlines()[0]
        )
        self.assertNotIn("reason", runs_header.split(","))
        self.assertTrue(
            (
                self.output_dir
                / "raw"
                / "runs"
                / "codex"
                / codex_run
                / "pipeline"
                / "outputs"
                / "sonar_follow_up.json"
            ).is_file()
        )
        mimir_row = bundle["mimir"][0]
        self.assertEqual(mimir_row["changed_methods"], 11)
        self.assertEqual(mimir_row["component_exact_methods"], 0.4)
        kvasir_row = bundle["kvasir"][0]
        self.assertEqual(kvasir_row["original_baseline_tests_discovered"], 20)
        self.assertEqual(kvasir_row["porting_tests_failed"], 1)
        self.assertEqual(kvasir_row["suite_total"], 6)
        self.assertEqual(kvasir_row["write_scope_violation_count"], 13)
        self.assertNotIn("suite_renamed", kvasir_row)
        self.assertNotIn("comparison_count", mimir_row)

    def _write_run(self, run_id: str, *, agent_suffix: str) -> None:
        run_root = self.runs_root / run_id
        write_file(
            run_root / "pipeline" / "manifest.yaml",
            "\n".join(
                [
                    "version: 1",
                    f"run_id: {run_id}",
                    "source:",
                    "  repo_url: https://github.com/owner/repo.git",
                    "  commit_sha: abc123",
                    "",
                ]
            ),
        )
        write_file(
            run_root / "pipeline" / "outputs" / "run_report.json",
            json.dumps(
                {
                    "status": "passed",
                    "reason": None,
                    "started_at": "2026-05-01T00:00:00Z",
                    "finished_at": "2026-05-01T00:10:00Z",
                },
                indent=2,
            )
            + "\n",
        )
        sonar_steps = {}
        project_keys = {
            "original": "owner_repo__original",
            "generated": f"owner_repo__generated_{agent_suffix}",
            "v2": f"owner_repo__generated_v2_{agent_suffix}",
            "v3": f"owner_repo__generated_v3_{agent_suffix}",
        }
        for variant, step in {
            "original": "lidskjalv-original",
            "generated": "lidskjalv-generated",
            "v2": "lidskjalv-generated-v2",
            "v3": "lidskjalv-generated-v3",
        }.items():
            project_key = project_keys[variant]
            scan_label = "generated" if variant == "generated" else variant
            sonar_steps[step] = {
                "step": step,
                "project_key": project_key,
                "scan_label": scan_label,
                "sonar_task_id": f"task-{project_key}",
                "status": "complete",
                "reason": None,
                "ce_task_status": "SUCCESS",
                "quality_gate_status": "OK",
                "data_status": "complete",
                "measures": {"coverage": "10.0", "bugs": "1"},
                "last_checked_at": "2026-05-01T00:20:00Z",
                "last_error": None,
            }
            write_file(
                run_root / "services" / step / "run" / "outputs" / "run_report.json",
                json.dumps(
                    {
                        "status": "passed",
                        "reason": None,
                        "project_key": project_key,
                        "scan_label": scan_label,
                    },
                    indent=2,
                )
                + "\n",
            )
        write_file(
            run_root / "pipeline" / "outputs" / "sonar_follow_up.json",
            json.dumps(
                {"run_id": run_id, "status": "complete", "steps": sonar_steps},
                indent=2,
            )
            + "\n",
        )
        for variant, mimir_step, kvasir_step in (
            ("generated", "mimir", "kvasir"),
            ("v2", "mimir-v2", "kvasir-v2"),
            ("v3", "mimir-v3", "kvasir-v3"),
        ):
            write_file(
                run_root
                / "services"
                / mimir_step
                / "run"
                / "outputs"
                / "run_report.json",
                json.dumps(
                    {
                        "status": "passed",
                        "comparison_count": 1,
                        "diagram_comparisons": {
                            f"andvari_{variant}": {
                                "exact_similarity": 0.5,
                                "fuzzy_similarity": 0.6,
                                "component_scores": {
                                    "exact": {
                                        "packages": 0.1,
                                        "types": 0.2,
                                        "fields": 0.3,
                                        "methods": 0.4,
                                        "relations": 0.5,
                                    },
                                    "fuzzy": {
                                        "matched_type_coverage": 0.6,
                                        "field_preservation": 0.7,
                                        "method_preservation": 0.8,
                                        "relation_preservation": 0.9,
                                        "name_package_retention": 1.0,
                                    },
                                },
                                "diff_counts": {
                                    "missing_packages": 1,
                                    "added_packages": 2,
                                    "missing_types": 3,
                                    "added_types": 4,
                                    "likely_renamed_or_moved_types": 5,
                                    "missing_fields": 6,
                                    "added_fields": 7,
                                    "changed_fields": 8,
                                    "missing_methods": 9,
                                    "added_methods": 10,
                                    "changed_methods": 11,
                                    "missing_relations": 12,
                                    "added_relations": 13,
                                    "changed_relations": 14,
                                },
                            }
                        },
                        "porting": {
                            "iterations_used": 2,
                            "adapter_nonzero_runs": 1,
                            "execution": {
                                "tests_discovered": 10,
                                "tests_executed": 9,
                                "tests_failed": 1,
                                "tests_errors": 0,
                                "tests_skipped": 2,
                                "junit_reports_found": 3,
                            },
                        },
                        "baselines": {
                            "original": {
                                "status": "passed",
                                "execution": {
                                    "tests_discovered": 20,
                                    "tests_executed": 19,
                                    "tests_failed": 0,
                                    "tests_errors": 0,
                                    "tests_skipped": 1,
                                    "junit_reports_found": 4,
                                },
                            },
                            "generated": {
                                "status": "passed",
                                "execution": {
                                    "tests_discovered": 18,
                                    "tests_executed": 17,
                                    "tests_failed": 1,
                                    "tests_errors": 0,
                                    "tests_skipped": 0,
                                    "junit_reports_found": 5,
                                },
                            },
                        },
                        "diagnostics": {"write_scope": {"violation_count": 13}},
                    },
                    indent=2,
                )
                + "\n",
            )
            write_file(
                run_root
                / "services"
                / kvasir_step
                / "run"
                / "outputs"
                / "test_port.json",
                json.dumps(
                    {
                        "result": {
                            "status": "passed",
                            "reason": None,
                            "verdict": "equivalent",
                            "failure_class": None,
                        },
                        "evidence": {
                            "behavioral": {
                                "junit_report_count": 2,
                                "failing_case_count": 0,
                                "failing_case_unique_count": 0,
                                "failing_case_occurrence_count": 0,
                            },
                            "suite_changes": {
                                "added": 0,
                                "modified": 1,
                                "deleted": 2,
                                "renamed": 3,
                                "total": 6,
                            },
                            "retention": {
                                "original_snapshot_file_count": 4,
                                "final_ported_test_file_count": 5,
                                "retained_original_test_file_count": 6,
                                "removed_original_test_file_count": 7,
                                "retention_ratio": 1.0,
                                "retained_modified_count": 8,
                                "retained_unchanged_count": 9,
                                "assertion_line_change_count": 10,
                                "undocumented_removed_test_count": 11,
                                "documented_removed_test_count": 12,
                            },
                        },
                        "porting": {
                            "iterations_used": 2,
                            "adapter_nonzero_runs": 1,
                            "execution": {
                                "tests_discovered": 10,
                                "tests_executed": 9,
                                "tests_failed": 1,
                                "tests_errors": 0,
                                "tests_skipped": 2,
                                "junit_reports_found": 3,
                            },
                        },
                        "baselines": {
                            "original": {
                                "status": "passed",
                                "execution": {
                                    "tests_discovered": 20,
                                    "tests_executed": 19,
                                    "tests_failed": 0,
                                    "tests_errors": 0,
                                    "tests_skipped": 1,
                                    "junit_reports_found": 4,
                                },
                            },
                            "generated": {
                                "status": "passed",
                                "execution": {
                                    "tests_discovered": 18,
                                    "tests_executed": 17,
                                    "tests_failed": 1,
                                    "tests_errors": 0,
                                    "tests_skipped": 0,
                                    "junit_reports_found": 5,
                                },
                            },
                        },
                        "diagnostics": {"write_scope": {"violation_count": 13}},
                    },
                    indent=2,
                )
                + "\n",
            )


if __name__ == "__main__":
    unittest.main()
