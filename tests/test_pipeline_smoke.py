from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tests.helpers import (
    build_pipeline_manifest,
    fake_env,
    install_fake_tools,
    load_fake_state,
    write_file,
)


class PipelineSmokeIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp(prefix="heimdall-pipeline-smoke-")
        self.root = Path(self.tempdir)
        self.runs_root = self.root / "runs"
        self.runs_root.mkdir(parents=True, exist_ok=True)
        self.bin_dir, self.home_dir, self.state_path = install_fake_tools(self.root)
        self.pipeline_path = self.root / "pipeline.yaml"
        write_file(self.pipeline_path, build_pipeline_manifest())

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def test_full_pipeline_smoke_runs_and_materializes_key_artifacts(self) -> None:
        completed = self._run_cli(
            [
                "run",
                str(self.pipeline_path),
                "--runs-root",
                str(self.runs_root),
                "--codex-bin-dir",
                str(self.bin_dir),
                "--codex-home-dir",
                str(self.home_dir),
            ],
            extra_env={"FAKE_DOCKER_KVASIR_SLEEP_SEC": "0.2"},
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        run_root = self.runs_root / "20260312T120000Z__heimdall"
        pipeline_report = json.loads(
            (run_root / "pipeline" / "outputs" / "run_report.json").read_text(
                encoding="utf-8"
            )
        )
        artifact_index = json.loads(
            (run_root / "pipeline" / "artifact_index.json").read_text(encoding="utf-8")
        )
        sonar_follow_up = json.loads(
            (run_root / "pipeline" / "outputs" / "sonar_follow_up.json").read_text(
                encoding="utf-8"
            )
        )
        eitri_report = json.loads(
            (
                run_root / "services" / "eitri" / "run" / "outputs" / "run_report.json"
            ).read_text(encoding="utf-8")
        )

        self.assertEqual(pipeline_report["status"], "passed")
        expected_steps = {
            "brokk",
            "eitri",
            "lidskjalv-original",
            "andvari",
            "eitri-generated",
            "mimir",
            "kvasir",
            "lidskjalv-generated",
            "andvari-v2",
            "eitri-generated-v2",
            "mimir-v2",
            "kvasir-v2",
            "lidskjalv-generated-v2",
            "andvari-v3",
            "eitri-generated-v3",
            "mimir-v3",
            "kvasir-v3",
            "lidskjalv-generated-v3",
        }
        self.assertEqual(set(pipeline_report["steps"]), expected_steps)
        for step in expected_steps:
            self.assertEqual(pipeline_report["steps"][step]["status"], "passed")
        self.assertEqual(sonar_follow_up["status"], "skipped")
        self.assertEqual(
            set(sonar_follow_up["steps"]),
            {
                "lidskjalv-original",
                "lidskjalv-generated",
                "lidskjalv-generated-v2",
                "lidskjalv-generated-v3",
            },
        )
        for step in sonar_follow_up["steps"]:
            self.assertEqual(sonar_follow_up["steps"][step]["status"], "skipped")
        self.assertEqual(eitri_report["status"], "passed")
        self.assertEqual(
            eitri_report["artifacts"]["diagram_path"],
            "/run/artifacts/model/diagram.puml",
        )
        self.assertEqual(
            eitri_report["artifacts"]["repository_stats_path"],
            "/run/artifacts/model/repository_stats.json",
        )
        self.assertEqual(
            eitri_report["artifacts"]["model_snapshot_path"],
            "/run/artifacts/model/model_snapshot.json",
        )
        self.assertEqual(
            pipeline_report["repository_stats"]["andvari_generated"]["source_step"],
            "eitri-generated",
        )
        self.assertEqual(
            pipeline_report["repository_stats"]["andvari_generated_v2"]["source_step"],
            "eitri-generated-v2",
        )
        self.assertEqual(
            pipeline_report["repository_stats"]["andvari_generated_v3"]["source_step"],
            "eitri-generated-v3",
        )
        self.assertEqual(
            pipeline_report["diagram_comparisons"]["andvari_generated"][
                "exact_similarity"
            ],
            1.0,
        )
        self.assertEqual(
            pipeline_report["diagram_comparisons"]["andvari_generated_v2"][
                "exact_similarity"
            ],
            1.0,
        )
        self.assertEqual(
            pipeline_report["diagram_comparisons"]["andvari_generated_v3"][
                "exact_similarity"
            ],
            1.0,
        )

        self.assertTrue(
            (
                run_root
                / "services"
                / "eitri"
                / "run"
                / "artifacts"
                / "model"
                / "diagram.puml"
            ).is_file()
        )
        self.assertTrue(
            (
                run_root
                / "services"
                / "eitri"
                / "run"
                / "artifacts"
                / "model"
                / "diagram_v2.puml"
            ).is_file()
        )
        self.assertTrue(
            (
                run_root
                / "services"
                / "eitri"
                / "run"
                / "artifacts"
                / "model"
                / "diagram_v3.puml"
            ).is_file()
        )
        self.assertTrue(
            (
                run_root
                / "services"
                / "eitri-generated"
                / "run"
                / "artifacts"
                / "model"
                / "model_snapshot.json"
            ).is_file()
        )
        self.assertTrue(
            (
                run_root
                / "services"
                / "eitri-generated-v2"
                / "run"
                / "artifacts"
                / "model"
                / "model_snapshot.json"
            ).is_file()
        )
        self.assertTrue(
            (
                run_root
                / "services"
                / "mimir"
                / "run"
                / "artifacts"
                / "comparisons"
                / "andvari_generated.json"
            ).is_file()
        )
        self.assertTrue(
            (
                run_root
                / "services"
                / "mimir-v2"
                / "run"
                / "artifacts"
                / "comparisons"
                / "andvari_generated_v2.json"
            ).is_file()
        )
        self.assertTrue(
            (
                run_root
                / "services"
                / "eitri-generated"
                / "run"
                / "artifacts"
                / "model"
                / "repository_stats.json"
            ).is_file()
        )
        self.assertTrue(
            (
                run_root
                / "services"
                / "andvari"
                / "run"
                / "artifacts"
                / "generated-repo"
                / "README.md"
            ).is_file()
        )
        self.assertTrue(
            (
                run_root
                / "services"
                / "andvari-v3"
                / "run"
                / "artifacts"
                / "generated-repo"
                / "README.md"
            ).is_file()
        )
        self.assertTrue(
            (
                run_root / "services" / "kvasir" / "run" / "outputs" / "test_port.json"
            ).is_file()
        )
        self.assertTrue(
            (
                run_root
                / "services"
                / "kvasir"
                / "run"
                / "artifacts"
                / "ported-tests-repo"
                / "README.md"
            ).is_file()
        )
        self.assertTrue(
            (
                run_root
                / "services"
                / "kvasir-v2"
                / "run"
                / "artifacts"
                / "ported-tests-repo"
                / "README.md"
            ).is_file()
        )
        self.assertEqual(
            set(artifact_index["artifacts"]),
            {
                "andvari_logs",
                "andvari_logs_v2",
                "andvari_logs_v3",
                "andvari_report_dir",
                "andvari_report_dir_v2",
                "andvari_report_dir_v3",
                "generated_repo",
                "generated_repo_v2",
                "generated_repo_v3",
                "generated_model_diagram",
                "generated_model_diagram_v2",
                "generated_model_diagram_v3",
                "generated_model_logs",
                "generated_model_logs_v2",
                "generated_model_logs_v3",
                "generated_model_repository_stats",
                "generated_model_repository_stats_v2",
                "generated_model_repository_stats_v3",
                "generated_model_snapshot",
                "generated_model_snapshot_v2",
                "generated_model_snapshot_v3",
                "diagram_comparison_aggregate",
                "diagram_comparison_aggregate_v2",
                "diagram_comparison_aggregate_v3",
                "diagram_comparison_andvari_generated",
                "diagram_comparison_andvari_generated_v2",
                "diagram_comparison_andvari_generated_v3",
                "kvasir_report",
                "kvasir_v2_report",
                "kvasir_v3_report",
                "lidskjalv_generated_report",
                "lidskjalv_generated_v2_report",
                "lidskjalv_generated_v3_report",
                "lidskjalv_original_report",
                "model_diagram",
                "model_logs",
                "model_repository_stats",
                "model_snapshot",
                "mimir_report",
                "mimir_v2_report",
                "mimir_v3_report",
                "original_repo",
                "ported_tests_repo",
                "ported_tests_repo_v2",
                "ported_tests_repo_v3",
                "source_manifest",
            },
        )

        docker_state = load_fake_state(self.state_path)
        runs = docker_state["runs"]
        run_by_step = {entry["step"]: entry for entry in runs}
        self.assertLess(run_by_step["brokk"]["seq"], run_by_step["eitri"]["seq"])
        self.assertLess(
            run_by_step["brokk"]["seq"], run_by_step["lidskjalv-original"]["seq"]
        )
        self.assertLess(run_by_step["eitri"]["seq"], run_by_step["andvari"]["seq"])
        self.assertLess(
            run_by_step["andvari"]["seq"], run_by_step["eitri-generated"]["seq"]
        )
        self.assertLess(
            run_by_step["eitri-generated"]["seq"], run_by_step["mimir"]["seq"]
        )
        self.assertLess(run_by_step["andvari"]["seq"], run_by_step["kvasir"]["seq"])
        self.assertLess(
            run_by_step["kvasir"]["seq"], run_by_step["lidskjalv-generated"]["seq"]
        )
        self.assertLess(
            run_by_step["mimir"]["seq"], run_by_step["lidskjalv-generated"]["seq"]
        )
        self.assertLess(run_by_step["mimir"]["seq"], run_by_step["andvari-v2"]["seq"])
        self.assertLess(
            run_by_step["lidskjalv-generated"]["seq"],
            run_by_step["andvari-v2"]["seq"],
        )
        self.assertLess(
            run_by_step["mimir-v2"]["seq"], run_by_step["andvari-v3"]["seq"]
        )
        self.assertLess(
            run_by_step["lidskjalv-generated-v2"]["seq"],
            run_by_step["andvari-v3"]["seq"],
        )
        lidskjalv_generated_mounts = {
            mount["container"]: Path(mount["host"])
            for mount in run_by_step["lidskjalv-generated"]["mounts"]
        }
        lidskjalv_generated_v2_mounts = {
            mount["container"]: Path(mount["host"])
            for mount in run_by_step["lidskjalv-generated-v2"]["mounts"]
        }
        self.assertEqual(
            lidskjalv_generated_mounts["/input/repo"].resolve(),
            (
                run_root
                / "services"
                / "kvasir"
                / "run"
                / "artifacts"
                / "ported-tests-repo"
            ).resolve(),
        )
        self.assertEqual(
            lidskjalv_generated_v2_mounts["/input/repo"].resolve(),
            (
                run_root
                / "services"
                / "kvasir-v2"
                / "run"
                / "artifacts"
                / "ported-tests-repo"
            ).resolve(),
        )
        for entry in runs:
            mount_hosts = {Path(mount["host"]) for mount in entry["mounts"]}
            self.assertNotIn(run_root / "pipeline" / "manifest.yaml", mount_hosts)

        eitri_manifest = run_by_step["eitri"]["manifest"]
        self.assertEqual(eitri_manifest["writer_extension"], ".puml")
        self.assertEqual(
            eitri_manifest["writers"]["plantuml"]["diagramName"], "diagram"
        )
        self.assertEqual(
            eitri_manifest["writers"]["plantuml"]["hidePrivate"],
            True,
        )

    def _run_cli(
        self, args: list[str], *, extra_env: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        env = fake_env(self.bin_dir, self.state_path, extra=extra_env)
        return subprocess.run(
            [sys.executable, "-m", "heimdall.cli", *args],
            check=False,
            capture_output=True,
            text=True,
            cwd=str(self.root),
            env=env,
        )


if __name__ == "__main__":
    unittest.main()
