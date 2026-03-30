from __future__ import annotations

import json
import shutil
import subprocess
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
            ]
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
        self.assertEqual(pipeline_report["steps"]["brokk"]["status"], "passed")
        self.assertEqual(pipeline_report["steps"]["eitri"]["status"], "passed")
        self.assertEqual(pipeline_report["steps"]["andvari"]["status"], "passed")
        self.assertEqual(
            pipeline_report["steps"]["eitri-generated"]["status"], "passed"
        )
        self.assertEqual(pipeline_report["steps"]["kvasir"]["status"], "passed")
        self.assertEqual(
            pipeline_report["steps"]["lidskjalv-original"]["status"], "passed"
        )
        self.assertEqual(
            pipeline_report["steps"]["lidskjalv-generated"]["status"], "passed"
        )
        self.assertEqual(sonar_follow_up["status"], "skipped")
        self.assertEqual(
            sonar_follow_up["steps"]["lidskjalv-original"]["status"], "skipped"
        )
        self.assertEqual(
            sonar_follow_up["steps"]["lidskjalv-generated"]["status"], "skipped"
        )
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
            pipeline_report["repository_stats"]["andvari_generated"]["source_step"],
            "eitri-generated",
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
        self.assertEqual(
            set(artifact_index["artifacts"]),
            {
                "andvari_logs",
                "andvari_report_dir",
                "generated_repo",
                "generated_model_diagram",
                "generated_model_logs",
                "generated_model_repository_stats",
                "kvasir_report",
                "lidskjalv_generated_report",
                "lidskjalv_original_report",
                "model_diagram",
                "model_logs",
                "model_repository_stats",
                "original_repo",
                "ported_tests_repo",
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
        self.assertLess(run_by_step["andvari"]["seq"], run_by_step["kvasir"]["seq"])
        self.assertLess(
            run_by_step["andvari"]["seq"], run_by_step["lidskjalv-generated"]["seq"]
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
            ["python3", "-m", "heimdall.cli", *args],
            check=False,
            capture_output=True,
            text=True,
            cwd=str(self.root),
            env=env,
        )


if __name__ == "__main__":
    unittest.main()
