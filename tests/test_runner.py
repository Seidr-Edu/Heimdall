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
    set_fake_image_id,
    write_file,
)


class RunnerIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp(prefix="heimdall-tests-")
        self.root = Path(self.tempdir)
        self.runs_root = self.root / "runs"
        self.runs_root.mkdir(parents=True, exist_ok=True)
        self.bin_dir, self.home_dir, self.state_path = install_fake_tools(self.root)
        self.pipeline_path = self.root / "pipeline.yaml"
        write_file(self.pipeline_path, build_pipeline_manifest())

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def test_full_run_writes_reports_and_obeys_dag(self) -> None:
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
        run_report = json.loads((run_root / "pipeline" / "outputs" / "run_report.json").read_text(encoding="utf-8"))
        artifact_index = json.loads((run_root / "pipeline" / "artifact_index.json").read_text(encoding="utf-8"))
        self.assertEqual(run_report["status"], "passed")
        self.assertEqual(run_report["steps"]["andvari"]["status"], "passed")
        self.assertEqual(set(artifact_index["artifacts"]), {
            "andvari_logs",
            "andvari_report_dir",
            "generated_repo",
            "kvasir_report",
            "lidskjalv_generated_report",
            "lidskjalv_original_report",
            "model_diagram",
            "model_logs",
            "original_repo",
            "source_manifest",
        })
        self.assertTrue((run_root / "pipeline" / "logs" / "brokk.log").is_file())
        self.assertTrue((run_root / "pipeline" / "logs" / "eitri.log").is_file())

        docker_state = load_fake_state(self.state_path)
        runs = docker_state["runs"]
        run_by_step = {entry["step"]: entry for entry in runs}
        self.assertLess(run_by_step["brokk"]["seq"], run_by_step["eitri"]["seq"])
        self.assertLess(run_by_step["brokk"]["seq"], run_by_step["lidskjalv-original"]["seq"])
        self.assertLess(run_by_step["eitri"]["seq"], run_by_step["andvari"]["seq"])
        self.assertLess(run_by_step["andvari"]["seq"], run_by_step["kvasir"]["seq"])
        self.assertLess(run_by_step["andvari"]["seq"], run_by_step["lidskjalv-generated"]["seq"])
        for entry in runs:
            mount_hosts = {Path(mount["host"]) for mount in entry["mounts"]}
            self.assertNotIn(run_root / "pipeline" / "manifest.yaml", mount_hosts)

        eitri_manifest = runs[1]["manifest"] if runs[1]["step"] == "eitri" else run_by_step["eitri"]["manifest"]
        self.assertEqual(eitri_manifest["writer_extension"], ".puml")
        self.assertEqual(eitri_manifest["writers"]["plantuml"]["diagramName"], "diagram")
        self.assertEqual(eitri_manifest["writers"]["plantuml"]["hidePrivate"], True)

    def test_resume_reruns_only_failed_kvasir(self) -> None:
        failing_env = {"FAKE_DOCKER_KVASIR_MODE": "behavioral-fail"}
        first = self._run_cli(
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
            extra_env=failing_env,
        )
        self.assertEqual(first.returncode, 0, first.stderr)

        run_root = self.runs_root / "20260312T120000Z__heimdall"
        failed_report = json.loads((run_root / "pipeline" / "outputs" / "run_report.json").read_text(encoding="utf-8"))
        self.assertEqual(failed_report["steps"]["kvasir"]["status"], "failed")

        resumed = self._run_cli(
            [
                "resume",
                str(run_root),
                "--codex-bin-dir",
                str(self.bin_dir),
                "--codex-home-dir",
                str(self.home_dir),
            ]
        )
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        resumed_report = json.loads((run_root / "pipeline" / "outputs" / "run_report.json").read_text(encoding="utf-8"))
        self.assertEqual(resumed_report["status"], "passed")
        self.assertEqual(resumed_report["steps"]["brokk"]["status"], "skipped")
        self.assertEqual(resumed_report["steps"]["kvasir"]["status"], "passed")
        runs = load_fake_state(self.state_path)["runs"]
        self.assertEqual([entry["step"] for entry in runs[-1:]], ["kvasir"])

    def test_resume_invalidates_downstream_on_image_change(self) -> None:
        first = self._run_cli(
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
        self.assertEqual(first.returncode, 0, first.stderr)

        run_root = self.runs_root / "20260312T120000Z__heimdall"
        set_fake_image_id(self.state_path, "fake/andvari:1", "sha256:changed-andvari")
        resumed = self._run_cli(
            [
                "resume",
                str(run_root),
                "--codex-bin-dir",
                str(self.bin_dir),
                "--codex-home-dir",
                str(self.home_dir),
            ]
        )
        self.assertEqual(resumed.returncode, 0, resumed.stderr)

        runs = load_fake_state(self.state_path)["runs"]
        rerun_steps = [entry["step"] for entry in runs[-3:]]
        self.assertEqual(rerun_steps[0], "andvari")
        self.assertEqual(set(rerun_steps[1:]), {"kvasir", "lidskjalv-generated"})

    def test_missing_report_marks_step_error_and_blocks_downstream(self) -> None:
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
            extra_env={"FAKE_DOCKER_EITRI_MODE": "missing-report"},
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        run_root = self.runs_root / "20260312T120000Z__heimdall"
        report = json.loads((run_root / "pipeline" / "outputs" / "run_report.json").read_text(encoding="utf-8"))
        self.assertEqual(report["status"], "error")
        self.assertEqual(report["steps"]["eitri"]["status"], "error")
        self.assertEqual(report["steps"]["eitri"]["reason"], "missing-canonical-report")
        self.assertEqual(report["steps"]["andvari"]["status"], "blocked")
        self.assertEqual(report["steps"]["kvasir"]["status"], "blocked")

    def test_preflight_requires_sonar_when_enabled(self) -> None:
        sonar_manifest = build_pipeline_manifest(skip_sonar=False, run_id="20260312T120000Z__sonar")
        write_file(self.pipeline_path, sonar_manifest)
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
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("SONAR_HOST_URL", completed.stderr)

    def _run_cli(self, args: list[str], *, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
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
