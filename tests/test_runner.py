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
        failed_report = json.loads(
            (run_root / "pipeline" / "outputs" / "run_report.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(failed_report["status"], "failed")
        self.assertEqual(failed_report["steps"]["kvasir"]["status"], "failed")
        self.assertEqual(
            failed_report["steps"]["lidskjalv-generated"]["status"], "passed"
        )
        self.assertEqual(failed_report["steps"]["andvari-v2"]["status"], "passed")
        self.assertEqual(failed_report["steps"]["mimir-v2"]["status"], "passed")
        self.assertEqual(
            failed_report["steps"]["lidskjalv-generated-v2"]["status"], "passed"
        )
        self.assertEqual(failed_report["steps"]["andvari-v3"]["status"], "passed")
        self.assertEqual(failed_report["steps"]["mimir-v3"]["status"], "passed")

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
        resumed_report = json.loads(
            (run_root / "pipeline" / "outputs" / "run_report.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(resumed_report["status"], "passed")
        self.assertEqual(resumed_report["steps"]["brokk"]["status"], "skipped")
        self.assertEqual(resumed_report["steps"]["kvasir"]["status"], "passed")
        self.assertEqual(
            resumed_report["steps"]["lidskjalv-generated"]["status"], "passed"
        )
        runs = load_fake_state(self.state_path)["runs"]
        self.assertEqual(
            [entry["step"] for entry in runs[-2:]],
            ["kvasir", "lidskjalv-generated"],
        )

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
        rerun_steps = [entry["step"] for entry in runs[-15:]]
        self.assertEqual(
            set(rerun_steps),
            {
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
            },
        )

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
        report = json.loads(
            (run_root / "pipeline" / "outputs" / "run_report.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(report["status"], "error")
        self.assertEqual(report["steps"]["eitri"]["status"], "error")
        self.assertEqual(report["steps"]["eitri"]["reason"], "missing-canonical-report")
        self.assertEqual(report["steps"]["andvari"]["status"], "blocked")
        self.assertEqual(report["steps"]["eitri-generated"]["status"], "blocked")
        self.assertEqual(report["steps"]["mimir"]["status"], "blocked")
        self.assertEqual(report["steps"]["kvasir"]["status"], "blocked")
        self.assertEqual(report["steps"]["lidskjalv-generated"]["status"], "blocked")
        self.assertEqual(report["steps"]["andvari-v2"]["status"], "blocked")
        self.assertEqual(report["steps"]["eitri-generated-v2"]["status"], "blocked")
        self.assertEqual(report["steps"]["mimir-v2"]["status"], "blocked")
        self.assertEqual(report["steps"]["kvasir-v2"]["status"], "blocked")
        self.assertEqual(report["steps"]["lidskjalv-generated-v2"]["status"], "blocked")
        self.assertEqual(report["steps"]["andvari-v3"]["status"], "blocked")
        self.assertEqual(report["steps"]["eitri-generated-v3"]["status"], "blocked")
        self.assertEqual(report["steps"]["mimir-v3"]["status"], "blocked")
        self.assertEqual(report["steps"]["kvasir-v3"]["status"], "blocked")
        self.assertEqual(report["steps"]["lidskjalv-generated-v3"]["status"], "blocked")

    def test_resume_missing_report_does_not_reuse_stale_report_file(self) -> None:
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
        set_fake_image_id(self.state_path, "fake/kvasir:1", "sha256:changed-kvasir")

        resumed = self._run_cli(
            [
                "resume",
                str(run_root),
                "--codex-bin-dir",
                str(self.bin_dir),
                "--codex-home-dir",
                str(self.home_dir),
            ],
            extra_env={"FAKE_DOCKER_KVASIR_MODE": "missing-report"},
        )
        self.assertEqual(resumed.returncode, 0, resumed.stderr)

        report = json.loads(
            (run_root / "pipeline" / "outputs" / "run_report.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(report["status"], "error")
        self.assertEqual(report["steps"]["kvasir"]["status"], "error")
        self.assertEqual(
            report["steps"]["kvasir"]["reason"], "missing-canonical-report"
        )
        self.assertEqual(report["steps"]["lidskjalv-generated"]["status"], "passed")

        runs = load_fake_state(self.state_path)["runs"]
        run_by_step = {entry["step"]: entry for entry in runs}
        generated_mount = self._mount_host_path(
            run_by_step["lidskjalv-generated"], "/input/repo"
        )
        self.assertEqual(
            generated_mount.resolve(),
            (
                run_root
                / "services"
                / "andvari"
                / "run"
                / "artifacts"
                / "generated-repo"
            ).resolve(),
        )

    def test_nonzero_service_exit_with_canonical_report_does_not_crash_pipeline(
        self,
    ) -> None:
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
            extra_env={"FAKE_DOCKER_KVASIR_MODE": "nonzero-after-report"},
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        run_root = self.runs_root / "20260312T120000Z__heimdall"
        report = json.loads(
            (run_root / "pipeline" / "outputs" / "run_report.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["steps"]["kvasir"]["status"], "failed")
        self.assertEqual(report["steps"]["kvasir"]["reason"], "invalid-service-config")
        self.assertEqual(report["steps"]["kvasir"]["report_status"], "skipped")
        self.assertEqual(report["steps"]["lidskjalv-generated"]["status"], "passed")
        self.assertTrue(
            (
                run_root / "services" / "kvasir" / "run" / "outputs" / "test_port.json"
            ).is_file()
        )

    def test_kvasir_missing_report_uses_generated_repo_fallback(self) -> None:
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
            extra_env={"FAKE_DOCKER_KVASIR_MODE": "missing-report"},
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        run_root = self.runs_root / "20260312T120000Z__heimdall"
        report = json.loads(
            (run_root / "pipeline" / "outputs" / "run_report.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(report["status"], "error")
        self.assertEqual(report["steps"]["kvasir"]["status"], "error")
        self.assertEqual(
            report["steps"]["kvasir"]["reason"], "missing-canonical-report"
        )
        self.assertEqual(report["steps"]["lidskjalv-generated"]["status"], "passed")

        runs = load_fake_state(self.state_path)["runs"]
        run_by_step = {entry["step"]: entry for entry in runs}
        generated_mount = self._mount_host_path(
            run_by_step["lidskjalv-generated"], "/input/repo"
        )
        self.assertEqual(
            generated_mount.resolve(),
            (
                run_root
                / "services"
                / "andvari"
                / "run"
                / "artifacts"
                / "generated-repo"
            ).resolve(),
        )

    def test_codex_home_is_staged_into_readable_provider_seed_mounts(self) -> None:
        write_file(self.home_dir / "auth.json", '{"token":"demo"}\n', mode=0o600)
        write_file(self.home_dir / "config.toml", 'provider = "chatgpt"\n', mode=0o600)
        write_file(self.home_dir / "tmp" / "arg0", "transient\n", mode=0o600)
        write_file(
            self.home_dir / "log" / "codex-login.log",
            "login ok\n",
            mode=0o600,
        )

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
        runs = load_fake_state(self.state_path)["runs"]
        run_by_step = {entry["step"]: entry for entry in runs}

        andvari_seed = self._mount_host_path(
            run_by_step["andvari"], "/opt/provider-seed/codex-home"
        )
        kvasir_seed = self._mount_host_path(
            run_by_step["kvasir"], "/opt/provider-seed/codex-home"
        )

        self.assertEqual(
            andvari_seed.resolve(),
            (run_root / "services" / "andvari" / "input" / "provider-seed").resolve(),
        )
        self.assertEqual(
            kvasir_seed.resolve(),
            (run_root / "services" / "kvasir" / "input" / "provider-seed").resolve(),
        )
        self.assertNotEqual(andvari_seed, self.home_dir)
        self.assertNotEqual(kvasir_seed, self.home_dir)

        self.assertEqual(
            (self.home_dir / "tmp" / "arg0").stat().st_mode & 0o777,
            0o600,
        )
        self.assertEqual((andvari_seed / "auth.json").stat().st_mode & 0o777, 0o644)
        self.assertEqual((andvari_seed / "config.toml").stat().st_mode & 0o777, 0o644)
        self.assertEqual((andvari_seed / "tmp").stat().st_mode & 0o777, 0o755)
        self.assertEqual((andvari_seed / "tmp" / "arg0").stat().st_mode & 0o777, 0o644)
        self.assertEqual(
            (andvari_seed / "log" / "codex-login.log").stat().st_mode & 0o777,
            0o644,
        )
        self.assertTrue((kvasir_seed / "tmp" / "arg0").is_file())

    def test_codex_bin_dir_is_staged_into_executable_provider_bin_mounts(self) -> None:
        real_provider_bin = self.root / "real-provider-bin"
        real_provider_bin.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self.bin_dir / "codex", real_provider_bin / "codex")
        (real_provider_bin / "codex").chmod(0o700)

        provider_bin_dir = self.root / "provider-symlink-bin"
        provider_bin_dir.mkdir(parents=True, exist_ok=True)
        (provider_bin_dir / "codex").symlink_to(real_provider_bin / "codex")

        completed = self._run_cli(
            [
                "run",
                str(self.pipeline_path),
                "--runs-root",
                str(self.runs_root),
                "--codex-bin-dir",
                str(provider_bin_dir),
                "--codex-home-dir",
                str(self.home_dir),
            ]
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        run_root = self.runs_root / "20260312T120000Z__heimdall"
        runs = load_fake_state(self.state_path)["runs"]
        run_by_step = {entry["step"]: entry for entry in runs}

        andvari_bin = self._mount_host_path(run_by_step["andvari"], "/opt/provider/bin")
        kvasir_bin = self._mount_host_path(run_by_step["kvasir"], "/opt/provider/bin")

        self.assertEqual(
            andvari_bin.resolve(),
            (run_root / "services" / "andvari" / "input" / "provider-bin").resolve(),
        )
        self.assertEqual(
            kvasir_bin.resolve(),
            (run_root / "services" / "kvasir" / "input" / "provider-bin").resolve(),
        )
        self.assertNotEqual(andvari_bin, provider_bin_dir)
        self.assertTrue((provider_bin_dir / "codex").is_symlink())
        self.assertFalse((andvari_bin / "codex").is_symlink())
        self.assertEqual((andvari_bin / "codex").stat().st_mode & 0o777, 0o755)
        self.assertEqual((kvasir_bin / "codex").stat().st_mode & 0o777, 0o755)

    def test_preflight_requires_sonar_when_enabled(self) -> None:
        sonar_manifest = build_pipeline_manifest(
            skip_sonar=False, run_id="20260312T120000Z__sonar"
        )
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

    def _mount_host_path(
        self, run_entry: dict[str, object], container_path: str
    ) -> Path:
        for mount in run_entry["mounts"]:
            if mount["container"] == container_path:
                return Path(mount["host"])
        self.fail(f"missing mount for {container_path}")


if __name__ == "__main__":
    unittest.main()
