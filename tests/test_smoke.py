from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from heimdall.cli import _preflight_provider_smoke
from heimdall.models import RuntimeConfig
from tests.helpers import (
    build_pipeline_manifest,
    fake_env,
    install_fake_tools,
    load_fake_state,
    write_file,
)


class ProviderSmokeIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp(prefix="heimdall-smoke-tests-")
        self.root = Path(self.tempdir)
        self.bin_dir, self.home_dir, self.state_path = install_fake_tools(self.root)
        self.pipeline_path = self.root / "pipeline.yaml"
        self.output_dir = self.root / "smoke-output"
        write_file(self.pipeline_path, build_pipeline_manifest())
        write_file(self.home_dir / "auth.json", '{"token":"demo"}\n', mode=0o600)
        write_file(self.home_dir / "config.toml", 'provider = "chatgpt"\n', mode=0o600)

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def test_smoke_provider_writes_logs_summary_and_runtime_seed(self) -> None:
        completed = self._run_cli(
            [
                "smoke-provider",
                str(self.pipeline_path),
                "--output-dir",
                str(self.output_dir),
                "--codex-bin-dir",
                str(self.bin_dir),
                "--codex-home-dir",
                str(self.home_dir),
            ]
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        summary = json.loads(
            (self.output_dir / "summary.json").read_text(encoding="utf-8")
        )
        self.assertEqual(summary["status"], "passed")
        self.assertEqual(summary["services"]["andvari"]["status"], "passed")
        self.assertEqual(summary["services"]["kvasir"]["status"], "passed")
        self.assertTrue((self.output_dir / "logs" / "andvari.log").is_file())
        self.assertTrue((self.output_dir / "logs" / "kvasir.log").is_file())
        self.assertTrue(
            (
                self.output_dir
                / "services"
                / "andvari"
                / "run"
                / "provider-state"
                / "codex-home"
                / "auth.json"
            ).is_file()
        )
        self.assertEqual(
            (
                self.output_dir
                / "services"
                / "andvari"
                / "run"
                / "workspace"
                / "smoke-result.txt"
            ).read_text(encoding="utf-8"),
            "heimdall-provider-smoke\n",
        )

        runs = load_fake_state(self.state_path)["runs"]
        run_by_step = {entry["step"]: entry for entry in runs}
        self.assertEqual(run_by_step["smoke-andvari"]["entrypoint"], "/bin/bash")
        self.assertEqual(run_by_step["smoke-kvasir"]["entrypoint"], "/bin/bash")
        self.assertEqual(
            self._mount_host_path(
                run_by_step["smoke-andvari"], "/opt/provider/bin"
            ).resolve(),
            (
                self.output_dir / "services" / "andvari" / "input" / "provider-bin"
            ).resolve(),
        )
        self.assertEqual(
            self._mount_host_path(
                run_by_step["smoke-andvari"], "/opt/provider-seed/codex-home"
            ).resolve(),
            (
                self.output_dir / "services" / "andvari" / "input" / "provider-seed"
            ).resolve(),
        )
        self.assertEqual(
            self._mount_host_path(run_by_step["smoke-andvari"], "/input").resolve(),
            (
                self.output_dir / "services" / "andvari" / "input" / "probe-input"
            ).resolve(),
        )

    def test_smoke_provider_classifies_exec_format_failures(self) -> None:
        completed = self._run_cli(
            [
                "smoke-provider",
                str(self.pipeline_path),
                "--output-dir",
                str(self.output_dir),
                "--codex-bin-dir",
                str(self.bin_dir),
                "--codex-home-dir",
                str(self.home_dir),
            ],
            extra_env={"FAKE_DOCKER_SMOKE_ANDVARI_MODE": "version-fail"},
        )
        self.assertEqual(completed.returncode, 1)

        summary = json.loads(
            (self.output_dir / "summary.json").read_text(encoding="utf-8")
        )
        self.assertEqual(summary["status"], "failed")
        self.assertEqual(
            summary["services"]["andvari"]["reason"],
            "codex-binary-incompatible-with-container",
        )
        self.assertIn(
            "Exec format error",
            summary["services"]["andvari"]["detail"],
        )
        self.assertIn(
            "Linux container architecture",
            summary["services"]["andvari"]["hint"],
        )
        self.assertEqual(summary["services"]["kvasir"]["status"], "passed")
        log_text = (self.output_dir / "logs" / "andvari.log").read_text(
            encoding="utf-8"
        )
        self.assertIn("Exec format error", log_text)
        self.assertIn("[heimdall][smoke] classified reason", log_text)
        self.assertNotIn("[heimdall][smoke] [smoke] service=andvari", log_text)

    def test_smoke_provider_classifies_codex_exec_workspace_failures(self) -> None:
        completed = self._run_cli(
            [
                "smoke-provider",
                str(self.pipeline_path),
                "--output-dir",
                str(self.output_dir),
                "--codex-bin-dir",
                str(self.bin_dir),
                "--codex-home-dir",
                str(self.home_dir),
            ],
            extra_env={"FAKE_DOCKER_SMOKE_ANDVARI_MODE": "exec-fail"},
        )
        self.assertEqual(completed.returncode, 1)

        summary = json.loads(
            (self.output_dir / "summary.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            summary["services"]["andvari"]["reason"],
            "codex-exec-workspace-access-failed",
        )
        self.assertIn(
            "sandbox denied access to /input/smoke.txt",
            summary["services"]["andvari"]["detail"],
        )
        self.assertIn(
            "--dangerously-bypass-approvals-and-sandbox",
            summary["services"]["andvari"]["hint"],
        )

    def test_smoke_provider_fails_when_codex_exec_skips_result_artifact(self) -> None:
        completed = self._run_cli(
            [
                "smoke-provider",
                str(self.pipeline_path),
                "--output-dir",
                str(self.output_dir),
                "--codex-bin-dir",
                str(self.bin_dir),
                "--codex-home-dir",
                str(self.home_dir),
            ],
            extra_env={"FAKE_DOCKER_SMOKE_ANDVARI_MODE": "exec-no-artifact"},
        )
        self.assertEqual(completed.returncode, 1)

        summary = json.loads(
            (self.output_dir / "summary.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            summary["services"]["andvari"]["reason"],
            "codex-exec-workspace-access-failed",
        )
        self.assertIn(
            "did not create /run/workspace/smoke-result.txt",
            summary["services"]["andvari"]["detail"],
        )

    def test_smoke_provider_accepts_distinct_host_codex_bin_dir(self) -> None:
        container_bin_dir = self.root / "container-bin"
        container_bin_dir.mkdir(parents=True, exist_ok=True)
        write_file(container_bin_dir / "codex", "not-a-host-binary\n", mode=0o755)

        completed = self._run_cli(
            [
                "smoke-provider",
                str(self.pipeline_path),
                "--output-dir",
                str(self.output_dir),
                "--codex-bin-dir",
                str(container_bin_dir),
                "--codex-host-bin-dir",
                str(self.bin_dir),
                "--codex-home-dir",
                str(self.home_dir),
            ]
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        summary = json.loads(
            (self.output_dir / "summary.json").read_text(encoding="utf-8")
        )
        self.assertEqual(summary["services"]["andvari"]["status"], "passed")
        self.assertEqual(
            summary["host"]["host_codex_executable"],
            str((self.bin_dir / "codex").resolve()),
        )
        self.assertEqual(
            summary["host"]["container_codex_executable"],
            str((container_bin_dir / "codex").resolve()),
        )

    def test_smoke_provider_rejects_output_dirs_inside_codex_bin_tree(self) -> None:
        wide_bin_dir = self.root / "wide-bin"
        wide_bin_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self.bin_dir / "codex", wide_bin_dir / "codex")
        (wide_bin_dir / "codex").chmod(0o755)
        output_dir = wide_bin_dir / "smoke-output"

        completed = self._run_cli(
            [
                "smoke-provider",
                str(self.pipeline_path),
                "--output-dir",
                str(output_dir),
                "--codex-bin-dir",
                str(wide_bin_dir),
                "--codex-home-dir",
                str(self.home_dir),
            ]
        )
        self.assertEqual(completed.returncode, 1)

        summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
        self.assertEqual(summary["services"]["andvari"]["reason"], "stage-provider-bin-failed")
        self.assertIn(
            "destination is inside the source tree",
            summary["services"]["andvari"]["detail"],
        )

    def test_preflight_provider_smoke_does_not_chmod_existing_parent(self) -> None:
        output_dir = self.root / "smoke-parent" / "provider-smoke"
        output_dir.parent.mkdir(parents=True, exist_ok=True)
        runtime = RuntimeConfig(
            runs_root=self.root / "runs",
            codex_bin_dir=self.bin_dir,
            codex_host_bin_dir=self.bin_dir,
            codex_home_dir=self.home_dir,
            pull_policy="if-missing",
            sonar_host_url=None,
            sonar_token_present=False,
            sonar_organization=None,
            verbose=False,
        )

        original_chmod = Path.chmod

        def guarded_chmod(path: Path, mode: int, *, follow_symlinks: bool = True) -> None:
            if path == output_dir.parent:
                raise PermissionError("simulated /private/tmp chmod denial")
            original_chmod(path, mode, follow_symlinks=follow_symlinks)

        with (
            mock.patch("heimdall.cli.ensure_docker_available"),
            mock.patch("heimdall.cli._check_codex_login"),
            mock.patch.object(Path, "chmod", autospec=True, side_effect=guarded_chmod),
        ):
            _preflight_provider_smoke(runtime, output_dir)

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

    def _mount_host_path(
        self, run_entry: dict[str, object], container_path: str
    ) -> Path:
        for mount in run_entry["mounts"]:
            if mount["container"] == container_path:
                return Path(mount["host"])
        self.fail(f"missing mount for {container_path}")


if __name__ == "__main__":
    unittest.main()
