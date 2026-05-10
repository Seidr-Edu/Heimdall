from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest import mock

from heimdall.cli import _preflight_provider_smoke
from heimdall.models import RuntimeConfig
from heimdall.smoke import _provider_probe_script
from tests.helpers import (
    build_pipeline_manifest,
    fake_env,
    install_fake_tools,
    load_fake_state,
    with_default_andvari_runtime_args,
    write_file,
)


class ProviderSmokeIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp(prefix="heimdall-smoke-tests-")
        self.root = Path(self.tempdir)
        self.bin_dir, self.home_dir, self.state_path = install_fake_tools(self.root)
        self.pipeline_path = self.root / "pipeline.yaml"
        self.output_dir = self.root / "smoke-output"
        self.claude_api_key_file = self.root / "anthropic-api-key.txt"
        write_file(self.pipeline_path, build_pipeline_manifest())
        write_file(
            self.claude_api_key_file,
            "sk-ant-test-smoke-secret\n",
            mode=0o600,
        )
        write_file(self.home_dir / "auth.json", '{"token":"demo"}\n', mode=0o600)
        write_file(self.home_dir / "config.toml", 'provider = "chatgpt"\n', mode=0o600)
        write_file(
            self.home_dir / "skills" / ".system" / "demo" / "SKILL.md",
            "System skill\n",
            mode=0o600,
        )
        write_file(
            self.home_dir / "skills" / "custom" / "SKILL.md",
            "User skill\n",
            mode=0o600,
        )
        write_file(self.home_dir / "history.jsonl", "history\n", mode=0o600)
        write_file(self.home_dir / "sessions" / "run.jsonl", "session\n", mode=0o600)

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
        proxy_access_log = (
            self.output_dir / "artifacts" / "proxy_access" / "andvari.jsonl"
        )
        blocked_egress_log = (
            self.output_dir / "artifacts" / "egress_block" / "andvari.jsonl"
        )
        self.assertTrue(proxy_access_log.is_file())
        self.assertTrue(blocked_egress_log.is_file())
        self.assertFalse(
            (self.output_dir / "artifacts" / "proxy_access" / "kvasir.jsonl").exists()
        )
        self.assertFalse(
            (self.output_dir / "artifacts" / "egress_block" / "kvasir.jsonl").exists()
        )
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
        proxy_log_text = proxy_access_log.read_text(encoding="utf-8")
        self.assertIn('"decision": "allow"', proxy_log_text)
        self.assertIn('"target": "example.com:443"', proxy_log_text)
        self.assertIn('"decision": "deny"', proxy_log_text)
        self.assertIn('"target": "github.com:443"', proxy_log_text)
        blocked_log_text = blocked_egress_log.read_text(encoding="utf-8")
        self.assertIn('"target": "github.com:22"', blocked_log_text)
        self.assertEqual(
            Path(summary["services"]["andvari"]["proxy_access_log_path"]).resolve(),
            proxy_access_log.resolve(),
        )
        self.assertEqual(
            Path(summary["services"]["andvari"]["egress_block_log_path"]).resolve(),
            blocked_egress_log.resolve(),
        )
        self.assertIsNone(summary["services"]["kvasir"]["proxy_access_log_path"])
        self.assertIsNone(summary["services"]["kvasir"]["egress_block_log_path"])

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
        self.assertEqual(run_by_step["smoke-andvari"]["network"], "andvari-egress")
        self.assertEqual(run_by_step["smoke-andvari"]["cap_drop"], ["ALL"])
        self.assertEqual(
            run_by_step["smoke-andvari"]["security_opts"], ["no-new-privileges"]
        )
        self.assertNotIn("HTTP_PROXY", run_by_step["smoke-andvari"]["env"])
        self.assertNotIn("HTTPS_PROXY", run_by_step["smoke-andvari"]["env"])
        self.assertNotIn("NO_PROXY", run_by_step["smoke-andvari"]["env"])
        self.assertEqual(
            run_by_step["smoke-andvari"]["provider_seed_entries"],
            [
                "auth.json",
                "config.toml",
                "skills/",
                "skills/.system/",
                "skills/.system/demo/",
                "skills/.system/demo/SKILL.md",
            ],
        )
        self.assertIn(
            "history.jsonl", run_by_step["smoke-kvasir"]["provider_seed_entries"]
        )
        self.assertIn(
            "skills/custom/SKILL.md",
            run_by_step["smoke-kvasir"]["provider_seed_entries"],
        )
        self.assertNotIn(
            "sessions/", run_by_step["smoke-kvasir"]["provider_seed_entries"]
        )

    def test_smoke_provider_runs_andvari_egress_probes_by_default(self) -> None:
        write_file(
            self.home_dir / "config.toml",
            """
provider = "chatgpt"

[plugins."github@openai-curated"]
enabled = true
""".strip()
            + "\n",
            mode=0o600,
        )

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
        log_text = (self.output_dir / "logs" / "andvari.log").read_text(
            encoding="utf-8"
        )
        self.assertIn("Andvari egress probes enabled", log_text)
        self.assertIn("egress probe allowed: https://example.com", log_text)
        self.assertIn("egress probe allowed: maven dependency resolution", log_text)
        self.assertIn("egress probe allowed: gradle dependency resolution", log_text)
        self.assertIn("egress probe blocked: https://github.com", log_text)
        self.assertIn("egress probe blocked: python raw tcp github.com:22", log_text)

        runs = load_fake_state(self.state_path)["runs"]
        run_by_step = {entry["step"]: entry for entry in runs}
        self.assertEqual(run_by_step["smoke-andvari"]["network"], "andvari-egress")
        self.assertEqual(run_by_step["smoke-andvari"]["cap_drop"], ["ALL"])
        self.assertEqual(
            run_by_step["smoke-andvari"]["security_opts"], ["no-new-privileges"]
        )
        self.assertNotIn("HTTP_PROXY", run_by_step["smoke-andvari"]["env"])
        self.assertNotIn("HTTPS_PROXY", run_by_step["smoke-andvari"]["env"])
        self.assertNotIn("NO_PROXY", run_by_step["smoke-andvari"]["env"])
        self.assertNotIn("http_proxy", run_by_step["smoke-andvari"]["env"])
        self.assertNotIn("https_proxy", run_by_step["smoke-andvari"]["env"])
        self.assertNotIn("no_proxy", run_by_step["smoke-andvari"]["env"])
        self.assertEqual(
            run_by_step["smoke-andvari"]["env"]["HEIMDALL_ANDVARI_EGRESS_ENFORCED"], "1"
        )
        proxy_access_log = (
            self.output_dir / "artifacts" / "proxy_access" / "andvari.jsonl"
        )
        blocked_egress_log = (
            self.output_dir / "artifacts" / "egress_block" / "andvari.jsonl"
        )
        self.assertTrue(proxy_access_log.is_file())
        self.assertTrue(blocked_egress_log.is_file())
        proxy_log_text = proxy_access_log.read_text(encoding="utf-8")
        self.assertIn('"target": "example.com:443"', proxy_log_text)
        self.assertIn('"tool": "maven"', proxy_log_text)
        self.assertIn('"tool": "gradle"', proxy_log_text)
        self.assertIn('"target": "api.github.com:443"', proxy_log_text)
        blocked_log_text = blocked_egress_log.read_text(encoding="utf-8")
        self.assertIn('"target": "github.com:22"', blocked_log_text)
        self.assertIsNone(run_by_step["smoke-kvasir"]["network"])
        self.assertEqual(run_by_step["smoke-kvasir"]["cap_drop"], ["ALL"])
        self.assertEqual(
            run_by_step["smoke-kvasir"]["security_opts"], ["no-new-privileges"]
        )
        self.assertNotIn("HTTP_PROXY", run_by_step["smoke-kvasir"]["env"])
        self.assertNotIn("http_proxy", run_by_step["smoke-kvasir"]["env"])
        andvari_config = tomllib.loads(
            run_by_step["smoke-andvari"]["provider_seed_config"]
        )
        self.assertEqual(andvari_config["web_search"], "disabled")
        self.assertFalse(andvari_config["plugins"]["github@openai-curated"]["enabled"])
        kvasir_config = tomllib.loads(
            run_by_step["smoke-kvasir"]["provider_seed_config"]
        )
        self.assertTrue(kvasir_config["plugins"]["github@openai-curated"]["enabled"])
        self.assertNotIn(
            "web_search",
            kvasir_config,
        )

    def test_smoke_provider_supports_claude_api_key_mode_for_andvari_only(self) -> None:
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
                "--provider",
                "claude",
                "--claude-auth-mode",
                "api-key-file",
                "--claude-api-key-file",
                str(self.claude_api_key_file),
            ]
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        summary = json.loads(
            (self.output_dir / "summary.json").read_text(encoding="utf-8")
        )
        self.assertEqual(summary["status"], "passed")
        self.assertEqual(summary["services"]["andvari"]["status"], "passed")
        self.assertEqual(summary["services"]["kvasir"]["status"], "passed")
        self.assertEqual(
            Path(summary["services"]["andvari"]["runtime_provider_home"]).name,
            "claude-home",
        )
        self.assertEqual(
            Path(summary["services"]["kvasir"]["runtime_provider_home"]).name,
            "codex-home",
        )
        self.assertTrue(
            (
                self.output_dir
                / "services"
                / "andvari"
                / "run"
                / "provider-state"
                / "claude-home"
                / "settings.json"
            ).is_file()
        )
        self.assertTrue(
            (
                self.output_dir
                / "services"
                / "andvari"
                / "run"
                / "provider-state"
                / "claude-home"
                / "api-key-helper.sh"
            ).is_file()
        )
        self.assertFalse(
            (
                self.output_dir
                / "services"
                / "andvari"
                / "run"
                / "provider-state"
                / "claude-home"
                / "credentials.json"
            ).exists()
        )

        runs = load_fake_state(self.state_path)["runs"]
        run_by_step = {entry["step"]: entry for entry in runs}
        self.assertEqual(
            self._mount_host_path(
                run_by_step["smoke-andvari"], "/opt/provider-seed/claude-home"
            ).resolve(),
            (
                self.output_dir / "services" / "andvari" / "input" / "provider-seed"
            ).resolve(),
        )
        self.assertEqual(
            self._mount_host_path(
                run_by_step["smoke-kvasir"], "/opt/provider-seed/codex-home"
            ).resolve(),
            (
                self.output_dir / "services" / "kvasir" / "input" / "provider-seed"
            ).resolve(),
        )
        secret_mount_path = self._mount_host_path(
            run_by_step["smoke-andvari"], "/opt/provider-secrets/anthropic_api_key"
        ).resolve()
        self.assertEqual(secret_mount_path.name, "anthropic-api-key.txt")
        self.assertNotEqual(secret_mount_path, self.claude_api_key_file.resolve())
        self.assertFalse(
            any(
                mount["container"] == "/opt/provider-secrets/anthropic_api_key"
                for mount in run_by_step["smoke-kvasir"]["mounts"]
            )
        )
        andvari_settings = json.loads(
            run_by_step["smoke-andvari"]["provider_seed_config"]
        )
        self.assertEqual(
            andvari_settings["apiKeyHelper"],
            "/run/provider-state/claude-home/api-key-helper.sh",
        )
        self.assertEqual(andvari_settings["model"], "claude-sonnet-4-6")
        self.assertEqual(
            andvari_settings["permissions"]["deny"],
            ["WebSearch", "WebFetch"],
        )
        self.assertEqual(
            run_by_step["smoke-andvari"]["provider_seed_entries"],
            ["api-key-helper.sh", "settings.json"],
        )
        self.assertIn("auth.json", run_by_step["smoke-kvasir"]["provider_seed_entries"])
        self.assertIn("/opt/provider/bin/claude", self._service_log("andvari"))
        self.assertIn("/opt/provider/bin/codex", self._service_log("kvasir"))
        self.assertNotIn(
            "sk-ant-test-smoke-secret", self._read_tree_text(self.output_dir)
        )
        self.assertNotIn(
            "sk-ant-test-smoke-secret",
            self.state_path.read_text(encoding="utf-8"),
        )

    def test_smoke_provider_claude_api_key_mode_requires_key_file(self) -> None:
        missing_key_file = self.root / "missing-anthropic-api-key.txt"
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
                "--provider",
                "claude",
                "--claude-auth-mode",
                "api-key-file",
                "--claude-api-key-file",
                str(missing_key_file),
            ]
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("Claude API key file does not exist", completed.stderr)

    def test_smoke_provider_claude_api_key_mode_still_requires_codex_login(
        self,
    ) -> None:
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
                "--provider",
                "claude",
                "--claude-auth-mode",
                "api-key-file",
                "--claude-api-key-file",
                str(self.claude_api_key_file),
            ],
            extra_env={"FAKE_CODEX_FAIL": "1"},
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("codex login status failed", completed.stderr)

    def test_provider_probe_script_uses_gradle_44_compatible_task_syntax(self) -> None:
        script = _provider_probe_script()
        self.assertIn("task resolveSmoke {", script)
        self.assertNotIn("tasks.register('resolveSmoke')", script)

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
            "provider-binary-incompatible-with-container",
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
            "provider-exec-workspace-access-failed",
        )
        self.assertIn(
            "sandbox denied access to /input/smoke.txt",
            summary["services"]["andvari"]["detail"],
        )
        self.assertIn(
            "--dangerously-bypass-approvals-and-sandbox",
            summary["services"]["andvari"]["hint"],
        )

    def test_smoke_provider_classifies_auth_failures(self) -> None:
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
            extra_env={"FAKE_DOCKER_SMOKE_ANDVARI_MODE": "login-fail"},
        )
        self.assertEqual(completed.returncode, 1)

        summary = json.loads(
            (self.output_dir / "summary.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            summary["services"]["andvari"]["reason"],
            "codex-auth-unusable-in-container",
        )
        self.assertIn("Not logged in", summary["services"]["andvari"]["detail"])

    def test_smoke_provider_classifies_proxy_probe_failures(self) -> None:
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
            extra_env={"FAKE_DOCKER_SMOKE_ANDVARI_MODE": "github-probe-fail"},
        )
        self.assertEqual(completed.returncode, 1)

        summary = json.loads(
            (self.output_dir / "summary.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            summary["services"]["andvari"]["reason"],
            "andvari-proxy-probe-failed",
        )
        self.assertIn(
            "egress probe unexpectedly succeeded: https://github.com",
            summary["services"]["andvari"]["detail"],
        )

    def test_smoke_provider_classifies_missing_proxy_source_log(self) -> None:
        missing_log = self.root / "missing" / "andvari-access.jsonl"
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
            extra_env={"HEIMDALL_ANDVARI_PROXY_ACCESS_LOG_PATH": str(missing_log)},
        )
        self.assertEqual(completed.returncode, 1)

        summary = json.loads(
            (self.output_dir / "summary.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            summary["services"]["andvari"]["reason"], "proxy-runtime-unavailable"
        )
        self.assertIn(
            "Andvari proxy access log unavailable",
            summary["services"]["andvari"]["detail"],
        )

    def test_smoke_provider_classifies_missing_blocked_egress_source_log(self) -> None:
        missing_log = self.root / "missing" / "blocked-egress.jsonl"
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
            extra_env={"HEIMDALL_ANDVARI_BLOCKED_EGRESS_LOG_PATH": str(missing_log)},
        )
        self.assertEqual(completed.returncode, 1)

        summary = json.loads(
            (self.output_dir / "summary.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            summary["services"]["andvari"]["reason"], "proxy-runtime-unavailable"
        )
        self.assertIn(
            "Andvari blocked egress log unavailable",
            summary["services"]["andvari"]["detail"],
        )

    def test_smoke_provider_classifies_post_run_proxy_capture_failures(self) -> None:
        fake_env_map = fake_env(self.bin_dir, self.state_path)
        Path(fake_env_map["HEIMDALL_ANDVARI_PROXY_ACCESS_LOG_PATH"]).write_text(
            '{"step":"before"}\n',
            encoding="utf-8",
        )
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
            extra_env={"FAKE_DOCKER_SMOKE_ANDVARI_MODE": "proxy-log-truncated"},
        )
        self.assertEqual(completed.returncode, 1)

        summary = json.loads(
            (self.output_dir / "summary.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            summary["services"]["andvari"]["reason"],
            "proxy-access-log-capture-failed",
        )
        self.assertIn(
            "was truncated during step execution",
            summary["services"]["andvari"]["detail"],
        )

    def test_smoke_provider_classifies_post_run_blocked_egress_capture_failures(
        self,
    ) -> None:
        fake_env_map = fake_env(self.bin_dir, self.state_path)
        Path(fake_env_map["HEIMDALL_ANDVARI_BLOCKED_EGRESS_LOG_PATH"]).write_text(
            '{"step":"before"}\n',
            encoding="utf-8",
        )
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
            extra_env={"FAKE_DOCKER_SMOKE_ANDVARI_MODE": "blocked-log-truncated"},
        )
        self.assertEqual(completed.returncode, 1)

        summary = json.loads(
            (self.output_dir / "summary.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            summary["services"]["andvari"]["reason"],
            "proxy-access-log-capture-failed",
        )
        self.assertIn(
            "Andvari blocked egress log was truncated during step execution",
            summary["services"]["andvari"]["detail"],
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
            "provider-exec-workspace-access-failed",
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
            summary["host"]["host_provider_executable"],
            str((self.bin_dir / "codex").resolve()),
        )
        self.assertEqual(
            summary["host"]["container_provider_executable"],
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
        self.assertEqual(
            summary["services"]["andvari"]["reason"], "stage-provider-bin-failed"
        )
        self.assertIn(
            "destination is inside the source tree",
            summary["services"]["andvari"]["detail"],
        )

    def test_preflight_provider_smoke_does_not_chmod_existing_parent(self) -> None:
        output_dir = self.root / "smoke-parent" / "provider-smoke"
        output_dir.parent.mkdir(parents=True, exist_ok=True)
        proxy_access_log = self.root / "andvari-access.jsonl"
        proxy_access_log.write_text("", encoding="utf-8")
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
            andvari_internal_network_name="andvari-egress",
        )

        original_chmod = Path.chmod

        def guarded_chmod(
            path: Path, mode: int, *, follow_symlinks: bool = True
        ) -> None:
            if path == output_dir.parent:
                raise PermissionError("simulated /private/tmp chmod denial")
            original_chmod(path, mode, follow_symlinks=follow_symlinks)

        with (
            mock.patch("heimdall.cli.ensure_docker_available"),
            mock.patch("heimdall.cli._check_provider_login"),
            mock.patch.dict(
                os.environ,
                {"HEIMDALL_ANDVARI_PROXY_ACCESS_LOG_PATH": str(proxy_access_log)},
                clear=False,
            ),
            mock.patch.object(Path, "chmod", autospec=True, side_effect=guarded_chmod),
        ):
            _preflight_provider_smoke(runtime, output_dir)

    def _run_cli(
        self, args: list[str], *, extra_env: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        env = fake_env(self.bin_dir, self.state_path, extra=extra_env)
        return subprocess.run(
            [
                sys.executable,
                "-m",
                "heimdall.cli",
                *with_default_andvari_runtime_args(args),
            ],
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

    def _service_log(self, service: str) -> str:
        return (self.output_dir / "logs" / f"{service}.log").read_text(encoding="utf-8")

    def _read_tree_text(self, root: Path) -> str:
        chunks: list[str] = []
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            chunks.append(path.read_text(encoding="utf-8"))
        return "\n".join(chunks)


if __name__ == "__main__":
    unittest.main()
