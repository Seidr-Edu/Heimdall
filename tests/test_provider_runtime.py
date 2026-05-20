from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from heimdall.provider_runtime import (
    sanitize_andvari_claude_seed,
    stage_andvari_claude_api_key_seed,
)


class ProviderRuntimeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp(prefix="heimdall-provider-runtime-")
        self.root = Path(self.tempdir)

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def test_stage_andvari_claude_api_key_seed_pins_sonnet_model(self) -> None:
        destination_seed = self.root / "provider-seed" / "claude-home"

        stage_andvari_claude_api_key_seed(destination_seed)

        settings = json.loads(
            (destination_seed / "settings.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            settings["apiKeyHelper"],
            "/run/provider-state/claude-home/api-key-helper.sh",
        )
        self.assertEqual(settings["model"], "claude-sonnet-4-6")
        self.assertEqual(settings["permissions"]["deny"], ["WebSearch", "WebFetch"])
        self.assertTrue((destination_seed / "api-key-helper.sh").is_file())

    def test_sanitize_andvari_claude_seed_removes_mcp_servers_and_pins_model(
        self,
    ) -> None:
        staged_claude_home = self.root / "staged-claude-home"
        staged_claude_home.mkdir(parents=True, exist_ok=True)
        (staged_claude_home / "settings.json").write_text(
            json.dumps(
                {
                    "theme": "dark",
                    "model": "claude-3-legacy",
                    "permissions": {
                        "allow": ["Bash(git diff:*)"],
                        "deny": ["Read(./.env)"],
                    },
                    "mcpServers": {"demo": {"command": "python3"}},
                }
            )
            + "\n",
            encoding="utf-8",
        )

        sanitize_andvari_claude_seed("andvari", staged_claude_home)

        settings = json.loads(
            (staged_claude_home / "settings.json").read_text(encoding="utf-8")
        )
        self.assertEqual(settings["theme"], "dark")
        self.assertEqual(settings["model"], "claude-sonnet-4-6")
        self.assertEqual(settings["permissions"]["allow"], ["Bash(git diff:*)"])
        self.assertEqual(
            settings["permissions"]["deny"],
            ["Read(./.env)", "WebSearch", "WebFetch"],
        )
        self.assertNotIn("mcpServers", settings)
