from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

from tests.helpers import write_file

MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "retry_lidskjalv_current_batch.py"
)
SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


SPEC = importlib.util.spec_from_file_location(
    "retry_lidskjalv_current_batch", MODULE_PATH
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class RetryLidskjalvSelectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(prefix="retry-lidskjalv-tests-")
        self.root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_prefers_failed_service_report_over_blocked_pipeline_status(self) -> None:
        run_root = self.root / "20260507T093724Z__Grt1228_chatgpt-java__ae761b89"
        write_file(
            run_root / "pipeline" / "outputs" / "run_report.json",
            json.dumps(
                {
                    "steps": {
                        "lidskjalv-original": {
                            "status": "blocked",
                            "reason": "blocked-by-upstream",
                        }
                    }
                },
                indent=2,
            )
            + "\n",
        )
        write_file(
            run_root
            / "services"
            / "lidskjalv-original"
            / "run"
            / "outputs"
            / "run_report.json",
            json.dumps(
                {
                    "status": "failed",
                    "reason": "cli_fallback_failed",
                    "project_key": "Grt1228_chatgpt-java__original",
                },
                indent=2,
            )
            + "\n",
        )

        selection = MODULE.select_step_target(
            run_root,
            "lidskjalv-original",
            MODULE.load_json_file(
                run_root / "pipeline" / "outputs" / "run_report.json"
            ),
        )

        self.assertIsNotNone(selection)
        assert selection is not None
        self.assertEqual(selection.source, "service_report")
        self.assertEqual(selection.status, "failed")
        self.assertEqual(selection.reason, "cli_fallback_failed")
        self.assertEqual(selection.project_key, "Grt1228_chatgpt-java__original")

    def test_falls_back_to_pipeline_error_when_service_report_is_missing(self) -> None:
        run_root = self.root / "20260508T095125Z__macrozheng_mall-tiny__a81ec474"
        write_file(
            run_root / "pipeline" / "outputs" / "run_report.json",
            json.dumps(
                {
                    "steps": {
                        "lidskjalv-original": {
                            "status": "error",
                            "reason": "lidskjalv-timeout",
                        }
                    }
                },
                indent=2,
            )
            + "\n",
        )
        write_file(
            run_root / "services" / "lidskjalv-original" / "config" / "manifest.yaml",
            "version: 1\nproject_key: macrozheng_mall-tiny__original\n",
        )

        selection = MODULE.select_step_target(
            run_root,
            "lidskjalv-original",
            MODULE.load_json_file(
                run_root / "pipeline" / "outputs" / "run_report.json"
            ),
        )

        self.assertIsNotNone(selection)
        assert selection is not None
        self.assertEqual(selection.source, "pipeline_report_fallback")
        self.assertEqual(selection.status, "error")
        self.assertEqual(selection.reason, "lidskjalv-timeout")
        self.assertEqual(selection.project_key, "macrozheng_mall-tiny__original")

    def test_generated_step_prefers_ported_repo_with_valid_kvasir_report(self) -> None:
        run_root = self.root / "20260507T093402Z__19MisterX98_SeedcrackerX__8e18d20d"
        ported_repo = (
            run_root
            / "services"
            / "kvasir-v3"
            / "run"
            / "artifacts"
            / "ported-tests-repo"
        )
        generated_repo = (
            run_root
            / "services"
            / "andvari-v3"
            / "run"
            / "artifacts"
            / "generated-repo"
        )
        ported_repo.mkdir(parents=True, exist_ok=True)
        generated_repo.mkdir(parents=True, exist_ok=True)
        write_file(
            run_root / "services" / "kvasir-v3" / "run" / "outputs" / "test_port.json",
            json.dumps(
                {"result": {"status": "skipped", "reason": "no-test-files-found"}},
                indent=2,
            )
            + "\n",
        )

        selected = MODULE.determine_input_repo(run_root, "lidskjalv-generated-v3")

        self.assertEqual(selected, ported_repo)


if __name__ == "__main__":
    unittest.main()
