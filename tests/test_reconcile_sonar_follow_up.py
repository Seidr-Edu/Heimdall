from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tests.helpers import write_file

MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "reconcile_sonar_follow_up.py"
)
SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


SPEC = importlib.util.spec_from_file_location("reconcile_sonar_follow_up", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class ReconcileSonarFollowUpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(prefix="reconcile-sonar-tests-")
        self.root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_discover_target_runs_selects_missing_follow_up_for_passed_submission(
        self,
    ) -> None:
        run_root = self.root / "20260510T154845Z__frohoff_ysoserial__218bcffc"
        self._write_state(
            run_root,
            {
                "lidskjalv-original": {
                    "status": "passed",
                    "report_status": "passed",
                }
            },
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
                    "status": "passed",
                    "scan_label": "original",
                    "project_key": "frohoff_ysoserial__original",
                    "scan": {"sonar_task_id": "AZ4Xpi0t-sqW6VKAsGUY"},
                },
                indent=2,
            )
            + "\n",
        )

        targets = MODULE.discover_target_runs(self.root)

        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0].run_id, run_root.name)
        self.assertEqual(targets[0].selection_reason, "missing_follow_up")

    def test_process_target_run_backfills_missing_follow_up_and_updates_metrics(
        self,
    ) -> None:
        run_root = (
            self.root / "20260430T141039Z__ulisesbocchio_jasypt-spring-boot__2243cb80"
        )
        self._write_state(
            run_root,
            {
                "lidskjalv-original": {
                    "status": "skipped",
                    "reason": "reused-passed-step",
                    "report_status": "passed",
                    "report_path": str(
                        run_root
                        / "services"
                        / "lidskjalv-original"
                        / "run"
                        / "outputs"
                        / "run_report.json"
                    ),
                }
            },
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
                    "status": "passed",
                    "scan_label": "original",
                    "project_key": "ulisesbocchio_jasypt-spring-boot__original",
                    "scan": {
                        "sonar_task_id": "AZ3ex2uMRvB3Y6UKUWBL",
                        "data_status": "pending",
                        "measures": {},
                    },
                },
                indent=2,
            )
            + "\n",
        )

        with mock.patch(
            "heimdall.sonar_follow_up._sonar_api_get_json",
            side_effect=[
                {"task": {"status": "SUCCESS"}},
                {"projectStatus": {"status": "OK"}},
                {
                    "component": {
                        "measures": [
                            {"metric": "bugs", "value": "0"},
                            {"metric": "coverage", "value": "81.4"},
                        ]
                    }
                },
            ],
        ):
            result = MODULE.process_target_run(
                MODULE.TargetRun(run_root, "missing_follow_up"),
                sonar_host_url="https://sonar.example.test",
                sonar_token="token",
            )

        follow_up = json.loads(
            (run_root / "pipeline" / "outputs" / "sonar_follow_up.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertTrue(result["created"])
        self.assertTrue(result["changed"])
        self.assertEqual(result["status"], "complete")
        self.assertEqual(follow_up["status"], "complete")
        self.assertEqual(
            follow_up["steps"]["lidskjalv-original"]["measures"]["bugs"], "0"
        )
        self.assertEqual(
            follow_up["steps"]["lidskjalv-original"]["measures"]["coverage"],
            "81.4",
        )

    def _write_state(self, run_root: Path, steps: dict[str, dict[str, object]]) -> None:
        write_file(
            run_root / "pipeline" / "state.json",
            json.dumps(
                {"schema_version": "heimdall_state.v1", "steps": steps}, indent=2
            )
            + "\n",
        )


if __name__ == "__main__":
    unittest.main()
