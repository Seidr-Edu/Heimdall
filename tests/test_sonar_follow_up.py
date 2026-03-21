from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from heimdall.cli import main
from tests.helpers import build_worker_config, write_file


class SonarWorkerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp(prefix="heimdall-sonar-worker-tests-")
        self.root = Path(self.tempdir)
        self.queue_root = self.root / "queue"
        self.runs_root = self.root / "runs"
        self.runs_root.mkdir(parents=True, exist_ok=True)
        self.worker_config_path = self.root / "worker.yaml"
        write_file(
            self.worker_config_path,
            build_worker_config(
                queue_root=self.queue_root,
                runs_root=self.runs_root,
                codex_bin_dir=self.root / "provider" / "bin",
                codex_home_dir=self.root / "provider" / "home",
            ),
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def test_sonar_worker_updates_pending_follow_up_to_complete(self) -> None:
        follow_up_path = self._write_follow_up()

        with (
            mock.patch.dict(
                "os.environ",
                {
                    "SONAR_HOST_URL": "https://sonar.example.test",
                    "SONAR_TOKEN": "token",
                },
                clear=False,
            ),
            mock.patch(
                "heimdall.sonar_follow_up._sonar_api_get_json",
                side_effect=[
                    {"task": {"status": "SUCCESS"}},
                    {"projectStatus": {"status": "OK"}},
                    {
                        "component": {
                            "measures": [
                                {"metric": "bugs", "value": "0"},
                                {"metric": "code_smells", "value": "3"},
                            ]
                        }
                    },
                ],
            ),
        ):
            return_code = main(
                [
                    "sonar-worker",
                    "--worker-config",
                    str(self.worker_config_path),
                    "--once",
                ]
            )

        self.assertEqual(return_code, 0)
        document = json.loads(follow_up_path.read_text(encoding="utf-8"))
        self.assertEqual(document["status"], "complete")
        self.assertEqual(document["steps"]["lidskjalv-original"]["status"], "complete")
        self.assertEqual(
            document["steps"]["lidskjalv-original"]["quality_gate_status"], "OK"
        )
        self.assertEqual(
            document["steps"]["lidskjalv-original"]["measures"]["bugs"], "0"
        )
        self.assertIsNone(document["steps"]["lidskjalv-original"]["reason"])

    def test_sonar_worker_records_ce_failure(self) -> None:
        follow_up_path = self._write_follow_up()

        with (
            mock.patch.dict(
                "os.environ",
                {
                    "SONAR_HOST_URL": "https://sonar.example.test",
                    "SONAR_TOKEN": "token",
                },
                clear=False,
            ),
            mock.patch(
                "heimdall.sonar_follow_up._sonar_api_get_json",
                return_value={"task": {"status": "FAILED"}},
            ),
        ):
            return_code = main(
                [
                    "sonar-worker",
                    "--worker-config",
                    str(self.worker_config_path),
                    "--once",
                ]
            )

        self.assertEqual(return_code, 0)
        document = json.loads(follow_up_path.read_text(encoding="utf-8"))
        self.assertEqual(document["status"], "failed")
        self.assertEqual(document["steps"]["lidskjalv-original"]["status"], "failed")
        self.assertEqual(
            document["steps"]["lidskjalv-original"]["reason"], "sonar-task-failed"
        )

    def test_sonar_worker_records_quality_gate_failure(self) -> None:
        follow_up_path = self._write_follow_up()

        with (
            mock.patch.dict(
                "os.environ",
                {
                    "SONAR_HOST_URL": "https://sonar.example.test",
                    "SONAR_TOKEN": "token",
                },
                clear=False,
            ),
            mock.patch(
                "heimdall.sonar_follow_up._sonar_api_get_json",
                side_effect=[
                    {"task": {"status": "SUCCESS"}},
                    {"projectStatus": {"status": "ERROR"}},
                    {"component": {"measures": [{"metric": "bugs", "value": "2"}]}},
                ],
            ),
        ):
            return_code = main(
                [
                    "sonar-worker",
                    "--worker-config",
                    str(self.worker_config_path),
                    "--once",
                ]
            )

        self.assertEqual(return_code, 0)
        document = json.loads(follow_up_path.read_text(encoding="utf-8"))
        self.assertEqual(document["status"], "failed")
        self.assertEqual(
            document["steps"]["lidskjalv-original"]["reason"], "quality-gate-failed"
        )
        self.assertEqual(
            document["steps"]["lidskjalv-original"]["quality_gate_status"], "ERROR"
        )

    def test_sonar_worker_keeps_pending_on_retryable_error(self) -> None:
        follow_up_path = self._write_follow_up()

        with (
            mock.patch.dict(
                "os.environ",
                {
                    "SONAR_HOST_URL": "https://sonar.example.test",
                    "SONAR_TOKEN": "token",
                },
                clear=False,
            ),
            mock.patch(
                "heimdall.sonar_follow_up._sonar_api_get_json",
                side_effect=RuntimeError("temporary sonar failure"),
            ),
        ):
            return_code = main(
                [
                    "sonar-worker",
                    "--worker-config",
                    str(self.worker_config_path),
                    "--once",
                ]
            )

        self.assertEqual(return_code, 0)
        document = json.loads(follow_up_path.read_text(encoding="utf-8"))
        self.assertEqual(document["status"], "pending")
        self.assertEqual(document["steps"]["lidskjalv-original"]["status"], "pending")
        self.assertIn(
            "temporary sonar failure",
            document["steps"]["lidskjalv-original"]["last_error"],
        )

    def _write_follow_up(self) -> Path:
        run_root = self.runs_root / "20260321T120000Z__heimdall"
        path = run_root / "pipeline" / "outputs" / "sonar_follow_up.json"
        payload = {
            "schema_version": "heimdall_sonar_follow_up.v1",
            "run_id": "20260321T120000Z__heimdall",
            "status": "pending",
            "updated_at": "2026-03-21T12:00:00Z",
            "steps": {
                "lidskjalv-original": {
                    "step": "lidskjalv-original",
                    "project_key": "example_demo__original",
                    "scan_label": "original",
                    "sonar_task_id": "fake-original-task",
                    "status": "pending",
                    "reason": None,
                    "ce_task_status": None,
                    "quality_gate_status": None,
                    "data_status": "pending",
                    "measures": {},
                    "last_checked_at": None,
                    "last_error": None,
                },
                "lidskjalv-generated": {
                    "step": "lidskjalv-generated",
                    "project_key": "example_demo__generated",
                    "scan_label": "generated",
                    "sonar_task_id": None,
                    "status": "skipped",
                    "reason": None,
                    "ce_task_status": None,
                    "quality_gate_status": "skipped",
                    "data_status": "skipped",
                    "measures": {},
                    "last_checked_at": None,
                    "last_error": None,
                },
            },
        }
        write_file(path, json.dumps(payload, indent=2) + "\n")
        return path


if __name__ == "__main__":
    unittest.main()
