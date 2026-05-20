from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tests.helpers import write_file


def load_script(name: str):
    path = REPO_ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


RESUBMIT = load_script("resubmit_missing_sonar")
BACKFILL = load_script("backfill_sonar_resubmissions")
RECOVER = load_script("recover_manual_sonar_submissions")


class SonarResubmissionScriptTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(prefix="sonar-resubmit-tests-")
        self.root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_current_batch_scope_is_frozen_to_40_runs_per_agent(self) -> None:
        self.assertEqual(len(RESUBMIT.CODEX_RUN_IDS), 40)
        self.assertEqual(len(RESUBMIT.CLAUDE_RUN_IDS), 40)

    def test_discovers_blocked_missing_submission_when_manifest_and_input_exist(
        self,
    ) -> None:
        run_id = "20260513T080251Z__Mojang_brigadier__b5419b18"
        run_root = self.root / run_id
        generated_repo = (
            run_root / "services" / "andvari" / "run" / "artifacts" / "generated-repo"
        )
        generated_repo.mkdir(parents=True)
        write_file(generated_repo / "src" / "Main.java", "class Main {}\n")
        write_file(
            run_root / "pipeline" / "outputs" / "sonar_follow_up.json",
            json.dumps(
                {
                    "steps": {
                        "lidskjalv-generated": {
                            "scan_label": "generated",
                            "sonar_task_id": None,
                            "status": "skipped",
                            "reason": "blocked-by-upstream",
                        }
                    }
                },
                indent=2,
            )
            + "\n",
        )
        write_file(
            run_root / "services" / "lidskjalv-generated" / "config" / "manifest.yaml",
            "\n".join(
                [
                    "version: 1",
                    f"run_id: {run_id}",
                    "scan_label: generated",
                    "project_key: Mojang_brigadier__generated_claude",
                    "project_name: Mojang/brigadier (generated) claude",
                    "skip_sonar: false",
                    "",
                ]
            ),
        )

        targets = RESUBMIT.discover_targets(
            runs_root=self.root,
            run_ids=(run_id,),
            steps=("lidskjalv-generated",),
        )

        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0].agent, "claude")
        self.assertEqual(targets[0].variant, "generated")
        self.assertEqual(targets[0].project_key, "Mojang_brigadier__generated_claude")
        self.assertEqual(targets[0].input_repo, generated_repo)

    def test_prior_success_is_idempotent(self) -> None:
        stable_dir = self.root / "run" / "lidskjalv-original"
        write_file(
            stable_dir / "manual-attempt-20260519T000000Z" / "summary.json",
            json.dumps(
                {
                    "submission_success": True,
                    "sonar_task_id": "task-1",
                    "attempt_dir": "manual-attempt-20260519T000000Z",
                },
                indent=2,
            )
            + "\n",
        )

        prior = RESUBMIT.load_prior_success(stable_dir)

        self.assertIsNotNone(prior)
        assert prior is not None
        self.assertEqual(prior["sonar_task_id"], "task-1")

    def test_manual_scanner_args_persist_report_task_metadata(self) -> None:
        target = RESUBMIT.MissingSonarTarget(
            agent="codex",
            run_id="run",
            step="lidskjalv-original",
            scan_label="original",
            variant="original",
            follow_up_status="failed",
            follow_up_reason="reason",
            project_key="demo__original",
            project_name="demo original",
            run_root=self.root,
            manifest_path=self.root / "manifest.yaml",
            input_repo=self.root / "repo",
        )

        args = RESUBMIT.manual_scanner_args(target)

        self.assertIn("-Dsonar.working.directory=/usr/src/.scannerwork", args)
        self.assertIn(
            "-Dsonar.scanner.metadataFilePath=/usr/src/.scannerwork/report-task.txt",
            args,
        )

    def test_public_project_preflight_updates_existing_project(self) -> None:
        target = RESUBMIT.MissingSonarTarget(
            agent="codex",
            run_id="run",
            step="lidskjalv-original",
            scan_label="original",
            variant="original",
            follow_up_status="failed",
            follow_up_reason="reason",
            project_key="demo__original",
            project_name="demo original",
            run_root=self.root,
            manifest_path=self.root / "manifest.yaml",
            input_repo=self.root / "repo",
        )

        with (
            mock.patch.dict(
                "os.environ",
                {
                    "SONAR_HOST_URL": "https://sonar.example.test",
                    "SONAR_TOKEN": "token",
                    "SONAR_ORGANIZATION": "org",
                },
            ),
            mock.patch.object(
                RESUBMIT,
                "sonar_api_request",
                side_effect=[
                    {"ok": True, "json": {"visibility": "private"}},
                    {"ok": True, "json": {}},
                ],
            ) as request,
        ):
            result = RESUBMIT.ensure_public_sonar_project(target)

        self.assertEqual(result["status"], "updated_existing")
        self.assertEqual(
            request.call_args_list[1].args[1], "/api/projects/update_visibility"
        )
        self.assertEqual(request.call_args_list[1].args[2]["visibility"], "public")

    def test_parse_report_task_reads_ce_task_id_and_project_key(self) -> None:
        report_task = self.root / "report-task.txt"
        write_file(
            report_task,
            "\n".join(
                [
                    "projectKey=demo_project",
                    "serverUrl=https://sonarcloud.io",
                    "ceTaskId=AZ-task",
                    "",
                ]
            ),
        )

        parsed = RESUBMIT.parse_report_task(report_task)

        self.assertEqual(parsed["projectKey"], "demo_project")
        self.assertEqual(parsed["ceTaskId"], "AZ-task")


class SonarResubmissionBackfillTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(prefix="sonar-backfill-tests-")
        self.root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_backfill_builds_sidecar_follow_up_and_metric_exports(self) -> None:
        run_id = "20260510T103653Z__19MisterX98_SeedcrackerX__8e18d20d"
        write_file(
            self.root
            / run_id
            / "lidskjalv-original"
            / "manual-attempt-20260519T000000Z"
            / "summary.json",
            json.dumps(
                {
                    "run_id": run_id,
                    "step": "lidskjalv-original",
                    "scan_label": "original",
                    "variant": "original",
                    "project_key": "19MisterX98_SeedcrackerX__original",
                    "project_name": "19MisterX98/SeedcrackerX (original)",
                    "sonar_task_id": "AZ-task",
                    "attempt_type": "manual",
                    "attempt_dir": "/tmp/attempt",
                    "submission_success": True,
                },
                indent=2,
            )
            + "\n",
        )
        successes = BACKFILL.discover_successful_submissions(self.root)
        paths = BACKFILL.sync_sidecar_follow_up(self.root, successes)

        with mock.patch(
            "heimdall.sonar_follow_up._sonar_api_get_json",
            side_effect=[
                {"task": {"status": "SUCCESS"}},
                {"projectStatus": {"status": "OK"}},
                {
                    "component": {
                        "measures": [
                            {"metric": "bugs", "value": "0"},
                            {"metric": "coverage", "value": "72.5"},
                        ]
                    }
                },
            ],
        ):
            from heimdall.sonar_follow_up import update_sonar_follow_up

            changed = update_sonar_follow_up(
                paths[0],
                sonar_host_url="https://sonar.example.test",
                sonar_token="token",
            )

        rows = BACKFILL.write_metric_exports(self.root, paths)
        follow_up = json.loads(paths[0].read_text(encoding="utf-8"))

        self.assertTrue(changed)
        self.assertEqual(follow_up["status"], "complete")
        self.assertEqual(rows[0]["bugs"], "0")
        self.assertEqual(rows[0]["coverage"], "72.5")
        self.assertTrue((self.root / "metrics.json").is_file())
        self.assertTrue((self.root / "metrics.csv").is_file())


if __name__ == "__main__":
    unittest.main()


class ManualSonarRecoveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(prefix="sonar-recovery-tests-")
        self.root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_discovers_recoverable_manual_submission_from_docker_log(self) -> None:
        summary_path = self._write_manual_attempt(
            log_text=(
                "ANALYSIS SUCCESSFUL\n"
                "More about the report processing at "
                "https://sonarcloud.io/api/ce/task?id=AZ4_testTask\n"
            )
        )

        results = RECOVER.discover_recoveries(self.root)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "recoverable")
        self.assertEqual(results[0]["sonar_task_id"], "AZ4_testTask")
        self.assertEqual(results[0]["summary_path"], str(summary_path))

    def test_apply_recovery_updates_only_sidecar_summary(self) -> None:
        summary_path = self._write_manual_attempt(
            log_text=(
                "ANALYSIS SUCCESSFUL\n"
                "More about the report processing at "
                "https://sonarcloud.io/api/ce/task?id=AZ4_applyTask\n"
            )
        )

        RECOVER.apply_recovery(summary_path, "AZ4_applyTask")

        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        self.assertEqual(summary["result"], "submission_success")
        self.assertTrue(summary["submission_success"])
        self.assertEqual(summary["sonar_task_id"], "AZ4_applyTask")
        self.assertIsNone(summary["error"])
        self.assertTrue(summary["recovered_from_docker_log"])

    def test_true_failure_when_log_has_no_successful_analysis(self) -> None:
        self._write_manual_attempt(log_text="EXECUTION FAILURE\n")

        results = RECOVER.discover_recoveries(self.root)

        self.assertEqual(results[0]["status"], "true_failure")
        self.assertIsNone(results[0]["sonar_task_id"])

    def _write_manual_attempt(self, *, log_text: str) -> Path:
        attempt_dir = (
            self.root
            / "20260506T082129Z__demo__abc123"
            / "lidskjalv-original"
            / "manual-attempt-20260519T000000Z"
        )
        summary_path = attempt_dir / "summary.json"
        docker_log_path = attempt_dir / "docker.log"
        write_file(
            summary_path,
            json.dumps(
                {
                    "run_id": "20260506T082129Z__demo__abc123",
                    "step": "lidskjalv-original",
                    "project_key": "demo__original",
                    "result": "manual_submission_failed",
                    "submission_success": False,
                    "sonar_task_id": None,
                    "error": "manual scanner did not emit a matching ceTaskId/projectKey",
                    "docker_log_path": str(docker_log_path),
                },
                indent=2,
            )
            + "\n",
        )
        write_file(docker_log_path, log_text)
        return summary_path
