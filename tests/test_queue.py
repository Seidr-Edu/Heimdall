from __future__ import annotations

import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from heimdall.cli import main
from heimdall.manifests.queue import (
    build_pipeline_manifest_for_job,
    load_queue_request_text,
    load_worker_config,
)
from heimdall.simpleyaml import dumps, loads
from tests.helpers import (
    build_queue_request,
    build_worker_config,
    fake_env,
    install_fake_tools,
    write_file,
)


class QueueIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp(prefix="heimdall-queue-tests-")
        self.root = Path(self.tempdir)
        self.queue_root = self.root / "queue"
        self.runs_root = self.root / "runs"
        self.bin_dir, self.home_dir, self.state_path = install_fake_tools(self.root)
        self.worker_config_path = self.root / "worker.yaml"
        write_file(
            self.worker_config_path,
            build_worker_config(
                queue_root=self.queue_root,
                runs_root=self.runs_root,
                codex_bin_dir=self.bin_dir,
                codex_home_dir=self.home_dir,
            ),
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def test_enqueue_writes_yaml_job_files(self) -> None:
        completed = self._run_cli(
            [
                "enqueue",
                "--worker-config",
                str(self.worker_config_path),
                "--stdin",
            ],
            input_text=build_queue_request(),
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        document = self._load_yaml_text(completed.stdout)
        job_id = self._require_job_id(document)
        self.assertEqual(document["status"], "pending")
        self.assertTrue((self.queue_root / "pending" / job_id).is_file())
        request_document = self._load_yaml_file(
            self.queue_root / "jobs" / job_id / "request.yaml"
        )
        self.assertEqual(request_document["version"], 1)
        self.assertTrue((self.queue_root / "jobs" / job_id / "job.yaml").is_file())
        self.assertFalse((self.queue_root / "jobs" / job_id / "pipeline.yaml").exists())

    def test_enqueue_rejects_invalid_request(self) -> None:
        completed = self._run_cli(
            [
                "enqueue",
                "--worker-config",
                str(self.worker_config_path),
                "--stdin",
            ],
            input_text=build_queue_request(commit_sha="not-a-sha"),
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("commit_sha", completed.stderr)

    def test_enqueue_invalid_repo_url_reports_queue_field_path(self) -> None:
        completed = self._run_cli(
            [
                "enqueue",
                "--worker-config",
                str(self.worker_config_path),
                "--stdin",
            ],
            input_text=build_queue_request(repo_url="http://github.com/example/demo"),
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("root.repo_url", completed.stderr)

    def test_worker_processes_pending_job_and_emits_structured_logs(self) -> None:
        job_id = self._enqueue_job()

        completed = self._run_cli(
            [
                "worker",
                "--worker-config",
                str(self.worker_config_path),
                "--once",
            ]
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        job_document = self._load_yaml_file(
            self.queue_root / "jobs" / job_id / "job.yaml"
        )
        self.assertEqual(job_document["status"], "passed")
        self.assertTrue((self.queue_root / "finished" / job_id).is_file())

        report = json.loads(
            (
                self.runs_root / job_id / "pipeline" / "outputs" / "run_report.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(report["status"], "passed")

        log_lines = [
            json.loads(line)
            for line in completed.stderr.splitlines()
            if line.startswith("{")
        ]
        self.assertTrue(any(line["event"] == "worker_start" for line in log_lines))
        self.assertTrue(any(line["event"] == "job_finished" for line in log_lines))

    def test_worker_resumes_running_job_from_saved_pipeline_manifest(self) -> None:
        request_text = build_queue_request()
        job_id = self._enqueue_job(request_text=request_text)
        job_dir = self.queue_root / "jobs" / job_id

        worker_config = load_worker_config(self.worker_config_path)
        request = load_queue_request_text(request_text)
        manifest_text = build_pipeline_manifest_for_job(
            worker_config,
            request,
            run_id=job_id,
        )
        write_file(job_dir / "pipeline.yaml", manifest_text)

        os.replace(
            self.queue_root / "pending" / job_id, self.queue_root / "running" / job_id
        )
        job_document = self._load_yaml_file(job_dir / "job.yaml")
        job_document["status"] = "running"
        job_document["started_at"] = "2026-03-12T12:00:00Z"
        write_file(job_dir / "job.yaml", dumps(job_document))

        completed = self._run_cli(
            [
                "worker",
                "--worker-config",
                str(self.worker_config_path),
                "--once",
            ]
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        final_document = self._load_yaml_file(job_dir / "job.yaml")
        self.assertEqual(final_document["status"], "passed")
        self.assertTrue(
            (self.runs_root / job_id / "pipeline" / "manifest.yaml").is_file()
        )

    def test_worker_rebuilds_missing_pipeline_manifest_for_running_job(self) -> None:
        job_id = self._enqueue_job()
        job_dir = self.queue_root / "jobs" / job_id

        os.replace(
            self.queue_root / "pending" / job_id, self.queue_root / "running" / job_id
        )
        job_document = self._load_yaml_file(job_dir / "job.yaml")
        job_document["status"] = "running"
        job_document["started_at"] = "2026-03-12T12:00:00Z"
        write_file(job_dir / "job.yaml", dumps(job_document))

        completed = self._run_cli(
            [
                "worker",
                "--worker-config",
                str(self.worker_config_path),
                "--once",
            ]
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        final_document = self._load_yaml_file(job_dir / "job.yaml")
        self.assertEqual(final_document["status"], "passed")
        self.assertTrue((job_dir / "pipeline.yaml").is_file())
        self.assertTrue(
            (self.runs_root / job_id / "pipeline" / "manifest.yaml").is_file()
        )

    def test_worker_regenerates_invalid_queue_pipeline_manifest(self) -> None:
        job_id = self._enqueue_job()
        job_dir = self.queue_root / "jobs" / job_id
        write_file(job_dir / "pipeline.yaml", "")

        os.replace(
            self.queue_root / "pending" / job_id, self.queue_root / "running" / job_id
        )
        job_document = self._load_yaml_file(job_dir / "job.yaml")
        job_document["status"] = "running"
        job_document["started_at"] = "2026-03-12T12:00:00Z"
        write_file(job_dir / "job.yaml", dumps(job_document))

        completed = self._run_cli(
            [
                "worker",
                "--worker-config",
                str(self.worker_config_path),
                "--once",
            ]
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        final_document = self._load_yaml_file(job_dir / "job.yaml")
        self.assertEqual(final_document["status"], "passed")
        self.assertTrue((job_dir / "pipeline.yaml").is_file())

    def test_status_reports_pending_and_completed_jobs(self) -> None:
        job_id = self._enqueue_job()

        pending = self._run_cli(
            [
                "status",
                "--worker-config",
                str(self.worker_config_path),
                job_id,
            ]
        )
        self.assertEqual(pending.returncode, 0, pending.stderr)
        pending_document = self._load_yaml_text(pending.stdout)
        self.assertEqual(pending_document["status"], "pending")
        self.assertNotIn("pipeline", pending_document)

        worker = self._run_cli(
            [
                "worker",
                "--worker-config",
                str(self.worker_config_path),
                "--once",
            ]
        )
        self.assertEqual(worker.returncode, 0, worker.stderr)

        finished = self._run_cli(
            [
                "status",
                "--worker-config",
                str(self.worker_config_path),
                job_id,
            ]
        )
        self.assertEqual(finished.returncode, 0, finished.stderr)
        finished_document = self._load_yaml_text(finished.stdout)
        self.assertEqual(finished_document["status"], "passed")
        self.assertEqual(finished_document["pipeline"]["status"], "passed")
        self.assertIn("brokk", finished_document["pipeline"]["steps"])

    def test_status_reports_live_pipeline_for_running_job(self) -> None:
        job_id = self._enqueue_job()
        job_dir = self.queue_root / "jobs" / job_id

        os.replace(
            self.queue_root / "pending" / job_id, self.queue_root / "running" / job_id
        )
        job_document = self._load_yaml_file(job_dir / "job.yaml")
        job_document["status"] = "running"
        job_document["started_at"] = "2026-03-12T12:00:00Z"
        write_file(job_dir / "job.yaml", dumps(job_document))

        state_path = self.runs_root / job_id / "pipeline" / "state.json"
        write_file(
            state_path,
            json.dumps(
                {
                    "schema_version": "heimdall_state.v1",
                    "steps": {
                        "brokk": {
                            "status": "passed",
                            "reason": None,
                            "blocked_by": [],
                            "started_at": "2026-03-12T12:00:01Z",
                            "finished_at": "2026-03-12T12:00:05Z",
                            "configured_image_ref": "fake/brokk:1",
                            "resolved_image_id": "sha256:brokk",
                            "fingerprint": "fp-brokk",
                            "report_path": "/tmp/brokk-report.json",
                            "report_status": "passed",
                        },
                        "eitri": {
                            "status": "running",
                            "reason": None,
                            "blocked_by": [],
                            "started_at": "2026-03-12T12:00:06Z",
                            "finished_at": None,
                            "configured_image_ref": None,
                            "resolved_image_id": None,
                            "fingerprint": None,
                            "report_path": None,
                            "report_status": None,
                        },
                        "andvari": {
                            "status": "blocked",
                            "reason": "blocked-by-upstream",
                            "blocked_by": ["eitri"],
                            "started_at": "2026-03-12T12:00:07Z",
                            "finished_at": "2026-03-12T12:00:07Z",
                            "configured_image_ref": "fake/andvari:1",
                            "resolved_image_id": "sha256:andvari",
                            "fingerprint": "fp-andvari",
                            "report_path": "/tmp/andvari-report.json",
                            "report_status": None,
                        },
                    },
                    "artifacts": {},
                },
                indent=2,
            )
            + "\n",
        )

        running = self._run_cli(
            [
                "status",
                "--worker-config",
                str(self.worker_config_path),
                job_id,
            ]
        )
        self.assertEqual(running.returncode, 0, running.stderr)
        running_document = self._load_yaml_text(running.stdout)
        self.assertEqual(running_document["status"], "running")
        self.assertEqual(running_document["pipeline"]["status"], "running")
        self.assertEqual(
            running_document["pipeline"]["started_at"], "2026-03-12T12:00:00Z"
        )
        self.assertEqual(
            running_document["pipeline"]["steps"]["brokk"]["status"], "passed"
        )
        self.assertEqual(
            running_document["pipeline"]["steps"]["eitri"]["status"], "running"
        )
        self.assertEqual(
            running_document["pipeline"]["steps"]["andvari"]["blocked_by"], ["eitri"]
        )
        self.assertEqual(
            running_document["pipeline_state_path"],
            str(state_path.resolve()),
        )

    def test_status_reports_pending_sonar_follow_up_for_async_submission(self) -> None:
        write_file(
            self.worker_config_path,
            build_worker_config(
                queue_root=self.queue_root,
                runs_root=self.runs_root,
                codex_bin_dir=self.bin_dir,
                codex_home_dir=self.home_dir,
                skip_sonar=False,
            ),
        )
        job_id = self._enqueue_job()

        worker = self._run_cli(
            [
                "worker",
                "--worker-config",
                str(self.worker_config_path),
                "--once",
            ],
            extra_env={
                "SONAR_HOST_URL": "https://sonar.example.test",
                "SONAR_TOKEN": "token",
                "SONAR_ORGANIZATION": "example-org",
            },
        )
        self.assertEqual(worker.returncode, 0, worker.stderr)

        finished = self._run_cli(
            [
                "status",
                "--worker-config",
                str(self.worker_config_path),
                job_id,
            ]
        )
        self.assertEqual(finished.returncode, 0, finished.stderr)
        finished_document = self._load_yaml_text(finished.stdout)
        self.assertEqual(finished_document["status"], "passed")
        self.assertEqual(finished_document["pipeline"]["status"], "passed")
        self.assertEqual(finished_document["sonar_follow_up"]["status"], "pending")
        self.assertEqual(
            finished_document["sonar_follow_up"]["steps"]["lidskjalv-original"][
                "status"
            ],
            "pending",
        )
        self.assertEqual(
            finished_document["sonar_follow_up"]["steps"]["lidskjalv-generated"][
                "status"
            ],
            "pending",
        )
        self.assertEqual(
            finished_document["sonar_follow_up"]["steps"]["lidskjalv-generated-v2"][
                "status"
            ],
            "pending",
        )
        self.assertEqual(
            finished_document["sonar_follow_up"]["steps"]["lidskjalv-generated-v3"][
                "status"
            ],
            "pending",
        )

    def test_worker_finalizes_from_pipeline_report_on_step_command_failure(
        self,
    ) -> None:
        job_id = self._enqueue_job()

        worker = self._run_cli(
            [
                "worker",
                "--worker-config",
                str(self.worker_config_path),
                "--once",
            ],
            extra_env={"FAKE_DOCKER_EITRI_MODE": "command-fail"},
        )
        self.assertEqual(worker.returncode, 0, worker.stderr)

        job_document = self._load_yaml_file(
            self.queue_root / "jobs" / job_id / "job.yaml"
        )
        self.assertEqual(job_document["status"], "error")
        self.assertEqual(job_document["pipeline_status"], "error")
        self.assertIn("fake docker command failure for eitri", job_document["reason"])

        report = json.loads(
            (
                self.runs_root / job_id / "pipeline" / "outputs" / "run_report.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(report["status"], "error")
        self.assertEqual(report["steps"]["eitri"]["status"], "error")
        self.assertTrue((self.queue_root / "failed" / job_id).is_file())

    def test_submit_shells_out_over_ssh(self) -> None:
        overrides_path = self.root / "overrides.yaml"
        write_file(overrides_path, "andvari:\n  max_iter: 3\n")
        with mock.patch("heimdall.queueing.worker.subprocess.run") as mocked_run:
            mocked_run.return_value = subprocess.CompletedProcess(
                args=["ssh"],
                returncode=0,
                stdout="job_id: demo\n",
                stderr="",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                return_code = main(
                    [
                        "submit",
                        "--remote",
                        "munin@example",
                        "--remote-worker-config",
                        "/srv/pipeline/worker.yaml",
                        "--remote-cli",
                        "/home/munin/Heimdall/.venv/bin/heimdall",
                        "--repo-url",
                        "https://github.com/example/demo-repo.git",
                        "--commit-sha",
                        "0123456789abcdef0123456789abcdef01234567",
                        "--overrides",
                        str(overrides_path),
                    ]
                )
        self.assertEqual(return_code, 0)
        self.assertEqual(stdout.getvalue(), "job_id: demo\n")
        call = mocked_run.call_args
        self.assertEqual(call.args[0][0], "ssh")
        self.assertIn("/home/munin/Heimdall/.venv/bin/heimdall", call.args[0][2])
        self.assertIn("enqueue", call.args[0][2])
        self.assertIn("repo_url:", call.kwargs["input"])
        self.assertIn("andvari:", call.kwargs["input"])

    def test_remote_status_shells_out_over_ssh(self) -> None:
        with mock.patch("heimdall.queueing.worker.subprocess.run") as mocked_run:
            mocked_run.return_value = subprocess.CompletedProcess(
                args=["ssh"],
                returncode=0,
                stdout="status: passed\n",
                stderr="",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                return_code = main(
                    [
                        "status",
                        "--remote",
                        "munin@example",
                        "--remote-worker-config",
                        "/srv/pipeline/worker.yaml",
                        "--remote-cli",
                        "/home/munin/Heimdall/.venv/bin/heimdall",
                        "20260312T120000Z__demo__01234567",
                    ]
                )
        self.assertEqual(return_code, 0)
        self.assertEqual(stdout.getvalue(), "status: passed\n")
        call = mocked_run.call_args
        self.assertEqual(call.args[0][0], "ssh")
        self.assertIn("/home/munin/Heimdall/.venv/bin/heimdall", call.args[0][2])
        self.assertIn("status", call.args[0][2])

    def test_submit_uses_remote_env_defaults(self) -> None:
        with mock.patch("heimdall.queueing.worker.subprocess.run") as mocked_run:
            mocked_run.return_value = subprocess.CompletedProcess(
                args=["ssh"],
                returncode=0,
                stdout="job_id: demo\n",
                stderr="",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                mock.patch.dict(
                    os.environ,
                    {
                        "HEIMDALL_REMOTE": "munin@example",
                        "HEIMDALL_REMOTE_WORKER_CONFIG": "/srv/pipeline/worker.yaml",
                        "HEIMDALL_REMOTE_CLI": (
                            "/home/munin/Heimdall/.venv/bin/heimdall"
                        ),
                    },
                    clear=False,
                ),
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                return_code = main(
                    [
                        "submit",
                        "--repo-url",
                        "https://github.com/example/demo-repo.git",
                        "--commit-sha",
                        "0123456789abcdef0123456789abcdef01234567",
                    ]
                )
        self.assertEqual(return_code, 0)
        call = mocked_run.call_args
        self.assertEqual(call.args[0][1], "munin@example")
        self.assertIn("/home/munin/Heimdall/.venv/bin/heimdall", call.args[0][2])

    def test_status_uses_remote_env_defaults(self) -> None:
        with mock.patch("heimdall.queueing.worker.subprocess.run") as mocked_run:
            mocked_run.return_value = subprocess.CompletedProcess(
                args=["ssh"],
                returncode=0,
                stdout="status: pending\n",
                stderr="",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                mock.patch.dict(
                    os.environ,
                    {
                        "HEIMDALL_REMOTE": "munin@example",
                        "HEIMDALL_REMOTE_WORKER_CONFIG": "/srv/pipeline/worker.yaml",
                        "HEIMDALL_REMOTE_CLI": (
                            "/home/munin/Heimdall/.venv/bin/heimdall"
                        ),
                    },
                    clear=False,
                ),
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                return_code = main(["status", "20260312T120000Z__demo__01234567"])
        self.assertEqual(return_code, 0)
        self.assertEqual(stdout.getvalue(), "status: pending\n")
        call = mocked_run.call_args
        self.assertEqual(call.args[0][1], "munin@example")
        self.assertIn("/home/munin/Heimdall/.venv/bin/heimdall", call.args[0][2])

    def test_worker_lock_failure_returns_error(self) -> None:
        stderr = io.StringIO()
        with (
            mock.patch(
                "heimdall.queueing.worker.fcntl.flock", side_effect=BlockingIOError()
            ),
            redirect_stderr(stderr),
        ):
            return_code = main(
                [
                    "worker",
                    "--worker-config",
                    str(self.worker_config_path),
                    "--once",
                ]
            )
        self.assertEqual(return_code, 1)
        self.assertIn("Another worker is already holding", stderr.getvalue())

    def _enqueue_job(self, *, request_text: str | None = None) -> str:
        completed = self._run_cli(
            [
                "enqueue",
                "--worker-config",
                str(self.worker_config_path),
                "--stdin",
            ],
            input_text=request_text or build_queue_request(),
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        document = self._load_yaml_text(completed.stdout)
        return self._require_job_id(document)

    def _run_cli(
        self,
        args: list[str],
        *,
        input_text: str | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        env = fake_env(self.bin_dir, self.state_path, extra=extra_env)
        return subprocess.run(
            [sys.executable, "-m", "heimdall.cli", *args],
            check=False,
            capture_output=True,
            text=True,
            cwd=str(self.root),
            env=env,
            input=input_text,
        )

    def _load_yaml_file(self, path: Path) -> dict[str, object]:
        return self._load_yaml_text(path.read_text(encoding="utf-8"))

    def _load_yaml_text(self, text: str) -> dict[str, object]:
        loaded = loads(text)
        self.assertIsInstance(loaded, dict)
        return dict(loaded)

    def _require_job_id(self, document: dict[str, object]) -> str:
        raw = document.get("job_id")
        self.assertIsInstance(raw, str)
        return raw


if __name__ == "__main__":
    unittest.main()
