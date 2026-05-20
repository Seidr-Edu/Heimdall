from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "retry_lidskjalv_latest_run.py"
)
SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tests.helpers import write_file


SPEC = importlib.util.spec_from_file_location("retry_lidskjalv_latest_run", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class RetryLidskjalvLatestRunTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(
            prefix="retry-lidskjalv-latest-tests-"
        )
        self.root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_resolve_target_run_defaults_to_latest_directory(self) -> None:
        (self.root / "20260510T103020Z__repo__aaaa1111").mkdir()
        latest = self.root / "20260510T103653Z__repo__bbbb2222"
        latest.mkdir()

        selected = MODULE.resolve_target_run(self.root, None)

        self.assertEqual(selected, latest)

    def test_discover_targets_for_run_limits_to_single_run(self) -> None:
        older = self.root / "20260510T103020Z__repo__aaaa1111"
        latest = self.root / "20260510T103653Z__repo__bbbb2222"
        write_file(
            older / "pipeline" / "outputs" / "run_report.json",
            json.dumps(
                {"steps": {"lidskjalv-original": {"status": "failed"}}}, indent=2
            )
            + "\n",
        )
        write_file(
            latest / "pipeline" / "outputs" / "run_report.json",
            json.dumps(
                {"steps": {"lidskjalv-generated-v2": {"status": "failed"}}}, indent=2
            )
            + "\n",
        )
        write_file(
            latest
            / "services"
            / "lidskjalv-generated-v2"
            / "run"
            / "outputs"
            / "run_report.json",
            json.dumps(
                {
                    "status": "failed",
                    "reason": "cli_fallback_failed",
                    "project_key": "repo__generated_v2",
                },
                indent=2,
            )
            + "\n",
        )

        selections = MODULE.discover_targets_for_run(
            latest,
            ("lidskjalv-original", "lidskjalv-generated-v2"),
        )

        self.assertEqual(len(selections), 1)
        self.assertEqual(selections[0].run_id, latest.name)
        self.assertEqual(selections[0].step, "lidskjalv-generated-v2")

    def test_build_replay_context_overrides_image_from_worker_config(self) -> None:
        run_root = self.root / "20260510T103653Z__repo__bbbb2222"
        write_file(
            run_root / "services" / "lidskjalv-original" / "config" / "manifest.yaml",
            "version: 1\nproject_key: repo__original\n",
        )
        write_file(
            run_root / "pipeline" / "manifest.yaml",
            "version: 1\nrun_id: x\nsource:\n  repo_url: https://github.com/example/repo.git\n  commit_sha: 0123456789abcdef0123456789abcdef01234567\nimages:\n  brokk: x\n  eitri: x\n  andvari: x\n  mimir: x\n  kvasir: x\n  lidskjalv: old\nlidskjalv:\n  skip_sonar: false\n",
        )
        write_file(
            run_root / "pipeline" / "state.json",
            json.dumps(
                {
                    "steps": {
                        "lidskjalv-original": {
                            "configured_image_ref": "ghcr.io/seidr-edu/lidskjalv@sha256:old",
                            "resolved_image_id": "sha256:old",
                        }
                    }
                },
                indent=2,
            )
            + "\n",
        )
        (run_root / "services" / "brokk" / "run" / "artifacts" / "original-repo").mkdir(
            parents=True, exist_ok=True
        )
        selection = MODULE.base.StepSelection(
            run_id=run_root.name,
            step="lidskjalv-original",
            source="service_report",
            status="failed",
            reason="unknown",
            project_key="repo__original",
            run_root=run_root,
        )

        context = MODULE.build_replay_context(
            selection,
            "ghcr.io/seidr-edu/lidskjalv:latest",
        )

        self.assertEqual(
            context.requested_image_ref, "ghcr.io/seidr-edu/lidskjalv:latest"
        )
        self.assertEqual(context.requested_image_source, "worker_config")
        self.assertEqual(
            context.configured_image_ref, "ghcr.io/seidr-edu/lidskjalv:latest"
        )


if __name__ == "__main__":
    unittest.main()
