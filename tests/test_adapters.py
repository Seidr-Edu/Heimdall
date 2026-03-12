from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from heimdall.adapters import AdapterContext, prepare_step, step_definitions
from heimdall.manifest import load_pipeline_manifest
from heimdall.models import ResolvedImages, RuntimeConfig
from heimdall.simpleyaml import loads
from tests.helpers import build_pipeline_manifest, write_file


class AdapterTest(unittest.TestCase):
    def test_andvari_depends_only_on_eitri(self) -> None:
        self.assertEqual(step_definitions()["andvari"].depends_on, ("eitri",))

    def test_prepare_eitri_and_lidskjalv_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "pipeline.yaml"
            write_file(manifest_path, build_pipeline_manifest())
            _raw, config = load_pipeline_manifest(manifest_path)
            brokk_run = root / "run-root" / "services" / "brokk" / "run"
            (brokk_run / "artifacts" / "original-repo").mkdir(parents=True, exist_ok=True)
            (brokk_run / "inputs").mkdir(parents=True, exist_ok=True)
            write_file(
                brokk_run / "inputs" / "source-manifest.json",
                '{"repo_url":"https://github.com/example/demo-repo.git"}\n',
            )
            runtime = RuntimeConfig(
                runs_root=root / "runs",
                codex_bin_dir=root / "provider" / "bin",
                codex_home_dir=root / "provider" / "home",
                pull_policy="if-missing",
                sonar_host_url=None,
                sonar_token_present=False,
                sonar_organization=None,
            )
            runtime.codex_bin_dir.mkdir(parents=True, exist_ok=True)
            runtime.codex_home_dir.mkdir(parents=True, exist_ok=True)
            context = AdapterContext(
                config=config,
                runtime=runtime,
                run_root=root / "run-root",
                resolved_images=ResolvedImages(
                    brokk="sha256:brokk",
                    eitri="sha256:eitri",
                    andvari="sha256:andvari",
                    kvasir="sha256:kvasir",
                    lidskjalv="sha256:lidskjalv",
                ),
            )

            eitri = prepare_step("eitri", context)
            original = prepare_step("lidskjalv-original", context)

        eitri_manifest = loads(eitri.manifest_text)
        original_manifest = loads(original.manifest_text)
        self.assertEqual(eitri_manifest["source_relpaths"], ["src/main/java", "shared/src/main/java"])
        self.assertEqual(eitri_manifest["writers"]["plantuml"]["diagramName"], "diagram")
        self.assertEqual(eitri_manifest["writers"]["plantuml"]["hidePrivate"], True)
        self.assertEqual(
            [(str(mount.host_path), mount.container_path, mount.read_only) for mount in eitri.mounts],
            [
                (str(context.run_root / "services" / "brokk" / "run" / "artifacts" / "original-repo"), "/input/repo", True),
                (str(context.run_root / "services" / "eitri" / "config"), "/run/config", True),
                (str(context.run_root / "services" / "eitri" / "run"), "/run", False),
            ],
        )
        self.assertEqual(original_manifest["project_key"], "example_demo-repo__original")
        self.assertEqual(original_manifest["project_name"], "example/demo-repo (original)")
        self.assertEqual(original_manifest["repo_subdir"], "app")
        self.assertTrue(original.report_path.name.endswith("run_report.json"))


if __name__ == "__main__":
    unittest.main()
