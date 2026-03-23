from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from heimdall.adapters import AdapterContext, prepare_step, step_definitions
from heimdall.manifests.pipeline import load_pipeline_manifest
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
            (brokk_run / "artifacts" / "original-repo").mkdir(
                parents=True, exist_ok=True
            )
            (brokk_run / "inputs").mkdir(parents=True, exist_ok=True)
            write_file(
                brokk_run / "inputs" / "source-manifest.json",
                '{"repo_url":"https://github.com/example/demo-repo.git"}\n',
            )
            runtime = RuntimeConfig(
                runs_root=root / "runs",
                codex_bin_dir=root / "provider" / "bin",
                codex_host_bin_dir=root / "provider" / "bin",
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
        self.assertEqual(
            eitri_manifest["source_relpaths"], ["src/main/java", "shared/src/main/java"]
        )
        self.assertEqual(
            eitri_manifest["writers"]["plantuml"]["diagramName"], "diagram"
        )
        self.assertEqual(eitri_manifest["writers"]["plantuml"]["hidePrivate"], True)
        self.assertEqual(
            [
                (str(mount.host_path), mount.container_path, mount.read_only)
                for mount in eitri.mounts
            ],
            [
                (
                    str(
                        context.run_root
                        / "services"
                        / "brokk"
                        / "run"
                        / "artifacts"
                        / "original-repo"
                    ),
                    "/input/repo",
                    True,
                ),
                (
                    str(context.run_root / "services" / "eitri" / "config"),
                    "/run/config",
                    True,
                ),
                (str(context.run_root / "services" / "eitri" / "run"), "/run", False),
            ],
        )
        self.assertEqual(
            original_manifest["project_key"], "example_demo-repo__original"
        )
        self.assertEqual(
            original_manifest["project_name"], "example/demo-repo (original)"
        )
        self.assertEqual(original_manifest["repo_subdir"], "app")
        self.assertNotIn("sonar_wait_timeout_sec", original_manifest)
        self.assertNotIn("sonar_wait_poll_sec", original_manifest)
        self.assertTrue(original.report_path.name.endswith("run_report.json"))

    def test_prepare_kvasir_stages_optional_build_hints(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "pipeline.yaml"
            write_file(manifest_path, build_pipeline_manifest())
            _raw, config = load_pipeline_manifest(manifest_path)
            brokk_run = root / "run-root" / "services" / "brokk" / "run"
            (brokk_run / "artifacts" / "original-repo").mkdir(
                parents=True, exist_ok=True
            )
            eitri_model = (
                root / "run-root" / "services" / "eitri" / "run" / "artifacts" / "model"
            )
            eitri_model.mkdir(parents=True, exist_ok=True)
            write_file(eitri_model / "diagram.puml", "@startuml\n@enduml\n")
            andvari_generated = (
                root
                / "run-root"
                / "services"
                / "andvari"
                / "run"
                / "artifacts"
                / "generated-repo"
            )
            andvari_generated.mkdir(parents=True, exist_ok=True)
            write_file(
                root
                / "run-root"
                / "services"
                / "lidskjalv-original"
                / "run"
                / "outputs"
                / "run_report.json",
                """
{
  "status": "failed",
  "scan": {
    "build_tool": "maven",
    "build_jdk": "8",
    "build_subdir": "app",
    "java_version_hint": "6"
  }
}
""".strip()
                + "\n",
            )
            runtime = RuntimeConfig(
                runs_root=root / "runs",
                codex_bin_dir=root / "provider" / "bin",
                codex_host_bin_dir=root / "provider" / "bin",
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

            kvasir = prepare_step("kvasir", context)
            hints = json.loads(
                (kvasir.config_dir / "build-hints.json").read_text(encoding="utf-8")
            )

        self.assertEqual(kvasir.env["KVASIR_BUILD_HINTS"], "/run/config/build-hints.json")
        self.assertEqual(hints["original"]["build_tool"], "maven")
        self.assertEqual(hints["original"]["build_jdk"], "8")
        self.assertEqual(hints["original"]["build_subdir"], "app")
        self.assertEqual(hints["original"]["java_version_hint"], "6")
        self.assertEqual(hints["original"]["source"], "lidskjalv-original")


if __name__ == "__main__":
    unittest.main()
