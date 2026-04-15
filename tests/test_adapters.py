from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from heimdall.adapters import (
    AdapterContext,
    classify_report,
    prepare_step,
    step_definitions,
)
from heimdall.manifests.pipeline import load_pipeline_manifest
from heimdall.models import ResolvedImages, RuntimeConfig
from heimdall.simpleyaml import loads
from tests.helpers import build_pipeline_manifest, write_file


class AdapterTest(unittest.TestCase):
    def test_andvari_depends_only_on_eitri(self) -> None:
        self.assertEqual(step_definitions()["andvari"].depends_on, ("eitri",))
        self.assertEqual(step_definitions()["andvari-v2"].depends_on, ("eitri",))
        self.assertEqual(step_definitions()["andvari-v3"].depends_on, ("eitri",))
        self.assertEqual(
            step_definitions()["andvari-v2"].order_after,
            ("mimir", "lidskjalv-generated"),
        )
        self.assertEqual(
            step_definitions()["andvari-v3"].order_after,
            ("mimir-v2", "lidskjalv-generated-v2"),
        )

    def test_generated_eitri_depends_on_andvari(self) -> None:
        self.assertEqual(step_definitions()["eitri-generated"].depends_on, ("andvari",))

    def test_generated_lidskjalv_depends_on_andvari_and_waits_for_kvasir(self) -> None:
        self.assertEqual(
            step_definitions()["lidskjalv-generated"].depends_on, ("andvari",)
        )
        self.assertEqual(
            step_definitions()["lidskjalv-generated"].order_after, ("kvasir",)
        )
        self.assertEqual(
            step_definitions()["lidskjalv-generated-v2"].depends_on, ("andvari-v2",)
        )
        self.assertEqual(
            step_definitions()["lidskjalv-generated-v2"].order_after, ("kvasir-v2",)
        )
        self.assertEqual(
            step_definitions()["lidskjalv-generated-v3"].depends_on, ("andvari-v3",)
        )
        self.assertEqual(
            step_definitions()["lidskjalv-generated-v3"].order_after, ("kvasir-v3",)
        )

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
                    mimir="sha256:mimir",
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
        self.assertNotIn(
            "generateDegradedDiagrams",
            eitri_manifest["writers"]["plantuml"],
        )
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
            write_file(
                eitri_model / "diagram_v2.puml", "@startuml\nclass V2\n@enduml\n"
            )
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
            write_file(andvari_generated / "README.md", "generated\n")
            andvari_v2_generated = (
                root
                / "run-root"
                / "services"
                / "andvari-v2"
                / "run"
                / "artifacts"
                / "generated-repo"
            )
            andvari_v2_generated.mkdir(parents=True, exist_ok=True)
            write_file(andvari_v2_generated / "README.md", "generated v2\n")
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
                    mimir="sha256:mimir",
                    kvasir="sha256:kvasir",
                    lidskjalv="sha256:lidskjalv",
                ),
            )

            kvasir = prepare_step("kvasir", context)
            kvasir_v2 = prepare_step("kvasir-v2", context)
            generated_eitri = prepare_step("eitri-generated", context)
            generated_eitri_v2 = prepare_step("eitri-generated-v2", context)
            generated_eitri_manifest = loads(generated_eitri.manifest_text)
            generated_eitri_v2_manifest = loads(generated_eitri_v2.manifest_text)
            hints = json.loads(
                (kvasir.config_dir / "build-hints.json").read_text(encoding="utf-8")
            )
            kvasir_v2_staged_diagram = (
                kvasir_v2.service_root / "input" / "model" / "diagram.puml"
            ).read_text(encoding="utf-8")

        self.assertEqual(
            kvasir.env["KVASIR_BUILD_HINTS"], "/run/config/build-hints.json"
        )
        self.assertEqual(hints["original"]["build_tool"], "maven")
        self.assertEqual(hints["original"]["build_jdk"], "8")
        self.assertEqual(hints["original"]["build_subdir"], "app")
        self.assertEqual(hints["original"]["java_version_hint"], "6")
        self.assertEqual(hints["original"]["source"], "lidskjalv-original")
        self.assertEqual(
            [
                (str(mount.host_path), mount.container_path, mount.read_only)
                for mount in generated_eitri.mounts
            ][0],
            (
                str(andvari_generated),
                "/input/repo",
                True,
            ),
        )
        self.assertEqual(
            [
                (str(mount.host_path), mount.container_path, mount.read_only)
                for mount in kvasir_v2.mounts
            ][1],
            (
                str(andvari_v2_generated),
                "/input/generated-repo",
                True,
            ),
        )
        self.assertEqual(
            kvasir_v2_staged_diagram,
            "@startuml\nclass V2\n@enduml\n",
        )
        self.assertEqual(
            [
                (str(mount.host_path), mount.container_path, mount.read_only)
                for mount in generated_eitri_v2.mounts
            ][0],
            (
                str(andvari_v2_generated),
                "/input/repo",
                True,
            ),
        )
        self.assertEqual(
            generated_eitri_manifest["writers"]["plantuml"]["diagramName"], "diagram"
        )
        self.assertEqual(
            generated_eitri_manifest["writers"]["plantuml"]["generateDegradedDiagrams"],
            False,
        )
        self.assertEqual(
            generated_eitri_v2_manifest["writers"]["plantuml"][
                "generateDegradedDiagrams"
            ],
            False,
        )

    def test_prepare_generated_lidskjalv_mounts_kvasir_ported_repo(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "pipeline.yaml"
            write_file(manifest_path, build_pipeline_manifest())
            _raw, config = load_pipeline_manifest(manifest_path)
            brokk_run = root / "run-root" / "services" / "brokk" / "run"
            (brokk_run / "artifacts" / "original-repo").mkdir(
                parents=True, exist_ok=True
            )
            ported_repo = (
                root
                / "run-root"
                / "services"
                / "kvasir"
                / "run"
                / "artifacts"
                / "ported-tests-repo"
            )
            ported_repo.mkdir(parents=True, exist_ok=True)
            write_file(ported_repo / "README.md", "ported\n")
            write_file(
                root
                / "run-root"
                / "services"
                / "kvasir"
                / "run"
                / "outputs"
                / "test_port.json",
                """
{
  "schema_version": "kvasir.test_port.v3",
  "result": {
    "status": "passed",
    "verdict": "no_difference_detected"
  }
}
""".strip()
                + "\n",
            )
            ported_repo_v2 = (
                root
                / "run-root"
                / "services"
                / "kvasir-v2"
                / "run"
                / "artifacts"
                / "ported-tests-repo"
            )
            ported_repo_v2.mkdir(parents=True, exist_ok=True)
            write_file(ported_repo_v2 / "README.md", "ported v2\n")
            write_file(
                root
                / "run-root"
                / "services"
                / "kvasir-v2"
                / "run"
                / "outputs"
                / "test_port.json",
                """
{
  "schema_version": "kvasir.test_port.v3",
  "result": {
    "status": "passed",
    "verdict": "no_difference_detected"
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
                    mimir="sha256:mimir",
                    kvasir="sha256:kvasir",
                    lidskjalv="sha256:lidskjalv",
                ),
            )

            generated = prepare_step("lidskjalv-generated", context)
            generated_v2 = prepare_step("lidskjalv-generated-v2", context)

        self.assertEqual(
            [
                (str(mount.host_path), mount.container_path, mount.read_only)
                for mount in generated.mounts
            ],
            [
                (str(ported_repo), "/input/repo", True),
                (
                    str(
                        context.run_root / "services" / "lidskjalv-generated" / "config"
                    ),
                    "/run/config",
                    True,
                ),
                (
                    str(context.run_root / "services" / "lidskjalv-generated" / "run"),
                    "/run",
                    False,
                ),
            ],
        )
        self.assertEqual(
            [
                (str(mount.host_path), mount.container_path, mount.read_only)
                for mount in generated_v2.mounts
            ][0],
            (str(ported_repo_v2), "/input/repo", True),
        )

    def test_prepare_generated_lidskjalv_falls_back_to_andvari_repo(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "pipeline.yaml"
            write_file(manifest_path, build_pipeline_manifest())
            _raw, config = load_pipeline_manifest(manifest_path)
            brokk_run = root / "run-root" / "services" / "brokk" / "run"
            (brokk_run / "artifacts" / "original-repo").mkdir(
                parents=True, exist_ok=True
            )
            generated_repo = (
                root
                / "run-root"
                / "services"
                / "andvari"
                / "run"
                / "artifacts"
                / "generated-repo"
            )
            generated_repo.mkdir(parents=True, exist_ok=True)
            write_file(generated_repo / "README.md", "generated\n")
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
                    mimir="sha256:mimir",
                    kvasir="sha256:kvasir",
                    lidskjalv="sha256:lidskjalv",
                ),
            )

            generated = prepare_step("lidskjalv-generated", context)

        self.assertEqual(
            [
                (str(mount.host_path), mount.container_path, mount.read_only)
                for mount in generated.mounts
            ][0],
            (str(generated_repo), "/input/repo", True),
        )

    def test_classify_kvasir_v3_report_records_promoted_ported_repo(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / "services" / "kvasir" / "run"
            report_path = run_dir / "outputs" / "test_port.json"
            report_path.parent.mkdir(parents=True, exist_ok=True)
            write_file(
                report_path,
                """
{
  "schema_version": "kvasir.test_port.v3",
  "result": {
    "status": "passed",
    "verdict": "no_difference_detected"
  }
}
""".strip()
                + "\n",
            )
            promoted_repo = run_dir / "artifacts" / "ported-tests-repo"
            promoted_repo.mkdir(parents=True, exist_ok=True)

            status, reason, artifacts = classify_report("kvasir", report_path)

        self.assertEqual(status, "passed")
        self.assertIsNone(reason)
        self.assertIn("kvasir_report", artifacts)
        self.assertIn("ported_tests_repo", artifacts)

    def test_classify_kvasir_legacy_report_fallback_still_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / "services" / "kvasir" / "run"
            report_path = run_dir / "outputs" / "test_port.json"
            report_path.parent.mkdir(parents=True, exist_ok=True)
            write_file(
                report_path,
                """
{
  "status": "passed",
  "behavioral_verdict": "pass"
}
""".strip()
                + "\n",
            )

            status, reason, _artifacts = classify_report("kvasir", report_path)

        self.assertEqual(status, "passed")
        self.assertIsNone(reason)

    def test_classify_report_rejects_non_object_json_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / "services" / "andvari" / "run"
            report_path = run_dir / "outputs" / "run_report.json"
            report_path.parent.mkdir(parents=True, exist_ok=True)
            write_file(report_path, "[]\n")

            status, reason, artifacts = classify_report("andvari", report_path)

        self.assertEqual(status, "error")
        self.assertEqual(reason, "invalid-canonical-report")
        self.assertEqual(artifacts, {})

    def test_prepare_mimir_manifest_and_mounts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "pipeline.yaml"
            write_file(manifest_path, build_pipeline_manifest())
            _raw, config = load_pipeline_manifest(manifest_path)
            original_diagram = (
                root
                / "run-root"
                / "services"
                / "eitri"
                / "run"
                / "artifacts"
                / "model"
                / "diagram.puml"
            )
            generated_diagram = (
                root
                / "run-root"
                / "services"
                / "eitri-generated"
                / "run"
                / "artifacts"
                / "model"
                / "diagram.puml"
            )
            write_file(original_diagram, "@startuml\nclass Original\n@enduml\n")
            write_file(generated_diagram, "@startuml\nclass Generated\n@enduml\n")
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
                    mimir="sha256:mimir",
                    kvasir="sha256:kvasir",
                    lidskjalv="sha256:lidskjalv",
                ),
            )

            mimir = prepare_step("mimir", context)
            staged_generated_diagram = (
                mimir.run_dir
                / "inputs"
                / "diagrams"
                / "andvari_generated"
                / "diagram.puml"
            ).read_text(encoding="utf-8")

        mimir_manifest = loads(mimir.manifest_text)
        self.assertEqual(mimir_manifest["mode"], "diagram")
        self.assertEqual(mimir_manifest["baseline_label"], "original")
        self.assertEqual(
            mimir_manifest["baseline_diagram_relpath"], "original/diagram.puml"
        )
        self.assertEqual(mimir_manifest["candidates"][0]["label"], "andvari_generated")
        self.assertEqual(
            mimir_manifest["candidates"][0]["diagram_relpath"],
            "andvari_generated/diagram.puml",
        )
        self.assertEqual(
            staged_generated_diagram,
            "@startuml\nclass Generated\n@enduml\n",
        )
        self.assertEqual(mimir.env["MIMIR_MANIFEST"], "/run/config/manifest.yaml")
        self.assertEqual(
            [
                (str(mount.host_path), mount.container_path, mount.read_only)
                for mount in mimir.mounts
            ],
            [
                (
                    str(context.run_root / "services" / "mimir" / "config"),
                    "/run/config",
                    True,
                ),
                (str(context.run_root / "services" / "mimir" / "run"), "/run", False),
            ],
        )


if __name__ == "__main__":
    unittest.main()
