from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from heimdall.manifests.pipeline import (
    ManifestValidationError,
    derive_lidskjalv_defaults,
    load_pipeline_manifest,
)
from tests.helpers import build_pipeline_manifest, write_file


class ManifestTest(unittest.TestCase):
    def test_load_pipeline_manifest_applies_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / "pipeline.yaml"
            write_file(manifest_path, build_pipeline_manifest())
            _raw, config = load_pipeline_manifest(manifest_path)

        self.assertEqual(config.version, 1)
        self.assertEqual(
            config.eitri.source_relpaths, ("src/main/java", "shared/src/main/java")
        )
        self.assertEqual(config.eitri.parser_extension, ".java")
        self.assertEqual(config.andvari.max_iter, 8)
        self.assertEqual(
            config.kvasir.write_scope_ignore_prefixes, ("completion/proof/logs", ".m2")
        )
        self.assertTrue(config.eitri.writers["plantuml"]["hidePrivate"])
        self.assertEqual(config.lidskjalv.sonar_wait_timeout_sec, 300)
        self.assertEqual(config.lidskjalv.sonar_wait_poll_sec, 5)

    def test_rejects_unknown_top_level_keys(self) -> None:
        source = build_pipeline_manifest() + "unexpected: true\n"
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / "pipeline.yaml"
            write_file(manifest_path, source)
            with self.assertRaises(ManifestValidationError):
                load_pipeline_manifest(manifest_path)

    def test_rejects_non_github_source(self) -> None:
        source = build_pipeline_manifest().replace(
            "https://github.com/example/demo-repo.git",
            "https://gitlab.com/example/demo-repo.git",
        )
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / "pipeline.yaml"
            write_file(manifest_path, source)
            with self.assertRaises(ManifestValidationError):
                load_pipeline_manifest(manifest_path)

    def test_derives_lidskjalv_defaults(self) -> None:
        defaults = derive_lidskjalv_defaults("https://github.com/acme/my.repo.git")
        self.assertEqual(defaults["original_key"], "acme_my.repo__original")
        self.assertEqual(defaults["generated_key"], "acme_my.repo__generated")
        self.assertEqual(defaults["generated_name"], "acme/my.repo (generated)")

    def test_accepts_legacy_sonar_wait_fields(self) -> None:
        source = build_pipeline_manifest().replace(
            "lidskjalv:\n  skip_sonar: true\n",
            "lidskjalv:\n  skip_sonar: true\n  sonar_wait_timeout_sec: 1\n  sonar_wait_poll_sec: 0\n",
        )
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / "pipeline.yaml"
            write_file(manifest_path, source)
            _raw, config = load_pipeline_manifest(manifest_path)

        self.assertEqual(config.lidskjalv.sonar_wait_timeout_sec, 1)
        self.assertEqual(config.lidskjalv.sonar_wait_poll_sec, 0)


if __name__ == "__main__":
    unittest.main()
