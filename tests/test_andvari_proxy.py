from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from heimdall.andvari_proxy import (
    PROXY_ACCESS_LOG_CAPTURE_FAILED,
    PROXY_ACCESS_LOG_PREFLIGHT_FAILED,
    PROXY_RUNTIME_UNAVAILABLE,
    ProxyAccessError,
    begin_blocked_egress_capture,
    begin_proxy_access_capture,
    finish_blocked_egress_capture,
    finish_proxy_access_capture,
    pipeline_blocked_egress_artifact_path,
    pipeline_proxy_access_artifact_path,
    smoke_blocked_egress_artifact_path,
    smoke_proxy_access_artifact_path,
)


class AndvariProxyHelperTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp(prefix="heimdall-andvari-proxy-")
        self.root = Path(self.tempdir)
        self.source_log = self.root / "andvari-access.jsonl"
        self.source_log.write_text('{"step":"before"}\n', encoding="utf-8")
        self.blocked_log = self.root / "blocked-egress.jsonl"
        self.blocked_log.write_text("", encoding="utf-8")
        self.env_patch = mock.patch.dict(
            os.environ,
            {
                "HEIMDALL_ANDVARI_PROXY_ACCESS_LOG_PATH": str(self.source_log),
                "HEIMDALL_ANDVARI_BLOCKED_EGRESS_LOG_PATH": str(self.blocked_log),
            },
            clear=False,
        )
        self.env_patch.start()

    def tearDown(self) -> None:
        self.env_patch.stop()
        shutil.rmtree(self.tempdir)

    def test_artifact_path_helpers_use_host_owned_locations(self) -> None:
        self.assertEqual(
            pipeline_proxy_access_artifact_path(self.root / "run-root", "andvari-v2"),
            self.root
            / "run-root"
            / "pipeline"
            / "artifacts"
            / "proxy_access"
            / "andvari-v2.jsonl",
        )
        self.assertEqual(
            smoke_proxy_access_artifact_path(self.root / "smoke-output", "andvari"),
            self.root / "smoke-output" / "artifacts" / "proxy_access" / "andvari.jsonl",
        )
        self.assertEqual(
            pipeline_blocked_egress_artifact_path(self.root / "run-root", "andvari-v2"),
            self.root
            / "run-root"
            / "pipeline"
            / "artifacts"
            / "egress_block"
            / "andvari-v2.jsonl",
        )
        self.assertEqual(
            smoke_blocked_egress_artifact_path(self.root / "smoke-output", "andvari"),
            self.root / "smoke-output" / "artifacts" / "egress_block" / "andvari.jsonl",
        )

    def test_begin_proxy_access_capture_classifies_missing_source(self) -> None:
        missing_log = self.root / "missing" / "andvari-access.jsonl"
        with mock.patch.dict(
            os.environ,
            {"HEIMDALL_ANDVARI_PROXY_ACCESS_LOG_PATH": str(missing_log)},
            clear=False,
        ), self.assertRaises(ProxyAccessError) as raised:
            begin_proxy_access_capture(
                    "andvari",
                    self.root
                    / "pipeline"
                    / "artifacts"
                    / "proxy_access"
                    / "andvari.jsonl",
                )
        self.assertEqual(raised.exception.reason, PROXY_RUNTIME_UNAVAILABLE)
        self.assertIn("Andvari proxy access log unavailable", str(raised.exception))

    def test_begin_blocked_egress_capture_classifies_missing_source(self) -> None:
        missing_log = self.root / "missing" / "blocked-egress.jsonl"
        with mock.patch.dict(
            os.environ,
            {"HEIMDALL_ANDVARI_BLOCKED_EGRESS_LOG_PATH": str(missing_log)},
            clear=False,
        ), self.assertRaises(ProxyAccessError) as raised:
            begin_blocked_egress_capture(
                    "andvari",
                    self.root
                    / "pipeline"
                    / "artifacts"
                    / "egress_block"
                    / "andvari.jsonl",
                )
        self.assertEqual(raised.exception.reason, PROXY_RUNTIME_UNAVAILABLE)
        self.assertIn("Andvari blocked egress log unavailable", str(raised.exception))

    def test_begin_proxy_access_capture_classifies_destination_probe_failure(
        self,
    ) -> None:
        destination = (
            self.root / "pipeline" / "artifacts" / "proxy_access" / "andvari.jsonl"
        )
        with mock.patch(
            "heimdall.andvari_proxy.tempfile.mkstemp",
            side_effect=PermissionError("simulated create denial"),
        ), self.assertRaises(ProxyAccessError) as raised:
            begin_proxy_access_capture("andvari", destination)
        self.assertEqual(raised.exception.reason, PROXY_ACCESS_LOG_PREFLIGHT_FAILED)
        self.assertIn("Failed to verify write access", str(raised.exception))

    def test_finish_proxy_access_capture_classifies_destination_write_failure(
        self,
    ) -> None:
        destination = (
            self.root / "pipeline" / "artifacts" / "proxy_access" / "andvari.jsonl"
        )
        capture = begin_proxy_access_capture("andvari", destination)
        assert capture is not None
        self.source_log.write_text(
            '{"step":"before"}\n{"step":"andvari","decision":"allow"}\n',
            encoding="utf-8",
        )
        with mock.patch(
            "heimdall.andvari_proxy.os.replace",
            side_effect=PermissionError("simulated replace denial"),
        ), self.assertRaises(ProxyAccessError) as raised:
            finish_proxy_access_capture(capture, destination)
        self.assertEqual(raised.exception.reason, PROXY_ACCESS_LOG_CAPTURE_FAILED)
        self.assertIn(
            "Failed to write Andvari proxy access artifact", str(raised.exception)
        )

    def test_finish_blocked_egress_capture_classifies_destination_write_failure(
        self,
    ) -> None:
        destination = (
            self.root / "pipeline" / "artifacts" / "egress_block" / "andvari.jsonl"
        )
        capture = begin_blocked_egress_capture("andvari", destination)
        assert capture is not None
        self.blocked_log.write_text(
            '{"step":"andvari","decision":"deny","target":"github.com:22"}\n',
            encoding="utf-8",
        )
        with mock.patch(
            "heimdall.andvari_proxy.os.replace",
            side_effect=PermissionError("simulated replace denial"),
        ), self.assertRaises(ProxyAccessError) as raised:
            finish_blocked_egress_capture(capture, destination)
        self.assertEqual(raised.exception.reason, PROXY_ACCESS_LOG_CAPTURE_FAILED)
        self.assertIn(
            "Failed to write Andvari blocked egress artifact",
            str(raised.exception),
        )


if __name__ == "__main__":
    unittest.main()
