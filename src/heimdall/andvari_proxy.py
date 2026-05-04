from __future__ import annotations

import contextlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from heimdall.models import STEP_ANDVARI, STEP_ANDVARI_V2, STEP_ANDVARI_V3

ANDVARI_PROXY_STEPS = frozenset({STEP_ANDVARI, STEP_ANDVARI_V2, STEP_ANDVARI_V3})
DEFAULT_ANDVARI_PROXY_ACCESS_LOG_PATH = Path("/var/log/squid/andvari-access.jsonl")
DEFAULT_ANDVARI_BLOCKED_EGRESS_LOG_PATH = Path("/var/log/andvari/blocked-egress.jsonl")
_PROXY_ACCESS_LOG_OVERRIDE_ENV = "HEIMDALL_ANDVARI_PROXY_ACCESS_LOG_PATH"
_BLOCKED_EGRESS_LOG_OVERRIDE_ENV = "HEIMDALL_ANDVARI_BLOCKED_EGRESS_LOG_PATH"
PROXY_RUNTIME_UNAVAILABLE = "proxy-runtime-unavailable"
PROXY_ACCESS_LOG_PREFLIGHT_FAILED = "proxy-access-log-preflight-failed"
PROXY_ACCESS_LOG_CAPTURE_FAILED = "proxy-access-log-capture-failed"


class ProxyAccessError(RuntimeError):
    def __init__(self, reason: str, detail: str):
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class ProxyAccessCapture:
    source_path: Path
    source_device: int
    source_inode: int
    start_offset: int


def uses_andvari_proxy_runtime(step: str) -> bool:
    return step in ANDVARI_PROXY_STEPS


def andvari_proxy_access_log_path() -> Path:
    return _resolve_source_log_path(
        _PROXY_ACCESS_LOG_OVERRIDE_ENV, DEFAULT_ANDVARI_PROXY_ACCESS_LOG_PATH
    )


def andvari_blocked_egress_log_path() -> Path:
    return _resolve_source_log_path(
        _BLOCKED_EGRESS_LOG_OVERRIDE_ENV, DEFAULT_ANDVARI_BLOCKED_EGRESS_LOG_PATH
    )


def validate_andvari_proxy_access_log() -> Path:
    path = andvari_proxy_access_log_path()
    return _validate_source_log(path, "Andvari proxy access log")


def validate_andvari_blocked_egress_log() -> Path:
    path = andvari_blocked_egress_log_path()
    return _validate_source_log(path, "Andvari blocked egress log")


def _resolve_source_log_path(override_env: str, default: Path) -> Path:
    override = os.environ.get(override_env)
    if override:
        return Path(override).expanduser().resolve()
    return default


def _validate_source_log(path: Path, source_label: str) -> Path:
    try:
        stat_result = path.stat()
    except OSError as exc:
        raise ProxyAccessError(
            PROXY_RUNTIME_UNAVAILABLE,
            f"{source_label} unavailable: {path} ({exc})",
        ) from exc
    if not path.is_file():
        raise ProxyAccessError(
            PROXY_RUNTIME_UNAVAILABLE,
            f"{source_label} is not a file: {path}",
        )
    if not os.access(path, os.R_OK):
        raise ProxyAccessError(
            PROXY_RUNTIME_UNAVAILABLE,
            f"{source_label} is not readable: {path}",
        )
    if stat_result.st_size < 0:
        raise ProxyAccessError(
            PROXY_RUNTIME_UNAVAILABLE,
            f"{source_label} has invalid size: {path}",
        )
    return path


def pipeline_proxy_access_artifact_path(run_root: Path, step: str) -> Path:
    return run_root / "pipeline" / "artifacts" / "proxy_access" / f"{step}.jsonl"


def pipeline_blocked_egress_artifact_path(run_root: Path, step: str) -> Path:
    return run_root / "pipeline" / "artifacts" / "egress_block" / f"{step}.jsonl"


def smoke_proxy_access_artifact_path(output_dir: Path, service: str) -> Path:
    return output_dir / "artifacts" / "proxy_access" / f"{service}.jsonl"


def smoke_blocked_egress_artifact_path(output_dir: Path, service: str) -> Path:
    return output_dir / "artifacts" / "egress_block" / f"{service}.jsonl"


def begin_proxy_access_capture(
    step: str, destination: Path | None
) -> ProxyAccessCapture | None:
    if not uses_andvari_proxy_runtime(step):
        return None
    return _begin_host_log_capture(
        step,
        destination,
        source_path=validate_andvari_proxy_access_log(),
        source_label="Andvari proxy access log",
        artifact_label="Andvari proxy access artifact",
    )


def begin_blocked_egress_capture(
    step: str, destination: Path | None
) -> ProxyAccessCapture | None:
    if not uses_andvari_proxy_runtime(step):
        return None
    return _begin_host_log_capture(
        step,
        destination,
        source_path=validate_andvari_blocked_egress_log(),
        source_label="Andvari blocked egress log",
        artifact_label="Andvari blocked egress artifact",
    )


def _begin_host_log_capture(
    step: str,
    destination: Path | None,
    *,
    source_path: Path,
    source_label: str,
    artifact_label: str,
) -> ProxyAccessCapture | None:
    if destination is None:
        raise ValueError(f"destination is required for {artifact_label}")
    validate_host_artifact_destination(destination, artifact_label)
    try:
        stat_result = source_path.stat()
    except OSError as exc:
        raise ProxyAccessError(
            PROXY_RUNTIME_UNAVAILABLE,
            f"{source_label} unavailable: {source_path} ({exc})",
        ) from exc
    return ProxyAccessCapture(
        source_path=source_path,
        source_device=stat_result.st_dev,
        source_inode=stat_result.st_ino,
        start_offset=stat_result.st_size,
    )


def validate_proxy_access_artifact_destination(destination: Path) -> Path:
    return validate_host_artifact_destination(
        destination, "Andvari proxy access artifact"
    )


def validate_blocked_egress_artifact_destination(destination: Path) -> Path:
    return validate_host_artifact_destination(
        destination, "Andvari blocked egress artifact"
    )


def validate_host_artifact_destination(destination: Path, artifact_label: str) -> Path:
    parent = destination.parent
    try:
        parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_PREFLIGHT_FAILED,
            f"Failed to create {artifact_label} directory {parent}: {exc}",
        ) from exc
    if destination.exists() and destination.is_dir():
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_PREFLIGHT_FAILED,
            f"{artifact_label} destination is a directory: {destination}",
        )

    probe_path: Path | None = None
    try:
        fd, probe_name = tempfile.mkstemp(
            prefix=f".{destination.stem}.probe-",
            suffix=".tmp",
            dir=str(parent),
        )
        probe_path = Path(probe_name)
        os.close(fd)
    except OSError as exc:
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_PREFLIGHT_FAILED,
            f"Failed to verify write access for {artifact_label} directory "
            f"{parent}: {exc}",
        ) from exc
    finally:
        if probe_path is not None:
            try:
                probe_path.unlink(missing_ok=True)
            except OSError as exc:
                raise ProxyAccessError(
                    PROXY_ACCESS_LOG_PREFLIGHT_FAILED,
                    f"Failed to clean up {artifact_label} probe file "
                    f"{probe_path}: {exc}",
                ) from exc
    return destination


def finish_proxy_access_capture(
    capture: ProxyAccessCapture | None, destination: Path
) -> None:
    _finish_host_log_capture(
        capture,
        destination,
        source_label="Andvari proxy access log",
        artifact_label="Andvari proxy access artifact",
    )


def finish_blocked_egress_capture(
    capture: ProxyAccessCapture | None, destination: Path
) -> None:
    _finish_host_log_capture(
        capture,
        destination,
        source_label="Andvari blocked egress log",
        artifact_label="Andvari blocked egress artifact",
    )


def _finish_host_log_capture(
    capture: ProxyAccessCapture | None,
    destination: Path,
    *,
    source_label: str,
    artifact_label: str,
) -> None:
    if capture is None:
        return
    try:
        stat_result = capture.source_path.stat()
    except OSError as exc:
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_CAPTURE_FAILED,
            f"{source_label} unavailable after step: {capture.source_path} ({exc})",
        ) from exc
    if not capture.source_path.is_file():
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_CAPTURE_FAILED,
            f"{source_label} stopped being a file: {capture.source_path}",
        )
    if (
        stat_result.st_dev != capture.source_device
        or stat_result.st_ino != capture.source_inode
    ):
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_CAPTURE_FAILED,
            f"{source_label} was replaced during step execution: {capture.source_path}",
        )
    if stat_result.st_size < capture.start_offset:
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_CAPTURE_FAILED,
            f"{source_label} was truncated during step execution: {capture.source_path}",
        )

    bytes_to_copy = stat_result.st_size - capture.start_offset
    try:
        with capture.source_path.open("rb") as handle:
            handle.seek(capture.start_offset)
            payload = handle.read(bytes_to_copy)
    except OSError as exc:
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_CAPTURE_FAILED,
            f"Failed to read {source_label} slice from {capture.source_path}: {exc}",
        ) from exc
    if len(payload) != bytes_to_copy:
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_CAPTURE_FAILED,
            f"Failed to read the expected {source_label} slice from "
            f"{capture.source_path}",
        )
    _write_host_artifact(destination, payload, artifact_label)


def _write_host_artifact(
    destination: Path, payload: bytes, artifact_label: str
) -> None:
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_CAPTURE_FAILED,
            f"Failed to create {artifact_label} directory {destination.parent}: {exc}",
        ) from exc

    temp_path: Path | None = None
    try:
        fd, temp_name = tempfile.mkstemp(
            prefix=f".{destination.stem}.capture-",
            suffix=".tmp",
            dir=str(destination.parent),
        )
        temp_path = Path(temp_name)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
        os.replace(temp_path, destination)
    except OSError as exc:
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_CAPTURE_FAILED,
            f"Failed to write {artifact_label} to {destination}: {exc}",
        ) from exc
    finally:
        if temp_path is not None:
            with contextlib.suppress(OSError):
                temp_path.unlink(missing_ok=True)
