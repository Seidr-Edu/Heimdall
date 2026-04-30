from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from heimdall.models import STEP_ANDVARI, STEP_ANDVARI_V2, STEP_ANDVARI_V3

ANDVARI_PROXY_STEPS = frozenset(
    {STEP_ANDVARI, STEP_ANDVARI_V2, STEP_ANDVARI_V3, "andvari"}
)
DEFAULT_ANDVARI_PROXY_ACCESS_LOG_PATH = Path("/var/log/squid/andvari-access.jsonl")
_PROXY_ACCESS_LOG_OVERRIDE_ENV = "HEIMDALL_ANDVARI_PROXY_ACCESS_LOG_PATH"
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
    override = os.environ.get(_PROXY_ACCESS_LOG_OVERRIDE_ENV)
    if override:
        return Path(override).expanduser().resolve()
    return DEFAULT_ANDVARI_PROXY_ACCESS_LOG_PATH


def validate_andvari_proxy_access_log() -> Path:
    path = andvari_proxy_access_log_path()
    try:
        stat_result = path.stat()
    except OSError as exc:
        raise ProxyAccessError(
            PROXY_RUNTIME_UNAVAILABLE,
            f"Andvari proxy access log unavailable: {path} ({exc})",
        ) from exc
    if not path.is_file():
        raise ProxyAccessError(
            PROXY_RUNTIME_UNAVAILABLE,
            f"Andvari proxy access log is not a file: {path}",
        )
    if not os.access(path, os.R_OK):
        raise ProxyAccessError(
            PROXY_RUNTIME_UNAVAILABLE,
            f"Andvari proxy access log is not readable: {path}",
        )
    if stat_result.st_size < 0:
        raise ProxyAccessError(
            PROXY_RUNTIME_UNAVAILABLE,
            f"Andvari proxy access log has invalid size: {path}",
        )
    return path


def pipeline_proxy_access_artifact_path(run_root: Path, step: str) -> Path:
    return run_root / "pipeline" / "artifacts" / "proxy_access" / f"{step}.jsonl"


def smoke_proxy_access_artifact_path(output_dir: Path, service: str) -> Path:
    return output_dir / "artifacts" / "proxy_access" / f"{service}.jsonl"


def begin_proxy_access_capture(
    step: str, destination: Path | None
) -> ProxyAccessCapture | None:
    if not uses_andvari_proxy_runtime(step):
        return None
    if destination is None:
        raise ValueError("destination is required for Andvari proxy capture")
    path = validate_andvari_proxy_access_log()
    validate_proxy_access_artifact_destination(destination)
    try:
        stat_result = path.stat()
    except OSError as exc:
        raise ProxyAccessError(
            PROXY_RUNTIME_UNAVAILABLE,
            f"Andvari proxy access log unavailable: {path} ({exc})",
        ) from exc
    return ProxyAccessCapture(
        source_path=path,
        source_device=stat_result.st_dev,
        source_inode=stat_result.st_ino,
        start_offset=stat_result.st_size,
    )


def validate_proxy_access_artifact_destination(destination: Path) -> Path:
    parent = destination.parent
    try:
        parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_PREFLIGHT_FAILED,
            f"Failed to create Andvari proxy artifact directory {parent}: {exc}",
        ) from exc
    if destination.exists() and destination.is_dir():
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_PREFLIGHT_FAILED,
            f"Andvari proxy artifact destination is a directory: {destination}",
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
            f"Failed to verify write access for Andvari proxy artifact "
            f"directory {parent}: {exc}",
        ) from exc
    finally:
        if probe_path is not None:
            try:
                probe_path.unlink(missing_ok=True)
            except OSError as exc:
                raise ProxyAccessError(
                    PROXY_ACCESS_LOG_PREFLIGHT_FAILED,
                    f"Failed to clean up Andvari proxy artifact probe file "
                    f"{probe_path}: {exc}",
                ) from exc
    return destination


def finish_proxy_access_capture(
    capture: ProxyAccessCapture | None, destination: Path
) -> None:
    if capture is None:
        return
    try:
        stat_result = capture.source_path.stat()
    except OSError as exc:
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_CAPTURE_FAILED,
            f"Andvari proxy access log unavailable after step: "
            f"{capture.source_path} ({exc})",
        ) from exc
    if not capture.source_path.is_file():
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_CAPTURE_FAILED,
            f"Andvari proxy access log stopped being a file: {capture.source_path}",
        )
    if (
        stat_result.st_dev != capture.source_device
        or stat_result.st_ino != capture.source_inode
    ):
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_CAPTURE_FAILED,
            f"Andvari proxy access log was replaced during step execution: "
            f"{capture.source_path}",
        )
    if stat_result.st_size < capture.start_offset:
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_CAPTURE_FAILED,
            f"Andvari proxy access log was truncated during step execution: "
            f"{capture.source_path}",
        )

    bytes_to_copy = stat_result.st_size - capture.start_offset
    try:
        with capture.source_path.open("rb") as handle:
            handle.seek(capture.start_offset)
            payload = handle.read(bytes_to_copy)
    except OSError as exc:
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_CAPTURE_FAILED,
            f"Failed to read Andvari proxy access log slice from "
            f"{capture.source_path}: {exc}",
        ) from exc
    if len(payload) != bytes_to_copy:
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_CAPTURE_FAILED,
            "Failed to read the expected Andvari proxy access log slice "
            f"from {capture.source_path}",
        )
    _write_proxy_access_artifact(destination, payload)


def _write_proxy_access_artifact(destination: Path, payload: bytes) -> None:
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ProxyAccessError(
            PROXY_ACCESS_LOG_CAPTURE_FAILED,
            f"Failed to create Andvari proxy artifact directory "
            f"{destination.parent}: {exc}",
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
            f"Failed to write Andvari proxy access artifact to {destination}: {exc}",
        ) from exc
    finally:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
