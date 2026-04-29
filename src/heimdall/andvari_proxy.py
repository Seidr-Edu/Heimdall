from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from heimdall.models import STEP_ANDVARI, STEP_ANDVARI_V2, STEP_ANDVARI_V3

ANDVARI_PROXY_STEPS = frozenset(
    {STEP_ANDVARI, STEP_ANDVARI_V2, STEP_ANDVARI_V3, "andvari"}
)
DEFAULT_ANDVARI_PROXY_ACCESS_LOG_PATH = Path("/var/log/squid/andvari-access.jsonl")
_PROXY_ACCESS_LOG_OVERRIDE_ENV = "HEIMDALL_ANDVARI_PROXY_ACCESS_LOG_PATH"


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
        raise RuntimeError(
            f"Andvari proxy access log unavailable: {path} ({exc})"
        ) from exc
    if not path.is_file():
        raise RuntimeError(f"Andvari proxy access log is not a file: {path}")
    if not os.access(path, os.R_OK):
        raise RuntimeError(f"Andvari proxy access log is not readable: {path}")
    if stat_result.st_size < 0:
        raise RuntimeError(f"Andvari proxy access log has invalid size: {path}")
    return path


def begin_proxy_access_capture(step: str) -> ProxyAccessCapture | None:
    if not uses_andvari_proxy_runtime(step):
        return None
    path = validate_andvari_proxy_access_log()
    stat_result = path.stat()
    return ProxyAccessCapture(
        source_path=path,
        source_device=stat_result.st_dev,
        source_inode=stat_result.st_ino,
        start_offset=stat_result.st_size,
    )


def proxy_access_artifact_path(run_dir: Path) -> Path:
    return run_dir / "artifacts" / "andvari" / "logs" / "proxy_access.jsonl"


def finish_proxy_access_capture(
    capture: ProxyAccessCapture | None, destination: Path
) -> None:
    if capture is None:
        return
    try:
        stat_result = capture.source_path.stat()
    except OSError as exc:
        raise RuntimeError(
            f"Andvari proxy access log unavailable after step: "
            f"{capture.source_path} ({exc})"
        ) from exc
    if not capture.source_path.is_file():
        raise RuntimeError(
            f"Andvari proxy access log stopped being a file: {capture.source_path}"
        )
    if (
        stat_result.st_dev != capture.source_device
        or stat_result.st_ino != capture.source_inode
    ):
        raise RuntimeError(
            f"Andvari proxy access log was replaced during step execution: "
            f"{capture.source_path}"
        )
    if stat_result.st_size < capture.start_offset:
        raise RuntimeError(
            f"Andvari proxy access log was truncated during step execution: "
            f"{capture.source_path}"
        )

    bytes_to_copy = stat_result.st_size - capture.start_offset
    destination.parent.mkdir(parents=True, exist_ok=True)
    with capture.source_path.open("rb") as handle:
        handle.seek(capture.start_offset)
        payload = handle.read(bytes_to_copy)
    if len(payload) != bytes_to_copy:
        raise RuntimeError(
            "Failed to read the expected Andvari proxy access log slice "
            f"from {capture.source_path}"
        )
    destination.write_bytes(payload)
