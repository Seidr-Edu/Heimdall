from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime
from pathlib import Path


def timestamp_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compact_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def ensure_directory(path: Path, mode: int = 0o755) -> None:
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(mode)


def write_text(path: Path, content: str, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    path.chmod(mode)


def read_json(path: Path) -> dict[str, object]:
    import json

    return json.loads(path.read_text(encoding="utf-8"))


def stage_readable_tree(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        shutil.rmtree(destination)
    try:
        shutil.copytree(source, destination, copy_function=shutil.copy2)
        _chmod_tree_readable(destination)
    except OSError as exc:
        raise RuntimeError(
            f"Failed to stage readable copy from {source} to {destination}: {exc}"
        ) from exc


def _chmod_tree_readable(root: Path) -> None:
    root.chmod(0o755)
    for current_root, dir_names, file_names in os.walk(root):
        current = Path(current_root)
        current.chmod(0o755)
        for dir_name in dir_names:
            (current / dir_name).chmod(0o755)
        for file_name in file_names:
            (current / file_name).chmod(0o644)
