from __future__ import annotations

import os
import shutil
from collections.abc import Iterable
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
    _stage_tree(source, destination, preserve_executable_files=False)


def stage_readable_paths(
    source: Path, destination: Path, relative_paths: Iterable[str]
) -> None:
    _stage_tree_subset(
        source,
        destination,
        relative_paths,
        preserve_executable_files=False,
    )


def stage_executable_tree(source: Path, destination: Path) -> None:
    _stage_tree(source, destination, preserve_executable_files=True)


def _stage_tree(
    source: Path, destination: Path, *, preserve_executable_files: bool
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        shutil.rmtree(destination)
    try:
        shutil.copytree(source, destination, copy_function=shutil.copy2)
        _chmod_tree(destination, preserve_executable_files=preserve_executable_files)
    except OSError as exc:
        raise RuntimeError(
            f"Failed to stage readable copy from {source} to {destination}: {exc}"
        ) from exc


def _stage_tree_subset(
    source: Path,
    destination: Path,
    relative_paths: Iterable[str],
    *,
    preserve_executable_files: bool,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        shutil.rmtree(destination)
    try:
        if not source.is_dir():
            raise FileNotFoundError(2, "No such file or directory", str(source))
        destination.mkdir(parents=True, exist_ok=True)
        for relpath in relative_paths:
            relative = Path(relpath)
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError(f"Invalid staged relative path: {relpath}")
            source_path = source / relative
            if not source_path.exists():
                continue
            destination_path = destination / relative
            if source_path.is_dir():
                shutil.copytree(
                    source_path, destination_path, copy_function=shutil.copy2
                )
            else:
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_path, destination_path)
        _chmod_tree(destination, preserve_executable_files=preserve_executable_files)
    except OSError as exc:
        raise RuntimeError(
            f"Failed to stage readable copy from {source} to {destination}: {exc}"
        ) from exc


def _chmod_tree(root: Path, *, preserve_executable_files: bool) -> None:
    root.chmod(0o755)
    for current_root, dir_names, file_names in os.walk(root):
        current = Path(current_root)
        current.chmod(0o755)
        for dir_name in dir_names:
            (current / dir_name).chmod(0o755)
        for file_name in file_names:
            file_path = current / file_name
            is_executable = bool(file_path.stat().st_mode & 0o111)
            if preserve_executable_files and is_executable:
                file_path.chmod(0o755)
            else:
                file_path.chmod(0o644)
