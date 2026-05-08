from __future__ import annotations

import os
import queue
import re
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import TextIO

from heimdall.models import ImageRefs, PullPolicy, ResolvedImages

_CONTAINER_SECURITY_OPTS = ("--cap-drop=ALL", "--security-opt=no-new-privileges")
_STREAM_POLL_SEC = 0.2


class DockerError(RuntimeError):
    """Raised when Docker CLI interactions fail."""


class DockerTimeoutError(DockerError):
    """Raised when a Docker CLI interaction exceeds its timeout."""

    def __init__(self, reason: str, detail: str):
        super().__init__(detail)
        self.reason = reason


def ensure_docker_available() -> None:
    _run_docker(["version", "--format", "{{.Server.Version}}"])


def resolve_images(
    images: ImageRefs, pull_policy: PullPolicy, *, verbose: bool = False
) -> ResolvedImages:
    return ResolvedImages(
        brokk=resolve_image(images.brokk, pull_policy, verbose=verbose),
        eitri=resolve_image(images.eitri, pull_policy, verbose=verbose),
        andvari=resolve_image(images.andvari, pull_policy, verbose=verbose),
        mimir=resolve_image(images.mimir, pull_policy, verbose=verbose),
        kvasir=resolve_image(images.kvasir, pull_policy, verbose=verbose),
        lidskjalv=resolve_image(images.lidskjalv, pull_policy, verbose=verbose),
    )


def image_id_map(
    images: ImageRefs, resolved: ResolvedImages
) -> dict[str, dict[str, str]]:
    return {
        "brokk": {"configured_ref": images.brokk, "resolved_image_id": resolved.brokk},
        "eitri": {"configured_ref": images.eitri, "resolved_image_id": resolved.eitri},
        "andvari": {
            "configured_ref": images.andvari,
            "resolved_image_id": resolved.andvari,
        },
        "mimir": {
            "configured_ref": images.mimir,
            "resolved_image_id": resolved.mimir,
        },
        "kvasir": {
            "configured_ref": images.kvasir,
            "resolved_image_id": resolved.kvasir,
        },
        "lidskjalv": {
            "configured_ref": images.lidskjalv,
            "resolved_image_id": resolved.lidskjalv,
        },
    }


def resolve_image(
    image_ref: str, pull_policy: PullPolicy, *, verbose: bool = False
) -> str:
    return _ensure_image(image_ref, pull_policy, verbose=verbose)


def run_container(
    image_ref: str,
    env: dict[str, str],
    mounts: list[tuple[Path, str, bool]],
    *,
    network_name: str | None = None,
    stream_output: bool = False,
    output_path: Path | None = None,
    log_prefix: str | None = None,
    entrypoint: str | None = None,
    command_args: list[str] | None = None,
    timeout_sec: float | None = None,
    timeout_reason: str = "container-timeout",
) -> subprocess.CompletedProcess[str]:
    command = ["docker", "run", "--rm"]
    timeout_container_name: str | None = None
    if timeout_sec is not None and timeout_sec > 0:
        timeout_container_name = _generated_container_name(image_ref)
        command.extend(["--name", timeout_container_name])
    command.extend(_CONTAINER_SECURITY_OPTS)
    if network_name is not None:
        command.extend(["--network", network_name])
    if entrypoint is not None:
        command.extend(["--entrypoint", entrypoint])
    for key, value in sorted(env.items()):
        command.extend(["-e", f"{key}={value}"])
    for host_path, container_path, read_only in mounts:
        suffix = ":ro" if read_only else ""
        command.extend(["-v", f"{host_path}:{container_path}{suffix}"])
    command.append(image_ref)
    if command_args:
        command.extend(command_args)
    if log_prefix is not None:
        print(f"[{log_prefix}] docker run {image_ref}", file=sys.stderr, flush=True)
    return _run_command(
        command,
        stream_output=stream_output,
        output_path=output_path,
        log_prefix=log_prefix,
        timeout_sec=timeout_sec if timeout_sec and timeout_sec > 0 else None,
        timeout_reason=timeout_reason,
        timeout_container_name=timeout_container_name,
    )


def _ensure_image(
    image_ref: str, pull_policy: PullPolicy, *, verbose: bool = False
) -> str:
    if pull_policy == "always":
        _pull_image(image_ref, verbose=verbose)
        return _inspect_image_id(image_ref, verbose=verbose)
    if pull_policy == "if-missing":
        try:
            return _inspect_image_id(image_ref, verbose=verbose)
        except DockerError:
            _pull_image(image_ref, verbose=verbose)
            return _inspect_image_id(image_ref, verbose=verbose)
    return _inspect_image_id(image_ref, verbose=verbose)


def _pull_image(image_ref: str, *, verbose: bool = False) -> None:
    if verbose:
        print(f"[heimdall] docker pull {image_ref}", file=sys.stderr, flush=True)
    _run_docker(["pull", image_ref], stream_output=verbose, log_prefix="docker-pull")


def _inspect_image_id(image_ref: str, *, verbose: bool = False) -> str:
    if verbose:
        print(f"[heimdall] docker inspect {image_ref}", file=sys.stderr, flush=True)
    completed = _run_docker(["image", "inspect", image_ref, "--format", "{{.Id}}"])
    image_id = completed.stdout.strip()
    if not image_id:
        raise DockerError(f"Docker did not return an image ID for {image_ref}")
    return image_id


def _run_docker(
    args: list[str],
    *,
    stream_output: bool = False,
    output_path: Path | None = None,
    log_prefix: str | None = None,
    timeout_sec: float | None = None,
    timeout_reason: str = "container-timeout",
    timeout_container_name: str | None = None,
) -> subprocess.CompletedProcess[str]:
    return _run_command(
        ["docker", *args],
        stream_output=stream_output,
        output_path=output_path,
        log_prefix=log_prefix,
        timeout_sec=timeout_sec,
        timeout_reason=timeout_reason,
        timeout_container_name=timeout_container_name,
    )


def _run_command(
    args: list[str],
    *,
    stream_output: bool = False,
    output_path: Path | None = None,
    log_prefix: str | None = None,
    timeout_sec: float | None = None,
    timeout_reason: str = "container-timeout",
    timeout_container_name: str | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if stream_output:
            return _run_streaming_command(
                args,
                env,
                output_path=output_path,
                log_prefix=log_prefix,
                timeout_sec=timeout_sec,
                timeout_reason=timeout_reason,
                timeout_container_name=timeout_container_name,
            )
        completed = subprocess.run(
            args,
            check=True,
            capture_output=True,
            text=True,
            env=env,
            timeout=timeout_sec,
        )
        if output_path is not None:
            output_path.write_text(
                f"{completed.stdout}{completed.stderr}", encoding="utf-8"
            )
    except subprocess.TimeoutExpired as exc:
        if output_path is not None:
            stdout = _coerce_timeout_output(exc.stdout)
            stderr = _coerce_timeout_output(exc.stderr)
            output_path.write_text(f"{stdout}{stderr}", encoding="utf-8")
        raise _build_docker_timeout_error(
            timeout_sec,
            timeout_reason,
            timeout_container_name,
        ) from exc
    except subprocess.CalledProcessError as exc:
        if output_path is not None:
            output_path.write_text(
                f"{exc.stdout or ''}{exc.stderr or ''}", encoding="utf-8"
            )
        stderr = (exc.stderr or "").strip()
        stdout = (exc.stdout or "").strip()
        detail = stderr or stdout or str(exc)
        raise DockerError(detail) from exc
    except OSError as exc:
        raise DockerError(str(exc)) from exc
    return completed


def _run_streaming_command(
    args: list[str],
    env: dict[str, str],
    *,
    output_path: Path | None,
    log_prefix: str | None,
    timeout_sec: float | None,
    timeout_reason: str,
    timeout_container_name: str | None,
) -> subprocess.CompletedProcess[str]:
    if timeout_sec is None:
        return _run_streaming_command_without_timeout(
            args,
            env,
            output_path=output_path,
            log_prefix=log_prefix,
        )
    return _run_streaming_command_with_timeout(
        args,
        env,
        output_path=output_path,
        log_prefix=log_prefix,
        timeout_sec=timeout_sec,
        timeout_reason=timeout_reason,
        timeout_container_name=timeout_container_name,
    )


def _run_streaming_command_without_timeout(
    args: list[str],
    env: dict[str, str],
    *,
    output_path: Path | None,
    log_prefix: str | None,
) -> subprocess.CompletedProcess[str]:
    with subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        bufsize=1,
    ) as process:
        if process.stdout is None:
            raise DockerError("Failed to capture command output")
        lines: list[str] = []
        log_handle = (
            output_path.open("w", encoding="utf-8") if output_path is not None else None
        )
        try:
            for line in process.stdout:
                lines.append(line)
                if log_handle is not None:
                    log_handle.write(line)
                    log_handle.flush()
                if log_prefix is None:
                    print(line, end="", file=sys.stderr, flush=True)
                else:
                    print(f"[{log_prefix}] {line}", end="", file=sys.stderr, flush=True)
        finally:
            if log_handle is not None:
                log_handle.close()
        returncode = process.wait()
    stdout = "".join(lines)
    if returncode != 0:
        detail = stdout.strip() or f"Command failed with exit code {returncode}"
        raise DockerError(detail)
    return subprocess.CompletedProcess(
        args=args, returncode=returncode, stdout=stdout, stderr=""
    )


def _run_streaming_command_with_timeout(
    args: list[str],
    env: dict[str, str],
    *,
    output_path: Path | None,
    log_prefix: str | None,
    timeout_sec: float,
    timeout_reason: str,
    timeout_container_name: str | None,
) -> subprocess.CompletedProcess[str]:
    with subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        bufsize=1,
    ) as process:
        if process.stdout is None:
            raise DockerError("Failed to capture command output")
        lines: list[str] = []
        line_queue: queue.Queue[str | None] = queue.Queue()
        reader = threading.Thread(
            target=_stream_reader,
            args=(process.stdout, line_queue),
            daemon=True,
        )
        reader.start()
        deadline = time.monotonic() + timeout_sec
        log_handle = (
            output_path.open("w", encoding="utf-8") if output_path is not None else None
        )
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    _terminate_process(process)
                    reader.join(timeout=1)
                    while True:
                        try:
                            leftover = line_queue.get_nowait()
                        except queue.Empty:
                            break
                        if leftover is None:
                            break
                        lines.append(leftover)
                        if log_handle is not None:
                            log_handle.write(leftover)
                    raise _build_docker_timeout_error(
                        timeout_sec,
                        timeout_reason,
                        timeout_container_name,
                    )
                try:
                    line = line_queue.get(timeout=min(remaining, _STREAM_POLL_SEC))
                except queue.Empty:
                    if process.poll() is not None and not reader.is_alive():
                        break
                    continue
                if line is None:
                    if process.poll() is not None:
                        break
                    continue
                lines.append(line)
                if log_handle is not None:
                    log_handle.write(line)
                    log_handle.flush()
                if log_prefix is None:
                    print(line, end="", file=sys.stderr, flush=True)
                else:
                    print(f"[{log_prefix}] {line}", end="", file=sys.stderr, flush=True)
        finally:
            if log_handle is not None:
                log_handle.close()
        returncode = process.wait()
        reader.join(timeout=1)
    stdout = "".join(lines)
    if returncode != 0:
        detail = stdout.strip() or f"Command failed with exit code {returncode}"
        raise DockerError(detail)
    return subprocess.CompletedProcess(
        args=args, returncode=returncode, stdout=stdout, stderr=""
    )


def _stream_reader(
    stdout: TextIO,
    line_queue: queue.Queue[str | None],
) -> None:
    try:
        for line in stdout:
            line_queue.put(line)
    finally:
        line_queue.put(None)


def _coerce_timeout_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _build_docker_timeout_error(
    timeout_sec: float | None,
    timeout_reason: str,
    timeout_container_name: str | None,
) -> DockerTimeoutError:
    detail = f"Command timed out after {_format_timeout_sec(timeout_sec)} seconds"
    cleanup_detail = _cleanup_timed_out_container(timeout_container_name)
    if cleanup_detail is not None:
        detail = f"{detail}; {cleanup_detail}"
    return DockerTimeoutError(timeout_reason, detail)


def _cleanup_timed_out_container(container_name: str | None) -> str | None:
    if container_name is None:
        return None
    try:
        _run_docker(["rm", "-f", container_name])
    except DockerError as exc:
        return f"failed to remove timed-out container {container_name}: {exc}"
    return f"removed timed-out container {container_name}"


def _format_timeout_sec(timeout_sec: float | None) -> str:
    if timeout_sec is None:
        return "unknown"
    if float(timeout_sec).is_integer():
        return str(int(timeout_sec))
    return str(timeout_sec)


def _generated_container_name(image_ref: str) -> str:
    stem = re.sub(r"[^a-zA-Z0-9_.-]+", "-", image_ref).strip("._-")
    if not stem:
        stem = "container"
    return f"heimdall-{stem[:40]}-{uuid.uuid4().hex[:12]}"


def _terminate_process(process: subprocess.Popen[str]) -> None:
    try:
        process.kill()
    except OSError:
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.wait()
