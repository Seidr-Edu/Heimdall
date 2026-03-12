from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from heimdall.models import ImageRefs, PullPolicy, ResolvedImages


class DockerError(RuntimeError):
    """Raised when Docker CLI interactions fail."""


def ensure_docker_available() -> None:
    _run_docker(["version", "--format", "{{.Server.Version}}"])


def resolve_images(images: ImageRefs, pull_policy: PullPolicy, *, verbose: bool = False) -> ResolvedImages:
    return ResolvedImages(
        brokk=_ensure_image(images.brokk, pull_policy, verbose=verbose),
        eitri=_ensure_image(images.eitri, pull_policy, verbose=verbose),
        andvari=_ensure_image(images.andvari, pull_policy, verbose=verbose),
        kvasir=_ensure_image(images.kvasir, pull_policy, verbose=verbose),
        lidskjalv=_ensure_image(images.lidskjalv, pull_policy, verbose=verbose),
    )


def image_id_map(images: ImageRefs, resolved: ResolvedImages) -> dict[str, dict[str, str]]:
    return {
        "brokk": {"configured_ref": images.brokk, "resolved_image_id": resolved.brokk},
        "eitri": {"configured_ref": images.eitri, "resolved_image_id": resolved.eitri},
        "andvari": {"configured_ref": images.andvari, "resolved_image_id": resolved.andvari},
        "kvasir": {"configured_ref": images.kvasir, "resolved_image_id": resolved.kvasir},
        "lidskjalv": {"configured_ref": images.lidskjalv, "resolved_image_id": resolved.lidskjalv},
    }


def run_container(
    image_ref: str,
    env: dict[str, str],
    mounts: list[tuple[Path, str, bool]],
    *,
    stream_output: bool = False,
    output_path: Path | None = None,
    log_prefix: str | None = None,
) -> subprocess.CompletedProcess[str]:
    command = ["docker", "run", "--rm"]
    for key, value in sorted(env.items()):
        command.extend(["-e", f"{key}={value}"])
    for host_path, container_path, read_only in mounts:
        suffix = ":ro" if read_only else ""
        command.extend(["-v", f"{host_path}:{container_path}{suffix}"])
    command.append(image_ref)
    if log_prefix is not None:
        print(f"[{log_prefix}] docker run {image_ref}", file=sys.stderr, flush=True)
    return _run_command(command, stream_output=stream_output, output_path=output_path, log_prefix=log_prefix)


def _ensure_image(image_ref: str, pull_policy: PullPolicy, *, verbose: bool = False) -> str:
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
) -> subprocess.CompletedProcess[str]:
    return _run_command(
        ["docker", *args],
        stream_output=stream_output,
        output_path=output_path,
        log_prefix=log_prefix,
    )


def _run_command(
    args: list[str],
    *,
    stream_output: bool = False,
    output_path: Path | None = None,
    log_prefix: str | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if stream_output:
            return _run_streaming_command(args, env, output_path=output_path, log_prefix=log_prefix)
        completed = subprocess.run(
            args,
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        if output_path is not None:
            output_path.write_text(f"{completed.stdout}{completed.stderr}", encoding="utf-8")
    except subprocess.CalledProcessError as exc:
        if output_path is not None:
            output_path.write_text(f"{exc.stdout or ''}{exc.stderr or ''}", encoding="utf-8")
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
        log_handle = output_path.open("w", encoding="utf-8") if output_path is not None else None
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
    return subprocess.CompletedProcess(args=args, returncode=returncode, stdout=stdout, stderr="")
