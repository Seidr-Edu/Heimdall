from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from heimdall.images import DockerError, ensure_docker_available, resolve_images
from heimdall.manifest import ManifestValidationError, load_pipeline_manifest
from heimdall.models import PipelineConfig, PullPolicy, RuntimeConfig
from heimdall.runner import PreflightError, run_pipeline
from heimdall.smoke import (
    SMOKE_SERVICES,
    default_provider_smoke_output_dir,
    run_provider_smoke,
)
from heimdall.utils import ensure_directory


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "run":
            return _run_command(args)
        if args.command == "resume":
            return _resume_command(args)
        return _smoke_provider_command(args)
    except (ManifestValidationError, DockerError, PreflightError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Heimdall pipeline orchestrator")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Start a new orchestrator run")
    run_parser.add_argument("pipeline_manifest", type=Path)
    run_parser.add_argument("--runs-root", type=Path, required=True)
    _add_runtime_args(run_parser)

    resume_parser = subparsers.add_parser("resume", help="Resume an existing run")
    resume_parser.add_argument("run_dir", type=Path)
    _add_runtime_args(resume_parser)

    smoke_parser = subparsers.add_parser(
        "smoke-provider",
        help="Probe Codex provider compatibility inside Andvari/Kvasir containers",
    )
    smoke_parser.add_argument("pipeline_manifest", type=Path)
    smoke_parser.add_argument(
        "--output-dir",
        type=Path,
        help="Write smoke logs and summary to this directory",
    )
    smoke_parser.add_argument(
        "--service",
        action="append",
        choices=SMOKE_SERVICES,
        help="Limit the smoke probe to a specific service (repeatable)",
    )
    _add_runtime_args(smoke_parser)
    return parser


def _add_runtime_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--codex-bin-dir", type=Path, required=True)
    parser.add_argument(
        "--codex-host-bin-dir",
        type=Path,
        help=(
            "Host-native Codex bin dir for preflight checks. Defaults to "
            "--codex-bin-dir."
        ),
    )
    parser.add_argument("--codex-home-dir", type=Path, required=True)
    parser.add_argument(
        "--pull-policy",
        choices=("if-missing", "always", "never"),
        default="if-missing",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print preflight and stream per-step container output to the terminal",
    )


def _run_command(args: argparse.Namespace) -> int:
    raw_manifest, config = load_pipeline_manifest(args.pipeline_manifest)
    runs_root = args.runs_root.resolve()
    run_root = runs_root / config.run_id
    if run_root.exists():
        raise PreflightError(
            f"Run directory already exists: {run_root}. Use resume instead."
        )
    runtime = _build_runtime(
        runs_root,
        args.codex_bin_dir,
        args.codex_host_bin_dir,
        args.codex_home_dir,
        args.pull_policy,
        args.verbose,
    )
    _preflight(config, runtime)
    resolved_images = resolve_images(
        config.images, runtime.pull_policy, verbose=runtime.verbose
    )
    run_pipeline(
        config=config,
        runtime=runtime,
        resolved_images=resolved_images,
        run_root=run_root,
        source_manifest_text=raw_manifest,
        fresh_run=True,
    )
    return 0


def _resume_command(args: argparse.Namespace) -> int:
    run_root = args.run_dir.resolve()
    manifest_path = run_root / "pipeline" / "manifest.yaml"
    if not manifest_path.is_file():
        raise PreflightError(f"Missing stored pipeline manifest: {manifest_path}")
    raw_manifest, config = load_pipeline_manifest(manifest_path)
    runtime = _build_runtime(
        run_root.parent,
        args.codex_bin_dir,
        args.codex_host_bin_dir,
        args.codex_home_dir,
        args.pull_policy,
        args.verbose,
    )
    _preflight(config, runtime)
    resolved_images = resolve_images(
        config.images, runtime.pull_policy, verbose=runtime.verbose
    )
    run_pipeline(
        config=config,
        runtime=runtime,
        resolved_images=resolved_images,
        run_root=run_root,
        source_manifest_text=raw_manifest,
        fresh_run=False,
    )
    return 0


def _smoke_provider_command(args: argparse.Namespace) -> int:
    _raw_manifest, config = load_pipeline_manifest(args.pipeline_manifest)
    output_dir = (
        args.output_dir.resolve()
        if args.output_dir is not None
        else default_provider_smoke_output_dir()
    )
    runtime = _build_runtime(
        output_dir.parent,
        args.codex_bin_dir,
        args.codex_host_bin_dir,
        args.codex_home_dir,
        args.pull_policy,
        args.verbose,
    )
    _preflight_provider_smoke(runtime, output_dir)
    if runtime.verbose:
        print(
            f"[heimdall] provider smoke output: {output_dir}",
            file=sys.stderr,
            flush=True,
        )
    run_provider_smoke(
        config=config,
        runtime=runtime,
        output_dir=output_dir,
        services=tuple(args.service or SMOKE_SERVICES),
    )
    summary_path = output_dir / "summary.json"
    if runtime.verbose:
        print(
            f"[heimdall] provider smoke summary: {summary_path}",
            file=sys.stderr,
            flush=True,
        )
    return _smoke_exit_code(summary_path)


def _build_runtime(
    runs_root: Path,
    codex_bin_dir: Path,
    codex_host_bin_dir: Path | None,
    codex_home_dir: Path,
    pull_policy: PullPolicy,
    verbose: bool,
) -> RuntimeConfig:
    runs_root = runs_root.resolve()
    codex_bin_dir = codex_bin_dir.resolve()
    codex_host_bin_dir = (
        codex_host_bin_dir.resolve() if codex_host_bin_dir is not None else codex_bin_dir
    )
    codex_home_dir = codex_home_dir.resolve()
    return RuntimeConfig(
        runs_root=runs_root,
        codex_bin_dir=codex_bin_dir,
        codex_host_bin_dir=codex_host_bin_dir,
        codex_home_dir=codex_home_dir,
        pull_policy=pull_policy,
        sonar_host_url=os.environ.get("SONAR_HOST_URL"),
        sonar_token_present=bool(os.environ.get("SONAR_TOKEN")),
        sonar_organization=os.environ.get("SONAR_ORGANIZATION"),
        verbose=verbose,
    )


def _preflight(config: PipelineConfig, runtime: RuntimeConfig) -> None:
    if runtime.verbose:
        print(
            f"[heimdall] validating runtime under {runtime.runs_root}",
            file=sys.stderr,
            flush=True,
        )
    ensure_directory(runtime.runs_root, 0o755)
    if not os.access(runtime.runs_root, os.W_OK):
        raise PreflightError(f"Runs root is not writable: {runtime.runs_root}")
    if not runtime.codex_bin_dir.is_dir():
        raise PreflightError(f"Codex bin dir does not exist: {runtime.codex_bin_dir}")
    if not runtime.codex_host_bin_dir.is_dir():
        raise PreflightError(
            f"Codex host bin dir does not exist: {runtime.codex_host_bin_dir}"
        )
    if not runtime.codex_home_dir.is_dir():
        raise PreflightError(f"Codex home dir does not exist: {runtime.codex_home_dir}")
    ensure_docker_available()
    if runtime.verbose:
        print("[heimdall] docker daemon reachable", file=sys.stderr, flush=True)
    _check_codex_login(runtime)
    if runtime.verbose:
        print("[heimdall] codex login status ok", file=sys.stderr, flush=True)
    if not config.lidskjalv.skip_sonar:
        missing = []
        if runtime.sonar_host_url is None:
            missing.append("SONAR_HOST_URL")
        if not runtime.sonar_token_present:
            missing.append("SONAR_TOKEN")
        if runtime.sonar_organization is None:
            missing.append("SONAR_ORGANIZATION")
        if missing:
            raise PreflightError(
                f"Missing Sonar environment variable(s): {', '.join(missing)}"
            )
    if runtime.verbose and not config.lidskjalv.skip_sonar:
        print("[heimdall] sonar environment present", file=sys.stderr, flush=True)


def _preflight_provider_smoke(runtime: RuntimeConfig, output_dir: Path) -> None:
    if runtime.verbose:
        print(
            f"[heimdall] validating provider smoke under {output_dir}",
            file=sys.stderr,
            flush=True,
        )
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    if not os.access(output_dir.parent, os.W_OK):
        raise PreflightError(f"Smoke output parent is not writable: {output_dir.parent}")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise PreflightError(f"Smoke output dir is not empty: {output_dir}")
    if not runtime.codex_bin_dir.is_dir():
        raise PreflightError(f"Codex bin dir does not exist: {runtime.codex_bin_dir}")
    if not runtime.codex_host_bin_dir.is_dir():
        raise PreflightError(
            f"Codex host bin dir does not exist: {runtime.codex_host_bin_dir}"
        )
    if not runtime.codex_home_dir.is_dir():
        raise PreflightError(f"Codex home dir does not exist: {runtime.codex_home_dir}")
    ensure_docker_available()
    if runtime.verbose:
        print("[heimdall] docker daemon reachable", file=sys.stderr, flush=True)
    _check_codex_login(runtime)
    if runtime.verbose:
        print("[heimdall] codex login status ok", file=sys.stderr, flush=True)


def _check_codex_login(runtime: RuntimeConfig) -> None:
    codex_executable = runtime.codex_host_bin_dir / "codex"
    if not codex_executable.is_file():
        raise PreflightError(f"Missing codex executable: {codex_executable}")
    env = os.environ.copy()
    env["PATH"] = f"{runtime.codex_host_bin_dir}{os.pathsep}{env.get('PATH', '')}"
    env["CODEX_HOME"] = str(runtime.codex_home_dir)
    try:
        subprocess.run(
            ["codex", "login", "status"],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
    except (subprocess.CalledProcessError, OSError) as exc:
        raise PreflightError(f"codex login status failed: {exc}") from exc


def _smoke_exit_code(summary_path: Path) -> int:
    import json

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    return 0 if payload.get("status") == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
