from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from heimdall.execution import (
    build_runtime,
    resume_run_root,
    run_pipeline_manifest_path,
)
from heimdall.execution import (
    check_codex_login as _check_codex_login,
)
from heimdall.images import DockerError, ensure_docker_available
from heimdall.manifests.pipeline import ManifestValidationError, load_pipeline_manifest
from heimdall.manifests.queue import (
    load_queue_request_text,
    load_worker_config,
    request_from_submit_args,
)
from heimdall.models import RuntimeConfig
from heimdall.queueing import (
    dump_job_status_document,
    enqueue_request,
    load_job_status_document,
    resolve_worker_config_path,
    status_remote,
    submit_remote,
    worker_loop,
)
from heimdall.runner import PreflightError
from heimdall.smoke import (
    SMOKE_SERVICES,
    default_provider_smoke_output_dir,
    run_provider_smoke,
)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "run":
            return _run_command(args)
        if args.command == "resume":
            return _resume_command(args)
        if args.command == "smoke-provider":
            return _smoke_provider_command(args)
        if args.command == "enqueue":
            return _enqueue_command(args)
        if args.command == "worker":
            return _worker_command(args)
        if args.command == "submit":
            return _submit_command(args)
        return _status_command(args)
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

    enqueue_parser = subparsers.add_parser(
        "enqueue", help="Validate and queue one run request on the VPS"
    )
    enqueue_parser.add_argument("--worker-config", type=Path, required=True)
    enqueue_parser.add_argument(
        "--stdin",
        action="store_true",
        help="Read the queue request YAML from stdin",
    )

    worker_parser = subparsers.add_parser(
        "worker", help="Run the long-lived FIFO queue worker"
    )
    worker_parser.add_argument("--worker-config", type=Path, required=True)
    worker_parser.add_argument("--poll-interval-sec", type=int, default=5)
    worker_parser.add_argument(
        "--once",
        action="store_true",
        help="Process at most one available job and then exit",
    )

    submit_parser = subparsers.add_parser(
        "submit", help="Submit one queue request to a remote Heimdall worker over SSH"
    )
    submit_parser.add_argument("--remote", required=True)
    submit_parser.add_argument("--remote-worker-config", required=True)
    submit_parser.add_argument("--repo-url", required=True)
    submit_parser.add_argument("--commit-sha", required=True)
    submit_parser.add_argument("--overrides", type=Path)

    status_parser = subparsers.add_parser(
        "status", help="Show the queue and pipeline status for one job"
    )
    status_parser.add_argument("job_id")
    status_parser.add_argument("--worker-config", type=Path)
    status_parser.add_argument("--remote")
    status_parser.add_argument("--remote-worker-config")

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
    runtime = build_runtime(
        args.runs_root,
        args.codex_bin_dir,
        args.codex_host_bin_dir,
        args.codex_home_dir,
        args.pull_policy,
        args.verbose,
    )
    run_pipeline_manifest_path(args.pipeline_manifest, runtime)
    return 0


def _resume_command(args: argparse.Namespace) -> int:
    runtime = build_runtime(
        args.run_dir.resolve().parent,
        args.codex_bin_dir,
        args.codex_host_bin_dir,
        args.codex_home_dir,
        args.pull_policy,
        args.verbose,
    )
    resume_run_root(args.run_dir, runtime)
    return 0


def _smoke_provider_command(args: argparse.Namespace) -> int:
    _raw_manifest, config = load_pipeline_manifest(args.pipeline_manifest)
    output_dir = (
        args.output_dir.resolve()
        if args.output_dir is not None
        else default_provider_smoke_output_dir()
    )
    runtime = build_runtime(
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
    return _smoke_exit_code(output_dir / "summary.json")


def _enqueue_command(args: argparse.Namespace) -> int:
    if not args.stdin:
        raise RuntimeError("enqueue currently requires --stdin")
    worker_config = load_worker_config(args.worker_config)
    request = load_queue_request_text(sys.stdin.read())
    document = enqueue_request(worker_config, request)
    print(dump_job_status_document(document), end="")
    return 0


def _worker_command(args: argparse.Namespace) -> int:
    if args.poll_interval_sec < 0:
        raise RuntimeError("--poll-interval-sec must be non-negative")
    worker_config = load_worker_config(args.worker_config)
    return worker_loop(
        worker_config,
        poll_interval_sec=args.poll_interval_sec,
        once=args.once,
    )


def _submit_command(args: argparse.Namespace) -> int:
    request = request_from_submit_args(
        args.repo_url,
        args.commit_sha,
        args.overrides,
    )
    completed = submit_remote(
        args.remote,
        args.remote_worker_config,
        request,
    )
    _emit_completed_process(completed)
    return completed.returncode


def _status_command(args: argparse.Namespace) -> int:
    if args.remote is not None:
        if not args.remote_worker_config:
            raise RuntimeError(
                "--remote-worker-config is required when --remote is used"
            )
        completed = status_remote(args.remote, args.remote_worker_config, args.job_id)
        _emit_completed_process(completed)
        return completed.returncode

    worker_config = load_worker_config(resolve_worker_config_path(args.worker_config))
    document = load_job_status_document(worker_config, args.job_id)
    print(dump_job_status_document(document), end="")
    return 0


def _preflight_provider_smoke(runtime: RuntimeConfig, output_dir: Path) -> None:
    if runtime.verbose:
        print(
            f"[heimdall] validating provider smoke under {output_dir}",
            file=sys.stderr,
            flush=True,
        )
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    if not os.access(output_dir.parent, os.W_OK):
        raise PreflightError(
            f"Smoke output parent is not writable: {output_dir.parent}"
        )
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


def _emit_completed_process(completed: subprocess.CompletedProcess[str]) -> None:
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)


def _smoke_exit_code(summary_path: Path) -> int:
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    return 0 if payload.get("status") == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
