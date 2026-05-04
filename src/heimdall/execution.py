from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from heimdall.images import ensure_docker_available, resolve_images
from heimdall.manifests.pipeline import load_pipeline_manifest
from heimdall.models import PipelineConfig, Provider, PullPolicy, RuntimeConfig
from heimdall.runner import PreflightError, run_pipeline
from heimdall.smoke import SMOKE_SERVICES, run_provider_smoke
from heimdall.utils import ensure_directory


def build_runtime(
    runs_root: Path,
    codex_bin_dir: Path,
    codex_host_bin_dir: Path | None,
    codex_home_dir: Path,
    pull_policy: PullPolicy,
    verbose: bool,
    *,
    andvari_internal_network_name: str,
    provider: Provider = "codex",
    claude_home_dir: Path | None = None,
) -> RuntimeConfig:
    runs_root = runs_root.resolve()
    codex_bin_dir = codex_bin_dir.resolve()
    codex_host_bin_dir = (
        codex_host_bin_dir.resolve()
        if codex_host_bin_dir is not None
        else codex_bin_dir
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
        andvari_internal_network_name=andvari_internal_network_name,
        provider=provider,
        claude_home_dir=claude_home_dir.resolve()
        if claude_home_dir is not None
        else None,
    )


def preflight(config: PipelineConfig, runtime: RuntimeConfig) -> None:
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
        raise PreflightError(
            f"Provider bin dir does not exist: {runtime.codex_bin_dir}"
        )
    if not runtime.codex_host_bin_dir.is_dir():
        raise PreflightError(
            f"Provider host bin dir does not exist: {runtime.codex_host_bin_dir}"
        )
    if not runtime.codex_home_dir.is_dir():
        raise PreflightError(f"Codex home dir does not exist: {runtime.codex_home_dir}")
    _check_claude_home_dir(runtime)
    validate_andvari_proxy_runtime(runtime)
    ensure_docker_available()
    if runtime.verbose:
        print("[heimdall] docker daemon reachable", file=sys.stderr, flush=True)
    check_provider_login(runtime)
    if runtime.verbose:
        print(
            f"[heimdall] {runtime.provider} provider credentials ok",
            file=sys.stderr,
            flush=True,
        )
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


def preflight_provider_smoke(runtime: RuntimeConfig, output_dir: Path) -> None:
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
        raise PreflightError(
            f"Provider bin dir does not exist: {runtime.codex_bin_dir}"
        )
    if not runtime.codex_host_bin_dir.is_dir():
        raise PreflightError(
            f"Provider host bin dir does not exist: {runtime.codex_host_bin_dir}"
        )
    if not runtime.codex_home_dir.is_dir():
        raise PreflightError(f"Codex home dir does not exist: {runtime.codex_home_dir}")
    _check_claude_home_dir(runtime)
    validate_andvari_proxy_runtime(runtime)
    ensure_docker_available()
    if runtime.verbose:
        print("[heimdall] docker daemon reachable", file=sys.stderr, flush=True)
    check_provider_login(runtime)
    if runtime.verbose:
        print(
            f"[heimdall] {runtime.provider} provider credentials ok",
            file=sys.stderr,
            flush=True,
        )


def _check_claude_home_dir(runtime: RuntimeConfig) -> None:
    if runtime.provider != "claude":
        return
    if runtime.claude_home_dir is None:
        raise PreflightError(
            "claude_home_dir must be set in worker.yaml (or --claude-home-dir) "
            "when provider is claude."
        )
    if not runtime.claude_home_dir.is_dir():
        raise PreflightError(
            f"Claude home dir does not exist: {runtime.claude_home_dir}"
        )


def check_provider_login(runtime: RuntimeConfig) -> None:
    if runtime.provider == "claude":
        _check_claude_credentials(runtime)
    else:
        check_codex_login(runtime)


def check_codex_login(runtime: RuntimeConfig) -> None:
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


def _check_claude_credentials(runtime: RuntimeConfig) -> None:
    claude_executable = runtime.codex_host_bin_dir / "claude"
    if not claude_executable.is_file():
        raise PreflightError(f"Missing claude executable: {claude_executable}")
    home = runtime.claude_home_dir or runtime.codex_home_dir
    credentials_path = home / "credentials.json"
    if not credentials_path.is_file():
        raise PreflightError(
            f"Claude credentials not found: {credentials_path}. "
            "Run 'claude' to authenticate and populate credentials.json."
        )
    try:
        content = credentials_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PreflightError(
            f"Failed to read Claude credentials {credentials_path}: {exc}"
        ) from exc
    if not content.strip():
        raise PreflightError(f"Claude credentials file is empty: {credentials_path}")


def validate_andvari_proxy_runtime(runtime: RuntimeConfig) -> None:
    if not runtime.andvari_internal_network_name.strip():
        raise PreflightError(
            "Andvari restricted egress requires an internal Docker network name."
        )


def run_pipeline_manifest_path(
    pipeline_manifest: Path, runtime: RuntimeConfig
) -> tuple[Path, PipelineConfig]:
    raw_manifest, config = load_pipeline_manifest(pipeline_manifest)
    run_root = runtime.runs_root / config.run_id
    if run_root.exists():
        raise PreflightError(
            f"Run directory already exists: {run_root}. Use resume instead."
        )
    preflight(config, runtime)
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
    return run_root, config


def resume_run_root(
    run_root: Path, runtime: RuntimeConfig
) -> tuple[Path, PipelineConfig]:
    run_root = run_root.resolve()
    manifest_path = run_root / "pipeline" / "manifest.yaml"
    if not manifest_path.is_file():
        raise PreflightError(f"Missing stored pipeline manifest: {manifest_path}")
    raw_manifest, config = load_pipeline_manifest(manifest_path)
    preflight(config, runtime)
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
    return run_root, config


def run_provider_smoke_manifest_path(
    pipeline_manifest: Path,
    runtime: RuntimeConfig,
    output_dir: Path,
    services: tuple[str, ...] = SMOKE_SERVICES,
) -> int:
    _raw_manifest, config = load_pipeline_manifest(pipeline_manifest)
    preflight_provider_smoke(runtime, output_dir)
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
        services=services,
    )
    if runtime.verbose:
        print(
            f"[heimdall] provider smoke summary: {output_dir / 'summary.json'}",
            file=sys.stderr,
            flush=True,
        )
    return 0
