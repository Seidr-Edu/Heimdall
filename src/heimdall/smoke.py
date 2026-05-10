from __future__ import annotations

import json
import platform
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TypedDict

from heimdall.andvari_proxy import (
    ProxyAccessCapture,
    ProxyAccessError,
    begin_blocked_egress_capture,
    begin_proxy_access_capture,
    finish_blocked_egress_capture,
    finish_proxy_access_capture,
    smoke_blocked_egress_artifact_path,
    smoke_proxy_access_artifact_path,
    uses_andvari_proxy_runtime,
)
from heimdall.images import DockerError, resolve_image, run_container
from heimdall.models import PipelineConfig, RuntimeConfig
from heimdall.provider_runtime import (
    CODEX_CONTAINER_SEED_PATH,
    docker_network_for_step,
    env_for_step,
    extra_mounts_for_service,
    provider_for_service,
    provider_home_dir_for_service,
    provider_home_subdir_for_service,
    provider_seed_container_path_for_service,
    stage_provider_seed,
)
from heimdall.utils import (
    compact_run_id,
    ensure_directory,
    stage_executable_tree,
    timestamp_utc,
    write_text,
)

SMOKE_SERVICES = ("andvari", "kvasir")
SMOKE_INPUT_FILENAME = "smoke.txt"
SMOKE_INPUT_CONTENT = "heimdall-provider-smoke\n"


class HostInfo(TypedDict):
    platform: str
    python_version: str
    provider: str
    provider_host_bin_dir: str
    provider_container_bin_dir: str
    provider_home_dir: str
    host_provider_executable: str
    host_provider_binary_format: str
    container_provider_executable: str
    container_provider_binary_format: str
    compatibility_hint: str | None


class ServiceProbeResult(TypedDict):
    status: str
    reason: str | None
    detail: str | None
    hint: str | None
    image_ref: str
    resolved_image_id: str | None
    log_path: str
    service_root: str
    provider_bin_dir: str
    provider_seed_dir: str
    probe_input_dir: str
    runtime_provider_home: str
    proxy_access_log_path: str | None
    egress_block_log_path: str | None


class SmokeSummary(TypedDict):
    status: str
    started_at: str
    finished_at: str
    host: HostInfo
    services: dict[str, ServiceProbeResult]


def default_provider_smoke_output_dir(base_dir: Path | None = None) -> Path:
    root = (
        base_dir.expanduser().resolve()
        if base_dir is not None
        else (Path.cwd() / ".heimdall-smoke").resolve()
    )
    return root / f"{compact_run_id()}__provider-smoke"


def run_provider_smoke(
    *,
    config: PipelineConfig,
    runtime: RuntimeConfig,
    output_dir: Path,
    services: Sequence[str],
) -> Path:
    output_dir = output_dir.resolve()
    ensure_directory(output_dir, 0o755)
    logs_dir = output_dir / "logs"
    services_dir = output_dir / "services"
    ensure_directory(logs_dir, 0o755)
    ensure_directory(services_dir, 0o755)

    started_at = timestamp_utc()
    host_info = _host_info(runtime)
    results: dict[str, ServiceProbeResult] = {}

    for service in services:
        results[service] = _run_service_probe(
            service=service,
            image_ref=_image_ref_for_service(config, service),
            runtime=runtime,
            services_dir=services_dir,
            logs_dir=logs_dir,
        )

    finished_at = timestamp_utc()
    overall_status = (
        "passed"
        if all(result["status"] == "passed" for result in results.values())
        else "failed"
    )
    summary: SmokeSummary = {
        "status": overall_status,
        "started_at": started_at,
        "finished_at": finished_at,
        "host": host_info,
        "services": results,
    }
    write_text(output_dir / "summary.json", json.dumps(summary, indent=2) + "\n")
    write_text(output_dir / "summary.md", _render_summary(summary))
    return output_dir


def _host_info(runtime: RuntimeConfig) -> HostInfo:
    binary_name = "claude" if runtime.provider == "claude" else "codex"
    host_executable = (runtime.codex_host_bin_dir / binary_name).resolve()
    host_binary_format = _detect_binary_format(host_executable)
    container_executable = (runtime.codex_bin_dir / binary_name).resolve()
    container_binary_format = _detect_binary_format(container_executable)
    info: HostInfo = {
        "platform": platform.platform(),
        "python_version": sys.version.split()[0],
        "provider": runtime.provider,
        "provider_host_bin_dir": str(runtime.codex_host_bin_dir),
        "provider_container_bin_dir": str(runtime.codex_bin_dir),
        "provider_home_dir": str(provider_home_dir_for_service("andvari", runtime)),
        "host_provider_executable": str(host_executable),
        "host_provider_binary_format": host_binary_format,
        "container_provider_executable": str(container_executable),
        "container_provider_binary_format": container_binary_format,
        "compatibility_hint": None,
    }
    if container_binary_format.startswith("mach-o"):
        info["compatibility_hint"] = (
            f"The container-facing {binary_name} binary is Mach-O, which is usually "
            "not executable inside Linux service containers."
        )
    return info


def _image_ref_for_service(config: PipelineConfig, service: str) -> str:
    if service == "andvari":
        return config.images.andvari
    if service == "kvasir":
        return config.images.kvasir
    raise RuntimeError(f"unsupported provider smoke service: {service}")


def _run_service_probe(
    *,
    service: str,
    image_ref: str,
    runtime: RuntimeConfig,
    services_dir: Path,
    logs_dir: Path,
) -> ServiceProbeResult:
    service_root = services_dir / service
    run_dir = service_root / "run"
    provider_bin_dir = service_root / "input" / "provider-bin"
    provider_seed_dir = service_root / "input" / "provider-seed"
    probe_input_dir = service_root / "input" / "probe-input"
    service_provider = provider_for_service(service, runtime)
    _service_home_dir = provider_home_dir_for_service(service, runtime)
    _seed_container_path = provider_seed_container_path_for_service(service, runtime)
    provider_home_subdir = provider_home_subdir_for_service(service, runtime)
    runtime_provider_home = run_dir / "provider-state" / provider_home_subdir
    proxy_log_artifact_path = (
        smoke_proxy_access_artifact_path(services_dir.parent, service)
        if uses_andvari_proxy_runtime(service)
        else None
    )
    blocked_egress_artifact_path = (
        smoke_blocked_egress_artifact_path(services_dir.parent, service)
        if uses_andvari_proxy_runtime(service)
        else None
    )
    log_path = logs_dir / f"{service}.log"

    ensure_directory(service_root, 0o755)
    ensure_directory(run_dir, 0o777)
    _write_probe_input(probe_input_dir)

    base_result: ServiceProbeResult = {
        "status": "failed",
        "reason": None,
        "detail": None,
        "hint": None,
        "image_ref": image_ref,
        "resolved_image_id": None,
        "log_path": str(log_path),
        "service_root": str(service_root),
        "provider_bin_dir": str(provider_bin_dir),
        "provider_seed_dir": str(provider_seed_dir),
        "probe_input_dir": str(probe_input_dir),
        "runtime_provider_home": str(runtime_provider_home),
        "proxy_access_log_path": (
            str(proxy_log_artifact_path)
            if proxy_log_artifact_path is not None
            else None
        ),
        "egress_block_log_path": (
            str(blocked_egress_artifact_path)
            if blocked_egress_artifact_path is not None
            else None
        ),
    }

    stage_conflict = _stage_conflict(runtime.codex_bin_dir, provider_bin_dir)
    if stage_conflict is not None:
        detail = (
            f"Refusing to stage provider bin dir {runtime.codex_bin_dir} into "
            f"{provider_bin_dir}: {stage_conflict}"
        )
        write_text(log_path, f"{detail}\n")
        return {
            **base_result,
            "reason": "stage-provider-bin-failed",
            "detail": detail,
        }

    stage_conflict = _stage_conflict(_service_home_dir, provider_seed_dir)
    if stage_conflict is not None:
        detail = (
            f"Refusing to stage provider home dir {_service_home_dir} into "
            f"{provider_seed_dir}: {stage_conflict}"
        )
        write_text(log_path, f"{detail}\n")
        return {
            **base_result,
            "reason": "stage-provider-seed-failed",
            "detail": detail,
        }

    try:
        resolved_image_id = resolve_image(
            image_ref, runtime.pull_policy, verbose=runtime.verbose
        )
    except DockerError as exc:
        detail = str(exc)
        write_text(log_path, f"{detail}\n")
        return {
            **base_result,
            "reason": "image-resolution-failed",
            "detail": detail,
        }

    try:
        stage_executable_tree(runtime.codex_bin_dir, provider_bin_dir)
    except RuntimeError as exc:
        detail = str(exc)
        write_text(log_path, f"{detail}\n")
        return {
            **base_result,
            "resolved_image_id": resolved_image_id,
            "reason": "stage-provider-bin-failed",
            "detail": detail,
        }

    try:
        stage_provider_seed(service, _service_home_dir, provider_seed_dir, runtime)
    except RuntimeError as exc:
        detail = str(exc)
        write_text(log_path, f"{detail}\n")
        return {
            **base_result,
            "resolved_image_id": resolved_image_id,
            "reason": "stage-provider-seed-failed",
            "detail": detail,
        }

    container_env = {
        "HEIMDALL_SMOKE_SERVICE": service,
        "HEIMDALL_SMOKE_PROVIDER": service_provider,
    }
    container_env.update(env_for_step(service, runtime))
    if uses_andvari_proxy_runtime(service):
        container_env["HEIMDALL_ANDVARI_EGRESS_ENFORCED"] = "1"
    proxy_capture: ProxyAccessCapture | None = None
    blocked_egress_capture: ProxyAccessCapture | None = None
    try:
        proxy_capture = begin_proxy_access_capture(service, proxy_log_artifact_path)
        blocked_egress_capture = begin_blocked_egress_capture(
            service, blocked_egress_artifact_path
        )
    except ProxyAccessError as exc:
        detail = str(exc)
        write_text(log_path, f"{detail}\n")
        return {
            **base_result,
            "resolved_image_id": resolved_image_id,
            "reason": exc.reason,
            "detail": detail,
            "hint": _proxy_failure_hint(exc.reason),
        }

    failure_result: ServiceProbeResult | None = None
    try:
        run_container(
            image_ref,
            container_env,
            [
                (provider_bin_dir, "/opt/provider/bin", True),
                (provider_seed_dir, _seed_container_path, True),
                (probe_input_dir, "/input", True),
                (run_dir, "/run", False),
                *extra_mounts_for_service(service, runtime),
            ],
            network_name=docker_network_for_step(service, runtime),
            stream_output=runtime.verbose,
            output_path=log_path,
            log_prefix=f"smoke-{service}" if runtime.verbose else None,
            entrypoint="/bin/bash",
            command_args=["-lc", _probe_script_for_provider(service_provider)],
        )
    except DockerError as exc:
        log_text = _read_log_text(log_path)
        probe_output = log_text or str(exc)
        reason = _classify_probe_failure(probe_output, service_provider)
        detail = _summarize_probe_failure(probe_output, reason)
        hint = _probe_failure_hint(reason, runtime, service_provider)
        _append_log(log_path, f"\n[heimdall][smoke] classified reason: {reason}\n")
        if hint is not None:
            _append_log(log_path, f"[heimdall][smoke] hint: {hint}\n")
        failure_result = {
            **base_result,
            "resolved_image_id": resolved_image_id,
            "reason": reason,
            "detail": detail,
            "hint": hint,
        }
    if failure_result is None and not runtime_provider_home.is_dir():
        detail = (
            f"Probe finished without creating "
            f"/run/provider-state/{provider_home_subdir} inside "
            "the service run directory."
        )
        _append_log(log_path, f"\n[heimdall][smoke] {detail}\n")
        failure_result = {
            **base_result,
            "resolved_image_id": resolved_image_id,
            "reason": "runtime-provider-home-not-created",
            "detail": detail,
        }
    proxy_capture_error = _finalize_proxy_capture(
        proxy_capture,
        proxy_log_artifact_path,
        blocked_egress_capture,
        blocked_egress_artifact_path,
        log_path,
    )
    if proxy_capture_error is not None:
        reason = getattr(
            proxy_capture_error, "reason", "proxy-access-log-capture-failed"
        )
        return {
            **base_result,
            "resolved_image_id": resolved_image_id,
            "reason": reason,
            "detail": str(proxy_capture_error),
            "hint": _proxy_failure_hint(reason),
        }
    if failure_result is not None:
        return failure_result

    return {
        **base_result,
        "status": "passed",
        "resolved_image_id": resolved_image_id,
    }


def _probe_script_for_provider(provider: str) -> str:
    if provider == "claude":
        return _claude_probe_script()
    return _provider_probe_script()


def _claude_probe_script() -> str:
    return """
set -Eeuo pipefail
trap 'status=$?; echo "[smoke][error] command failed: ${BASH_COMMAND} (exit ${status})" >&2; exit ${status}' ERR

require_tool() {
  local tool="$1"
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "[smoke][error] required tool unavailable: ${tool}" >&2
    exit 1
  fi
}

expect_blocked() {
  local label="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "[smoke][error] egress probe unexpectedly succeeded: ${label}" >&2
    exit 1
  fi
  echo "[smoke] egress probe blocked: ${label}"
}

resolve_python() {
  if command -v python3 >/dev/null 2>&1; then
    echo "python3"
    return 0
  fi
  if command -v python >/dev/null 2>&1; then
    echo "python"
    return 0
  fi
  echo "[smoke][error] required tool unavailable: python3 or python" >&2
  exit 1
}

echo "[smoke] service=${HEIMDALL_SMOKE_SERVICE:-unknown}"
echo "[smoke] uname=$(uname -srm)"
echo "[smoke] provider bin dir:"
ls -la /opt/provider/bin
echo "[smoke] provider seed highlights:"
if [[ -d /opt/provider-seed/claude-home ]]; then
  find /opt/provider-seed/claude-home -mindepth 1 -maxdepth 4 | LC_ALL=C sort
else
  echo "[smoke] provider seed dir missing"
fi
echo "[smoke] smoke input dir:"
if [[ -d /input ]]; then
  ls -la /input
else
  echo "[smoke] smoke input dir missing"
fi

runtime_dir=/run/provider-state/claude-home
workspace_dir=/run/workspace
prompt_file=/run/smoke-prompt.txt
mkdir -p "${runtime_dir}"
mkdir -p "${workspace_dir}"
if [[ -d /opt/provider-seed/claude-home ]]; then
  echo "[smoke] copying provider seed into ${runtime_dir}"
  cp -R /opt/provider-seed/claude-home/. "${runtime_dir}/"
fi

export PATH="/opt/provider/bin:${PATH}"
export CLAUDE_CONFIG_DIR="${runtime_dir}"
echo "[smoke] PATH=${PATH}"
echo "[smoke] CLAUDE_CONFIG_DIR=${CLAUDE_CONFIG_DIR}"
echo "[smoke] claude path:"
command -v claude
echo "[smoke] claude details:"
ls -l "$(command -v claude)"
echo "[smoke] claude version:"
claude --version
cat > "${prompt_file}" <<'PROMPT_EOF'
You are running a Heimdall provider smoke test inside a Linux service container.
Read /input/smoke.txt and create ./smoke-result.txt containing exactly the same contents.
Do not modify any other files.
When finished, reply with exactly SMOKE_OK.
PROMPT_EOF
echo "[smoke] claude smoke task:"
cd "${workspace_dir}"
claude \
  --dangerously-skip-permissions \
  --add-dir /input \
  --output-format json \
  -p "$(cat ${prompt_file})"
if [[ ! -f "${workspace_dir}/smoke-result.txt" ]]; then
  echo "[smoke][error] claude did not create ${workspace_dir}/smoke-result.txt" >&2
  exit 1
fi
if ! cmp -s /input/smoke.txt "${workspace_dir}/smoke-result.txt"; then
  echo "[smoke][error] claude created ${workspace_dir}/smoke-result.txt, but it did not match /input/smoke.txt" >&2
  exit 1
fi
echo "[smoke] claude workspace probe passed"
if [[ "${HEIMDALL_ANDVARI_EGRESS_ENFORCED:-0}" == "1" ]]; then
  echo "[smoke] Andvari egress probes enabled"
  require_tool curl
  require_tool git
  python_bin="$(resolve_python)"
  curl -fsS --max-time 15 https://example.com >/dev/null
  echo "[smoke] egress probe allowed: https://example.com"
  expect_blocked "https://github.com" curl -fsS --max-time 15 https://github.com
  expect_blocked "https://api.github.com" curl -fsS --max-time 15 https://api.github.com
  expect_blocked \
    "https://raw.githubusercontent.com/octocat/Hello-World/master/README" \
    curl -fsS --max-time 15 https://raw.githubusercontent.com/octocat/Hello-World/master/README
  expect_blocked \
    "git ls-remote https://github.com/octocat/Hello-World.git" \
    git ls-remote https://github.com/octocat/Hello-World.git HEAD
  expect_blocked \
    "python raw tcp github.com:22" \
    "${python_bin}" -c "import socket; socket.create_connection(('github.com', 22), timeout=10).close()"
fi
echo "[smoke] provider smoke passed"
""".strip()


def _provider_probe_script() -> str:
    return """
set -Eeuo pipefail
trap 'status=$?; echo "[smoke][error] command failed: ${BASH_COMMAND} (exit ${status})" >&2; exit ${status}' ERR

require_tool() {
  local tool="$1"
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "[smoke][error] required tool unavailable: ${tool}" >&2
    exit 1
  fi
}

expect_blocked() {
  local label="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "[smoke][error] egress probe unexpectedly succeeded: ${label}" >&2
    exit 1
  fi
  echo "[smoke] egress probe blocked: ${label}"
}

resolve_python() {
  if command -v python3 >/dev/null 2>&1; then
    echo "python3"
    return 0
  fi
  if command -v python >/dev/null 2>&1; then
    echo "python"
    return 0
  fi
  echo "[smoke][error] required tool unavailable: python3 or python" >&2
  exit 1
}

echo "[smoke] service=${HEIMDALL_SMOKE_SERVICE:-unknown}"
echo "[smoke] uname=$(uname -srm)"
echo "[smoke] provider bin dir:"
ls -la /opt/provider/bin
echo "[smoke] provider seed highlights:"
if [[ -d /opt/provider-seed/codex-home ]]; then
  find /opt/provider-seed/codex-home -mindepth 1 -maxdepth 4 | LC_ALL=C sort
else
  echo "[smoke] provider seed dir missing"
fi
echo "[smoke] smoke input dir:"
if [[ -d /input ]]; then
  ls -la /input
else
  echo "[smoke] smoke input dir missing"
fi

runtime_dir=/run/provider-state/codex-home
workspace_dir=/run/workspace
prompt_file=/run/smoke-prompt.txt
mkdir -p "${runtime_dir}/sessions"
mkdir -p "${workspace_dir}"
if [[ -d /opt/provider-seed/codex-home ]]; then
  echo "[smoke] copying provider seed into ${runtime_dir}"
  cp -R /opt/provider-seed/codex-home/. "${runtime_dir}/"
fi

export PATH="/opt/provider/bin:${PATH}"
export CODEX_HOME="${runtime_dir}"
echo "[smoke] PATH=${PATH}"
echo "[smoke] CODEX_HOME=${CODEX_HOME}"
echo "[smoke] codex path:"
command -v codex
echo "[smoke] codex details:"
ls -l "$(command -v codex)"
echo "[smoke] codex version:"
codex --version
echo "[smoke] codex login status:"
codex login status
cat > "${prompt_file}" <<'PROMPT_EOF'
You are running a Heimdall provider smoke test inside a Linux service container.
Read /input/smoke.txt and create ./smoke-result.txt containing exactly the same contents.
Do not modify any other files.
When finished, reply with exactly SMOKE_OK.
PROMPT_EOF
echo "[smoke] codex exec smoke task:"
codex exec \
  --skip-git-repo-check \
  --dangerously-bypass-approvals-and-sandbox \
  --cd "${workspace_dir}" \
  --add-dir /input \
  --json \
  --output-last-message "${workspace_dir}/last-message.txt" \
  - < "${prompt_file}"
if [[ ! -f "${workspace_dir}/smoke-result.txt" ]]; then
  echo "[smoke][error] codex exec did not create ${workspace_dir}/smoke-result.txt" >&2
  exit 1
fi
if ! cmp -s /input/smoke.txt "${workspace_dir}/smoke-result.txt"; then
  echo "[smoke][error] codex exec created ${workspace_dir}/smoke-result.txt, but it did not match /input/smoke.txt" >&2
  exit 1
fi
if [[ -f "${workspace_dir}/last-message.txt" ]]; then
  echo "[smoke] codex exec last message:"
  cat "${workspace_dir}/last-message.txt"
fi
echo "[smoke] codex exec workspace probe passed"
if [[ "${HEIMDALL_ANDVARI_EGRESS_ENFORCED:-0}" == "1" ]]; then
  echo "[smoke] Andvari egress probes enabled"
  require_tool curl
  require_tool git
  require_tool mvn
  require_tool gradle
  python_bin="$(resolve_python)"
  curl -fsS --max-time 15 https://example.com >/dev/null
  echo "[smoke] egress probe allowed: https://example.com"
  maven_smoke_dir=/run/maven-smoke
  mkdir -p "${maven_smoke_dir}"
  cat > "${maven_smoke_dir}/pom.xml" <<'MAVEN_POM_EOF'
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>heimdall.smoke</groupId>
  <artifactId>maven-smoke</artifactId>
  <version>1.0.0</version>
  <dependencies>
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <version>4.13.2</version>
    </dependency>
  </dependencies>
</project>
MAVEN_POM_EOF
  mvn -q -B -Dmaven.repo.local=/run/.m2 -f "${maven_smoke_dir}/pom.xml" dependency:go-offline >/dev/null
  echo "[smoke] egress probe allowed: maven dependency resolution"
  gradle_smoke_dir=/run/gradle-smoke
  mkdir -p "${gradle_smoke_dir}"
  cat > "${gradle_smoke_dir}/settings.gradle" <<'GRADLE_SETTINGS_EOF'
rootProject.name = 'heimdall-smoke'
GRADLE_SETTINGS_EOF
  cat > "${gradle_smoke_dir}/build.gradle" <<'GRADLE_BUILD_EOF'
configurations {
  smoke
}

repositories {
  mavenCentral()
}

dependencies {
  smoke 'junit:junit:4.13.2'
}

task resolveSmoke {
  doLast {
    configurations.smoke.files.each { println it.name }
  }
}
GRADLE_BUILD_EOF
  GRADLE_USER_HOME=/run/.gradle gradle -q --no-daemon -p "${gradle_smoke_dir}" resolveSmoke >/dev/null
  echo "[smoke] egress probe allowed: gradle dependency resolution"
  expect_blocked "https://github.com" curl -fsS --max-time 15 https://github.com
  expect_blocked "https://api.github.com" curl -fsS --max-time 15 https://api.github.com
  expect_blocked \
    "https://raw.githubusercontent.com/octocat/Hello-World/master/README" \
    curl -fsS --max-time 15 https://raw.githubusercontent.com/octocat/Hello-World/master/README
  expect_blocked \
    "git ls-remote https://github.com/octocat/Hello-World.git" \
    git ls-remote https://github.com/octocat/Hello-World.git HEAD
  expect_blocked \
    "python raw tcp github.com:22" \
    "${python_bin}" -c "import socket; socket.create_connection(('github.com', 22), timeout=10).close()"
fi
echo "[smoke] provider smoke passed"
""".strip()


def _classify_probe_failure(detail: str, provider: str = "codex") -> str:
    lowered = detail.lower()
    if "exec format error" in lowered or "cannot execute binary file" in lowered:
        return "provider-binary-incompatible-with-container"
    if provider == "claude":
        if (
            "command failed: command -v claude" in lowered
            or "claude: not found" in lowered
        ):
            return "provider-cli-unavailable-in-container"
        if "authentication failed" in lowered or "invalid api key" in lowered:
            return "claude-auth-unusable-in-container"
        if (
            "claude did not create /run/workspace/smoke-result.txt" in lowered
            or "claude created /run/workspace/smoke-result.txt, but it did not match /input/smoke.txt"
            in lowered
            or ("command failed: claude" in lowered and "/input/smoke.txt" in lowered)
            or ("command failed: claude" in lowered and "/run/workspace" in lowered)
        ):
            return "provider-exec-workspace-access-failed"
        if "command failed: claude" in lowered:
            return "provider-exec-failed"
    else:
        if (
            "command failed: command -v codex" in lowered
            or "codex: not found" in lowered
        ):
            return "provider-cli-unavailable-in-container"
        if (
            "command failed: codex login status" in lowered
            or "not logged in" in lowered
        ):
            return "codex-auth-unusable-in-container"
        if (
            "codex exec did not create /run/workspace/smoke-result.txt" in lowered
            or "codex exec created /run/workspace/smoke-result.txt, but it did not match /input/smoke.txt"
            in lowered
            or ("command failed: codex exec" in lowered and "sandbox" in lowered)
            or (
                "command failed: codex exec" in lowered
                and "/input/smoke.txt" in lowered
            )
            or ("command failed: codex exec" in lowered and "/run/workspace" in lowered)
        ):
            return "provider-exec-workspace-access-failed"
        if "command failed: codex exec" in lowered:
            return "provider-exec-failed"
    if "permission denied" in lowered and "provider-seed" in lowered:
        return "provider-seed-unreadable-in-container"
    if "permission denied" in lowered and "provider/bin" in lowered:
        return "provider-bin-unreadable-in-container"
    if (
        "proxy probe unexpectedly succeeded" in lowered
        or "egress probe unexpectedly succeeded" in lowered
        or "required tool unavailable" in lowered
    ):
        return "andvari-proxy-probe-failed"
    return "probe-command-failed"


def _summarize_probe_failure(probe_output: str, reason: str) -> str:
    lines = [line.strip() for line in probe_output.splitlines() if line.strip()]
    lowered = [line.lower() for line in lines]

    if reason == "provider-binary-incompatible-with-container":
        for index, line in enumerate(lowered):
            if "exec format error" in line or "cannot execute binary file" in line:
                return lines[index]
    if reason == "provider-cli-unavailable-in-container":
        for index, line in enumerate(lowered):
            if (
                "codex: not found" in line
                or "command failed: command -v codex" in line
                or "claude: not found" in line
                or "command failed: command -v claude" in line
            ):
                return lines[index]
    if reason == "codex-auth-unusable-in-container":
        for index, line in enumerate(lowered):
            if "not logged in" in line or "command failed: codex login status" in line:
                return lines[index]
    if reason == "claude-auth-unusable-in-container":
        for index, line in enumerate(lowered):
            if "authentication failed" in line or "invalid api key" in line:
                return lines[index]
    if reason == "provider-exec-workspace-access-failed":
        for index, line in enumerate(lowered):
            if (
                "did not create /run/workspace/smoke-result.txt" in line
                or "did not match /input/smoke.txt" in line
                or "sandbox" in line
                or "/input/smoke.txt" in line
                or "/run/workspace" in line
            ):
                return lines[index]
    if reason == "provider-exec-failed":
        for index, line in enumerate(lowered):
            if "command failed: codex exec" in line or "command failed: claude" in line:
                return lines[index]
    if reason == "andvari-proxy-probe-failed":
        for index, line in enumerate(lowered):
            if (
                "proxy probe unexpectedly succeeded" in line
                or "egress probe unexpectedly succeeded" in line
                or "required tool unavailable" in line
            ):
                return lines[index]
    for line in reversed(lines):
        if line.startswith("[smoke][error]"):
            return line
    if lines:
        return lines[-1]
    return "Probe failed without producing any diagnostic output."


def _probe_failure_hint(
    reason: str, runtime: RuntimeConfig, provider: str
) -> str | None:
    binary_name = "claude" if provider == "claude" else "codex"
    binary_format = _detect_binary_format(
        (runtime.codex_bin_dir / binary_name).resolve()
    )
    if reason == "provider-binary-incompatible-with-container":
        if binary_format == "mach-o":
            return (
                f"Use a Linux ELF {binary_name} binary in --codex-bin-dir, or run "
                "Heimdall on a Linux host. A macOS Mach-O binary cannot execute "
                "inside Linux service containers."
            )
        return (
            f"Use a {binary_name} binary built for the Linux container architecture "
            "and point --codex-bin-dir at a dedicated directory containing only that binary."
        )
    if reason == "codex-auth-unusable-in-container":
        return (
            "The binary runs inside the container, but the staged CODEX_HOME does not "
            "authenticate there. Check auth.json/config.toml and rerun the smoke test."
        )
    if reason == "claude-auth-unusable-in-container":
        return (
            "The binary runs inside the container, but Claude auth is unusable there. "
            "Confirm the staged settings.json points at the mounted secret helper and "
            "the mounted API key file is readable inside the container."
        )
    if reason == "provider-cli-unavailable-in-container":
        return (
            f"The staged provider bin is not exposing a runnable {binary_name} executable "
            f"inside the container. Use a dedicated bin directory containing only {binary_name}."
        )
    if reason == "provider-exec-workspace-access-failed":
        if provider == "claude":
            return (
                "The binary and credentials work, but claude could not use the mounted "
                "service workspace. Confirm the container image invokes claude with "
                "--dangerously-skip-permissions and --add-dir for the required mounts."
            )
        return (
            "The binary and auth work, but codex exec could not use the mounted "
            "service workspace. In service containers, use the same container-safe "
            "flags as Andvari/Kvasir: --dangerously-bypass-approvals-and-sandbox, "
            "--cd <workspace>, and any required --add-dir mounts."
        )
    if reason == "provider-exec-failed":
        return (
            f"{binary_name} started inside the container, but the smoke task did not "
            "complete. Inspect the per-service log for the exact stderr/stdout."
        )
    if reason == "andvari-proxy-probe-failed":
        return (
            "The Andvari smoke did not prove restricted egress. Confirm the Andvari "
            "container is on the restricted Docker network, allowed HTTPS traffic "
            "works, GitHub HTTPS is denied, and raw bypasses like "
            "direct TCP to github.com:22 are blocked."
        )
    return None


def _proxy_failure_hint(reason: str) -> str:
    if reason == "proxy-runtime-unavailable":
        return (
            "Heimdall could not read one of the host egress log sources. Confirm "
            "/var/log/squid/andvari-access.jsonl and "
            "/var/log/andvari/blocked-egress.jsonl exist and are readable by the "
            "worker."
        )
    if reason == "proxy-access-log-preflight-failed":
        return (
            "Heimdall could not prepare a writable host-owned destination for the "
            "captured proxy slice. Confirm the smoke output artifact directory is "
            "host-owned and writable by the worker."
        )
    return (
        "Heimdall could not preserve one of the Andvari host egress log slices. "
        "Confirm both host log sources exist, are readable by the worker, stay "
        "stable during the probe, and the destination artifact paths remain "
        "writable."
    )


def _finalize_proxy_capture(
    capture: ProxyAccessCapture | None,
    destination: Path | None,
    blocked_egress_capture: ProxyAccessCapture | None,
    blocked_egress_destination: Path | None,
    log_path: Path,
) -> RuntimeError | None:
    first_error = None
    try:
        if capture is not None and destination is not None:
            finish_proxy_access_capture(capture, destination)
    except ProxyAccessError as exc:
        _append_log(log_path, f"\n[heimdall][smoke] {exc}\n")
        first_error = exc
    try:
        if (
            blocked_egress_capture is not None
            and blocked_egress_destination is not None
        ):
            finish_blocked_egress_capture(
                blocked_egress_capture,
                blocked_egress_destination,
            )
    except ProxyAccessError as exc:
        _append_log(log_path, f"\n[heimdall][smoke] {exc}\n")
        if first_error is None:
            first_error = exc
    return first_error


def _write_probe_input(path: Path) -> None:
    ensure_directory(path, 0o755)
    write_text(path / SMOKE_INPUT_FILENAME, SMOKE_INPUT_CONTENT)
    write_text(path / "diagram.puml", "@startuml\nclass Smoke\n@enduml\n")


def _stage_conflict(source: Path, destination: Path) -> str | None:
    source_resolved = source.resolve()
    destination_resolved = destination.resolve()
    if destination_resolved == source_resolved:
        return "destination equals source"
    if _is_relative_to(destination_resolved, source_resolved):
        return "destination is inside the source tree"
    return None


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _append_log(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(content)


def _read_log_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _detect_binary_format(path: Path) -> str:
    try:
        prefix = path.read_bytes()[:4]
    except OSError:
        return "unknown"
    if prefix.startswith(b"#!"):
        return "script"
    if prefix == b"\x7fELF":
        return "elf"
    if prefix in {
        b"\xfe\xed\xfa\xce",
        b"\xce\xfa\xed\xfe",
        b"\xfe\xed\xfa\xcf",
        b"\xcf\xfa\xed\xfe",
        b"\xca\xfe\xba\xbe",
        b"\xbe\xba\xfe\xca",
    }:
        return "mach-o"
    return "unknown"


def _render_summary(summary: SmokeSummary) -> str:
    host = summary["host"]
    lines = [
        "# Provider Smoke",
        "",
        f"- Status: **{summary['status']}**",
        f"- Started at: {summary['started_at']}",
        f"- Finished at: {summary['finished_at']}",
        f"- Host platform: `{host['platform']}`",
        f"- Host preflight provider executable: `{host['host_provider_executable']}`",
        f"- Host preflight provider binary format: `{host['host_provider_binary_format']}`",
        f"- Container provider executable: `{host['container_provider_executable']}`",
        f"- Container provider binary format: `{host['container_provider_binary_format']}`",
    ]
    compatibility_hint = host.get("compatibility_hint")
    if compatibility_hint is not None:
        lines.append(f"- Compatibility hint: {compatibility_hint}")
    lines.append("")
    for service, result in summary["services"].items():
        lines.extend(
            [
                f"## {service}",
                "",
                f"- Status: **{result['status']}**",
                f"- Reason: {result['reason'] or '<none>'}",
                f"- Image: `{result['image_ref']}`",
                f"- Resolved image id: `{result['resolved_image_id'] or '<unresolved>'}`",
                f"- Log: `{result['log_path']}`",
            ]
        )
        proxy_access_log = result.get("proxy_access_log_path")
        if proxy_access_log is not None:
            lines.append(f"- Proxy access log: `{proxy_access_log}`")
        egress_block_log = result.get("egress_block_log_path")
        if egress_block_log is not None:
            lines.append(f"- Blocked egress log: `{egress_block_log}`")
        detail = result.get("detail")
        if detail is not None:
            lines.append(f"- Detail: {detail}")
        hint = result.get("hint")
        if hint is not None:
            lines.append(f"- Hint: {hint}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
