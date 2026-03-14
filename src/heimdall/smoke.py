from __future__ import annotations

import json
import platform
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TypedDict

from heimdall.images import DockerError, resolve_image, run_container
from heimdall.models import PipelineConfig, RuntimeConfig
from heimdall.utils import (
    compact_run_id,
    ensure_directory,
    stage_executable_tree,
    stage_readable_tree,
    timestamp_utc,
    write_text,
)

SMOKE_SERVICES = ("andvari", "kvasir")
SMOKE_INPUT_FILENAME = "smoke.txt"
SMOKE_INPUT_CONTENT = "heimdall-provider-smoke\n"


class HostInfo(TypedDict):
    platform: str
    python_version: str
    codex_host_bin_dir: str
    codex_container_bin_dir: str
    codex_home_dir: str
    host_codex_executable: str
    host_codex_binary_format: str
    container_codex_executable: str
    container_codex_binary_format: str
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
    runtime_codex_home: str


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
    host_codex_executable = (runtime.codex_host_bin_dir / "codex").resolve()
    host_binary_format = _detect_binary_format(host_codex_executable)
    container_codex_executable = (runtime.codex_bin_dir / "codex").resolve()
    container_binary_format = _detect_binary_format(container_codex_executable)
    info: HostInfo = {
        "platform": platform.platform(),
        "python_version": sys.version.split()[0],
        "codex_host_bin_dir": str(runtime.codex_host_bin_dir),
        "codex_container_bin_dir": str(runtime.codex_bin_dir),
        "codex_home_dir": str(runtime.codex_home_dir),
        "host_codex_executable": str(host_codex_executable),
        "host_codex_binary_format": host_binary_format,
        "container_codex_executable": str(container_codex_executable),
        "container_codex_binary_format": container_binary_format,
        "compatibility_hint": None,
    }
    if container_binary_format.startswith("mach-o"):
        info["compatibility_hint"] = (
            "The container-facing Codex binary is Mach-O, which is usually not "
            "executable inside Linux service containers."
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
    runtime_codex_home = run_dir / "provider-state" / "codex-home"
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
        "runtime_codex_home": str(runtime_codex_home),
    }

    stage_conflict = _stage_conflict(runtime.codex_bin_dir, provider_bin_dir)
    if stage_conflict is not None:
        detail = (
            f"Refusing to stage codex bin dir {runtime.codex_bin_dir} into "
            f"{provider_bin_dir}: {stage_conflict}"
        )
        write_text(log_path, f"{detail}\n")
        return {
            **base_result,
            "reason": "stage-provider-bin-failed",
            "detail": detail,
        }

    stage_conflict = _stage_conflict(runtime.codex_home_dir, provider_seed_dir)
    if stage_conflict is not None:
        detail = (
            f"Refusing to stage codex home dir {runtime.codex_home_dir} into "
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
        stage_readable_tree(runtime.codex_home_dir, provider_seed_dir)
    except RuntimeError as exc:
        detail = str(exc)
        write_text(log_path, f"{detail}\n")
        return {
            **base_result,
            "resolved_image_id": resolved_image_id,
            "reason": "stage-provider-seed-failed",
            "detail": detail,
        }

    try:
        run_container(
            image_ref,
            {"HEIMDALL_SMOKE_SERVICE": service},
            [
                (provider_bin_dir, "/opt/provider/bin", True),
                (provider_seed_dir, "/opt/provider-seed/codex-home", True),
                (probe_input_dir, "/input", True),
                (run_dir, "/run", False),
            ],
            stream_output=runtime.verbose,
            output_path=log_path,
            log_prefix=f"smoke-{service}" if runtime.verbose else None,
            entrypoint="/bin/bash",
            command_args=["-lc", _provider_probe_script()],
        )
    except DockerError as exc:
        log_text = _read_log_text(log_path)
        probe_output = log_text or str(exc)
        reason = _classify_probe_failure(probe_output)
        detail = _summarize_probe_failure(probe_output, reason)
        hint = _probe_failure_hint(reason, runtime)
        _append_log(log_path, f"\n[heimdall][smoke] classified reason: {reason}\n")
        if hint is not None:
            _append_log(log_path, f"[heimdall][smoke] hint: {hint}\n")
        return {
            **base_result,
            "resolved_image_id": resolved_image_id,
            "reason": reason,
            "detail": detail,
            "hint": hint,
        }

    if not runtime_codex_home.is_dir():
        detail = (
            "Probe finished without creating /run/provider-state/codex-home inside "
            "the service run directory."
        )
        _append_log(log_path, f"\n[heimdall][smoke] {detail}\n")
        return {
            **base_result,
            "resolved_image_id": resolved_image_id,
            "reason": "runtime-codex-home-not-created",
            "detail": detail,
        }

    return {
        **base_result,
        "status": "passed",
        "resolved_image_id": resolved_image_id,
    }


def _provider_probe_script() -> str:
    return """
set -Eeuo pipefail
trap 'status=$?; echo "[smoke][error] command failed: ${BASH_COMMAND} (exit ${status})" >&2; exit ${status}' ERR

echo "[smoke] service=${HEIMDALL_SMOKE_SERVICE:-unknown}"
echo "[smoke] uname=$(uname -srm)"
echo "[smoke] provider bin dir:"
ls -la /opt/provider/bin
echo "[smoke] provider seed highlights:"
if [[ -d /opt/provider-seed/codex-home ]]; then
  for rel in auth.json config.toml sessions log tmp version.json .personality_migration; do
    if [[ -e "/opt/provider-seed/codex-home/${rel}" ]]; then
      ls -ld "/opt/provider-seed/codex-home/${rel}"
    fi
  done
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
echo "[smoke] provider smoke passed"
""".strip()


def _classify_probe_failure(detail: str) -> str:
    lowered = detail.lower()
    if "exec format error" in lowered or "cannot execute binary file" in lowered:
        return "codex-binary-incompatible-with-container"
    if "command failed: command -v codex" in lowered or "codex: not found" in lowered:
        return "codex-cli-unavailable-in-container"
    if "login status" in lowered or "not logged in" in lowered:
        return "codex-auth-unusable-in-container"
    if (
        "codex exec did not create /run/workspace/smoke-result.txt" in lowered
        or "codex exec created /run/workspace/smoke-result.txt, but it did not match /input/smoke.txt"
        in lowered
        or ("command failed: codex exec" in lowered and "sandbox" in lowered)
        or ("command failed: codex exec" in lowered and "/input/smoke.txt" in lowered)
        or ("command failed: codex exec" in lowered and "/run/workspace" in lowered)
    ):
        return "codex-exec-workspace-access-failed"
    if "permission denied" in lowered and "provider-seed" in lowered:
        return "provider-seed-unreadable-in-container"
    if "permission denied" in lowered and "provider/bin" in lowered:
        return "provider-bin-unreadable-in-container"
    if "command failed: codex exec" in lowered:
        return "codex-exec-failed"
    return "probe-command-failed"


def _summarize_probe_failure(probe_output: str, reason: str) -> str:
    lines = [line.strip() for line in probe_output.splitlines() if line.strip()]
    lowered = [line.lower() for line in lines]

    if reason == "codex-binary-incompatible-with-container":
        for index, line in enumerate(lowered):
            if "exec format error" in line or "cannot execute binary file" in line:
                return lines[index]
    if reason == "codex-cli-unavailable-in-container":
        for index, line in enumerate(lowered):
            if "codex: not found" in line or "command failed: command -v codex" in line:
                return lines[index]
    if reason == "codex-auth-unusable-in-container":
        for index, line in enumerate(lowered):
            if "not logged in" in line or "login status" in line:
                return lines[index]
    if reason == "codex-exec-workspace-access-failed":
        for index, line in enumerate(lowered):
            if (
                "codex exec did not create /run/workspace/smoke-result.txt" in line
                or "codex exec created /run/workspace/smoke-result.txt, but it did not match /input/smoke.txt"
                in line
                or "sandbox" in line
                or "/input/smoke.txt" in line
                or "/run/workspace" in line
            ):
                return lines[index]
    if reason == "codex-exec-failed":
        for index, line in enumerate(lowered):
            if "command failed: codex exec" in line:
                return lines[index]
    for line in reversed(lines):
        if line.startswith("[smoke][error]"):
            return line
    if lines:
        return lines[-1]
    return "Probe failed without producing any diagnostic output."


def _probe_failure_hint(reason: str, runtime: RuntimeConfig) -> str | None:
    binary_format = _detect_binary_format((runtime.codex_bin_dir / "codex").resolve())
    if reason == "codex-binary-incompatible-with-container":
        if binary_format == "mach-o":
            return (
                "Use a Linux ELF Codex binary in --codex-bin-dir, or run Heimdall on "
                "a Linux host. A macOS Mach-O binary cannot execute inside Linux "
                "service containers."
            )
        return (
            "Use a Codex binary built for the Linux container architecture and point "
            "--codex-bin-dir at a dedicated directory containing only that binary."
        )
    if reason == "codex-auth-unusable-in-container":
        return (
            "The binary runs inside the container, but the staged CODEX_HOME does not "
            "authenticate there. Check auth.json/config.toml and rerun the smoke test."
        )
    if reason == "codex-cli-unavailable-in-container":
        return (
            "The staged provider bin is not exposing a runnable codex executable "
            "inside the container. Use a dedicated bin directory containing only codex."
        )
    if reason == "codex-exec-workspace-access-failed":
        return (
            "The binary and auth work, but codex exec could not use the mounted "
            "service workspace. In service containers, use the same container-safe "
            "flags as Andvari/Kvasir: --dangerously-bypass-approvals-and-sandbox, "
            "--cd <workspace>, and any required --add-dir mounts."
        )
    if reason == "codex-exec-failed":
        return (
            "codex exec started inside the container, but the smoke task did not "
            "complete. Inspect the per-service log for the exact stderr/stdout."
        )
    return None


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
        f"- Host preflight Codex executable: `{host['host_codex_executable']}`",
        f"- Host preflight Codex binary format: `{host['host_codex_binary_format']}`",
        f"- Container Codex executable: `{host['container_codex_executable']}`",
        f"- Container Codex binary format: `{host['container_codex_binary_format']}`",
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
        detail = result.get("detail")
        if detail is not None:
            lines.append(f"- Detail: {detail}")
        hint = result.get("hint")
        if hint is not None:
            lines.append(f"- Hint: {hint}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
