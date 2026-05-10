from __future__ import annotations

import json
import re
import shutil
import tomllib
from collections.abc import Mapping
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

from heimdall.andvari_proxy import uses_andvari_proxy_runtime
from heimdall.models import Provider, RuntimeConfig
from heimdall.utils import stage_readable_paths, stage_readable_tree, write_text

_GITHUB_PLUGIN_NAME = "github@openai-curated"
_BARE_KEY_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_CLAUDE_API_KEY_HELPER_NAME = "api-key-helper.sh"
_CLAUDE_API_KEY_HELPER_RUNTIME_PATH = (
    f"/run/provider-state/claude-home/{_CLAUDE_API_KEY_HELPER_NAME}"
)

_MINIMAL_ANDVARI_CODEX_SEED_RELPATHS = ("auth.json", "config.toml", "skills/.system")
_MINIMAL_ANDVARI_CLAUDE_SEED_RELPATHS = ("credentials.json", "settings.json")
_KVASIR_CODEX_SEED_RELPATHS = (
    "auth.json",
    "config.toml",
    "skills/.system",
    "skills/custom",
    "memories",
    "history.jsonl",
    "models_cache.json",
)

CODEX_CONTAINER_SEED_PATH = "/opt/provider-seed/codex-home"
CLAUDE_CONTAINER_SEED_PATH = "/opt/provider-seed/claude-home"
CLAUDE_API_KEY_SECRET_CONTAINER_PATH = "/opt/provider-secrets/anthropic_api_key"
CODEX_HOME_SUBDIR = "codex-home"
CLAUDE_HOME_SUBDIR = "claude-home"


def provider_for_service(service_name: str, runtime: RuntimeConfig) -> Provider:
    if service_name == "kvasir" or service_name.startswith("kvasir-"):
        return "codex"
    return runtime.provider


def provider_home_dir_for_service(service_name: str, runtime: RuntimeConfig) -> Path:
    if (
        provider_for_service(service_name, runtime) == "claude"
        and runtime.claude_home_dir is not None
    ):
        return runtime.claude_home_dir
    return runtime.codex_home_dir


def provider_seed_container_path_for_service(
    service_name: str, runtime: RuntimeConfig
) -> str:
    if provider_for_service(service_name, runtime) == "claude":
        return CLAUDE_CONTAINER_SEED_PATH
    return CODEX_CONTAINER_SEED_PATH


def provider_home_subdir_for_service(service_name: str, runtime: RuntimeConfig) -> str:
    if provider_for_service(service_name, runtime) == "claude":
        return CLAUDE_HOME_SUBDIR
    return CODEX_HOME_SUBDIR


def extra_mounts_for_service(
    service_name: str, runtime: RuntimeConfig
) -> tuple[tuple[Path, str, bool], ...]:
    if provider_for_service(service_name, runtime) != "claude":
        return ()
    if runtime.claude_auth_mode != "api-key-file":
        return ()
    if runtime.claude_api_key_file is None:
        return ()
    return ((runtime.claude_api_key_file, CLAUDE_API_KEY_SECRET_CONTAINER_PATH, True),)


def andvari_home_dir(runtime: RuntimeConfig) -> Path:
    return provider_home_dir_for_service("andvari", runtime)


def provider_seed_container_path(runtime: RuntimeConfig) -> str:
    return provider_seed_container_path_for_service("andvari", runtime)


def andvari_network_name(runtime: RuntimeConfig) -> str | None:
    return runtime.andvari_internal_network_name


def docker_network_for_step(step: str, runtime: RuntimeConfig) -> str | None:
    if not uses_andvari_proxy_runtime(step):
        return None
    return andvari_network_name(runtime)


def env_for_step(step: str, runtime: RuntimeConfig) -> dict[str, str]:
    del runtime
    del step
    return {}


def stage_provider_seed(
    service_name: str,
    source_home: Path,
    destination_seed: Path,
    runtime: RuntimeConfig,
) -> None:
    if uses_andvari_proxy_runtime(service_name):
        if provider_for_service(service_name, runtime) == "claude":
            if runtime.claude_auth_mode == "api-key-file":
                stage_andvari_claude_api_key_seed(destination_seed)
            else:
                stage_readable_paths(
                    source_home,
                    destination_seed,
                    _MINIMAL_ANDVARI_CLAUDE_SEED_RELPATHS,
                )
                sanitize_andvari_claude_seed(service_name, destination_seed)
        else:
            stage_readable_paths(
                source_home,
                destination_seed,
                _MINIMAL_ANDVARI_CODEX_SEED_RELPATHS,
            )
            sanitize_andvari_codex_seed(service_name, destination_seed)
        return

    if service_name == "kvasir" or service_name.startswith("kvasir-"):
        stage_readable_paths(
            source_home,
            destination_seed,
            _KVASIR_CODEX_SEED_RELPATHS,
        )
        return

    stage_readable_tree(source_home, destination_seed)


def stage_andvari_claude_api_key_seed(destination_seed: Path) -> None:
    destination_seed.parent.mkdir(parents=True, exist_ok=True)
    if destination_seed.exists():
        shutil.rmtree(destination_seed)
    destination_seed.mkdir(parents=True, exist_ok=True)
    write_text(
        destination_seed / "settings.json",
        json.dumps({"apiKeyHelper": _CLAUDE_API_KEY_HELPER_RUNTIME_PATH}, indent=2)
        + "\n",
        mode=0o644,
    )
    write_text(
        destination_seed / _CLAUDE_API_KEY_HELPER_NAME,
        """#!/bin/sh
set -eu
key_path='/opt/provider-secrets/anthropic_api_key'
if [ ! -r "${key_path}" ]; then
  echo "Claude API key file unavailable: ${key_path}" >&2
  exit 1
fi
head -n 1 "${key_path}" | tr -d '\\r\\n'
""",
        mode=0o755,
    )


def sanitize_andvari_claude_seed(
    service_name: str,
    staged_claude_home: Path,
) -> None:
    if not uses_andvari_proxy_runtime(service_name):
        return
    settings_path = staged_claude_home / "settings.json"
    if not settings_path.is_file():
        return
    try:
        payload = json.loads(settings_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Failed to read staged Claude settings {settings_path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        return
    payload.pop("mcpServers", None)
    try:
        settings_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(
            f"Failed to write staged Claude settings {settings_path}: {exc}"
        ) from exc


def sanitize_andvari_codex_seed(
    service_name: str,
    staged_codex_home: Path,
) -> None:
    if not uses_andvari_proxy_runtime(service_name):
        return
    config_path = staged_codex_home / "config.toml"
    payload = _load_toml_document(config_path)
    payload["web_search"] = "disabled"

    plugins = payload.get("plugins")
    if not isinstance(plugins, dict):
        plugins = {}
    github_plugin = plugins.get(_GITHUB_PLUGIN_NAME)
    if not isinstance(github_plugin, dict):
        github_plugin = {}
    github_plugin["enabled"] = False
    plugins[_GITHUB_PLUGIN_NAME] = github_plugin
    payload["plugins"] = plugins

    write_text(config_path, _dump_toml_document(payload))


def _load_toml_document(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        loaded = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise RuntimeError(f"Failed to read staged Codex config {path}: {exc}") from exc
    return dict(loaded)


def _dump_toml_document(document: Mapping[str, Any]) -> str:
    lines: list[str] = []
    _dump_table_body(lines, (), document)
    return "\n".join(lines).rstrip() + "\n"


def _dump_table(
    lines: list[str], prefix: tuple[str, ...], table: Mapping[str, Any]
) -> None:
    if lines:
        lines.append("")
    lines.append(f"[{'.'.join(_encode_key(part) for part in prefix)}]")
    _dump_table_body(lines, prefix, table)


def _dump_table_body(
    lines: list[str], prefix: tuple[str, ...], table: Mapping[str, Any]
) -> None:
    scalar_items: list[tuple[str, Any]] = []
    nested_items: list[tuple[str, Mapping[str, Any]]] = []
    array_table_items: list[tuple[str, list[Mapping[str, Any]]]] = []
    for key, value in table.items():
        if isinstance(value, Mapping):
            nested_items.append((key, value))
            continue
        if _is_array_of_tables(value):
            array_table_items.append((key, value))
            continue
        scalar_items.append((key, value))

    for key, value in scalar_items:
        lines.append(f"{_encode_key(key)} = {_encode_value(value)}")
    for key, value in nested_items:
        _dump_table(lines, (*prefix, key), value)
    for key, value in array_table_items:
        _dump_table_array(lines, (*prefix, key), value)


def _dump_table_array(
    lines: list[str], prefix: tuple[str, ...], tables: list[Mapping[str, Any]]
) -> None:
    for table in tables:
        if lines:
            lines.append("")
        lines.append(f"[[{'.'.join(_encode_key(part) for part in prefix)}]]")
        _dump_table_body(lines, prefix, table)


def _is_array_of_tables(value: Any) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and all(isinstance(item, Mapping) for item in value)
    )


def _encode_key(key: str) -> str:
    if _BARE_KEY_RE.fullmatch(key):
        return key
    escaped = key.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _encode_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        return f'"{escaped}"'
    if isinstance(value, list):
        if any(isinstance(item, Mapping) for item in value):
            raise RuntimeError(
                "Unsupported inline TOML array containing mapping/object values"
            )
        return "[" + ", ".join(_encode_value(item) for item in value) + "]"
    raise RuntimeError(f"Unsupported TOML value in staged Codex config: {value!r}")
