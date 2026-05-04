from __future__ import annotations

import json
import re
import tomllib
from collections.abc import Mapping
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

from heimdall.andvari_proxy import uses_andvari_proxy_runtime
from heimdall.models import RuntimeConfig
from heimdall.utils import stage_readable_paths, stage_readable_tree, write_text

_GITHUB_PLUGIN_NAME = "github@openai-curated"
_BARE_KEY_RE = re.compile(r"^[A-Za-z0-9_-]+$")

_MINIMAL_ANDVARI_CODEX_SEED_RELPATHS = ("auth.json", "config.toml", "skills/.system")
_MINIMAL_ANDVARI_CLAUDE_SEED_RELPATHS = ("credentials.json", "settings.json")

CODEX_CONTAINER_SEED_PATH = "/opt/provider-seed/codex-home"
CLAUDE_CONTAINER_SEED_PATH = "/opt/provider-seed/claude-home"


def andvari_home_dir(runtime: RuntimeConfig) -> Path:
    if runtime.provider == "claude" and runtime.claude_home_dir is not None:
        return runtime.claude_home_dir
    return runtime.codex_home_dir


def provider_seed_container_path(runtime: RuntimeConfig) -> str:
    if runtime.provider == "claude":
        return CLAUDE_CONTAINER_SEED_PATH
    return CODEX_CONTAINER_SEED_PATH


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
        if runtime.provider == "claude":
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
    stage_readable_tree(source_home, destination_seed)


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
