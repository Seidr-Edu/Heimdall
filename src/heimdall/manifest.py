from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlparse

from heimdall.models import (
    AndvariConfig,
    EitriConfig,
    ImageRefs,
    KvasirConfig,
    LidskjalvConfig,
    LidskjalvTargetConfig,
    PipelineConfig,
    RuntimeConfig,
    SourceConfig,
)
from heimdall.simpleyaml import YamlError, dumps, loads
from heimdall.utils import compact_run_id


class ManifestValidationError(ValueError):
    """Raised when the public pipeline manifest is invalid."""


_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_KEY_SANITIZE_RE = re.compile(r"[^a-zA-Z0-9_.-]")


def load_pipeline_manifest(path: Path) -> tuple[str, PipelineConfig]:
    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ManifestValidationError(f"Failed to read pipeline manifest: {path} ({exc})") from exc

    try:
        loaded = loads(raw_text)
    except YamlError as exc:
        raise ManifestValidationError(f"Invalid pipeline manifest YAML: {exc}") from exc

    if not isinstance(loaded, Mapping):
        raise ManifestValidationError("Pipeline manifest root must be a mapping/object")

    config = _parse_pipeline_mapping(dict(loaded))
    return raw_text, config


def runtime_snapshot(runtime: RuntimeConfig) -> dict[str, object]:
    return {
        "runs_root": str(runtime.runs_root),
        "pull_policy": runtime.pull_policy,
        "codex_bin_dir": str(runtime.codex_bin_dir),
        "codex_home_dir": str(runtime.codex_home_dir),
        "sonar_host_url": runtime.sonar_host_url,
        "sonar_organization": runtime.sonar_organization,
    }


def pipeline_to_document(config: PipelineConfig) -> dict[str, object]:
    return {
        "version": config.version,
        "run_id": config.run_id,
        "source": {
            "repo_url": config.source.repo_url,
            "commit_sha": config.source.commit_sha,
        },
        "images": {
            "brokk": config.images.brokk,
            "eitri": config.images.eitri,
            "andvari": config.images.andvari,
            "kvasir": config.images.kvasir,
            "lidskjalv": config.images.lidskjalv,
        },
        "eitri": {
            "source_relpaths": list(config.eitri.source_relpaths),
            "parser_extension": config.eitri.parser_extension,
            "writer_extension": config.eitri.writer_extension,
            "verbose": config.eitri.verbose,
            "writers": config.eitri.writers,
        },
        "andvari": {
            "gating_mode": config.andvari.gating_mode,
            "max_iter": config.andvari.max_iter,
            "max_gate_revisions": config.andvari.max_gate_revisions,
            "model_gate_timeout_sec": config.andvari.model_gate_timeout_sec,
        },
        "kvasir": {
            "original_subdir": config.kvasir.original_subdir,
            "generated_subdir": config.kvasir.generated_subdir,
            "max_iter": config.kvasir.max_iter,
            "write_scope_ignore_prefixes": list(config.kvasir.write_scope_ignore_prefixes),
        },
        "lidskjalv": {
            "skip_sonar": config.lidskjalv.skip_sonar,
            "sonar_wait_timeout_sec": config.lidskjalv.sonar_wait_timeout_sec,
            "sonar_wait_poll_sec": config.lidskjalv.sonar_wait_poll_sec,
            "original": {
                "repo_subdir": config.lidskjalv.original.repo_subdir,
                "project_key": config.lidskjalv.original.project_key,
                "project_name": config.lidskjalv.original.project_name,
            },
            "generated": {
                "repo_subdir": config.lidskjalv.generated.repo_subdir,
                "project_key": config.lidskjalv.generated.project_key,
                "project_name": config.lidskjalv.generated.project_name,
            },
        },
    }


def dumps_pipeline(config: PipelineConfig) -> str:
    return dumps(pipeline_to_document(config))


def sanitize_project_key(value: str) -> str:
    return _KEY_SANITIZE_RE.sub("_", value)


def derive_repo_identity(repo_url: str) -> tuple[str, str, str]:
    parsed = urlparse(repo_url)
    segments = [segment for segment in parsed.path.split("/") if segment]
    if len(segments) < 2:
        raise ManifestValidationError("repo_url must identify a GitHub repository")
    org = segments[0]
    repo = segments[1]
    if repo.endswith(".git"):
        repo = repo[:-4]
    if not org or not repo:
        raise ManifestValidationError("repo_url must identify a GitHub repository")
    display = f"{org}/{repo}"
    base_key = sanitize_project_key(f"{org}_{repo}")
    return display, repo, base_key


def derive_lidskjalv_defaults(repo_url: str) -> dict[str, str]:
    display, _repo_name, base_key = derive_repo_identity(repo_url)
    return {
        "original_key": f"{base_key}__original",
        "generated_key": f"{base_key}__generated",
        "original_name": f"{display} (original)",
        "generated_name": f"{display} (generated)",
    }


def _parse_pipeline_mapping(data: dict[str, object]) -> PipelineConfig:
    _reject_unknown_keys(
        data,
        {
            "version",
            "run_id",
            "source",
            "images",
            "eitri",
            "andvari",
            "kvasir",
            "lidskjalv",
        },
        "root",
    )
    version = _require_int(data, "version", "root")
    if version != 1:
        raise ManifestValidationError(f"Unsupported pipeline manifest version: {version!r}")
    run_id = _optional_str(data, "run_id", "root") or compact_run_id()

    source_data = _require_mapping(data, "source", "root")
    _reject_unknown_keys(source_data, {"repo_url", "commit_sha"}, "source")
    repo_url = _require_str(source_data, "repo_url", "source")
    commit_sha = _require_str(source_data, "commit_sha", "source")
    _validate_repo_url(repo_url)
    if not _SHA_RE.fullmatch(commit_sha):
        raise ManifestValidationError("source.commit_sha must be a full 40-character lowercase SHA")
    source = SourceConfig(repo_url=repo_url, commit_sha=commit_sha)

    images_data = _require_mapping(data, "images", "root")
    _reject_unknown_keys(images_data, {"brokk", "eitri", "andvari", "kvasir", "lidskjalv"}, "images")
    images = ImageRefs(
        brokk=_require_str(images_data, "brokk", "images"),
        eitri=_require_str(images_data, "eitri", "images"),
        andvari=_require_str(images_data, "andvari", "images"),
        kvasir=_require_str(images_data, "kvasir", "images"),
        lidskjalv=_require_str(images_data, "lidskjalv", "images"),
    )

    eitri_data = _optional_mapping(data, "eitri", "root")
    _reject_unknown_keys(
        eitri_data,
        {"source_relpaths", "parser_extension", "writer_extension", "verbose", "writers"},
        "eitri",
    )
    source_relpaths = tuple(_string_list(eitri_data.get("source_relpaths"), "eitri.source_relpaths") or ["."])
    writers = eitri_data.get("writers", {})
    if not isinstance(writers, Mapping):
        raise ManifestValidationError("eitri.writers must be a mapping/object")
    eitri = EitriConfig(
        source_relpaths=source_relpaths,
        parser_extension=_optional_str(eitri_data, "parser_extension", "eitri"),
        writer_extension=_optional_str(eitri_data, "writer_extension", "eitri"),
        verbose=_optional_bool(eitri_data, "verbose", "eitri", False),
        writers=dict(writers),
    )

    andvari_data = _optional_mapping(data, "andvari", "root")
    _reject_unknown_keys(
        andvari_data,
        {"gating_mode", "max_iter", "max_gate_revisions", "model_gate_timeout_sec"},
        "andvari",
    )
    gating_mode = _optional_str(andvari_data, "gating_mode", "andvari") or "model"
    if gating_mode not in {"model", "fixed"}:
        raise ManifestValidationError("andvari.gating_mode must be one of: model, fixed")
    andvari = AndvariConfig(
        gating_mode=gating_mode,
        max_iter=_optional_int(andvari_data, "max_iter", "andvari", 8),
        max_gate_revisions=_optional_int(andvari_data, "max_gate_revisions", "andvari", 3),
        model_gate_timeout_sec=_optional_int(andvari_data, "model_gate_timeout_sec", "andvari", 120),
    )

    kvasir_data = _optional_mapping(data, "kvasir", "root")
    _reject_unknown_keys(
        kvasir_data,
        {"original_subdir", "generated_subdir", "max_iter", "write_scope_ignore_prefixes"},
        "kvasir",
    )
    kvasir = KvasirConfig(
        original_subdir=_optional_str(kvasir_data, "original_subdir", "kvasir"),
        generated_subdir=_optional_str(kvasir_data, "generated_subdir", "kvasir"),
        max_iter=_optional_int(kvasir_data, "max_iter", "kvasir", 5),
        write_scope_ignore_prefixes=tuple(
            _string_list(kvasir_data.get("write_scope_ignore_prefixes"), "kvasir.write_scope_ignore_prefixes")
            or []
        ),
    )

    lidskjalv_data = _optional_mapping(data, "lidskjalv", "root")
    _reject_unknown_keys(
        lidskjalv_data,
        {"skip_sonar", "sonar_wait_timeout_sec", "sonar_wait_poll_sec", "original", "generated"},
        "lidskjalv",
    )
    lidskjalv = LidskjalvConfig(
        skip_sonar=_optional_bool(lidskjalv_data, "skip_sonar", "lidskjalv", False),
        sonar_wait_timeout_sec=_optional_int(
            lidskjalv_data, "sonar_wait_timeout_sec", "lidskjalv", 300
        ),
        sonar_wait_poll_sec=_optional_int(
            lidskjalv_data, "sonar_wait_poll_sec", "lidskjalv", 5
        ),
        original=_parse_lidskjalv_target(lidskjalv_data.get("original"), "lidskjalv.original"),
        generated=_parse_lidskjalv_target(lidskjalv_data.get("generated"), "lidskjalv.generated"),
    )
    return PipelineConfig(
        version=version,
        run_id=run_id,
        source=source,
        images=images,
        eitri=eitri,
        andvari=andvari,
        kvasir=kvasir,
        lidskjalv=lidskjalv,
    )


def _parse_lidskjalv_target(value: object, path: str) -> LidskjalvTargetConfig:
    if value is None:
        return LidskjalvTargetConfig()
    if not isinstance(value, Mapping):
        raise ManifestValidationError(f"{path} must be a mapping/object")
    data = dict(value)
    _reject_unknown_keys(data, {"repo_subdir", "project_key", "project_name"}, path)
    return LidskjalvTargetConfig(
        repo_subdir=_optional_str(data, "repo_subdir", path),
        project_key=_optional_str(data, "project_key", path),
        project_name=_optional_str(data, "project_name", path),
    )


def _validate_repo_url(repo_url: str) -> None:
    parsed = urlparse(repo_url)
    if parsed.scheme != "https":
        raise ManifestValidationError("source.repo_url must use https")
    if parsed.hostname != "github.com":
        raise ManifestValidationError("source.repo_url must point to github.com")
    if parsed.username or parsed.password:
        raise ManifestValidationError("source.repo_url must not include credentials")
    if parsed.query or parsed.fragment:
        raise ManifestValidationError("source.repo_url must not include query or fragment components")
    segments = [segment for segment in parsed.path.split("/") if segment]
    if len(segments) != 2:
        raise ManifestValidationError("source.repo_url must match https://github.com/<owner>/<repo>[.git]")
    if not segments[0] or not segments[1]:
        raise ManifestValidationError("source.repo_url must identify a GitHub repository")


def _reject_unknown_keys(data: Mapping[str, object], allowed: set[str], path: str) -> None:
    unknown = sorted(set(data.keys()) - allowed)
    if unknown:
        raise ManifestValidationError(f"Unknown key(s) at {path}: {', '.join(unknown)}")


def _require_mapping(data: Mapping[str, object], key: str, path: str) -> dict[str, object]:
    value = data.get(key)
    if not isinstance(value, Mapping):
        raise ManifestValidationError(f"{path}.{key} must be a mapping/object")
    return dict(value)


def _optional_mapping(data: Mapping[str, object], key: str, path: str) -> dict[str, object]:
    value = data.get(key)
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ManifestValidationError(f"{path}.{key} must be a mapping/object")
    return dict(value)


def _require_str(data: Mapping[str, object], key: str, path: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ManifestValidationError(f"{path}.{key} must be a non-empty string")
    return value.strip()


def _optional_str(data: Mapping[str, object], key: str, path: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ManifestValidationError(f"{path}.{key} must be a non-empty string")
    return value.strip()


def _require_int(data: Mapping[str, object], key: str, path: str) -> int:
    value = data.get(key)
    if not isinstance(value, int):
        raise ManifestValidationError(f"{path}.{key} must be an integer")
    return value


def _optional_int(data: Mapping[str, object], key: str, path: str, default: int) -> int:
    value = data.get(key)
    if value is None:
        return default
    if not isinstance(value, int) or value < 0:
        raise ManifestValidationError(f"{path}.{key} must be a non-negative integer")
    return value


def _optional_bool(data: Mapping[str, object], key: str, path: str, default: bool) -> bool:
    value = data.get(key)
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ManifestValidationError(f"{path}.{key} must be a boolean")
    return value


def _string_list(value: object, path: str) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise ManifestValidationError(f"{path} must be an array of non-empty strings")
    return [item.strip() for item in value]
