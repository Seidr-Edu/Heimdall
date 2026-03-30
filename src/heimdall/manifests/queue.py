from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import cast

from heimdall.models import (
    AndvariConfig,
    EitriConfig,
    ImageRefs,
    KvasirConfig,
    LidskjalvConfig,
    LidskjalvTargetConfig,
    PullPolicy,
    QueueRequest,
    WorkerConfig,
)
from heimdall.simpleyaml import YamlError, dumps, loads

from . import pipeline as pipeline_mod
from .pipeline import ManifestValidationError, pipeline_to_document


def load_worker_config(path: Path) -> WorkerConfig:
    data = _load_yaml_mapping(path, "worker config")
    return _parse_worker_config_mapping(data, path.resolve().parent)


def load_queue_request(path: Path) -> QueueRequest:
    data = _load_yaml_mapping(path, "queue request")
    return _parse_queue_request_mapping(data)


def load_queue_request_text(text: str) -> QueueRequest:
    try:
        loaded = loads(text)
    except YamlError as exc:
        raise ManifestValidationError(f"Invalid queue request YAML: {exc}") from exc
    if not isinstance(loaded, Mapping):
        raise ManifestValidationError("Queue request root must be a mapping/object")
    return _parse_queue_request_mapping(dict(loaded))


def dump_queue_request(request: QueueRequest) -> str:
    return dumps(queue_request_to_document(request))


def queue_request_to_document(request: QueueRequest) -> dict[str, object]:
    document: dict[str, object] = {
        "version": request.version,
        "repo_url": request.repo_url,
        "commit_sha": request.commit_sha,
    }
    if request.eitri:
        document["eitri"] = request.eitri
    if request.andvari:
        document["andvari"] = request.andvari
    if request.kvasir:
        document["kvasir"] = request.kvasir
    if request.lidskjalv:
        document["lidskjalv"] = request.lidskjalv
    return document


def request_from_submit_args(
    repo_url: str,
    commit_sha: str,
    overrides_path: Path | None,
) -> QueueRequest:
    document: dict[str, object] = {
        "repo_url": repo_url,
        "commit_sha": commit_sha,
    }
    if overrides_path is not None:
        overrides = _load_yaml_mapping(overrides_path, "queue overrides")
        pipeline_mod._reject_unknown_keys(
            overrides,
            {"eitri", "andvari", "kvasir", "lidskjalv"},
            "root",
        )
        document.update(overrides)
    return _parse_queue_request_mapping(document)


def build_pipeline_manifest_for_job(
    worker_config: WorkerConfig,
    request: QueueRequest,
    *,
    run_id: str,
) -> str:
    document: dict[str, object] = {
        "version": 1,
        "run_id": run_id,
        "source": {
            "repo_url": request.repo_url,
            "commit_sha": request.commit_sha,
        },
        "images": {
            "brokk": worker_config.images.brokk,
            "eitri": worker_config.images.eitri,
            "andvari": worker_config.images.andvari,
            "mimir": worker_config.images.mimir,
            "kvasir": worker_config.images.kvasir,
            "lidskjalv": worker_config.images.lidskjalv,
        },
        "eitri": _eitri_to_document(worker_config.eitri),
        "andvari": _andvari_to_document(worker_config.andvari),
        "kvasir": _kvasir_to_document(worker_config.kvasir),
        "lidskjalv": _lidskjalv_to_document(worker_config.lidskjalv),
    }
    _deep_merge(document["eitri"], request.eitri)
    _deep_merge(document["andvari"], request.andvari)
    _deep_merge(document["kvasir"], request.kvasir)
    _deep_merge(document["lidskjalv"], request.lidskjalv)
    parsed = pipeline_mod._parse_pipeline_mapping(document)
    return dumps(pipeline_to_document(parsed))


def _load_yaml_mapping(path: Path, label: str) -> dict[str, object]:
    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ManifestValidationError(
            f"Failed to read {label}: {path} ({exc})"
        ) from exc
    try:
        loaded = loads(raw_text)
    except YamlError as exc:
        raise ManifestValidationError(f"Invalid {label} YAML: {exc}") from exc
    if not isinstance(loaded, Mapping):
        raise ManifestValidationError(
            f"{label.capitalize()} root must be a mapping/object"
        )
    return dict(loaded)


def _parse_worker_config_mapping(
    data: dict[str, object], base_dir: Path
) -> WorkerConfig:
    pipeline_mod._reject_unknown_keys(
        data,
        {
            "version",
            "queue_root",
            "runs_root",
            "codex_bin_dir",
            "codex_host_bin_dir",
            "codex_home_dir",
            "pull_policy",
            "verbose",
            "images",
            "eitri",
            "andvari",
            "kvasir",
            "lidskjalv",
        },
        "root",
    )
    version = data.get("version")
    if version is not None and version != 1:
        raise ManifestValidationError(f"Unsupported worker config version: {version!r}")
    pull_policy_raw = pipeline_mod._optional_str(data, "pull_policy", "root")
    pull_policy = pull_policy_raw or "if-missing"
    if pull_policy not in {"if-missing", "always", "never"}:
        raise ManifestValidationError(
            "root.pull_policy must be one of: if-missing, always, never"
        )
    return WorkerConfig(
        queue_root=_resolve_path(
            pipeline_mod._require_str(data, "queue_root", "root"), base_dir
        ),
        runs_root=_resolve_path(
            pipeline_mod._require_str(data, "runs_root", "root"), base_dir
        ),
        codex_bin_dir=_resolve_path(
            pipeline_mod._require_str(data, "codex_bin_dir", "root"), base_dir
        ),
        codex_host_bin_dir=_optional_resolve_path(
            pipeline_mod._optional_str(data, "codex_host_bin_dir", "root"), base_dir
        ),
        codex_home_dir=_resolve_path(
            pipeline_mod._require_str(data, "codex_home_dir", "root"), base_dir
        ),
        pull_policy=cast(PullPolicy, pull_policy),
        verbose=pipeline_mod._optional_bool(data, "verbose", "root", False),
        images=_parse_images_config(
            pipeline_mod._require_mapping(data, "images", "root"), "images"
        ),
        eitri=_parse_eitri_config(
            pipeline_mod._optional_mapping(data, "eitri", "root")
        ),
        andvari=_parse_andvari_config(
            pipeline_mod._optional_mapping(data, "andvari", "root")
        ),
        kvasir=_parse_kvasir_config(
            pipeline_mod._optional_mapping(data, "kvasir", "root")
        ),
        lidskjalv=_parse_lidskjalv_config(
            pipeline_mod._optional_mapping(data, "lidskjalv", "root")
        ),
    )


def _parse_queue_request_mapping(data: dict[str, object]) -> QueueRequest:
    pipeline_mod._reject_unknown_keys(
        data,
        {
            "version",
            "repo_url",
            "commit_sha",
            "eitri",
            "andvari",
            "kvasir",
            "lidskjalv",
        },
        "root",
    )
    version = data.get("version")
    if version is not None and version != 1:
        raise ManifestValidationError(f"Unsupported queue request version: {version!r}")
    parsed_version = 1
    repo_url = pipeline_mod._require_str(data, "repo_url", "root")
    commit_sha = pipeline_mod._require_str(data, "commit_sha", "root")
    pipeline_mod._validate_repo_url(repo_url, field_path="root.repo_url")
    if not pipeline_mod._SHA_RE.fullmatch(commit_sha):
        raise ManifestValidationError(
            "root.commit_sha must be a full 40-character lowercase SHA"
        )
    return QueueRequest(
        repo_url=repo_url,
        commit_sha=commit_sha,
        eitri=_parse_eitri_override(
            pipeline_mod._optional_mapping(data, "eitri", "root"), "eitri"
        ),
        andvari=_parse_andvari_override(
            pipeline_mod._optional_mapping(data, "andvari", "root"), "andvari"
        ),
        kvasir=_parse_kvasir_override(
            pipeline_mod._optional_mapping(data, "kvasir", "root"), "kvasir"
        ),
        lidskjalv=_parse_lidskjalv_override(
            pipeline_mod._optional_mapping(data, "lidskjalv", "root"), "lidskjalv"
        ),
        version=parsed_version,
    )


def _parse_images_config(data: dict[str, object], path: str) -> ImageRefs:
    pipeline_mod._reject_unknown_keys(
        data, {"brokk", "eitri", "andvari", "mimir", "kvasir", "lidskjalv"}, path
    )
    return ImageRefs(
        brokk=pipeline_mod._require_str(data, "brokk", path),
        eitri=pipeline_mod._require_str(data, "eitri", path),
        andvari=pipeline_mod._require_str(data, "andvari", path),
        mimir=pipeline_mod._require_str(data, "mimir", path),
        kvasir=pipeline_mod._require_str(data, "kvasir", path),
        lidskjalv=pipeline_mod._require_str(data, "lidskjalv", path),
    )


def _parse_eitri_config(data: dict[str, object]) -> EitriConfig:
    pipeline_mod._reject_unknown_keys(
        data,
        {
            "source_relpaths",
            "parser_extension",
            "writer_extension",
            "verbose",
            "writers",
        },
        "eitri",
    )
    writers = data.get("writers", {})
    if not isinstance(writers, Mapping):
        raise ManifestValidationError("eitri.writers must be a mapping/object")
    return EitriConfig(
        source_relpaths=tuple(
            pipeline_mod._string_list(
                data.get("source_relpaths"), "eitri.source_relpaths"
            )
            or ["."]
        ),
        parser_extension=pipeline_mod._optional_str(data, "parser_extension", "eitri"),
        writer_extension=pipeline_mod._optional_str(data, "writer_extension", "eitri"),
        verbose=pipeline_mod._optional_bool(data, "verbose", "eitri", False),
        writers=dict(writers),
    )


def _parse_andvari_config(data: dict[str, object]) -> AndvariConfig:
    pipeline_mod._reject_unknown_keys(
        data,
        {"gating_mode", "max_iter", "max_gate_revisions", "model_gate_timeout_sec"},
        "andvari",
    )
    gating_mode = pipeline_mod._optional_str(data, "gating_mode", "andvari") or "model"
    if gating_mode not in {"model", "fixed"}:
        raise ManifestValidationError(
            "andvari.gating_mode must be one of: model, fixed"
        )
    return AndvariConfig(
        gating_mode=gating_mode,
        max_iter=pipeline_mod._optional_int(data, "max_iter", "andvari", 8),
        max_gate_revisions=pipeline_mod._optional_int(
            data, "max_gate_revisions", "andvari", 3
        ),
        model_gate_timeout_sec=pipeline_mod._optional_int(
            data, "model_gate_timeout_sec", "andvari", 120
        ),
    )


def _parse_kvasir_config(data: dict[str, object]) -> KvasirConfig:
    pipeline_mod._reject_unknown_keys(
        data,
        {
            "original_subdir",
            "generated_subdir",
            "max_iter",
            "write_scope_ignore_prefixes",
        },
        "kvasir",
    )
    return KvasirConfig(
        original_subdir=pipeline_mod._optional_str(data, "original_subdir", "kvasir"),
        generated_subdir=pipeline_mod._optional_str(data, "generated_subdir", "kvasir"),
        max_iter=pipeline_mod._optional_int(data, "max_iter", "kvasir", 5),
        write_scope_ignore_prefixes=tuple(
            pipeline_mod._string_list(
                data.get("write_scope_ignore_prefixes"),
                "kvasir.write_scope_ignore_prefixes",
            )
            or []
        ),
    )


def _parse_lidskjalv_config(data: dict[str, object]) -> LidskjalvConfig:
    pipeline_mod._reject_unknown_keys(
        data,
        {
            "skip_sonar",
            "sonar_wait_timeout_sec",
            "sonar_wait_poll_sec",
            "original",
            "generated",
        },
        "lidskjalv",
    )
    return LidskjalvConfig(
        skip_sonar=pipeline_mod._optional_bool(data, "skip_sonar", "lidskjalv", False),
        sonar_wait_timeout_sec=pipeline_mod._optional_int(
            data, "sonar_wait_timeout_sec", "lidskjalv", 300
        ),
        sonar_wait_poll_sec=pipeline_mod._optional_int(
            data, "sonar_wait_poll_sec", "lidskjalv", 5
        ),
        original=_parse_lidskjalv_target_override(
            pipeline_mod._optional_mapping(data, "original", "lidskjalv"),
            "lidskjalv.original",
        ),
        generated=_parse_lidskjalv_target_override(
            pipeline_mod._optional_mapping(data, "generated", "lidskjalv"),
            "lidskjalv.generated",
        ),
    )


def _parse_eitri_override(data: dict[str, object], path: str) -> dict[str, object]:
    pipeline_mod._reject_unknown_keys(
        data,
        {
            "source_relpaths",
            "parser_extension",
            "writer_extension",
            "verbose",
            "writers",
        },
        path,
    )
    result: dict[str, object] = {}
    if "source_relpaths" in data:
        relpaths = pipeline_mod._string_list(
            data.get("source_relpaths"), f"{path}.source_relpaths"
        )
        result["source_relpaths"] = relpaths or []
    if "parser_extension" in data:
        result["parser_extension"] = pipeline_mod._optional_str(
            data, "parser_extension", path
        )
    if "writer_extension" in data:
        result["writer_extension"] = pipeline_mod._optional_str(
            data, "writer_extension", path
        )
    if "verbose" in data:
        result["verbose"] = pipeline_mod._optional_bool(data, "verbose", path, False)
    if "writers" in data:
        writers = data["writers"]
        if not isinstance(writers, Mapping):
            raise ManifestValidationError(f"{path}.writers must be a mapping/object")
        result["writers"] = dict(writers)
    return result


def _parse_andvari_override(data: dict[str, object], path: str) -> dict[str, object]:
    pipeline_mod._reject_unknown_keys(
        data,
        {"gating_mode", "max_iter", "max_gate_revisions", "model_gate_timeout_sec"},
        path,
    )
    result: dict[str, object] = {}
    if "gating_mode" in data:
        gating_mode = pipeline_mod._optional_str(data, "gating_mode", path)
        if gating_mode not in {"model", "fixed"}:
            raise ManifestValidationError(
                f"{path}.gating_mode must be one of: model, fixed"
            )
        result["gating_mode"] = gating_mode
    if "max_iter" in data:
        result["max_iter"] = pipeline_mod._optional_int(data, "max_iter", path, 0)
    if "max_gate_revisions" in data:
        result["max_gate_revisions"] = pipeline_mod._optional_int(
            data, "max_gate_revisions", path, 0
        )
    if "model_gate_timeout_sec" in data:
        result["model_gate_timeout_sec"] = pipeline_mod._optional_int(
            data, "model_gate_timeout_sec", path, 0
        )
    return result


def _parse_kvasir_override(data: dict[str, object], path: str) -> dict[str, object]:
    pipeline_mod._reject_unknown_keys(
        data,
        {
            "original_subdir",
            "generated_subdir",
            "max_iter",
            "write_scope_ignore_prefixes",
        },
        path,
    )
    result: dict[str, object] = {}
    if "original_subdir" in data:
        result["original_subdir"] = pipeline_mod._optional_str(
            data, "original_subdir", path
        )
    if "generated_subdir" in data:
        result["generated_subdir"] = pipeline_mod._optional_str(
            data, "generated_subdir", path
        )
    if "max_iter" in data:
        result["max_iter"] = pipeline_mod._optional_int(data, "max_iter", path, 0)
    if "write_scope_ignore_prefixes" in data:
        prefixes = pipeline_mod._string_list(
            data.get("write_scope_ignore_prefixes"),
            f"{path}.write_scope_ignore_prefixes",
        )
        result["write_scope_ignore_prefixes"] = prefixes or []
    return result


def _parse_lidskjalv_override(data: dict[str, object], path: str) -> dict[str, object]:
    pipeline_mod._reject_unknown_keys(
        data,
        {
            "skip_sonar",
            "sonar_wait_timeout_sec",
            "sonar_wait_poll_sec",
            "original",
            "generated",
        },
        path,
    )
    result: dict[str, object] = {}
    if "skip_sonar" in data:
        result["skip_sonar"] = pipeline_mod._optional_bool(
            data, "skip_sonar", path, False
        )
    if "sonar_wait_timeout_sec" in data:
        result["sonar_wait_timeout_sec"] = pipeline_mod._optional_int(
            data, "sonar_wait_timeout_sec", path, 0
        )
    if "sonar_wait_poll_sec" in data:
        result["sonar_wait_poll_sec"] = pipeline_mod._optional_int(
            data, "sonar_wait_poll_sec", path, 0
        )
    if "original" in data:
        result["original"] = _lidskjalv_target_to_document(
            _parse_lidskjalv_target_override(
                pipeline_mod._optional_mapping(data, "original", path),
                f"{path}.original",
            )
        )
    if "generated" in data:
        result["generated"] = _lidskjalv_target_to_document(
            _parse_lidskjalv_target_override(
                pipeline_mod._optional_mapping(data, "generated", path),
                f"{path}.generated",
            )
        )
    return result


def _parse_lidskjalv_target_override(
    data: dict[str, object], path: str
) -> LidskjalvTargetConfig:
    pipeline_mod._reject_unknown_keys(
        data, {"repo_subdir", "project_key", "project_name"}, path
    )
    return LidskjalvTargetConfig(
        repo_subdir=pipeline_mod._optional_str(data, "repo_subdir", path),
        project_key=pipeline_mod._optional_str(data, "project_key", path),
        project_name=pipeline_mod._optional_str(data, "project_name", path),
    )


def _resolve_path(raw_path: str, base_dir: Path) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _optional_resolve_path(raw_path: str | None, base_dir: Path) -> Path | None:
    if raw_path is None:
        return None
    return _resolve_path(raw_path, base_dir)


def _eitri_to_document(config: EitriConfig) -> dict[str, object]:
    return {
        "source_relpaths": list(config.source_relpaths),
        "parser_extension": config.parser_extension,
        "writer_extension": config.writer_extension,
        "verbose": config.verbose,
        "writers": config.writers,
    }


def _andvari_to_document(config: AndvariConfig) -> dict[str, object]:
    return {
        "gating_mode": config.gating_mode,
        "max_iter": config.max_iter,
        "max_gate_revisions": config.max_gate_revisions,
        "model_gate_timeout_sec": config.model_gate_timeout_sec,
    }


def _kvasir_to_document(config: KvasirConfig) -> dict[str, object]:
    return {
        "original_subdir": config.original_subdir,
        "generated_subdir": config.generated_subdir,
        "max_iter": config.max_iter,
        "write_scope_ignore_prefixes": list(config.write_scope_ignore_prefixes),
    }


def _lidskjalv_to_document(config: LidskjalvConfig) -> dict[str, object]:
    return {
        "skip_sonar": config.skip_sonar,
        "original": _lidskjalv_target_to_document(config.original),
        "generated": _lidskjalv_target_to_document(config.generated),
    }


def _lidskjalv_target_to_document(config: LidskjalvTargetConfig) -> dict[str, object]:
    return {
        "repo_subdir": config.repo_subdir,
        "project_key": config.project_key,
        "project_name": config.project_name,
    }


def _deep_merge(target: object, override: object) -> None:
    if not isinstance(target, dict) or not isinstance(override, dict):
        return
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_merge(target[key], value)
        else:
            target[key] = value
