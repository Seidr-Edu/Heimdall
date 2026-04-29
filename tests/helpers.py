from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path

from heimdall.simpleyaml import dumps

REPO_ROOT = Path(__file__).resolve().parents[1]
FAKES_DIR = REPO_ROOT / "tests" / "fakes"
DEFAULT_ANDVARI_NETWORK_NAME = "andvari-egress"
DEFAULT_ANDVARI_PROXY_URL = "http://proxy.internal:3128"
_PROXY_ACCESS_LOG_ENV = "HEIMDALL_ANDVARI_PROXY_ACCESS_LOG_PATH"


def make_temp_dir(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=prefix))


def build_pipeline_manifest(
    *,
    run_id: str = "20260312T120000Z__heimdall",
    skip_sonar: bool = True,
    images: dict[str, str] | None = None,
    eitri_writers: dict[str, object] | None = None,
    andvari_overrides: dict[str, object] | None = None,
) -> str:
    image_refs = {
        "brokk": "fake/brokk:1",
        "eitri": "fake/eitri:1",
        "andvari": "fake/andvari:1",
        "mimir": "fake/mimir:1",
        "kvasir": "fake/kvasir:1",
        "lidskjalv": "fake/lidskjalv:1",
    }
    if images:
        image_refs.update(images)
    andvari = {
        "gating_mode": "model",
        "max_iter": 8,
        "max_gate_revisions": 3,
        "model_gate_timeout_sec": 120,
    }
    if andvari_overrides:
        andvari.update(andvari_overrides)
    document = {
        "version": 1,
        "run_id": run_id,
        "source": {
            "repo_url": "https://github.com/example/demo-repo.git",
            "commit_sha": "0123456789abcdef0123456789abcdef01234567",
        },
        "images": image_refs,
        "eitri": {
            "source_relpaths": ["src/main/java", "shared/src/main/java"],
            "parser_extension": ".java",
            "writer_extension": ".puml",
            "verbose": True,
            "writers": eitri_writers
            or {
                "plantuml": {
                    "diagramName": "diagram",
                    "hidePrivate": True,
                }
            },
        },
        "andvari": andvari,
        "kvasir": {
            "original_subdir": "app",
            "generated_subdir": "generated/app",
            "max_iter": 5,
            "runner_timeout_sec": 7200,
            "write_scope_ignore_prefixes": ["completion/proof/logs", ".m2"],
        },
        "lidskjalv": {
            "skip_sonar": skip_sonar,
            "original": {
                "repo_subdir": "app",
            },
            "generated": {
                "repo_subdir": "generated/app",
            },
        },
    }
    return dumps(document)


def build_worker_config(
    *,
    queue_root: Path,
    runs_root: Path,
    codex_bin_dir: Path,
    codex_home_dir: Path,
    codex_host_bin_dir: Path | None = None,
    skip_sonar: bool = True,
    verbose: bool = False,
    andvari_internal_network_name: str = DEFAULT_ANDVARI_NETWORK_NAME,
    andvari_proxy_url: str = DEFAULT_ANDVARI_PROXY_URL,
) -> str:
    document: dict[str, object] = {
        "version": 1,
        "queue_root": str(queue_root),
        "runs_root": str(runs_root),
        "codex_bin_dir": str(codex_bin_dir),
        "codex_home_dir": str(codex_home_dir),
        "pull_policy": "if-missing",
        "verbose": verbose,
        "andvari_internal_network_name": andvari_internal_network_name,
        "andvari_proxy_url": andvari_proxy_url,
        "images": {
            "brokk": "fake/brokk:1",
            "eitri": "fake/eitri:1",
            "andvari": "fake/andvari:1",
            "mimir": "fake/mimir:1",
            "kvasir": "fake/kvasir:1",
            "lidskjalv": "fake/lidskjalv:1",
        },
        "eitri": {
            "source_relpaths": ["src/main/java", "shared/src/main/java"],
            "parser_extension": ".java",
            "writer_extension": ".puml",
            "verbose": True,
            "writers": {
                "plantuml": {
                    "diagramName": "diagram",
                    "hidePrivate": True,
                }
            },
        },
        "andvari": {
            "gating_mode": "model",
            "max_iter": 8,
            "max_gate_revisions": 3,
            "model_gate_timeout_sec": 120,
        },
        "kvasir": {
            "original_subdir": "app",
            "generated_subdir": "generated/app",
            "max_iter": 5,
            "runner_timeout_sec": 7200,
            "write_scope_ignore_prefixes": ["completion/proof/logs", ".m2"],
        },
        "lidskjalv": {
            "skip_sonar": skip_sonar,
            "original": {
                "repo_subdir": "app",
            },
            "generated": {
                "repo_subdir": "generated/app",
            },
        },
    }
    if codex_host_bin_dir is not None:
        document["codex_host_bin_dir"] = str(codex_host_bin_dir)
    return dumps(document)


def build_queue_request(
    *,
    repo_url: str = "https://github.com/example/demo-repo.git",
    commit_sha: str = "0123456789abcdef0123456789abcdef01234567",
    eitri: dict[str, object] | None = None,
    andvari: dict[str, object] | None = None,
    kvasir: dict[str, object] | None = None,
    lidskjalv: dict[str, object] | None = None,
) -> str:
    document: dict[str, object] = {
        "version": 1,
        "repo_url": repo_url,
        "commit_sha": commit_sha,
    }
    if eitri:
        document["eitri"] = eitri
    if andvari:
        document["andvari"] = andvari
    if kvasir:
        document["kvasir"] = kvasir
    if lidskjalv:
        document["lidskjalv"] = lidskjalv
    return dumps(document)


def write_file(path: Path, content: str, mode: int = 0o644) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    path.chmod(mode)
    return path


def install_fake_tools(root: Path) -> tuple[Path, Path, Path]:
    bin_dir = root / "provider" / "bin"
    home_dir = root / "provider" / "home"
    state_path = root / "fake-docker-state.json"
    bin_dir.mkdir(parents=True, exist_ok=True)
    home_dir.mkdir(parents=True, exist_ok=True)
    for tool_name in ("docker", "codex"):
        source = FAKES_DIR / tool_name
        target = bin_dir / tool_name
        shutil.copy2(source, target)
        target.chmod(0o755)
    state_path.write_text(
        json.dumps(
            {"available": {}, "commands": [], "runs": [], "next_seq": 1}, indent=2
        )
        + "\n"
    )
    return bin_dir, home_dir, state_path


def fake_env(
    bin_dir: Path, state_path: Path, *, extra: dict[str, str] | None = None
) -> dict[str, str]:
    env = os.environ.copy()
    pythonpath = str(REPO_ROOT / "src")
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        pythonpath
        if not existing_pythonpath
        else f"{pythonpath}{os.pathsep}{existing_pythonpath}"
    )
    env["PATH"] = f"{bin_dir}{os.pathsep}{env.get('PATH', '')}"
    env["FAKE_DOCKER_STATE"] = str(state_path)
    proxy_access_log = state_path.parent / "andvari-access.jsonl"
    proxy_access_log.parent.mkdir(parents=True, exist_ok=True)
    proxy_access_log.touch(exist_ok=True)
    env[_PROXY_ACCESS_LOG_ENV] = str(proxy_access_log)
    if extra:
        env.update(extra)
    return env


def with_default_andvari_runtime_args(args: list[str]) -> list[str]:
    if not args or args[0] not in {"run", "resume", "smoke-provider"}:
        return list(args)
    result = list(args)
    if "--andvari-internal-network-name" not in result:
        result.extend(["--andvari-internal-network-name", DEFAULT_ANDVARI_NETWORK_NAME])
    if "--andvari-proxy-url" not in result:
        result.extend(["--andvari-proxy-url", DEFAULT_ANDVARI_PROXY_URL])
    return result


def load_fake_state(state_path: Path) -> dict[str, object]:
    return json.loads(state_path.read_text(encoding="utf-8"))


def set_fake_image_id(state_path: Path, image_ref: str, image_id: str) -> None:
    state = load_fake_state(state_path)
    available = state.setdefault("available", {})
    available[image_ref] = image_id
    state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
