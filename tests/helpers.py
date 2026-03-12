from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path

from heimdall.simpleyaml import dumps

REPO_ROOT = Path(__file__).resolve().parents[1]
FAKES_DIR = REPO_ROOT / "tests" / "fakes"


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
            "write_scope_ignore_prefixes": ["completion/proof/logs", ".m2"],
        },
        "lidskjalv": {
            "skip_sonar": skip_sonar,
            "sonar_wait_timeout_sec": 300,
            "sonar_wait_poll_sec": 5,
            "original": {
                "repo_subdir": "app",
            },
            "generated": {
                "repo_subdir": "generated/app",
            },
        },
    }
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
    if extra:
        env.update(extra)
    return env


def load_fake_state(state_path: Path) -> dict[str, object]:
    return json.loads(state_path.read_text(encoding="utf-8"))


def set_fake_image_id(state_path: Path, image_ref: str, image_id: str) -> None:
    state = load_fake_state(state_path)
    available = state.setdefault("available", {})
    available[image_ref] = image_id
    state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
