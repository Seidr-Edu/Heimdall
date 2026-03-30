#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

python_bin="python3"
if [[ -x ".venv/bin/python" ]]; then
  python_bin=".venv/bin/python"
fi

"$python_bin" - <<'PY'
import sys

if sys.version_info < (3, 12):
    raise SystemExit(
        "error: Heimdall tests require Python 3.12+. "
        f"Current interpreter: {sys.executable} ({sys.version.split()[0]})"
    )
PY

PYTHONPATH=src "$python_bin" -m unittest discover -s tests -p 'test_*.py'
