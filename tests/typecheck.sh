#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if command -v mypy >/dev/null 2>&1; then
  mypy
elif [[ -x ".venv/bin/mypy" ]]; then
  .venv/bin/mypy
else
  printf 'error: mypy is not installed; install it or create .venv/bin/mypy\n' >&2
  exit 1
fi
