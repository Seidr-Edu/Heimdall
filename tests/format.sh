#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if command -v ruff >/dev/null 2>&1; then
  ruff format --check src tests
elif [[ -x ".venv/bin/ruff" ]]; then
  .venv/bin/ruff format --check src tests
else
  printf 'error: ruff is not installed; install it or create .venv/bin/ruff\n' >&2
  exit 1
fi
