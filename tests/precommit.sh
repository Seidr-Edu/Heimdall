#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

bash tests/format.sh
bash tests/lint.sh
bash tests/typecheck.sh
bash tests/run.sh
