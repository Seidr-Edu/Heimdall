#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EITRI_ROOT="/Users/oleremidahl/Documents/Master/Eitri"
IMAGE_TAG="${EITRI_IMAGE_TAG:-eitri:heimdall-smoke}"

if ! docker version --format '{{.Server.Version}}' >/dev/null 2>&1; then
  echo "Skipping Eitri container smoke: Docker daemon unavailable." >&2
  exit 0
fi

tmp="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp"
}
trap cleanup EXIT

repo_dir="$tmp/repo"
run_dir="$tmp/run"
config_dir="$run_dir/config"

mkdir -p "$repo_dir/src/main/java/example" "$config_dir"
cat > "$repo_dir/src/main/java/example/Demo.java" <<'EOF'
package example;

public class Demo {
}
EOF

cat > "$config_dir/manifest.yaml" <<'EOF'
version: 1
run_id: eitri-smoke
source_relpaths:
  - src/main/java
parser_extension: .java
writer_extension: .puml
verbose: true
writers:
  plantuml:
    include_packages: true
EOF

chmod 0644 "$config_dir/manifest.yaml"
chmod 0777 "$run_dir"

docker build -t "$IMAGE_TAG" "$EITRI_ROOT"

docker run --rm \
  -e EITRI_MANIFEST=/run/config/manifest.yaml \
  -v "$repo_dir:/input/repo:ro" \
  -v "$config_dir:/run/config:ro" \
  -v "$run_dir:/run" \
  "$IMAGE_TAG"

python3 - <<'PY' "$run_dir"
from __future__ import annotations

import json
import sys
from pathlib import Path

run_dir = Path(sys.argv[1])
report_path = run_dir / "outputs" / "run_report.json"
diagram_path = run_dir / "artifacts" / "model" / "diagram.puml"

if not report_path.is_file():
    raise SystemExit("missing run_report.json")
if not diagram_path.is_file():
    raise SystemExit("missing diagram.puml")

report = json.loads(report_path.read_text(encoding="utf-8"))
if report.get("status") != "passed":
    raise SystemExit(f"unexpected Eitri smoke status: {report.get('status')!r}")
PY

echo "Eitri container smoke passed."
