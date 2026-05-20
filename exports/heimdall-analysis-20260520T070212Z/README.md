# Heimdall Analysis Bundle

Primary tables are in `tables/` as both CSV and JSONL.

- `runs`: one row per pipeline run.
- `variants`: one row per run variant: original, generated, v2, v3; use status/reason columns for coverage.
- `sonar_projects`: one row per valid deduplicated Sonar project key.
- `mimir`: extracted Mimir comparison stats for real Mimir reports only.
- `kvasir`: extracted Kvasir behavioral/test-porting stats for real Kvasir reports only.

Use non-empty `variants.project_key` to join to `sonar_projects.project_key`.
Empty `variants.project_key` means there is no valid Sonar row for that observation.
Only original Sonar rows intentionally share project keys across Codex and Claude.
Generated, v2, and v3 rows are agent-specific Sonar submissions when project_key is present.
Raw reports are copied under `raw/` only when export runs with `--include-raw`.
