# Heimdall

Heimdall is the host-side orchestrator for the fixed pipeline graph:

1. `Brokk`
2. then in parallel: `Eitri`, `Lidskjalv(original)`
3. then `Andvari`
4. then in parallel: `Kvasir`, `Lidskjalv(generated)`

It uses the Docker CLI through `subprocess`, renders one service-specific
manifest per step, stores run state under `runs/<run_id>/`, and branches on
canonical service reports rather than container exit codes.

## Runtime model

- The public interface is a single `pipeline.yaml`.
- Host-specific paths such as the runs root and Codex mount directories are
  supplied by CLI flags, not by the public manifest.
- Image fingerprints use the resolved local Docker image ID, not only the
  configured image ref.
- Resume reuses only previously passed steps whose fingerprint still matches.

## CLI

```bash
python3 -m heimdall run /abs/path/pipeline.yaml \
  --runs-root /abs/path/runs \
  --codex-bin-dir /abs/path/provider/bin \
  --codex-home-dir /abs/path/provider/home \
  --verbose

python3 -m heimdall resume /abs/path/runs/<run_id> \
  --codex-bin-dir /abs/path/provider/bin \
  --codex-home-dir /abs/path/provider/home
```

`python3 -m heimdall.cli ...` works as well. After installation the console
entrypoint is `orchestrator`.

## Public manifest

See [examples/pipeline.example.yaml](/Users/oleremidahl/Documents/Master/Heimdall/examples/pipeline.example.yaml).

Key rules:

- `version` must be `1`
- `source.repo_url` must be a public GitHub HTTPS URL
- `source.commit_sha` must be a full 40-character lowercase SHA
- `images.*` accept any Docker image ref string, but production manifests should
  use immutable digests
- no public `provider.*` host-path block is allowed
- unknown top-level keys are rejected
- `eitri.writers` is passed through directly to Eitri, so nested keys must match
  Eitri's real PlantUML config schema such as `diagramName` or `hidePrivate`

## Run layout

Each run is written under `runs/<run_id>/`:

- `pipeline/manifest.yaml`
- `pipeline/resolved.yaml`
- `pipeline/state.json`
- `pipeline/artifact_index.json`
- `pipeline/outputs/run_report.json`
- `pipeline/outputs/summary.md`
- `pipeline/logs/<step>.log`
- `services/<step>/config/manifest.yaml`
- `services/<step>/run/...`

Service `run/` directories are created with mode `0777` so the service images
can write as `uid=10001` on both local hosts and the `munin` VPS user.

With `--verbose`, Heimdall prints preflight progress, step start/finish events,
and streams each container's combined stdout/stderr to the terminal with a step
prefix. The same output is also written to `pipeline/logs/<step>.log`.

## Development

The package metadata targets Python `3.14.x` and is pinned to the current
stable line in `pyproject.toml`. The local test suite uses only the standard
library plus fake `docker` and `codex` shims.

Run the full test suite:

```bash
bash tests/run.sh
```

Run linting locally:

```bash
bash tests/lint.sh
```

GitHub Actions runs both commands on pushes and pull requests via
`.github/workflows/ci.yml`.

## Eitri smoke

There is an opt-in real-container smoke test for the local Eitri service
wrapper:

```bash
bash tests/test_eitri_container_smoke.sh
```

That script:

- builds a local Eitri image from `/Users/oleremidahl/Documents/Master/Eitri`
- runs the service wrapper against a staged sample repo
- verifies `diagram.puml` and `run_report.json`

It skips immediately when the Docker daemon is unavailable.
