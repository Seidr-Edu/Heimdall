# Heimdall

Heimdall is the host-side orchestrator for a fixed branch-serial DAG:

1. `Brokk`
2. then in parallel: `Eitri`, `Lidskjalv(original)`
3. then the best-effort generated branch:
   `Andvari -> Eitri(generated) -> Mimir` and `Andvari -> Kvasir -> Lidskjalv(generated)`
4. then the `v2` generated branch with the same shape
5. then the `v3` generated branch with the same shape

The best-effort, `v2`, and `v3` generated branches all live under the same
Heimdall run and share one `run_id`. Later branches wait only for the prior
branch to reach a terminal state; they do not require the prior branch to pass.

Within a branch, `Mimir` starts after `Eitri(generated*)` and compares the
original Eitri `model_snapshot.json` against that branch's generated
`model_snapshot.json`.
`Kvasir` and `Lidskjalv(generated*)` are independent of `Mimir`.

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

## Install

From the repo root, activate the virtualenv and install Heimdall in editable
mode:

```bash
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

That installs the `heimdall` and `orchestrator` console entrypoints into the
virtualenv so you can run Heimdall commands directly.

## CLI

```bash
heimdall run /abs/path/pipeline.yaml \
  --runs-root /abs/path/runs \
  --codex-bin-dir /abs/path/provider/bin \
  --codex-host-bin-dir /abs/path/host/provider/bin \
  --codex-home-dir /abs/path/provider/home \
  --andvari-internal-network-name andvari-egress \
  --verbose

heimdall resume /abs/path/runs/<run_id> \
  --codex-bin-dir /abs/path/provider/bin \
  --codex-host-bin-dir /abs/path/host/provider/bin \
  --codex-home-dir /abs/path/provider/home \
  --andvari-internal-network-name andvari-egress \
```

`python3 -m heimdall.cli ...` works as well. After installation the console
entrypoints are `heimdall` and `orchestrator`. If `--codex-host-bin-dir` is
omitted, Heimdall uses `--codex-bin-dir` for both host preflight and container
mounts.

Heimdall always routes `andvari`, `andvari-v2`, and `andvari-v3` through the
configured restricted Docker network. Provide the required runtime flag:

```bash
  --andvari-internal-network-name andvari-egress
```

Heimdall leaves every other step unchanged, attaches only the `andvari*` steps
to that Docker network, does not inject proxy environment variables, and
rewrites only the staged Andvari `config.toml` copy to disable GitHub tools.
It requires readable host-side egress logs at:

- `/var/log/squid/andvari-access.jsonl`
- `/var/log/andvari/blocked-egress.jsonl`

Heimdall writes the host-generated per-step request-log slice into:

- `<run_root>/pipeline/artifacts/proxy_access/andvari.jsonl`
- `<run_root>/pipeline/artifacts/proxy_access/andvari-v2.jsonl`
- `<run_root>/pipeline/artifacts/proxy_access/andvari-v3.jsonl`

It also writes the host-generated blocked-egress slice into:

- `<run_root>/pipeline/artifacts/egress_block/andvari.jsonl`
- `<run_root>/pipeline/artifacts/egress_block/andvari-v2.jsonl`
- `<run_root>/pipeline/artifacts/egress_block/andvari-v3.jsonl`

These egress slices are host-generated evidence artifacts, not service-produced
container outputs, so they intentionally live under `pipeline/` rather than
inside `services/<step>/run/artifacts/...`.

That does not by itself prove all outbound traffic is forced through Squid. The
actual "everything goes through the proxy" guarantee depends on the VPS-side
network enforcement described in
[docs/andvari_proxy_infra.md](docs/andvari_proxy_infra.md).

That guarantee should not depend on tools politely honoring `HTTP_PROXY`.
`andvari*` steps must be able to use normal outbound HTTP/S while the VPS
transparently intercepts and logs those requests; direct attempts such as raw
TCP, SSH, or direct DNS must be blocked by the VPS network policy. Heimdall
keeps only cheap global startup validation here; the source-log and
artifact-destination checks happen immediately before an `andvari*` step
launches, so egress logging problems fail the step before model execution
starts.

Heimdall stages a minimal provider seed only for `andvari*`. The staged seed
retains:

- `auth.json`
- `config.toml`
- `skills/.system/**`

It does not copy the full `CODEX_HOME` tree, so prior sessions, logs,
memories, caches, and temporary files are not carried into Andvari.

## Queue worker

Heimdall can also run as a long-lived VPS worker that owns a FIFO queue. The
queue uses YAML request/job records under `queue/`, while the canonical
pipeline outputs remain under `runs/<run_id>/`.

Each queued `(repo_url, commit_sha)` becomes one queue job and one Heimdall
run. The worker uses the allocated `job_id` as the pipeline `run_id`, so a
single repo/commit produces one run directory containing the best-effort, `v2`,
and `v3` branches together.

Worker config example:

- [examples/worker.example.yaml](examples/worker.example.yaml)
- [examples/heimdall-worker.service](examples/heimdall-worker.service)

The worker config requires the Andvari restricted-egress network setting:

- `andvari_internal_network_name: andvari-egress`

Heimdall assumes the host transparently intercepts and logs allowed/denied
HTTP/S traffic for the Andvari containers while separately logging blocked
bypasses. See [docs/andvari_proxy_infra.md](docs/andvari_proxy_infra.md) for the
VPS-side Squid and firewall requirements.

Submit one job from your local machine over SSH:

```bash
heimdall submit \
  --remote munin@example-vps \
  --remote-worker-config /srv/pipeline/worker.yaml \
  --remote-cli /home/munin/Heimdall/.venv/bin/heimdall \
  --repo-url https://github.com/example/demo-repo.git \
  --commit-sha 0123456789abcdef0123456789abcdef01234567 \
  --overrides /abs/path/to/overrides.yaml
```

If you submit to the same VPS regularly, set remote defaults once in your shell:

```bash
export HEIMDALL_REMOTE=seidr-munin
export HEIMDALL_REMOTE_WORKER_CONFIG=/srv/pipeline/worker.yaml
export HEIMDALL_REMOTE_CLI=/home/munin/Heimdall/.venv/bin/heimdall
```

Then the short forms work:

```bash
heimdall submit \
  --repo-url https://github.com/example/demo-repo.git \
  --commit-sha 0123456789abcdef0123456789abcdef01234567
```

Queue one job directly on the VPS:

```bash
cat request.yaml | heimdall enqueue \
  --worker-config /srv/pipeline/worker.yaml \
  --stdin
```

Run the worker once for testing:

```bash
heimdall worker \
  --worker-config /srv/pipeline/worker.yaml \
  --once
```

Run the long-lived worker under `systemd`:

```bash
sudo systemctl enable --now heimdall-worker
sudo journalctl -u heimdall-worker -f
```

Inspect job status locally or over SSH:

```bash
heimdall status \
  --worker-config /srv/pipeline/worker.yaml \
  20260314T120000Z__example_demo-repo__01234567

heimdall status \
  20260314T120000Z__example_demo-repo__01234567
```

The worker emits structured JSON log lines to stderr for operators, but
job state and run outcomes should be read from `queue/jobs/<job_id>/job.yaml`
and `runs/<run_id>/pipeline/outputs/run_report.json`, not from `journalctl`.

## Provider smoke

When you want to check whether `Andvari` and `Kvasir` can actually use your
host Codex install from inside their Linux service containers, run the provider
smoke command:

```bash
python3 -m heimdall.cli smoke-provider /abs/path/pipeline.yaml \
  --output-dir /abs/path/provider-smoke \
  --codex-bin-dir /abs/path/provider/bin \
  --codex-host-bin-dir /abs/path/host/provider/bin \
  --codex-home-dir /abs/path/provider/home \
  --andvari-internal-network-name andvari-egress \
  --verbose
```

This is especially useful on macOS, where host-side `codex login status` can
work while the Linux service containers still fail because:

- the mounted `codex` binary is a Mach-O executable instead of a Linux binary
- the staged auth seed copies into `/run/provider-state/codex-home`, but
  `codex login status` still fails inside the container

If you provision a Linux-only container bin on macOS, keep using your native
Mac Codex binary for host preflight via `--codex-host-bin-dir` and point
`--codex-bin-dir` at the Linux bundle that the containers should execute.

One workable macOS flow is:

```bash
mkdir -p /tmp/heimdall-codex-mac-bin
ln -sf "$(python3 -c 'import os, shutil; print(os.path.realpath(shutil.which("codex")))' )" \
  /tmp/heimdall-codex-mac-bin/codex

rm -rf /tmp/heimdall-codex-linux-bin
mkdir -p /tmp/heimdall-codex-linux-bin
docker run --rm \
  -v /tmp/heimdall-codex-linux-bin:/out \
  node:20-bookworm-slim \
  bash -lc '
    set -euo pipefail
    npm install -g @openai/codex >/tmp/npm-install.log 2>&1
    mkdir -p /out/lib/node_modules/@openai
    cp /usr/local/bin/node /out/node
    cp -R /usr/local/lib/node_modules/@openai/codex /out/lib/node_modules/@openai/codex
    cat > /out/codex <<'"'"'EOF'"'"'
#!/usr/bin/env bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$DIR/node" "$DIR/lib/node_modules/@openai/codex/bin/codex.js" "$@"
EOF
    chmod 755 /out/codex /out/node
  '

python3 -m heimdall.cli smoke-provider /abs/path/pipeline.yaml \
  --output-dir /tmp/heimdall-provider-smoke \
  --codex-bin-dir /tmp/heimdall-codex-linux-bin \
  --codex-host-bin-dir /tmp/heimdall-codex-mac-bin \
  --codex-home-dir "$HOME/.codex" \
  --andvari-internal-network-name andvari-egress \
  --verbose
```

If the smoke passes, use the same `--codex-bin-dir`, `--codex-host-bin-dir`,
and `--codex-home-dir` values for real `run` and `resume` commands.

The smoke command stages the provider bin/home the same way Heimdall does for a
real run, then probes `Andvari` and `Kvasir` separately. The probe now runs a
small `codex exec` task with the same container-safe flags used by the service
adapters, verifies it can read a mounted `/input` fixture, and verifies it can
write a result file into the mounted `/run/workspace`. It writes:

- `summary.json`
- `summary.md`
- `logs/andvari.log`
- `logs/kvasir.log`
- `artifacts/proxy_access/andvari.jsonl`
- `artifacts/egress_block/andvari.jsonl`

Those egress slices are also host-generated evidence, so smoke writes them
under the smoke output root instead of the Andvari service run directory.

The summary includes the host provider binary format and a classified failure
reason such as `provider-binary-incompatible-with-container`,
`codex-auth-unusable-in-container` (Codex only), or
`provider-exec-workspace-access-failed`.

For `smoke-provider`, Heimdall always applies the Andvari restricted-egress path
to the Andvari probe container. Heimdall also:

- rewrites only the staged Andvari `config.toml` copy to force
  `web_search = "disabled"`
- rewrites only the staged Andvari `config.toml` copy to force
  `plugins."github@openai-curated".enabled = false`
- attaches only the Andvari probe container to the configured Docker network
- verifies an allowed `curl https://example.com` and captures it in the Andvari
  request-log artifact
- verifies Maven dependency resolution works
- verifies Gradle dependency resolution works
- verifies `curl` to `github.com`, `api.github.com`, and
  `raw.githubusercontent.com` fails
- verifies `git ls-remote https://github.com/...` fails
- verifies a raw Python TCP connect to `github.com:22` fails and captures it in
  the blocked-egress artifact

Heimdall does not implement the network controls itself. The host must log both
allowed and denied HTTP/S requests to `/var/log/squid/andvari-access.jsonl`,
log blocked bypasses to `/var/log/andvari/blocked-egress.jsonl`, and block
direct bypasses such as raw TCP, SSH, and direct DNS from the
`andvari-egress` subnet. The proxy or egress policy must
deny at least the following effective destinations:

- `github.com`
- `api.github.com`
- `gist.github.com`
- `raw.githubusercontent.com`
- `codeload.github.com`
- `objects.githubusercontent.com`
- `*.githubusercontent.com`
- `*.githubassets.com`
- `ghcr.io`

For Squid `dstdomain` ACLs, that policy may be represented in a more compact,
Squid-safe form such as:

- `github.com`
- `.github.com`
- `ghcr.io`
- `.githubusercontent.com`
- `.githubassets.com`

If Maven, Gradle, or similar tools use HTTP(S) through Squid, that traffic
should also appear in the Squid log as allowed. If they bypass Squid entirely,
Squid will not see them; that is why the VPS-side egress enforcement matters.

## Local run

Heimdall reads Sonar credentials from the shell environment, not from the
public pipeline manifest. Keep them in a local `.env` file that is not
committed:

```bash
cd /abs/path/Heimdall

cat > .env <<'EOF'
SONAR_HOST_URL=https://sonarcloud.io
SONAR_TOKEN=replace-me
SONAR_ORGANIZATION=replace-me
EOF

chmod 600 .env
grep -qxF '.env' .git/info/exclude || printf '\n.env\n' >> .git/info/exclude
```

Linux:

```bash
cd /abs/path/Heimdall
set -a
. ./.env
set +a
export PYTHONPATH="$PWD/src"

python3 -m heimdall.cli run /abs/path/pipeline.yaml \
  --runs-root /abs/path/runs \
  --codex-bin-dir /abs/path/provider/bin \
  --codex-home-dir /abs/path/provider/home \
  --andvari-internal-network-name andvari-egress \
  --verbose
```

macOS:

```bash
cd /abs/path/Heimdall
set -a
. ./.env
set +a
export PYTHONPATH="$PWD/src"

python3 -m heimdall.cli run /abs/path/pipeline.yaml \
  --runs-root /abs/path/runs \
  --codex-bin-dir /abs/path/linux/provider/bin \
  --codex-host-bin-dir /abs/path/mac/provider/bin \
  --codex-home-dir /abs/path/provider/home \
  --andvari-internal-network-name andvari-egress \
  --verbose
```

## Public manifest

See [examples/pipeline.example.yaml](./examples/pipeline.example.yaml).

Key rules:

- `version` must be `1`
- `source.repo_url` must be a public GitHub HTTPS URL
- `source.commit_sha` must be a full 40-character lowercase SHA
- `images.*` accept any Docker image ref string, but production manifests should
  use immutable digests
- `images.mimir` is required and points to the Mimir comparison image
- no public `provider.*` host-path block is allowed
- do not put secrets such as `SONAR_TOKEN` in the manifest
- unknown top-level keys are rejected
- `eitri.writers` is passed through to original `eitri`, but Heimdall forces
  `writers.plantuml.generateDegradedDiagrams: false` for `eitri-generated*` so
  generated-repo Eitri runs still emit the base `diagram.puml` and
  `model_snapshot.json`, but not degraded diagrams
- nested `eitri.writers` keys must still match Eitri's real PlantUML config
  schema such as `diagramName`, `hidePrivate`, or `generateDegradedDiagrams`

## Run layout

Each run is written under `runs/<run_id>/`:

- `pipeline/manifest.yaml`
- `pipeline/resolved.yaml`
- `pipeline/state.json`
- `pipeline/artifact_index.json`
- `pipeline/outputs/run_report.json`
- `pipeline/outputs/summary.md`
- `pipeline/outputs/sonar_follow_up.json`
- `pipeline/logs/<step>.log`
- `services/<step>/config/manifest.yaml`
- `services/<step>/run/...`

For one queued repo/commit, the run contains service directories for:

- original/source steps: `brokk`, `eitri`, `lidskjalv-original`
- best-effort generated branch: `andvari`, `eitri-generated`, `mimir`, `kvasir`, `lidskjalv-generated`
- `v2` generated branch: `andvari-v2`, `eitri-generated-v2`, `mimir-v2`, `kvasir-v2`, `lidskjalv-generated-v2`
- `v3` generated branch: `andvari-v3`, `eitri-generated-v3`, `mimir-v3`, `kvasir-v3`, `lidskjalv-generated-v3`

The per-step container logs are all written under the same run at
`pipeline/logs/<step>.log`, for example `andvari.log`, `andvari-v2.log`, and
`mimir-v3.log`.

Service `run/` directories are created with mode `0777` so the service images
can write as `uid=10001` on both local hosts and the `munin` VPS user.

Pipeline reports may now include top-level `repository_stats` and
`diagram_comparisons` aggregated across `mimir`, `mimir-v2`, and `mimir-v3`.
The artifact index may include best-effort keys such as `mimir_report` and
`diagram_comparison_*`, plus suffixed branch keys such as `generated_repo_v2`,
`ported_tests_repo_v3`, `mimir_v2_report`, and
`lidskjalv_generated_v3_report`.

With `--verbose`, Heimdall prints preflight progress, step start/finish events,
and streams each container's combined stdout/stderr to the terminal with a step
prefix. The same output is also written to `pipeline/logs/<step>.log`.

## Development

The package metadata currently supports Python `>=3.12,<3.15` as declared in
`pyproject.toml`. The local test suite uses only the standard library plus fake
`docker` and `codex` shims.

Run the full test suite:

```bash
bash tests/run.sh
```

Run linting locally:

```bash
bash tests/lint.sh
```

Run formatting checks locally:

```bash
bash tests/format.sh
```

Run type checking locally:

```bash
bash tests/typecheck.sh
```

GitHub Actions runs lint, formatting, type checking, and tests on pushes and
pull requests via `.github/workflows/ci.yml`.

The lint baseline uses `ruff` with:

- `E4`, `E7`, `E9`
- `F`
- `I`
- `UP`
- `B`
- `SIM`

Type checking uses `mypy` against `src/heimdall` with:

- `no_implicit_optional`
- `check_untyped_defs`
- `disallow_untyped_defs`
- `disallow_incomplete_defs`
- `warn_unused_ignores`
- `warn_redundant_casts`
- `strict_equality`

## Eitri smoke

There is an opt-in real-container smoke test for the local Eitri service
wrapper:

```bash
bash tests/test_eitri_container_smoke.sh
```

That script:

- builds a local Eitri image from `/Users/oleremidahl/Documents/Master/Eitri`
- runs the service wrapper against a staged sample repo
- verifies `diagram.puml`, `model_snapshot.json`, and `run_report.json`

It skips immediately when the Docker daemon is unavailable.
