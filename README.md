# GapForge

GapForge v0.1 is a private, single-user, evidence-backed business-gap research harness. It
collects source evidence, extracts and clusters recurring pain, researches alternatives, builds
Evidence Cards, scores opportunities, applies an adversarial critic, and preserves the complete
PostgreSQL history. A healthy run may produce zero `VALIDATE` opportunities.

Python owns workflow state, budgets, retries, persistence, and all hard gates. Codex CLI performs
bounded semantic reasoning only. Raw source text is untrusted data and is never executed.

## Supported deployment

The production target is Ubuntu Server 24.04 LTS `amd64`, including an Ubuntu VM hosted by Hyper-V
on Windows Server 2019. Windows-native containers and Docker Desktop on Windows Server are not
supported. The Compose stack contains PostgreSQL 16, a non-root worker with pinned Codex CLI,
one-shot migrations, dedicated device-auth helpers, and an optional backup service. It exposes no
HTTP service.

Follow [the Ubuntu-on-Hyper-V deployment guide](docs/deployment-ubuntu-hyper-v.md) for host and VM
requirements, Docker installation, secure configuration, Codex device login, backup/restore,
upgrades, and operational limitations.

## Quick start

For credential-free infrastructure validation, use the fake provider:

```sh
cp .env.example .env
chmod 600 .env
# Replace the PostgreSQL password and AUTHOR_HMAC_KEY before continuing.
# Set AGENT_PROVIDER=fake for this credential-free check.
docker compose config --quiet
docker compose build worker
docker compose up -d
docker compose exec worker gap health --json
```

For normal background research, set `AGENT_PROVIDER=codex_cli`, authenticate only the dedicated
Compose volume, verify it, and restart the worker:

```sh
scripts/codex-auth
scripts/codex-status
docker compose restart worker
docker compose exec worker gap health --json
```

The checked-in `.env.example` contains development placeholders. Never deploy those values or
commit `.env`, source credentials, Codex auth data, reports, dumps, or backup archives.

## CLI-first workflow

Machine-facing commands use `--json`. JSON is written to stdout and diagnostics to stderr. Query
stored intelligence before scheduling new collection:

```sh
docker compose exec worker gap opportunity list --json
docker compose exec worker gap evidence list --json
docker compose exec worker gap changes --json
docker compose exec worker gap rejected --json
```

Create a natural-language mission, run a one-shot HUNT, and inspect the persisted run:

```sh
docker compose exec worker gap mission create \
  "Find recurring month-end close workflow pain" --output-locale en --json
docker compose exec worker gap hunt --mission <mission-id> --json
docker compose exec worker gap run show <run-id> --json
docker compose exec worker gap report run <run-id> --json
```

MONITOR is opt-in: mission creation never activates it. Product Hypotheses are also explicit; the
user must supply the proposition and request ID, and the referenced assessment must currently be
`VALIDATE` with an exact persisted final snapshot.

The canonical repository workflow is in
[`.agents/skills/business-gap/SKILL.md`](.agents/skills/business-gap/SKILL.md). PostgreSQL is the
source of truth; Markdown reports under `reports/` are deterministic derived artifacts.

## Operations

```sh
docker compose exec worker gap health --json
docker compose exec worker gap backup create --json
docker compose exec worker gap backup list --json
docker compose exec worker gap backup verify <backup-name> --json
```

Start scheduled daily backups with `docker compose --profile backup up -d backup`. Copy verified
archives off the VM; the Compose backup volume alone is not disaster recovery. Restore always
requires an explicit new target database and `--yes` confirmation. Codex auth is held in a separate
named volume and is excluded from backups.

## Security and v0.1 limits

- Private, single-user deployment; only one research run is active globally.
- No web UI, HTTP API, multi-user auth, embeddings, browser automation, Redis, or AI API.
- HN is credential-free. GitHub supports an optional token; Reddit OAuth and Brave search need
  credentials. Missing capabilities fail closed and cannot be promoted to `VALIDATE`.
- Static fetch handles bounded public HTTP(S) pages only. JavaScript, login, and CAPTCHA content is
  unavailable rather than browser-automated.
- Background Codex receives bounded structured evidence without database or source credentials and
  cannot use shell, agents, MCP, plugins, project writes, or unrelated configuration.
- Raw evidence is retained indefinitely by default. Operators are responsible for source terms,
  privacy obligations, VM access, host patching, and off-VM backups.
- The protected credentialed smoke has not run. `main` is intended to be the trusted ref but is
  currently unprotected; do not dispatch that workflow until branch protection and the release
  decision are recorded.

## Development and verification

Python 3.12+ and `uv` are required for local development:

```sh
uv sync --frozen --extra dev
uv run --frozen ruff check .
uv run --frozen ruff format --check .
uv run --frozen mypy src
uv run --frozen pytest -q
```

Public CI is credential-free. It runs the Python/PostgreSQL suite, an amd64 Compose
migration/health/persistence/backup/restore check, and a native arm64 image build. Explicit live
smoke commands are documented in [scripts/README.md](scripts/README.md); credentialed commands are
never part of public CI.

The canonical contract and reviewed delivery state are in
[docs/spec-v0.1.md](docs/spec-v0.1.md) and [docs/status-v0.1.md](docs/status-v0.1.md).
