# Lane A Checklist — Platform, Persistence, Provider, Operations

Branch: `agent/platform-foundation`

PR target: `integration/v0.1`

Owner: Platform developer agent

Last checkpoint: A1-A8 complete; final review fixes pushed in `2880fe3`.

Mark `[x]` only after implementation and tests are committed and pushed. For partial work, leave `[ ]` and add a `Progress:` note with commit and next action.

## Owned paths

```text
pyproject.toml
alembic.ini
migrations/**
src/gapforge/__init__.py
src/gapforge/config.py
src/gapforge/logging.py
src/gapforge/cli.py
src/gapforge/storage/**
src/gapforge/queue/**
src/gapforge/worker/**
src/gapforge/providers/**
src/gapforge/health/**
src/gapforge/backup/**
tests/platform/**
tests/conftest.py
Dockerfile
docker-compose.yml
.env.example
.dockerignore
.github/workflows/**
scripts/**
```

## A1 — Project foundation

- [x] Create Python 3.12 `src/` package and project metadata.
- [x] Configure Ruff, mypy, pytest, coverage, and console entrypoint `gap`.
- [x] Add typed settings with every budget/default from spec section 5.
- [x] Add structured logging and secret redaction.
- [x] Add focused tests for settings and logging.

Acceptance: clean install works; invalid settings fail early; logs never include configured secrets.

## A2 — Persistence and migrations

- [x] Implement all persistent entities from spec section 6.
- [x] Add constraints, indexes, opaque ID strategy, and append-only timestamps.
- [x] Add async engine/session and unit-of-work/repository seams.
- [x] Create the initial Alembic migration.
- [x] Test clean upgrade and important uniqueness/lineage constraints.

Acceptance: a fresh PostgreSQL database migrates without manual SQL and can persist the complete lineage skeleton.

## A3 — Durable queue and run control

- [x] Implement research task leases using `FOR UPDATE SKIP LOCKED`.
- [x] Implement idempotency keys, checkpoints, retry metadata, and lease expiry.
- [x] Enforce one global active run and two-call in-run semaphore.
- [x] Implement deterministic retry/error classification and run statuses.
- [x] Test crash reclaim, duplicate suppression, deadline, and partial completion.

Acceptance: a killed worker resumes from committed state without duplicating effects.

## A4 — Agent providers

- [x] Define stable `AgentProvider`, request, result, usage, and error contracts.
- [x] Implement deterministic `FakeAgentProvider`.
- [x] Implement real `CodexCliProvider` using subprocess argument arrays.
- [x] Enforce timeout, process-group termination, temporary workspace, environment allowlist, JSONL/output schema, and output bounds.
- [x] Disable shell, agents, MCP/plugins, writes, user/project configuration, and rules.
- [x] Detect Codex binary/version/auth and return `AUTH_REQUIRED` without retrying.
- [x] Test command injection resistance, secret exclusion, parsing, timeout, and one repair attempt.

Acceptance: untrusted prompt text cannot alter process arguments or inherit application secrets.

## A5 — CLI and health

- [x] Implement Typer command tree from spec section 19.
- [x] Implement stable JSON envelope and exit-code mapping.
- [x] Make JSON stdout clean and send diagnostics to stderr.
- [x] Implement functional mission lifecycle, run/task inspection, worker, health, and bounded read-only admin SQL commands.
- [x] Test JSON snapshots, errors, and SQL safeguards.

Acceptance: an agent can create/list/revise/activate/pause missions and inspect run state without SQL.

## A6 — Containers and authentication

- [x] Build non-root worker image with pinned Codex CLI.
- [x] Add PostgreSQL, worker, persistent DB volume, and dedicated `CODEX_HOME` volume to Compose.
- [x] Add migration/startup and healthcheck behavior.
- [x] Add documented device-auth helper and auth status checks.
- [x] Confirm no auth/source secret enters an image layer or Git.

Acceptance: Compose can bootstrap after one device login and restart without losing data or auth state.

## A7 — Backups and CI

- [x] Implement backup create/list/verify/restore and retention.
- [x] Add optional Compose backup profile and exclude Codex auth.
- [x] Test valid restore and corrupt archive detection using a temporary database.
- [x] Add GitHub Actions for Ruff, formatting, mypy, and pytest.
- [x] Add credential-gated platform smoke commands without running them in public CI.

Acceptance: CI is credential-free and a verified backup can restore the tested database state.

## A8 — PR handoff

- [x] Re-read `docs/spec-v0.1.md` and audit owned deliverables.
- [x] Run all available checks and record exact output in the PR.
- [x] Update this file's `Last checkpoint` and every completed box.
- [x] Push all commits and open/update a draft PR targeting `integration/v0.1`.
- [x] Document cross-lane integration needs without editing Lane B paths.

## Resume note

Current state: A1-A8 complete; draft PR #5 is ready for integration-lead review.

Last pushed follow-up commit: `2880fe3` (`fix queued deadlines and disaster restore`).

Next action: confirm final GitHub Actions results, then integration lead reviews PR #5. Worker
finalization/auth semantics remain tracked in integration I2; no Lane A checklist work remains.
