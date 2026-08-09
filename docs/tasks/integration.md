# Integration Checklist — End-to-End Research Runtime

Branch: `integration/v0.1`

PR target: `main`

Owner: integration lead

GitHub issue: #7

Last checkpoint: Lane A, Lane B, and I1-I6 integration are reviewed and merged. I7 integrated
verification is active in draft PR #14.

Mark `[x]` only after implementation and tests are committed and pushed. For partial work,
leave `[ ]` and add a `Progress:` note with the commit and exact next action.

## I1 — Contract reconciliation

- [x] Map Lane B domain contracts to Lane A persistence models without importing SQLAlchemy into research modules.
- [x] Reconcile task status mapping (`QUEUED/COMPLETED` versus `PENDING/SUCCEEDED`) and duplicated run, provider, enum, ID, timestamp, checkpoint-cursor, and locale contracts explicitly.
- [x] Add the missing bounded `QUERY_PLAN` semantic operation and its explicit effort policy.
- [x] Add a migration for bounded pain frequency/severity, opportunity-scoped Evidence Cards, competitor research status, claim contradictions/citations, duplicate groups, lossless score components/pre-penalty score, constrained final verdicts, and AgentCall operation/schema identity.
- [x] Enable and index PostgreSQL `pg_trgm`/full-text candidate discovery required by the canonical deduplication order.
- [x] Resolve domain string revision IDs versus storage UUID revision IDs without losing lineage.
- [x] Add typed mapping tests that reject lossy or invalid conversions.
- [x] Narrow root ignore rules so source packages such as `src/gapforge/reports/` remain trackable.

Progress: PR #8 was squash-merged as `a5b36b1` after independent reproduction of 249 passed,
1 environment-gated skip, whole-repository Ruff/format, strict mypy, Alembic drift, diff-check,
and green GitHub Python/container-backup jobs. Pre-release databases containing legacy raw
revisions or competitor evidence are refused transactionally because their missing original
lineage cannot be reconstructed; export/reset is required rather than inventing identifiers.

Acceptance: every persisted research entity has one deliberate domain-to-storage mapping and no
research module depends on a database session.

## I2 — Run admission and durable task graph

- [x] Make `gap hunt` and `gap monitor --once` atomically create an idempotent root task with each queued run.
- [x] Promote eligible queued runs through `RunController` while preserving global RUNNING exclusivity.
- [x] Start the configured run-duration clock at the `QUEUED` to `RUNNING` transition, not while waiting in the queue.
- [x] Register real worker handlers; remove the empty production handler registry.
- [x] Finalize runs only after their durable task graph is terminal, preserving warnings and checkpoints.
- [x] Map authentication and total/permanent handler failure to `AUTH_REQUIRED`/`FAILED`; use `COMPLETED_WITH_WARNINGS` only when useful partial work survived.
- [x] Test concurrent admission, duplicate scheduling, restart, lease loss, and deadline exhaustion.

Progress: PR #9 was squash-merged as `f30fc7b` after independent reproduction of 208 passed,
1 environment-gated skip, whole-repository Ruff/format, strict mypy, diff-check, and green
GitHub Python/container-backup jobs. I3 PR #11 registered the production `research.run`
handler with one shared database/settings/worker identity and was squash-merged as `a491d40`.

Acceptance: a queued HUNT is structurally reachable by a worker and resumes safely after process
or host restart without duplicating committed effects.

## I3 — Evidence pipeline orchestration

- [x] Query existing intelligence before scheduling new collection.
- [x] Orchestrate query planning, time windows, source collection, normalization, revisions, and deduplication.
- [x] Orchestrate pain extraction, independence-aware clustering, Evidence Cards, hypotheses, competitor research, scoring, critic, and bounded follow-up rounds.
- [x] Persist deterministic duplicate groups before Evidence Card and trend calculations.
- [x] Persist a deterministic checkpoint and idempotency key at every stage boundary.
- [x] Isolate source failures as warnings and keep valid partial evidence.

Progress: PR #11 code checkpoint `1281ab2` implements the canonical R1 and coherent R2 pipeline,
production handler/collector/search/fetch wiring, deterministic target lineage, conservative
pre-call external reservations, per-source contract isolation, immutable source timestamps,
and Python-owned competitor capture IDs. It was independently reviewed and squash-merged as
`a491d40`. GitHub Quality run `31299658851` passed with 342 tests,
1 environment-gated skip, whole-repository Ruff/111-file format, strict mypy over 61 source
files, whitespace checks, and the 53-second container migration/health/backup/restore job.

Acceptance: one fake-provider HUNT executes the canonical pipeline with full evidence lineage and
may correctly return zero `VALIDATE` opportunities.

## I4 — Provider admission and audit

- [x] Adapt Lane B agent requests/results to the stable Lane A provider boundary.
- [x] Reserve persisted call budget and a durable provider-call lease before every Codex invocation.
- [x] Persist one `AgentCall` audit record per subprocess, including explicit repair calls.
- [x] Persist semantic operation and request/output schema identity for every agent call.
- [x] Enforce remaining run time, output schema, permitted evidence IDs, and no-retry auth failures.
- [x] Test repair, malformed output, parallel-call cap, stale lease, and secret exclusion end to end.

  Progress: PR #10 was independently reviewed and squash-merged as `26859dc` after exact
  reproduction of 285 passed/1 skipped, whole-repository Ruff/format, strict mypy, Alembic
  drift, diff-check, and green GitHub Python/container-backup jobs. The replay migration fails
  closed when pre-release
  `agent_calls` or `provider_call_leases` exist because their missing request/output identity
  cannot be reconstructed safely; stop workers, export if needed, and reset those pre-release
  tables before upgrading.

Acceptance: semantic work cannot bypass durable budgets, concurrency, deadlines, schema validation,
or provider audit history.

## I5 — Persistence and query surfaces

- [x] Persist raw evidence, revisions, pain signals, clusters, merge decisions, claims, cards, hypotheses, competitors, opportunities, assessments, snapshots, critic results, and lifecycle events.
- [x] Keep global evidence identity separate from mission-revision relevance, score, and verdict.
- [x] Complete opportunity, evidence, changes, rejections, merge, and report CLI commands over persisted data.
- [x] Record reversible manual merge decisions with actor, reason, and lifecycle/event history.
- [x] Make report identity, claims, scores, verdicts, citations, and locale derive from the same persisted snapshot.

Progress: PR #12 implementation checkpoint `ed7a008` completes typed R1/R2 artifact persistence
through the I3 same-transaction writer seam, append-only merge/lifecycle/final-snapshot journals,
global evidence and observed-at-bound competitor capture identity, bounded lineage validation,
and persisted opportunity/evidence/change/rejection/merge/report query surfaces. GitHub Actions run
`31300043062` passed 358 tests with 1 environment-gated skip, Ruff, formatting across 118 files,
strict mypy across 63 source files, whitespace checks, and the 58-second container migration,
health, backup, and restore job. The local Docker daemon remains unavailable because of the shared
host overlay2 I/O failure; GitHub PostgreSQL/Compose CI provides the completed database gate.

Acceptance: evidence, scores, verdicts, rejections, changes, merges, and reopen events are queryable
without direct SQL, and every supported claim resolves to stored evidence.

## I6 — Reports and repository skill

- [x] Wire Thai and English deterministic report renderers to `gap report` commands.
- [x] Persist report artifacts atomically under the configured reports directory.
- [x] Verify the canonical repository skill invokes only implemented CLI surfaces and follows up using persisted IDs/history.
- [x] Keep Product Hypothesis creation explicit and available only after `VALIDATE`.

Progress: PR #13 code checkpoint `6a32890` wires terminal Thai/English run reports and
on-demand opportunity reports to I5 query projections, atomically persists immutable dated run
artifacts plus a chronology-safe `latest.md`, and adds the explicit no-agent Product Hypothesis CLI.
Product Hypotheses use deterministic request identity, replay immutable content even after a later
MONITOR snapshot, and require a current `VALIDATE` assessment with its exact latest final-snapshot
Evidence Card for every new request. Migration `e5b1a6c02f9d` adds authoritative bounds, lineage,
locking, uniqueness, and downgrade/refusal tests; upgrading a pre-release database containing legacy
Product Hypotheses deliberately requires export/reset because request lineage cannot be invented.
GitHub Quality run `31301260341` passed whole-repository Ruff/format, strict mypy across 65 source
files, 377 PostgreSQL-backed tests with 1 skipped, whitespace checks, and the 53-second Compose
migration/health/backup/restore job. The canonical skill command guide is parser-valid against the
implemented CLI and documents persisted-ID/history follow-up, but the acceptance-level fake-provider
HUNT inspected through a fresh skill/CLI process is intentionally left unchecked for I7 rather than
replaced with synthetic seeding. The implementation was independently reviewed and PR #13 was
squash-merged as `55d546d`. PR #14 checkpoint `c9f47cb` completes that deferred acceptance with a
production-wired fake-provider HUNT and fresh executable CLI processes that render the persisted run
report, then follow its persisted opportunity/evidence IDs through `opportunity show`,
`evidence show`, and `changes`; the deterministic report bytes and `latest.md` survive process
restart.

Acceptance: the fake-provider HUNT reaches a deterministic persisted report and the repository skill
can inspect it after a fresh process starts.

## I7 — Integrated verification

- [x] Run Ruff, formatting, strict mypy, and the complete PostgreSQL-backed pytest suite from a clean install.
- [x] Add a deterministic fake-provider end-to-end HUNT regression with lineage assertions.
- [x] Test hard-gate refusal, zero-opportunity success, partial-source completion, and crash resume.
- [x] Build both target architectures where available and validate Compose migration, health, persistence, auth volume, and backup restore.
- [x] Run credential-free HN smoke; prove gated GitHub, Reddit, Brave, and Codex integrations fail gracefully without credentials.
- [ ] Load manual-smoke credentials from a protected post-checkout source and execute bounded real calls from a trusted ref; do not rely on an untracked `.env` surviving checkout.

Progress: Draft PR #14 checkpoint `c9f47cb` adds versioned, sanitized `gap-smoke` entrypoints;
strict protected external-env validation; isolated auth/database named-volume and restart checks;
amd64/arm64 pinned builds; and a production-wired fake-provider HUNT through RunScheduler,
RunController, Worker, audited semantic admission, PostgreSQL stage persistence, lifecycle/report
projections, and a fresh executable CLI process. The vertical matrix covers clean completion,
hard-gate refusal, zero opportunities, partial-source warnings, and real task-lease loss/reclaim from
a committed CARD_SCORE checkpoint without repeating earlier provider or external calls. A discovered
semantic-admission/heartbeat deadlock was fixed in `7512c99` by enforcing task-before-run locking and
is covered by a deterministic concurrent PostgreSQL regression. GitHub Quality run `31303250007`
passed Ruff, formatting across 131 files, strict mypy across 66 source files, 414 tests with 1
environment-gated skip, the 22-second native arm64 build, and the 3-minute Compose migration,
health, persistence, backup, and restore job. The manual credential-free command
`uv run --frozen gap-smoke hacker-news --query 'manual workflow pain' --json` returned AVAILABLE
with exactly 1 request and 5 items; `uv run --frozen gap-smoke missing-credentials --json` returned
the expected zero-request GitHub CREDENTIAL_MISSING, Reddit SOURCE_UNAVAILABLE, Brave
RESEARCH_UNAVAILABLE, and isolated Codex AUTH_REQUIRED outcomes. Protected post-checkout credential
loading and bounded real source/Codex commands are implemented, but no trusted credentialed
execution record exists yet, so the final manual-smoke item remains deliberately unchecked.
The integration lead independently reviewed the full diff and green head `a9b8112`; PR #14 was
squash-merged into `integration/v0.1` as `fb44e06`.

Acceptance: all public CI checks are credential-free and every live integration remains an explicit,
non-public smoke command.

## I8 — Release handoff

- [x] Re-read `docs/spec-v0.1.md` and audit every master acceptance item against code and test evidence.
- [x] Update `docs/status-v0.1.md`, this checklist, README, and Ubuntu-on-Hyper-V deployment guide.
- [x] Record exact commands/results, remaining limitations, and security boundaries in PR #4.
- [x] Review the complete `integration/v0.1...main` diff before marking PR #4 ready.
- [ ] Merge only after the final integration review is clean and close the lane issues.

Progress: PR #15 was independently reviewed and squash-merged as `21bef48`; docs-head Quality run
`31304363528` is green with Ruff, formatting across 132 files, strict mypy across 66 source files,
414 tests with 1 environment-gated skip, Compose migration/health/persistence/backup/restore, and
native arm64. The integration lead updated the master checklist, status, PR #4 evidence, and enabled
strict `main` branch protection. The final audit covered 148 changed files, linear Alembic history,
the locked dependency/container/workflow surfaces, provider/network boundaries, generated/secret
hygiene, and full-range whitespace; PR-event run `31304612051` passed all required jobs at
`e93791f`. Live Codex skill-following, protected credentialed source/Codex execution, and merge
remain deliberately open.

## Resume note

Current state: I8 documentation PR #15 is reviewed and merged as `21bef48`; the integration lead has
applied the evidence-backed master/status checkpoint and exact PR #4 handoff. `main` now has strict
required Quality checks and PR-only protection. The independent full-diff review is complete with
no blocking finding. The protected credentialed live-call execution record and final merge remain
pending.

Exact next action: wait for the final review-checkpoint Quality run, then mark PR #4 ready and merge
only if all required checks stay green. Credentialed dispatch remains prohibited until the required
self-hosted runner/environment/auth prerequisites exist; capture any later bounded smoke in a
follow-up checkpoint/PR.
