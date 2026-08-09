# Integration Checklist — End-to-End Research Runtime

Branch: `integration/v0.1`

PR target: `main`

Owner: integration lead

GitHub issue: #7

Last checkpoint: Lane A, Lane B, I1 contract/schema reconciliation, I2 runtime admission, and
I4 provider admission/audit are reviewed and merged. I3 evidence orchestration is in progress.

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
- [ ] Register real worker handlers; remove the empty production handler registry.
- [x] Finalize runs only after their durable task graph is terminal, preserving warnings and checkpoints.
- [x] Map authentication and total/permanent handler failure to `AUTH_REQUIRED`/`FAILED`; use `COMPLETED_WITH_WARNINGS` only when useful partial work survived.
- [x] Test concurrent admission, duplicate scheduling, restart, lease loss, and deadline exhaustion.

Progress: PR #9 was squash-merged as `f30fc7b` after independent reproduction of 208 passed,
1 environment-gated skip, whole-repository Ruff/format, strict mypy, diff-check, and green
GitHub Python/container-backup jobs. The production registry deliberately remains empty and
fail-closed until I3 supplies the real `research.run` orchestrator; no fake handler was added.

Acceptance: a queued HUNT is structurally reachable by a worker and resumes safely after process
or host restart without duplicating committed effects.

## I3 — Evidence pipeline orchestration

- [ ] Query existing intelligence before scheduling new collection.
- [ ] Orchestrate query planning, time windows, source collection, normalization, revisions, and deduplication.
- [ ] Orchestrate pain extraction, independence-aware clustering, Evidence Cards, hypotheses, competitor research, scoring, critic, and bounded follow-up rounds.
- [ ] Persist deterministic duplicate groups before Evidence Card and trend calculations.
- [ ] Persist a deterministic checkpoint and idempotency key at every stage boundary.
- [ ] Isolate source failures as warnings and keep valid partial evidence.

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

- [ ] Persist raw evidence, revisions, pain signals, clusters, merge decisions, claims, cards, hypotheses, competitors, opportunities, assessments, snapshots, critic results, and lifecycle events.
- [ ] Keep global evidence identity separate from mission-revision relevance, score, and verdict.
- [ ] Complete opportunity, evidence, changes, rejections, merge, and report CLI commands over persisted data.
- [ ] Record reversible manual merge decisions with actor, reason, and lifecycle/event history.
- [ ] Make report identity, claims, scores, verdicts, citations, and locale derive from the same persisted snapshot.

Acceptance: evidence, scores, verdicts, rejections, changes, merges, and reopen events are queryable
without direct SQL, and every supported claim resolves to stored evidence.

## I6 — Reports and repository skill

- [ ] Wire Thai and English deterministic report renderers to `gap report` commands.
- [ ] Persist report artifacts atomically under the configured reports directory.
- [ ] Verify the canonical repository skill invokes only implemented CLI surfaces and follows up using persisted IDs/history.
- [ ] Keep Product Hypothesis creation explicit and available only after `VALIDATE`.

Acceptance: the fake-provider HUNT reaches a deterministic persisted report and the repository skill
can inspect it after a fresh process starts.

## I7 — Integrated verification

- [ ] Run Ruff, formatting, strict mypy, and the complete PostgreSQL-backed pytest suite from a clean install.
- [ ] Add a deterministic fake-provider end-to-end HUNT regression with lineage assertions.
- [ ] Test hard-gate refusal, zero-opportunity success, partial-source completion, and crash resume.
- [ ] Build both target architectures where available and validate Compose migration, health, persistence, auth volume, and backup restore.
- [ ] Run credential-free HN smoke; prove gated GitHub, Reddit, Brave, and Codex integrations fail gracefully without credentials.
- [ ] Load manual-smoke credentials from a protected post-checkout source and execute bounded real calls from a trusted ref; do not rely on an untracked `.env` surviving checkout.

Acceptance: all public CI checks are credential-free and every live integration remains an explicit,
non-public smoke command.

## I8 — Release handoff

- [ ] Re-read `docs/spec-v0.1.md` and audit every master acceptance item against code and test evidence.
- [ ] Update `docs/status-v0.1.md`, this checklist, README, and Ubuntu-on-Hyper-V deployment guide.
- [ ] Record exact commands/results, remaining limitations, and security boundaries in PR #4.
- [ ] Review the complete `integration/v0.1...main` diff before marking PR #4 ready.
- [ ] Merge only after the final integration review is clean and close the lane issues.

## Resume note

Current state: I1, the I2 runtime admission/finalization checkpoint, and I4 are reviewed and
merged. The I2 real handler-registration item remains open and is owned by I3; I3 and I5-I8
remain pending.

Exact next action: merge `26859dc` into the I3 branch, connect the reviewed semantic provider
boundary, register the real `research.run` handler and adapters, then review the bounded
follow-up, partial-source, budget, and crash-resume acceptance cases.
