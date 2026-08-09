# GapForge v0.1 Execution Specification

Status: approved for implementation

Target: private, single-user deployment

Primary interactive interface: Codex CLI or Claude Code in this repository

Primary background reasoner: Codex CLI

Progress owner: integration lead

This is both the canonical product contract and the master acceptance checklist. Developers update only their lane checklist under `docs/tasks/`; the integration lead marks this master list after reviewing commits, tests, and PRs.

## 1. Product contract

GapForge is an agent-native, evidence-backed business-gap research harness. It collects persistent evidence, extracts and clusters pains, researches competitors and alternatives, forms falsifiable gap hypotheses, scores opportunities, applies an adversarial critic, and preserves history.

It is not a startup-idea generator. A successful run may return zero `VALIDATE` opportunities. Python owns deterministic work, state, budgets, persistence, and orchestration. Codex performs bounded semantic reasoning only.

### Master product checklist

- [x] Thai or English natural-language missions persist without hand-written YAML.
- [x] Existing intelligence is queried before new collection.
- [x] HUNT completes end to end with evidence lineage.
- [x] MONITOR is opt-in and resumes safely after restart.
- [x] ASK helpers inspect evidence, comparisons, changes, and rejections without SQL.
- [x] Every supported factual conclusion resolves to stored evidence.
- [x] No Evidence Card or failed hard gate can produce `VALIDATE`.
- [x] Product Hypotheses are explicit, post-`VALIDATE` requests only.

## 2. Scope boundaries

v0.1 excludes a web UI, HTTP API, multi-user auth, billing, Redis, Kubernetes, embeddings, vector infrastructure, browser automation, nested agents, AI model APIs, automatic Product Hypotheses, semantic auto-merge, and a production Claude background provider.

- [x] No out-of-scope runtime dependency or service is introduced.
- [x] `CodexCliProvider` and `FakeAgentProvider` are the only agent implementations.
- [x] Claude compatibility is limited to repository instructions/skill invoking `gap`.

## 3. Deployment and technology

Production runs in Ubuntu Server 24.04 LTS `amd64`. Windows Server 2019 is supported as a Hyper-V host for the Linux VM. macOS `arm64` with Docker Desktop is the development target. Windows-native containers are not supported.

The Compose stack contains a non-root worker image with Python and pinned Codex CLI, plus PostgreSQL 16 with a named volume. An optional backup profile is permitted. No HTTP service is present.

Technology: Python 3.12+, SQLAlchemy 2, Alembic, PostgreSQL, Pydantic 2, pydantic-settings, Typer, httpx, pytest/pytest-asyncio, Ruff, and mypy.

- [x] Docker image builds reproducibly.
- [x] Compose starts, migrates, healthchecks, and persists PostgreSQL data.
- [ ] Ubuntu-on-Hyper-V setup and limitations are documented.

## 4. Architecture

```text
Interactive Codex / Claude Code
             |
      repository skill
             |
       `gap` CLI (JSON)
             |
 Python modular monolith
 | missions | queue | collectors | analysis |
 | scoring  | critic | reports   | backups  |
             |
         PostgreSQL

Background:
durable worker -> bounded CodexCliProvider -> schema-validated result
```

Only Python connects to PostgreSQL and source/search APIs. Codex receives bounded evidence batches without database or source credentials.

- [x] Package uses `src/gapforge/` with clean module boundaries.
- [x] Research modules do not depend directly on SQLAlchemy sessions.
- [x] Worker tasks are durable, idempotent, leased, and checkpointed.
- [x] Machine-facing contracts are versioned and schema validated.

## 5. Configuration and budgets

```env
AGENT_PROVIDER=codex_cli
CODEX_MODEL=
RUN_INTERVAL_HOURS=12
MAX_RESEARCH_ROUNDS=2
MAX_AGENT_CALLS_PER_RUN=6
MAX_PARALLEL_AGENT_CALLS=2
MAX_RUN_DURATION_MINUTES=30
MAX_COLLECTOR_REQUESTS_PER_RUN=60
MAX_SEARCH_CALLS_PER_RUN=20
MAX_RAW_SIGNALS_PER_RUN=300
RAW_SIGNAL_RETENTION_DAYS=0
INITIAL_LOOKBACK_DAYS=365
MONITOR_OVERLAP_HOURS=24
REJECTED_REOPEN_COOLDOWN_DAYS=30
```

An empty `CODEX_MODEL` inherits the authenticated CLI default. Reasoning effort is `low` for extraction/relevance, `medium` for clustering/hypotheses/gap/critic, and `high` only for explicit deep research.

- [x] Every hard limit is validated, persisted, and tested.
- [x] A hit limit checkpoints as `BUDGET_EXHAUSTED` without losing work.
- [x] Codex output cannot create extra calls or rounds.
- [x] Provider, model request, effort, CLI version, timing, and status are recorded.

## 6. Persistent domain model

Required entities:

- `ResearchMission`, `MissionRevision`
- `ResearchRun`, `ResearchTask`, `SourceCheckpoint`
- `RawSignal`, `RawSignalRevision`, `PainSignal`
- `CanonicalProblem`, `ProblemCluster`, `ProblemClusterMembership`, `MergeCandidate`
- `EvidenceCard`, `AtomicClaim`, `ProblemHypothesis`
- `Competitor`, `CompetitorEvidence`, `GapHypothesis`
- `Opportunity`, `MissionOpportunityAssessment`, `OpportunityScoreSnapshot`
- `CriticResult`, `ProductHypothesis`, `LifecycleEvent`, `AgentCall`

Mission IDs are stable; revisions are immutable. Evidence and canonical entities are global. Mission relevance, score, and verdict belong to mission-revision assessments.

```text
ProductHypothesis
 -> MissionOpportunityAssessment
 -> Opportunity -> GapHypothesis -> CanonicalProblem
 -> ProblemClusterMembership -> PainSignal
 -> RawSignalRevision -> source URL
```

- [x] All entities have keys, timestamps, constraints, and indexes.
- [x] Every run references its exact immutable mission revision.
- [x] Scores, claims, revisions, and lifecycle events are append-only.
- [x] Alembic upgrades a clean database and constraints are tested.
- [x] Repository/unit-of-work interfaces prevent routine hand-written SQL.

## 7. Mission, run, and queue state

Mission lifecycle: `DRAFT`, `ACTIVE`, `PAUSED`, `ARCHIVED`. Creation never activates MONITOR. HUNT is one-shot; activation is explicit.

Run statuses: `QUEUED`, `RUNNING`, `COMPLETED`, `COMPLETED_WITH_WARNINGS`, `BUDGET_EXHAUSTED`, `AUTH_REQUIRED`, `FAILED`, `CANCELLED`.

Only one research run is active globally. At most two Codex calls run concurrently inside it. PostgreSQL leases and `FOR UPDATE SKIP LOCKED` drive the queue. Manual HUNT has priority but does not interrupt a commit.

- [x] Mission revisions record parent and change reason.
- [x] Activation, pause, archive, and HUNT are distinct.
- [x] Expired leases are reclaimable without duplicate effects.
- [x] One mission revision cannot have overlapping runs.
- [x] Crash recovery resumes from the last committed checkpoint.

## 8. Collectors and raw evidence

`Collector.collect(CollectRequest) -> CollectResult` is async and returns normalized items, a checkpoint, request count, warnings, and structured availability.

Raw evidence stores source/external ID, canonical URL, parent thread, author identity input, title/body, original language, source/collection/edit/delete timestamps, engagement, metadata, content hash, and normalization version.

- Hacker News uses a public API without credentials.
- GitHub uses the official API with optional `GITHUB_TOKEN`; PRs, bots, templates, and generic bugs are not promoted automatically.
- Reddit uses official OAuth credentials. BuildRadar may later be a fallback. No scraping, browser automation, or public-JSON workaround.

Raw text/metadata are retained indefinitely by default without media. Edits create revisions; deletions create tombstones. The 300-signal budget starts at 100 per source; no source exceeds 50%. A thread contributes its parent and at most 20 comments, with diminishing weight after item five.

- [x] All three collectors implement one contract.
- [x] Missing credentials return `SOURCE_UNAVAILABLE`.
- [x] Checkpoints, pagination, timeouts, limits, and retries are tested.
- [x] Source/thread quotas prevent demand inflation.
- [x] Content revisions and tombstones preserve history.

## 9. Search and static fetch

`BraveSearchProvider` is the v0.1 non-AI search implementation. If unavailable, pain discovery continues but competitor/gap research is `RESEARCH_UNAVAILABLE` and cannot reach `VALIDATE`.

Competitor URLs originate only from search results or explicit user input. Static fetching permits `http`/`https`, blocks private/loopback/link-local/metadata networks and DNS rebinding, bounds redirects/body/time, validates content types, and extracts visible text. Search snippets alone cannot support price or feature claims. JavaScript/login/CAPTCHA content is `CONTENT_UNAVAILABLE`.

- [x] Brave results normalize into evidence records.
- [x] Static fetch prevents SSRF and bounds redirects, size, type, and time.
- [x] Unavailable search blocks `VALIDATE` but not collection.
- [x] Agent-created URLs are rejected.

## 10. Query planning and time sampling

Codex produces semantic `QueryIntent` objects. Python compiles source syntax, adds deterministic behavioral patterns and negative filters, deduplicates, persists rationale/yield, and enforces budgets.

Limits: eight broad intents, four targeted intents per opportunity batch, two research rounds. Initial HUNT samples a 365-day lookback: 35% from 0–30 days, 25% from 31–90, 20% from 91–180, and 20% from 181–365. Emerging missions default to 90 days. MONITOR uses the last successful watermark with a 24-hour overlap.

- [x] Codex outputs intents rather than uncontrolled requests.
- [x] Python compilers enforce syntax, caps, and deduplication.
- [x] Low-yield history changes later priority deterministically.
- [x] Time stratification and monitor overlap are tested.

## 11. Deduplication, identity, and clustering

Process order: source/external ID, normalized URL, normalized text hash, MinHash near-duplicates, then PostgreSQL FTS/`pg_trgm` candidates. v0.1 has no embeddings.

Exact duplicates merge automatically. Semantic similarity creates reversible `MergeCandidate` records only; background work never performs a semantic canonical merge.

One PainSignal remains `UNCLUSTERED`. A provisional cluster needs two similar signals from different known authors, or one unusually specific behavioral/spend signal. An Opportunity normally needs three known authors and two threads. One explicit-spend signal may create only `RESEARCH_MORE`. A cluster with no growth for 90 days becomes `DORMANT`.

Authors use HMAC of source and normalized identity. Unknown/deleted/anonymous and bot/service accounts do not increase independent-user counts. Cross-source identities are not linked.

- [x] Exact, URL, hash, and near-duplicate cases are deterministic.
- [x] Pseudonyms are stable without exposing usernames by default.
- [x] Unknown/bot authors cannot satisfy independence gates.
- [x] Semantic candidates never auto-merge; accepted merges are reversible/audited.
- [x] Cluster and opportunity creation thresholds are enforced.

## 12. Claims and Evidence Cards

Pain extraction yields structured pain, user/context, JTBD, severity/frequency, workaround, existing solution, switching/payment/urgency/emotion signals, confidence, excerpt, and source ID. No fabricated context.

Evidence Cards store independent authors/threads/sources, time diversity, recency, severity, behavioral/paid workarounds, WTP, supporting and contradicting claims, representative IDs, confidence, and missing evidence.

Factual conclusions are atomic claims with status `SUPPORTED`, `HYPOTHESIS`, `UNKNOWN`, `INSUFFICIENT_EVIDENCE`, or `RESEARCH_UNAVAILABLE`. `SUPPORTED` requires validated evidence IDs. Price/feature claims require URL, captured excerpt, and observation time. Invalid IDs, URLs, enums, or schemas reject the output; one repair retry is allowed within budget.

- [x] Extraction schemas preserve evidence IDs and prohibit unsupported context.
- [x] Evidence Card independence/diversity metrics are deterministic.
- [x] Claim validation rejects invented IDs, URLs, prices, and statuses.
- [x] Contradicting evidence is first-class.
- [x] No Evidence Card can produce `VALIDATE`.

## 13. Hypotheses and competitors

A Problem Hypothesis is falsifiable and specifies ICP, JTBD, trigger, current behavior, pain, workflow failure, support, and contradictions.

Competitors include SaaS/apps, spreadsheets, manual work, employees, agencies, internal scripts, open source, platform features, and doing nothing. No obvious SaaS does not imply no competition.

A Gap Hypothesis requires both user evidence and competitor/alternative evidence. Gap types include workflow, integration, UX, price, segment, localization, trust, automation, privacy, collaboration, distribution, complexity, speed, mobile, and business model.

- [x] Problem hypotheses are falsifiable and evidence-linked.
- [x] Alternatives include non-software and doing-nothing behavior.
- [x] Gap creation requires both user and competitor evidence.
- [x] Unsupported price, features, revenue, market size, or traction are never facts.

## 14. Scoring and hard gates

`EvidenceStrength` covers severity, frequency, independent diversity, behavioral workaround, WTP/spend, and recency/trend. `OpportunityFit` covers gap strength, competitor dissatisfaction, reachability, technical/small-team feasibility, inverse switching friction, and why-now support.

The pre-penalty score is `sqrt(EvidenceStrength * OpportunityFit)`. Penalties cover concentration, duplicates, low specificity, hypothetical demand, missing workaround/payment, saturation, unclear ICP, and unavailable research. Each snapshot stores raw metrics, components, weights, penalties, algorithm version, final score, confidence, and explanation data.

All `VALIDATE` gates must pass:

```text
unique known authors     >= 5
independent threads      >= 3
user-evidence sources    >= 2
behavior/workaround      >= 1
WTP or existing spend    >= 1
competitor research      = COMPLETE
gap evidence             = PRESENT
fatal flags              = 0
overall score            >= 70
evidence confidence      >= 0.65
critic verdict           = VALIDATE
critic confidence        >= 0.70
```

- [x] Both axes and all components/penalties are explainable and versioned.
- [x] Geometric mean prevents one strong axis hiding a weak one.
- [x] Every hard gate is independently tested.
- [x] Scores cannot bypass hard gates.
- [x] Historical snapshots are never overwritten.

## 15. Critic, research loop, lifecycle, and trend

The critic is a separate Codex call and does not see the prior verdict, promotional prose, or Product Hypothesis. It returns exactly `REJECT`, `RESEARCH_MORE`, or `VALIDATE`, plus confidence, fatal flags, weak assumptions, contradictions, missing evidence, recommended intents, and summary.

Lifecycle is `DISCOVERED -> RESEARCHING -> RESEARCH_MORE -> VALIDATE` or `REJECTED`. Every transition is an event.

MONITOR may reopen `REJECTED` to `RESEARCH_MORE`, never directly to `VALIDATE`, after three new independent users, a new source, first WTP/spend evidence, a material competitor change, or a qualified trend. Default cooldown is 30 days except new WTP.

Trend compares seven current days with the preceding 28, normalized by examined source volume and smoothed. `RISING` needs five signals, three known authors, and two threads. Insufficient data is not `FLAT`.

- [x] Critic input is blind to prior verdict and product pitch.
- [x] Critic schema and citations are validated.
- [x] Research-more cannot exceed rounds or budget.
- [x] Reopen triggers, cooldown, event history, and score deltas are tested.
- [x] A viral thread cannot create `RISING` or reopen alone.

## 16. Codex provider and credentials

`AgentProvider.run(AgentRequest) -> AgentResult` is the stable seam. `CodexCliProvider` uses an argument array, never shell concatenation, with timeout, temporary directory, environment allowlist, JSONL capture, output schema, bounded stdout/stderr, and process-group termination.

Background invocations are ephemeral, ignore user/project configuration and rules, disable shell and multi-agent tools, load no MCP/plugins, have no writable project, and inherit no database/search/collector secrets.

Docker uses a dedicated named `CODEX_HOME` volume and file credential store. First login uses device auth. The volume is writable only for refresh, owned by the non-root worker, never baked into images or backups, and is not the host's entire `~/.codex`. Missing/expired auth returns `AUTH_REQUIRED` without retries.

- [x] Command construction cannot execute untrusted shell text.
- [x] Shell, agents, MCP/plugins, writes, and unrelated config are disabled.
- [x] Environment allowlist excludes application/source secrets.
- [x] Timeout, termination, output limits, parsing, and repair retry are tested.
- [x] Device login, status, expiry, and reauthentication are documented.

## 17. Prompt injection and network security

External text remains evidence and is not deleted because it resembles instructions. It is sent as bounded structured JSON with immutable IDs and explicit data boundaries. The security model is isolation, least capability, and validation.

- [x] Collected instructions are never executed.
- [x] Codex cannot receive application/source credentials.
- [x] Citation and URL allowlists are validated before persistence/fetch.
- [x] Fetching cannot reach internal, metadata, loopback, or link-local networks.
- [x] Credentials and authorization headers are redacted from logs/errors.

## 18. Retry and partial-failure behavior

Transient network/rate-limit errors retry at most three times with exponential backoff and jitter. Invalid Codex output gets one repair attempt. Auth failures do not retry. One collector failure does not stop others. Unavailable search prevents `VALIDATE` but not collection. Database migration/integrity failures stop immediately. No retry crosses a hard budget or deadline.

Partial success is `COMPLETED_WITH_WARNINGS`. All stages are idempotent.

- [x] Error classes map deterministically to retry/no-retry behavior.
- [x] Collector and search isolation is tested.
- [x] Partial work persists with warnings.
- [x] No backoff or subprocess outlives the run deadline.

## 19. CLI, language, and reports

Required command families:

```text
gap mission create|list|show|revise|activate|pause
gap hunt --mission <id>
gap monitor --once
gap worker
gap run list|show
gap opportunity list|show|compare
gap evidence list|show
gap changes
gap rejected
gap merge-candidate list|accept|reject
gap report run|opportunity
gap health
gap backup create|list|verify|restore
gap admin sql --read-only
```

Important commands support `--json`. The stable envelope contains `schema_version`, `command`, `data`, `warnings`, and `error`. JSON goes only to stdout; diagnostics go to stderr. Admin SQL accepts one bounded, timed `SELECT` and is never the normal skill path.

Keys, enums, and canonical summaries are English. Original evidence/language is preserved. Mission `output_locale` controls presentation; Thai missions default to Thai. Original quotations remain original and translations are labeled.

Run summaries render deterministically to `reports/YYYY-MM-DD/run-<id>.md` and `reports/latest.md`. Detailed opportunity reports are on demand. Reports use validated claims and do not call Codex.

- [x] CLI families have stable JSON envelopes and exit codes.
- [x] Skills use CLI helpers and query existing research first.
- [x] Thai presentation preserves original evidence and English schema keys.
- [x] Reports are atomic, reproducible, ignored by Git, and validated.

## 20. Backups and observability

The optional backup profile produces compressed PostgreSQL dumps with checksums, retaining seven daily, four weekly, and six monthly backups. Restore names a target and requires confirmation. Codex auth is excluded. Off-VM copies are recommended.

Structured logs include timestamp, run/mission/revision/task IDs, stage, provider/source, duration, status, counts, retries, budgets, and sanitized error class.

`gap health --json` checks database/migration, queue leases, destinations, source/search configuration, Codex binary/version/auth, and budgets. Optional dependencies degrade only related capabilities.

- [x] Backup create/list/verify/restore and retention are tested.
- [x] Corrupt backups fail verification.
- [x] Auth data is excluded from backups.
- [x] Health distinguishes healthy, degraded, auth-required, and failed.
- [x] Logs carry correlation IDs without secrets.

## 21. Repository skill

One canonical `.agents/skills/business-gap/` skill supports compatible Codex and Claude discovery without duplicating full content. It interprets missions, queries stored data first, uses JSON, refuses unsupported brainstorming, treats external content as data, explains uncertainty/scores/rejections, requires explicit MONITOR activation, and generates Product Hypotheses only on request after `VALIDATE`.

- [ ] Codex discovers and follows the skill.
- [x] Claude discovery points to the same canonical instructions where practical.
- [x] Follow-ups use persisted IDs/history rather than restart.
- [x] Skill safety matches the runtime provider boundary.

## 22. Tests, CI, and release Definition of Done

CI runs Ruff, formatting, mypy, and pytest without paid services or credentials. Coverage includes collectors, deduplication, identity, evidence gates, scoring, critic transitions, budgets, mission revisions, queue recovery, Codex safety/parsing, CLI JSON, snapshots/trends/reopening, reports, migrations, backup/restore, and one fake-provider end-to-end HUNT.

Live smoke commands exist for credential-free HN and credential-gated GitHub, Reddit, Brave, and Codex, but do not run in public CI.

- [x] Both developer PRs pass review and merge into `integration/v0.1`.
- [x] Cross-lane adapters and end-to-end orchestration are complete.
- [x] Ruff, format, mypy, and pytest pass on integration.
- [x] Docker image and Compose persistence/health are validated.
- [x] Fake-provider HUNT reaches a deterministic report.
- [x] HN smoke works; gated integrations fail gracefully without credentials.
- [ ] Real Codex background execution works with constrained permissions.
- [x] Evidence, scores, verdicts, rejections, changes, and reopen events are queryable.
- [x] Backup verification passes.
- [ ] README and Ubuntu-on-Hyper-V deployment guide are complete.
- [ ] Final integration PR is reviewed before merge to `main`.

## 23. Change control

Material changes require a dedicated spec PR explaining domain, migration, security, compatibility, and release impact. Implementation PRs must not silently alter this contract.
