# Lane B Checklist — Research Engine, Sources, Analysis, Reports

Branch: `agent/research-engine`

PR target: `integration/v0.1`

Owner: Research developer agent

Last checkpoint: B1-B2 pushed in `4a9ca79`; B3 collectors next

Mark `[x]` only after implementation and tests are committed and pushed. For partial work, leave `[ ]` and add a `Progress:` note with commit and next action.

## Owned paths

```text
src/gapforge/domain/**
src/gapforge/collectors/**
src/gapforge/search/**
src/gapforge/research/**
src/gapforge/analysis/**
src/gapforge/scoring/**
src/gapforge/reports/**
src/gapforge/security/**
tests/research/**
.agents/**
.claude/**
```

Do not modify `pyproject.toml`. Use dependencies mandated by the spec and list a missing dependency as an integration note.

## B1 — Domain contracts

- [ ] Define Pydantic contracts for missions/revisions and queries.
- [ ] Define raw/pain signals, clusters, claims, Evidence Cards, hypotheses, competitors, gaps, opportunities, scores, critic results, and source/provider results.
- [ ] Add schema versioning, strict enums, bounded strings/lists, and serialization tests.
- [ ] Keep contracts storage-neutral.
  Progress: Core contracts and query planning are pushed in `4a9ca79`; the full required-entity/provider audit remains before B1 is complete.

Acceptance: contracts reject malformed/unbounded agent and source data predictably.

## B2 — Query planning

- [x] Implement semantic `QueryIntent` schemas and deterministic source compilers.
- [x] Enforce eight broad, four targeted, and two-round caps.
- [x] Add behavior patterns, negative filters, query deduplication, and yield ranking.
- [x] Implement 365-day time-stratified sampling and 24-hour monitor overlap.
- [x] Test cap bypass attempts and low-yield reprioritization.

Acceptance: model output cannot directly produce unlimited network calls.

## B3 — Collectors

- [ ] Implement common collector request/result/error contract.
- [ ] Implement HN collector with public API and comments/thread metadata.
- [ ] Implement GitHub issue/comment collector with optional token and bot/PR/template filtering.
- [ ] Implement official OAuth Reddit collector with structured missing-credential status.
- [ ] Enforce pagination/request/signal/source/thread/time budgets.
- [ ] Test with fixtures/fake transports only.

Acceptance: one collector failure does not invalidate normalized results from another.

## B4 — Search and secure fetch

- [ ] Implement Brave Search provider and normalized results.
- [ ] Implement static HTTP fetcher with protocol, DNS/IP, redirect, size, type, and timeout controls.
- [ ] Extract visible text and preserve snapshot hash/time/source URL.
- [ ] Reject agent-created URLs and unsupported search-snippet facts.
- [ ] Test SSRF, redirect-to-private, rebinding defense seam, oversized body, and unavailable content.

Acceptance: only approved public results can be fetched and analyzed.

## B5 — Normalization, deduplication, identity, clustering

- [ ] Normalize URLs/text and hash exact content.
- [ ] Implement MinHash near-duplicate detection and FTS/`pg_trgm` candidate contract.
- [ ] Implement author HMAC contract and unknown/bot handling.
- [ ] Implement cluster/opportunity thresholds, dormant state, and reversible merge candidates.
- [ ] Ensure semantic auto-merge and embeddings are absent.
- [ ] Test viral-thread, repeat-author, and duplicate-score inflation cases.

Acceptance: duplicate or dependent observations cannot fake independent demand.

## B6 — Evidence and claims

- [ ] Define pain extraction schemas without fabricated context.
- [ ] Build deterministic Evidence Cards and independence metrics.
- [ ] Implement atomic claim/citation validation and contradiction support.
- [ ] Require captured URL/excerpt/time for price and feature claims.
- [ ] Implement invalid-output repair request structure with a single-attempt marker.
- [ ] Test invented evidence IDs, URLs, claims, and missing cards.

Acceptance: every `SUPPORTED` claim points to permitted evidence.

## B7 — Hypotheses, scoring, and critic

- [ ] Implement falsifiable problem hypothesis checks.
- [ ] Model software and non-software alternatives and gap evidence requirements.
- [ ] Implement EvidenceStrength and OpportunityFit components.
- [ ] Implement geometric mean, penalties, algorithm version, and snapshot explanation.
- [ ] Implement all `VALIDATE` hard gates.
- [ ] Build blind critic input and exact verdict output schema.
- [ ] Implement research-more intent derivation, lifecycle transitions, reopen triggers/cooldown, and trend gates.
- [ ] Test high-score gate failures and critic blindness.

Acceptance: no score, single source, viral thread, or critic prose can bypass hard evidence requirements.

## B8 — Reports and repository skill

- [ ] Implement deterministic run and opportunity Markdown renderers.
- [ ] Render only validated claims; preserve originals and label translations.
- [ ] Implement canonical `.agents/skills/business-gap/` instructions and references.
- [ ] Add compatible Claude discovery without duplicating canonical content where practical.
- [ ] Ensure skills query stored intelligence first and require explicit MONITOR activation.
- [ ] Test report determinism and skill command examples.

Acceptance: Codex and Claude can use CLI JSON without knowing database internals.

## B9 — PR handoff

- [ ] Re-read `docs/spec-v0.1.md` and audit owned deliverables.
- [ ] Run all available checks and record exact output in the PR.
- [ ] Update this file's `Last checkpoint` and completed boxes.
- [ ] Push all commits and open/update a draft PR targeting `integration/v0.1`.
- [ ] Document cross-lane integration needs without editing Lane A paths.

## Resume note

Current state: B1 and B2 complete; collector implementation is next.

Last pushed commit: `4a9ca79` (`add research contracts and query planning`).

Next action: implement B3 common collector budgets and HN/GitHub/Reddit clients with fake transports.
