# Reference Repository Review

Reviewed: 2026-08-09

This review records architectural lessons for v0.1 code review. It does not authorize copying implementation or prose. GapForge code is independently implemented against `docs/spec-v0.1.md`.

## ZeroToShip

Repository: `paretoimproved/ZeroToShip`

License observation: `package.json` and README declare MIT, but the inspected checkout did not contain a root license file.

Useful ideas:

- explicit graph/run budget checks;
- provider seams and batch-oriented semantic work;
- source-specific collectors and content filters;
- persistent run and evidence-strength migrations.

Do not inherit:

- embedding and model-API dependencies;
- in-memory/global budget baselines as the durable source of truth;
- engagement-heavy representative selection as evidence strength;
- fallback summaries that could be mistaken for supported claims.

GapForge response: persist counters per run, batch Codex CLI calls, keep deterministic evidence gates, and reject rather than silently convert failed semantic output into fact.

## Pain Point Miner

Repository: `vincent-peng/pain-point-miner`

License observation: no license file or explicit license declaration was found in the inspected checkout. Treat all content as conceptual reference only; do not copy its text or templates.

Useful ideas:

- compact evidence cards;
- explicit independent-source confidence;
- fatal flags and score caps;
- separating product bugs from unmet market demand;
- adversarial challenge before recommendation.

GapForge response: independently defined Evidence Cards, atomic claims, hard `VALIDATE` gates, two-axis scoring, penalties, and a blind critic.

## AI Community Intelligence

Repository: `akshayturtle/ai-community-intelligence`

License observation: MIT license file present.

Useful ideas:

- common scraper lifecycle and structured run metrics;
- persistent multi-source intelligence;
- update/upsert semantics and historical timestamps;
- modular processors and source breadth.

Do not inherit:

- broad dashboard/API scope;
- collector classes that directly own database sessions and commits;
- very large initial source surface;
- naive coupling among scrapers, persistence, and processors.

GapForge response: three v0.1 sources, storage-neutral collector results, repository-owned transactions, and no UI/HTTP layer.

## Reddit Intelligence MCP / BuildRadar

Repository: `Houseofmvps/reddit-intel-agent-mcp`

License observation: MIT license file present.

Useful ideas:

- typed pattern categories for pain, workarounds, buyer intent, switching, feature requests, and pricing objections;
- direct Reddit adapter isolation;
- rate limiter and cache separation;
- structured opportunity and lead score breakdowns.

Do not inherit:

- treating regex matches as final semantic evidence;
- engagement or subscriber counts as direct pain severity;
- scoring that allows volume from one community to dominate;
- product-lead/reply automation outside GapForge scope.

GapForge response: patterns are cheap candidate filters only; authors, threads, source diversity, behavior, WTP, contradictions, and hard gates control evidence quality.

## Review checklist derived from references

- [ ] Budgets and checkpoints persist in PostgreSQL rather than process globals.
- [ ] Collectors return normalized data and do not commit their own transactions.
- [ ] Regex and engagement metrics cannot create supported claims by themselves.
- [ ] One viral thread/community cannot dominate evidence or trend.
- [ ] Failed semantic output remains failed/unknown rather than becoming a plausible fallback fact.
- [ ] No reference implementation or unlicensed prose is copied.

