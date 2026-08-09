# GapForge CLI command guide

Use `--json` on machine-facing calls. Expect the envelope keys `schema_version`, `command`, `data`, `warnings`, and `error`. Parse stdout as JSON; treat stderr as diagnostics.

## Inspect before collecting

```bash
gap opportunity list --json
gap opportunity show <opportunity-id> --json
gap opportunity compare <left-id> <right-id> --json
gap evidence list --json
gap evidence show <evidence-id> --json
gap changes --json
gap rejected --json
gap run list --json
gap run show <run-id> --json
```

## Missions and bounded research

```bash
gap mission list --json
gap mission show <mission-id> --json
gap mission create "<natural-language mission>" --json
gap mission revise <mission-id> "<revised mission>" --reason "<change reason>" --json
gap hunt --mission <mission-id> --json
```

Creation does not activate monitoring. Use `gap mission activate <mission-id> --json` only after an explicit request. Pause with `gap mission pause <mission-id> --json`. Use `gap monitor --once --json` for one opted-in monitor cycle.

## Explain and report

```bash
gap report run <run-id> --json
gap report opportunity <opportunity-id> --json
gap merge-candidate list --json
```

Accept or reject a merge candidate only when the user explicitly chooses the candidate. Reports are derived artifacts; persisted records remain authoritative.

## Explicit post-VALIDATE Product Hypothesis

First inspect `gap opportunity show <opportunity-id> --json` and select its current persisted
`VALIDATE` assessment ID. Only when the user explicitly supplies both a stable request ID and the
proposition, persist and then follow up by ID:

```bash
gap product-hypothesis create <assessment-id> --request-id "<request-id>" --proposition "<user-supplied proposition>" --json
gap product-hypothesis show <product-hypothesis-id> --json
```

The command is append-only and makes no Codex or AI API call. Never infer the proposition, reuse a
request ID for changed content, or treat an older `VALIDATE` snapshot as current eligibility.
