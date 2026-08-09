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
gap mission revise <mission-id> "<revised mission>" --json
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
