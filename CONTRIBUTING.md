# Contributing to GapForge

## Branch model

- `main` is the reviewed release line.
- `integration/v0.1` is the temporary integration branch.
- Development branches use `agent/<short-description>`.
- Developer PRs target `integration/v0.1`.
- Only the final integration PR targets `main`.

## Pull requests

Open PRs as drafts. The description must state specification tasks implemented, owned files changed, migrations, tests and results, known limitations, and security implications.

Before requesting review:

```bash
ruff check .
ruff format --check .
mypy src
pytest
```

Never commit `.env`, Codex auth data, source credentials, database dumps, generated reports, or backup archives.

## Review policy

The integration lead reviews architecture, correctness, tests, migrations, security, and specification compliance. Critical and important findings must be fixed before merge. Developers never approve or merge their own PRs.
