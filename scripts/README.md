# Container operations

Copy `.env.example` to `.env`, replace every placeholder, then bootstrap with:

```sh
scripts/codex-auth
scripts/codex-status
docker compose up -d --build
docker compose ps
docker compose exec worker gap health --json
```

`codex-auth` uses the CLI device flow. The resulting refreshable credentials live only in
the Compose-managed `codex_auth` volume, mounted at the worker's dedicated `CODEX_HOME`.
They are not copied from the host's `~/.codex`, added to the image, or included in database
backups. If status reports missing or expired authentication, rerun `scripts/codex-auth`;
the volume survives container recreation.

For credential-free infrastructure validation, set `AGENT_PROVIDER=fake`. This checks the
database, migration, queue, destinations, and budgets without claiming Codex is authenticated.

The manually dispatched credentialed smoke workflow uses Node 24 GitHub Actions. Its dedicated
`self-hosted`, `linux`, `gapforge-smoke` runner must run a current GitHub Actions runner release
with Node 24 action support. To prevent an unreviewed workflow-dispatch branch from executing on
that credentialed host, the workflow always checks out protected `main` without persisting a Git
credential before it runs the smoke script.

The runner must set the GitHub environment variable `GAPFORGE_SMOKE_ENV_FILE` to an absolute
path outside the checkout. That external file must be owned by the runner user, mode `0600`, and
contain the complete `.env.example` key set. PostgreSQL credentials and `DATABASE_URL` must agree,
the public development password is rejected, and the HMAC/source credentials must be nonempty.
The validator parses the file as data and Compose receives it through `--env-file`; the shell never
sources it and the checkout's untracked `.env` is never used. Keep Codex device credentials only in
the separate `codex_auth` volume, not in this file.

`gap-smoke hacker-news` makes one credential-free HN request with at most five returned items.
`gap-smoke credentialed-sources` preflights all smoke credentials, then permits at most one GitHub
request, two Reddit requests (OAuth plus search), and one Brave request. The normal GitHub collector
still permits an absent token; only this credential-gated live smoke requires one. Finally,
`gap-smoke credentialed-codex --confirm run` makes exactly one no-repair semantic call with a
30-second timeout and 4 KiB output cap. The Codex provider environment allowlist excludes the
database and every source credential. None of these live commands run in public CI.

Start daily verified PostgreSQL backups with `docker compose --profile backup up -d backup`.
Retention keeps the union of the newest backup in seven daily, four ISO-weekly, and six monthly
buckets, so one archive can satisfy more than one tier. The backup service mounts only the database
and backup destination; the `codex_auth` volume is deliberately absent. Copy verified archives
off the VM for disaster recovery. Restore creates and cleans up target databases through
`BACKUP_MAINTENANCE_DATABASE` (`postgres` by default), so recovery still works when the source
database no longer exists.
