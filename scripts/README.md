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

Start daily verified PostgreSQL backups with `docker compose --profile backup up -d backup`.
Retention keeps the union of the newest backup in seven daily, four ISO-weekly, and six monthly
buckets, so one archive can satisfy more than one tier. The backup service mounts only the database
and backup destination; the `codex_auth` volume is deliberately absent. Copy verified archives
off the VM for disaster recovery.
