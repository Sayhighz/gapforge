# Deploy GapForge on Ubuntu Server under Windows Server 2019 Hyper-V

This guide deploys GapForge v0.1 in its supported production topology:

```text
Windows Server 2019 host
└── Hyper-V Generation 2 VM (Ubuntu Server 24.04 LTS amd64)
    └── Docker Engine + Docker Compose plugin
        ├── PostgreSQL 16
        ├── one-shot migration service
        ├── non-root GapForge worker + pinned Codex CLI
        ├── dedicated Codex device-auth helpers
        └── optional PostgreSQL backup service
```

Windows Server is the Hyper-V host only. Do not run GapForge in Windows containers, install Docker
Desktop on Windows Server, or mount the Windows host's entire Codex home into the VM.

## 1. Release and security prerequisites

Deploy only a reviewed commit from the intended trusted `main` ref. At this checkpoint, GitHub
reports that `main` is **not protected**. Do not dispatch the credentialed platform smoke or treat
`main` as a trusted execution boundary until an administrator records the release decision and
enables branch protection with all of the following:

- changes enter through pull requests;
- branches must be up to date before merge and the Quality checks (`python`, `container-backup`,
  and `arm64-build`) are required;
- review conversations must be resolved;
- linear history is required;
- force pushes are disabled; and
- branch deletion is disabled.

This repository does not mutate GitHub settings. Branch protection is an operator/repository-owner
gate outside the deployment itself. Because the current repository has one owner and GitHub does
not permit self-approval, required approving reviews may remain at zero for v0.1; that is an explicit
single-owner limitation, not evidence that PR #4 received an independent GitHub approval. The
integration lead must still record the final full-diff review before merge.

Prepare these external dependencies:

- a Windows Server 2019 host with hardware virtualization and data-execution prevention enabled;
- the Hyper-V role and management tools;
- the Ubuntu Server 24.04 LTS `amd64` ISO;
- outbound HTTPS and DNS from the VM for Ubuntu/Docker packages, source APIs, static public pages,
  and Codex authentication/reasoning;
- a private Git checkout path and an operator account allowed to administer Docker; and
- a separate off-VM destination for verified database backups.

Microsoft supports Ubuntu 24.04 on Generation 2 Hyper-V VMs. As an operational starting point—not
a measured minimum—allocate 4 virtual CPUs, 8 GiB RAM, and at least 80 GiB dynamically expanding
storage. Increase disk space for Docker layers, indefinitely retained raw evidence, reports, and
local backups. Monitor actual usage and avoid overcommitting memory during image builds.

Official references:

- [Install Hyper-V on Windows Server](https://learn.microsoft.com/windows-server/virtualization/hyper-v/get-started/Install-Hyper-V)
- [Choose a Hyper-V VM generation](https://learn.microsoft.com/windows-server/virtualization/hyper-v/plan/should-i-create-a-generation-1-or-2-virtual-machine-in-hyper-v)
- [Install Docker Engine on Ubuntu](https://docs.docker.com/engine/install/ubuntu/)
- [Install the Docker Compose plugin](https://docs.docker.com/compose/install/linux/)

## 2. Create and secure the Ubuntu VM

On the Windows Server host, install Hyper-V with Server Manager or an elevated PowerShell session:

```powershell
Install-WindowsFeature -Name Hyper-V -IncludeManagementTools -Restart
```

After the host restarts:

1. Create an external or appropriately routed virtual switch. Do not expose the VM directly to an
   untrusted network unless the surrounding firewall policy requires it.
2. Create a Generation 2 VM, attach the Ubuntu Server ISO, and allocate the planned CPU, memory,
   and disk. For Secure Boot, use the Microsoft UEFI Certificate Authority template supported by
   Ubuntu.
3. Install Ubuntu Server 24.04 LTS `amd64`, enable OpenSSH only if remote administration is needed,
   and use a DHCP reservation or a deliberately managed static address.
4. Create a named non-root operator account. Do not share it with application users; Docker
   administration is root-equivalent.
5. Apply updates and reboot if required:

   ```sh
   sudo apt update
   sudo apt full-upgrade -y
   sudo reboot
   ```

6. Restrict SSH at the Hyper-V/network firewall to known administration addresses. GapForge and
   PostgreSQL publish no host ports in the supplied Compose file. If `ufw` is used, remember that
   Docker-published ports can bypass ordinary `ufw` rules; keep the Compose stack unmodified or add
   reviewed rules to Docker's supported firewall chain before publishing anything.

Keep Ubuntu security updates enabled, patch the Windows host, take deliberate Hyper-V host backups,
and configure time synchronization. VM checkpoints are not a substitute for application-consistent
PostgreSQL backups.

## 3. Install Docker Engine and Compose

Use Docker's official Ubuntu repository, not the convenience script and not an unofficial
`docker.io`/legacy Compose package:

```sh
sudo apt remove -y docker.io docker-compose docker-compose-v2 docker-doc \
  docker-buildx podman-docker containerd runc || true
sudo apt update
sudo apt install -y ca-certificates curl git openssl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
sudo tee /etc/apt/sources.list.d/docker.sources >/dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io \
  docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
sudo docker version
sudo docker compose version
sudo docker run --rm hello-world
```

These commands intentionally use `sudo`. Membership in the `docker` group grants root-equivalent
control of the VM; add only a dedicated trusted operator if you choose that convenience.

## 4. Check out the reviewed release

Clone the repository inside the Ubuntu VM and pin the reviewed release commit rather than deploying
an arbitrary workflow-dispatch branch:

```sh
git clone <reviewed-gapforge-repository-url> gapforge
cd gapforge
git fetch --tags origin
git switch main
git pull --ff-only origin main
git status --short
git rev-parse HEAD
```

Require an empty `git status` and compare the commit with the recorded release decision. Do not
deploy from this I8 branch, `integration/v0.1`, or an unreviewed pull-request head.

## 5. Configure secrets and hard limits

Create a deployment-only environment file:

```sh
cp .env.example .env
chmod 600 .env
openssl rand -hex 32
openssl rand -hex 32
```

Use the first generated value as `POSTGRES_PASSWORD` and inside `DATABASE_URL`; use the second as
`AUTHOR_HMAC_KEY`. Hex avoids URL-encoding ambiguity. Replace every checked-in placeholder. Set:

```env
AGENT_PROVIDER=codex_cli
POSTGRES_PASSWORD=<64-hex-character-random-value>
DATABASE_URL=postgresql+asyncpg://gapforge:<same-value>@postgres:5432/gapforge
AUTHOR_HMAC_KEY=<different-64-hex-character-random-value>
```

Add only the source credentials you intend to use:

- `GITHUB_TOKEN` is optional but improves official GitHub API limits;
- `REDDIT_CLIENT_ID` and `REDDIT_CLIENT_SECRET` are required for Reddit OAuth; and
- `BRAVE_API_KEY` is required for competitor search.

An unavailable source remains explicit. Missing Reddit credentials produce `SOURCE_UNAVAILABLE`;
missing Brave search produces `RESEARCH_UNAVAILABLE` and prevents `VALIDATE` while allowing pain
collection to continue.

Review every budget in `.env`. The defaults permit one active run, at most two concurrent agent
calls, six total agent calls, 60 collector requests, 20 search calls, 300 raw signals, two research
rounds, and 30 minutes. Increasing limits changes cost and risk; do not raise them merely to force a
`VALIDATE` result.

Validate the rendered configuration without printing or committing `.env`:

```sh
sudo docker compose config --quiet
git status --short
```

Never paste `docker compose config` output into tickets or logs because it contains resolved
secrets.

## 6. Build, authenticate, migrate, and start

Build the pinned image once, then authenticate the dedicated Codex volume:

```sh
sudo docker compose build worker
sudo ./scripts/codex-auth
sudo ./scripts/codex-status
```

The device flow writes only the Compose-managed `codex_auth` volume at the worker's dedicated
`CODEX_HOME`. It does not copy the host's `~/.codex`, bake auth into an image, expose source/database
credentials to the auth helper, or include auth in PostgreSQL backups. Rerun both commands when
credentials expire.

Start PostgreSQL, run the one-shot Alembic migration, and start the worker:

```sh
sudo docker compose up -d postgres
sudo docker compose run --rm migrate
sudo docker compose up -d worker
sudo docker compose ps
sudo docker compose exec worker gap health --json
```

`gap health --json` should report the database/migration, queue, destinations, provider binary and
authentication, source/search configuration, and budgets. Optional missing sources may be
`degraded`; database/migration failure is not optional. Inspect sanitized logs if startup fails:

```sh
sudo docker compose logs --no-color --tail 200 migrate worker
```

Do not log `.env`, authorization headers, raw Codex auth files, or Compose's resolved environment.

## 7. CLI-first operation

PostgreSQL is the source of truth. Use the CLI's stable JSON surfaces instead of routine SQL.
Query existing intelligence first:

```sh
sudo docker compose exec worker gap opportunity list --json
sudo docker compose exec worker gap evidence list --json
sudo docker compose exec worker gap changes --json
sudo docker compose exec worker gap rejected --json
```

Create a Thai or English mission and run one bounded HUNT:

```sh
sudo docker compose exec worker gap mission create \
  "ค้นหาปัญหาที่เกิดซ้ำในกระบวนการปิดบัญชี" \
  --output-locale th --json
sudo docker compose exec worker gap hunt --mission <mission-id> --json
sudo docker compose exec worker gap run show <run-id> --json
sudo docker compose exec worker gap report run <run-id> --json
```

Mission creation does not enable monitoring. Activate it only after an explicit operator decision:

```sh
sudo docker compose exec worker gap mission activate <mission-id> --json
sudo docker compose exec worker gap monitor --once --json
sudo docker compose exec worker gap mission pause <mission-id> --json
```

A Product Hypothesis is never generated automatically. First inspect the current opportunity and
its assessment; only an explicit user-supplied proposition against a current `VALIDATE` assessment
can be persisted:

```sh
sudo docker compose exec worker gap opportunity show <opportunity-id> --json
sudo docker compose exec worker gap product-hypothesis create <assessment-id> \
  --request-id "<stable-request-id>" \
  --proposition "<explicit-user-supplied-proposition>" --json
```

Scores cannot override Evidence Card or hard-gate failures. Preserve and surface `AUTH_REQUIRED`,
`SOURCE_UNAVAILABLE`, `CONTENT_UNAVAILABLE`, `RESEARCH_UNAVAILABLE`, warnings, contradictions, and
zero-opportunity success.

## 8. Back up and restore

Create and verify a backup before every upgrade and on a regular schedule:

```sh
sudo docker compose exec worker gap backup create --json
sudo docker compose exec worker gap backup list --json
sudo docker compose exec worker gap backup verify <backup-name> --json
sudo docker compose --profile backup up -d backup
```

The scheduled service keeps the union of seven daily, four ISO-weekly, and six monthly buckets.
Backups are compressed PostgreSQL custom-format archives with SHA-256 sidecars. The backup service
does not mount `codex_auth`. Copy verified archive and checksum pairs to encrypted off-VM storage and
test restoration periodically.

Restore creates a new database and refuses an existing/unsafe target:

```sh
sudo docker compose exec worker gap backup restore <backup-name> \
  --target gapforge_restore_YYYYMMDD --yes --json
```

Verify the restored target before changing any production connection. A VM checkpoint, copied
PostgreSQL data directory, Markdown report directory, or unverified dump is not a supported restore
procedure.

## 9. Upgrade and migration boundaries

Before an upgrade:

1. pause new operational work and let the current stage commit finish;
2. create, verify, and copy a backup off the VM;
3. record the current application commit and Alembic revision;
4. fetch the reviewed release and compare it with the release decision;
5. rebuild the worker image; and
6. run `sudo docker compose run --rm migrate` before restarting the worker.

The v0.1 pre-release migrations deliberately fail closed when old rows lack identity that cannot be
reconstructed honestly:

- I1 refuses legacy raw-signal revisions or competitor evidence without original domain lineage;
- I4 refuses legacy `agent_calls` or `provider_call_leases` without canonical request/output
  identity; and
- I6 refuses legacy Product Hypotheses without explicit request lineage.

Export any records that must be retained and reset the affected pre-release database only after a
verified backup. Do not edit identifiers, disable constraints, or invent lineage to make migration
pass. No generic destructive reset command is supplied; choose and review a recovery plan for the
specific pre-release dataset.

## 10. Protected credentialed smoke gate

Public CI and normal release checks use no paid credentials. The protected workflow for bounded real
GitHub, Reddit, Brave, and Codex calls is implemented but has **not** been executed. The repository
currently has no enrolled self-hosted Actions runner, no configured `platform-smoke` environment,
and an unprotected `main` branch. Do not dispatch it until all of these gates are complete:

1. protect `main` as described in section 1 and record the reviewed release commit;
2. enroll a dedicated, patched Ubuntu runner with exact labels
   `self-hosted`, `linux`, and `gapforge-smoke`;
3. create the GitHub environment `platform-smoke` with appropriate reviewer controls;
4. set its `GAPFORGE_SMOKE_ENV_FILE` variable to an absolute path outside the checkout;
5. create that external file with the complete `.env.example` key set, owned by the runner user and
   mode `0600`; never source it in a shell;
6. use non-development PostgreSQL credentials at least 16 characters long and matching
   `DATABASE_URL`, plus the intended source credentials and HMAC key; and
7. complete and verify device auth in the isolated `codex_auth` volume.

The workflow hard-checks out `main` without persisting a Git credential. `scripts/platform-smoke`
validates the external file, starts an isolated Compose project, checks health, then runs at most one
GitHub request, two Reddit requests, one Brave request, and one separately confirmed 30-second,
4-KiB, no-repair Codex semantic call. It must remain an explicit non-public dispatch.

## 11. Security boundaries and known limitations

- GapForge is private and single-user. There is no web UI, HTTP API, multi-user authorization,
  billing, Redis, Kubernetes, browser automation, embedding service, or AI API.
- Only Python connects to PostgreSQL and source APIs. Background Codex receives bounded structured
  evidence with immutable IDs and an environment allowlist that excludes database/source secrets.
- Collected content is untrusted data. It is never executed or followed as instructions.
- Static fetch accepts bounded public HTTP(S) content and blocks private, loopback, link-local, and
  metadata networks plus DNS rebinding. JavaScript/login/CAPTCHA pages remain unavailable.
- Only one research run is active globally. HUNT is one-shot; MONITOR is explicit and persistent.
- Raw evidence is retained indefinitely by default (`RAW_SIGNAL_RETENTION_DAYS=0`) without media.
  Confirm source terms, privacy obligations, and storage capacity before collection.
- Reports are derived artifacts. Back up PostgreSQL; do not treat `reports/latest.md` as the source
  of truth.
- Compose named volumes reside on the VM. Copy verified backups off the VM and protect Hyper-V host,
  VM console, SSH keys, Docker socket, repository checkout, `.env`, and GitHub runner account.
- `admin sql --read-only` is a bounded diagnostic escape hatch, not the normal repository-skill or
  operator path.
- A successful run may return zero `VALIDATE` opportunities. Missing research, unavailable search,
  a score, or promotional language never overrides hard gates.

Stop and investigate any database integrity/migration error, auth-volume exposure, unexpected
published port, credential in logs, or mismatch between the deployed commit and the recorded
release decision.
