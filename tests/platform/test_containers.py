from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SMOKE_ENV = {
    "AGENT_PROVIDER": "codex_cli",
    "AUTHOR_HMAC_KEY": "a" * 32,
    "BACKUP_INTERVAL_SECONDS": "86400",
    "BACKUP_MAINTENANCE_DATABASE": "postgres",
    "BRAVE_API_KEY": "brave-secret",
    "CODEX_MODEL": "",
    "DATABASE_URL": "postgresql+asyncpg://smoke-user:long-random-db-password@postgres:5432/smoke-db",
    "GITHUB_TOKEN": "github-secret",
    "INITIAL_LOOKBACK_DAYS": "365",
    "LOG_JSON": "true",
    "LOG_LEVEL": "INFO",
    "MAX_AGENT_CALLS_PER_RUN": "6",
    "MAX_COLLECTOR_REQUESTS_PER_RUN": "60",
    "MAX_PARALLEL_AGENT_CALLS": "2",
    "MAX_RAW_SIGNALS_PER_RUN": "300",
    "MAX_RESEARCH_ROUNDS": "2",
    "MAX_RUN_DURATION_MINUTES": "30",
    "MAX_SEARCH_CALLS_PER_RUN": "20",
    "MONITOR_OVERLAP_HOURS": "24",
    "POSTGRES_DB": "smoke-db",
    "POSTGRES_PASSWORD": "long-random-db-password",
    "POSTGRES_USER": "smoke-user",
    "RAW_SIGNAL_RETENTION_DAYS": "0",
    "REDDIT_CLIENT_ID": "reddit-id",
    "REDDIT_CLIENT_SECRET": "reddit-secret",
    "REJECTED_REOPEN_COOLDOWN_DAYS": "30",
    "RUN_INTERVAL_HOURS": "12",
}


def _compose_command() -> list[str] | None:
    candidates: list[list[str]] = []
    standalone = shutil.which("docker-compose")
    docker = shutil.which("docker")
    if standalone is not None:
        candidates.append([standalone])
    if docker is not None:
        candidates.append([docker, "compose"])
    for candidate in candidates:
        probe = subprocess.run(  # noqa: S603 -- resolved trusted Docker executable
            [*candidate, "version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if probe.returncode == 0:
            return candidate
    return None


def _write_smoke_env(
    path: Path,
    *,
    mode: int = 0o600,
    updates: dict[str, str] | None = None,
) -> None:
    values = {**SMOKE_ENV, **(updates or {})}
    path.write_text(
        "\n".join(f"{key}={value}" for key, value in sorted(values.items())) + "\n",
        encoding="utf-8",
    )
    path.chmod(mode)


def _validate_smoke_env(path: Path, checkout: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 -- resolved test interpreter and repository script
        [sys.executable, str(ROOT / "scripts/validate-smoke-env"), str(path), str(checkout)],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_manual_smoke_credential_file_is_external_private_complete_and_silent(
    tmp_path: Path,
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    external = tmp_path / "protected.env"
    _write_smoke_env(external)

    accepted = _validate_smoke_env(external, checkout)

    assert accepted.returncode == 0
    assert accepted.stdout == ""
    assert accepted.stderr == ""

    inside = checkout / "untracked.env"
    _write_smoke_env(inside)
    rejected_inside = _validate_smoke_env(inside, checkout)
    assert rejected_inside.returncode == 2
    assert "outside the checkout" in rejected_inside.stderr

    external.chmod(0o640)
    rejected_mode = _validate_smoke_env(external, checkout)
    assert rejected_mode.returncode == 2
    assert "0600" in rejected_mode.stderr
    assert "long-random-db-password" not in rejected_mode.stderr


def test_manual_smoke_credential_file_rejects_development_database_defaults(
    tmp_path: Path,
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    external = tmp_path / "protected.env"
    _write_smoke_env(
        external,
        updates={
            "POSTGRES_DB": "gapforge",
            "POSTGRES_USER": "gapforge",
            "POSTGRES_PASSWORD": "gapforge",
            "DATABASE_URL": "postgresql+asyncpg://gapforge:gapforge@postgres:5432/gapforge",
        },
    )

    rejected = _validate_smoke_env(external, checkout)

    assert rejected.returncode == 2
    assert "development database credentials" in rejected.stderr
    assert "postgresql+asyncpg" not in rejected.stderr


def test_worker_image_is_non_root_pins_codex_and_copies_no_environment() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "@openai/codex@${CODEX_CLI_VERSION}" in dockerfile
    assert "ARG CODEX_CLI_VERSION=0.144.5" in dockerfile
    assert "ARG UV_VERSION=0.11.7" in dockerfile
    assert "postgres:16.9-bookworm@sha256:" in dockerfile
    assert 'pg_dump --version | grep -F "pg_dump (PostgreSQL) 16.9"' in dockerfile
    assert "ENV LD_LIBRARY_PATH" not in dockerfile
    assert "COPY --from=postgres-client /pg-client/ /" in dockerfile
    assert "scripts/postgres-tool /usr/local/libexec/gapforge-postgres-tool" in dockerfile
    assert "COPY pyproject.toml uv.lock README.md" in dockerfile
    assert "uv export --frozen" in dockerfile
    assert "--only-group build" in dockerfile
    assert "uv build --no-build-isolation" in dockerfile
    assert "--require-hashes" in dockerfile
    assert "USER gapforge:gapforge" in dockerfile
    assert "COPY ." not in dockerfile
    assert ".env" not in dockerfile
    assert "codex login" not in dockerfile


def test_dockerignore_excludes_credentials_and_derived_artifacts() -> None:
    ignored = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()

    assert ".env" in ignored
    assert ".env.*" in ignored
    assert "backups" in ignored
    assert "reports" in ignored
    assert ".git" in ignored


def test_backup_roundtrip_builds_shared_worker_image_only_once() -> None:
    script = (ROOT / "scripts/ci-backup-roundtrip").read_text(encoding="utf-8")

    assert "compose build worker" in script
    assert "compose run --rm migrate" in script
    health_command = (
        "compose run --rm --no-deps worker python /app/scripts/container-healthcheck.py"
    )
    assert health_command in script
    assert script.count("compose run --rm --no-deps") >= 6
    assert "compose exec" not in script
    assert "compose up -d --build" not in script
    assert "trap cleanup EXIT" in script
    assert "trap 'exit 130' INT" in script
    assert "trap 'exit 143' TERM" in script
    assert "compose stop postgres" in script
    assert "SELECT count(*) FROM research_missions" in script
    assert "container-volume-check.py auth-write" in script
    assert "--entrypoint python codex-status" in script
    assert "container-volume-check.py auth-readonly" in script


def test_manual_smoke_checks_out_only_reviewed_main_without_persisting_credentials() -> None:
    workflow = (ROOT / ".github/workflows/platform-smoke.yml").read_text(encoding="utf-8")
    script = (ROOT / "scripts/platform-smoke").read_text(encoding="utf-8")

    assert "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1" in workflow
    assert "ref: main" in workflow
    assert "persist-credentials: false" in workflow
    assert "GAPFORGE_SMOKE_ENV_FILE: ${{ vars.GAPFORGE_SMOKE_ENV_FILE }}" in workflow
    assert 'scripts/validate-smoke-env "$smoke_env_file" "$checkout_root"' in script
    assert '--env-file "$smoke_env_file"' in script
    assert "source " not in script
    assert '. "$smoke_env_file"' not in script
    assert "deployment .env" not in script
    assert "gap-smoke hacker-news --json" in script
    assert "gap-smoke credentialed-sources --json" in script
    assert "gap-smoke credentialed-codex --confirm run --json" in script
    assert script.count("compose build worker") == 1
    assert "compose up -d --no-build postgres migrate worker" in script
    assert "container-volume-check.py auth-write" in script
    assert "container-volume-check.py auth-readonly" in script


def test_manual_smoke_uses_compose_run_compatible_arguments(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    external = tmp_path / "protected.env"
    _write_smoke_env(external)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    compose_log = tmp_path / "compose.log"
    fake_compose = fake_bin / "docker-compose"
    fake_compose.write_text(
        """#!/bin/sh
set -eu
printf '%s\\n' "$*" >> "$COMPOSE_LOG"
case "$*" in
    *run*--no-build*) exit 64 ;;
esac
""",
        encoding="utf-8",
    )
    fake_compose.chmod(0o755)

    completed = subprocess.run(  # noqa: S603 -- fixed repository script path
        [str(ROOT / "scripts/platform-smoke")],
        cwd=ROOT,
        env={
            "COMPOSE_LOG": str(compose_log),
            "GAPFORGE_PLATFORM_SMOKE_CONFIRM": "run",
            "GAPFORGE_SMOKE_ENV_FILE": str(external),
            "PATH": f"{fake_bin}:{Path(sys.executable).parent}:/usr/bin:/bin",
        },
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode == 0, completed.stderr
    calls = compose_log.read_text(encoding="utf-8").splitlines()
    assert calls[0].endswith("build worker")
    assert sum(call.endswith("build worker") for call in calls) == 1
    assert all(not (" run " in f" {call} " and "--no-build" in call) for call in calls)


def test_compose_configuration_is_valid_and_auth_service_has_no_app_secrets() -> None:
    command = _compose_command()
    if command is None:
        pytest.skip("Docker Compose is not installed")
    completed = subprocess.run(  # noqa: S603 -- resolved trusted Docker executable
        [*command, "--profile", "auth", "--profile", "backup", "config"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    rendered = completed.stdout
    auth_section = rendered.split("  codex-auth:", 1)[1].split("  codex-status:", 1)[0]
    assert "CODEX_HOME:" in auth_section
    assert "DATABASE_URL:" not in auth_section
    assert "GITHUB_TOKEN:" not in auth_section
    assert "REDDIT_CLIENT_SECRET:" not in auth_section
    assert "AUTHOR_HMAC_KEY:" not in auth_section
    assert 'cli_auth_credentials_store="file"' in auth_section
    backup_section = rendered.split("  backup:", 1)[1].split("  codex-auth:", 1)[0]
    assert "backups_data" in backup_section
    assert "codex_auth" not in backup_section
    assert "GITHUB_TOKEN:" not in backup_section
    assert "REDDIT_CLIENT_SECRET:" not in backup_section


def test_quality_builds_the_pinned_worker_on_native_arm64() -> None:
    workflow = (ROOT / ".github/workflows/quality.yml").read_text(encoding="utf-8")

    assert "arm64-build:" in workflow
    assert "runs-on: ubuntu-24.04-arm" in workflow
    assert "docker build --tag gapforge-worker:arm64 ." in workflow
    assert 'test "$(uname -m)" = "aarch64"' in workflow
    assert "codex --version" in workflow
    assert "pg_dump --version" in workflow
