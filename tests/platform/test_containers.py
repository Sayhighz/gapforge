from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


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


def test_manual_smoke_checks_out_only_reviewed_main_without_persisting_credentials() -> None:
    workflow = (ROOT / ".github/workflows/platform-smoke.yml").read_text(encoding="utf-8")

    assert "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1" in workflow
    assert "ref: main" in workflow
    assert "persist-credentials: false" in workflow


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
