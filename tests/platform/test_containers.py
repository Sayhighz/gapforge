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


def test_compose_configuration_is_valid_and_auth_service_has_no_app_secrets() -> None:
    command = _compose_command()
    if command is None:
        pytest.skip("Docker Compose is not installed")
    completed = subprocess.run(  # noqa: S603 -- resolved trusted Docker executable
        [*command, "--profile", "auth", "config"],
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
