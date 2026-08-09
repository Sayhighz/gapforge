from __future__ import annotations

from pathlib import Path

import pytest

from gapforge.config import Settings
from gapforge.health import HealthService, HealthStatus
from gapforge.providers.codex_cli import CodexProbe
from gapforge.storage.database import Database


@pytest.mark.postgres
async def test_health_is_healthy_when_required_and_optional_capabilities_are_configured(
    migrated_postgres_url: str, tmp_path: Path
) -> None:
    settings = Settings(
        _env_file=None,
        database_url=migrated_postgres_url,
        agent_provider="fake",
        author_hmac_key="hmac",
        github_token="github",
        reddit_client_id="reddit-id",
        reddit_client_secret="reddit-secret",
        brave_api_key="brave",
        reports_dir=tmp_path / "reports",
        backups_dir=tmp_path / "backups",
    )
    database = Database.from_url(migrated_postgres_url)
    try:
        report = await HealthService(settings, database).check()
    finally:
        await database.dispose()

    assert report.status is HealthStatus.HEALTHY
    assert {check.name for check in report.checks} >= {
        "database",
        "migration",
        "queue_leases",
        "agent_provider",
        "author_hmac",
        "budgets",
    }


@pytest.mark.postgres
async def test_missing_optional_configuration_is_degraded(
    migrated_postgres_url: str, tmp_path: Path
) -> None:
    settings = Settings(
        _env_file=None,
        database_url=migrated_postgres_url,
        agent_provider="fake",
        reports_dir=tmp_path / "reports",
        backups_dir=tmp_path / "backups",
    )
    database = Database.from_url(migrated_postgres_url)
    try:
        report = await HealthService(settings, database).check()
    finally:
        await database.dispose()

    assert report.status is HealthStatus.DEGRADED
    assert any(
        check.name == "author_hmac" and check.status is HealthStatus.DEGRADED
        for check in report.checks
    )


class AuthRequiredProbe:
    async def probe(self) -> CodexProbe:
        return CodexProbe(True, "codex-cli test", False, "login required")


@pytest.mark.postgres
async def test_health_distinguishes_auth_required(
    migrated_postgres_url: str, tmp_path: Path
) -> None:
    settings = Settings(
        _env_file=None,
        database_url=migrated_postgres_url,
        agent_provider="codex_cli",
        author_hmac_key="hmac",
        github_token="github",
        reddit_client_id="reddit-id",
        reddit_client_secret="reddit-secret",
        brave_api_key="brave",
        reports_dir=tmp_path,
        backups_dir=tmp_path,
    )
    database = Database.from_url(migrated_postgres_url)
    try:
        report = await HealthService(settings, database, codex_provider=AuthRequiredProbe()).check()
    finally:
        await database.dispose()

    assert report.status is HealthStatus.AUTH_REQUIRED


@pytest.mark.asyncio
async def test_health_distinguishes_failed_database(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://gapforge:gapforge@127.0.0.1:1/gapforge",
        agent_provider="fake",
        reports_dir=tmp_path,
        backups_dir=tmp_path,
    )
    database = Database.from_url(settings.database_url.get_secret_value())
    try:
        report = await HealthService(settings, database).check()
    finally:
        await database.dispose()

    assert report.status is HealthStatus.FAILED
