"""Database, queue, destination, source, and Codex health checks."""

from __future__ import annotations

import os
from enum import StrEnum
from typing import Protocol

from alembic.config import Config
from alembic.script import ScriptDirectory
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select, text

from gapforge.config import AgentProviderName, Settings
from gapforge.providers.codex_cli import CodexProbe
from gapforge.storage.database import Database
from gapforge.storage.models import ResearchTask


class HealthStatus(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    AUTH_REQUIRED = "auth_required"
    FAILED = "failed"


class HealthCheck(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    status: HealthStatus
    detail: str


class HealthReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: HealthStatus
    checks: tuple[HealthCheck, ...]


class ProbeableProvider(Protocol):
    async def probe(self) -> CodexProbe: ...


class HealthService:
    def __init__(
        self,
        settings: Settings,
        database: Database,
        *,
        codex_provider: ProbeableProvider | None = None,
    ) -> None:
        self.settings = settings
        self.database = database
        self.codex_provider = codex_provider

    async def check(self) -> HealthReport:
        checks: list[HealthCheck] = []
        database_ok = await self._database_checks(checks)
        self._destination_checks(checks)
        self._configuration_checks(checks)
        await self._provider_check(checks)
        if not database_ok or any(check.status is HealthStatus.FAILED for check in checks):
            status = HealthStatus.FAILED
        elif any(check.status is HealthStatus.AUTH_REQUIRED for check in checks):
            status = HealthStatus.AUTH_REQUIRED
        elif any(check.status is HealthStatus.DEGRADED for check in checks):
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.HEALTHY
        return HealthReport(status=status, checks=tuple(checks))

    async def _database_checks(self, checks: list[HealthCheck]) -> bool:
        try:
            async with self.database.session() as session:
                await session.execute(text("SELECT 1"))
                current_revision = await session.scalar(
                    text("SELECT version_num FROM alembic_version")
                )
                expired_leases = await session.scalar(
                    select(func.count())
                    .select_from(ResearchTask)
                    .where(
                        ResearchTask.status == "LEASED",
                        ResearchTask.lease_expires_at < func.now(),
                    )
                )
            expected_revision = ScriptDirectory.from_config(
                Config("alembic.ini")
            ).get_current_head()
            checks.append(
                HealthCheck(name="database", status=HealthStatus.HEALTHY, detail="reachable")
            )
            migration_status = (
                HealthStatus.HEALTHY
                if current_revision == expected_revision
                else HealthStatus.FAILED
            )
            checks.append(
                HealthCheck(
                    name="migration",
                    status=migration_status,
                    detail=f"database={current_revision}; expected={expected_revision}",
                )
            )
            checks.append(
                HealthCheck(
                    name="queue_leases",
                    status=HealthStatus.DEGRADED if expired_leases else HealthStatus.HEALTHY,
                    detail=f"expired={expired_leases or 0}",
                )
            )
            return migration_status is not HealthStatus.FAILED
        except Exception as error:  # health must convert infrastructure errors into state
            checks.append(
                HealthCheck(
                    name="database",
                    status=HealthStatus.FAILED,
                    detail=f"{type(error).__name__}: database unavailable",
                )
            )
            return False

    def _destination_checks(self, checks: list[HealthCheck]) -> None:
        for name, path in (
            ("reports_destination", self.settings.reports_dir),
            ("backups_destination", self.settings.backups_dir),
        ):
            target = path if path.exists() else path.parent
            writable = target.exists() and os.access(target, os.W_OK)
            checks.append(
                HealthCheck(
                    name=name,
                    status=HealthStatus.HEALTHY if writable else HealthStatus.FAILED,
                    detail=str(path),
                )
            )

    def _configuration_checks(self, checks: list[HealthCheck]) -> None:
        configured = {
            "author_hmac": self.settings.author_hmac_key,
            "github": self.settings.github_token,
            "reddit": self.settings.reddit_client_id and self.settings.reddit_client_secret,
            "brave_search": self.settings.brave_api_key,
        }
        for name, value in configured.items():
            checks.append(
                HealthCheck(
                    name=name,
                    status=HealthStatus.HEALTHY if value else HealthStatus.DEGRADED,
                    detail="configured" if value else "not configured",
                )
            )
        checks.append(
            HealthCheck(
                name="budgets",
                status=HealthStatus.HEALTHY,
                detail=f"validated {len(self.settings.budget_snapshot())} limits",
            )
        )

    async def _provider_check(self, checks: list[HealthCheck]) -> None:
        if self.settings.agent_provider is AgentProviderName.FAKE:
            checks.append(
                HealthCheck(name="agent_provider", status=HealthStatus.HEALTHY, detail="fake")
            )
            return
        if self.codex_provider is None:
            checks.append(
                HealthCheck(
                    name="agent_provider",
                    status=HealthStatus.FAILED,
                    detail="Codex provider probe unavailable",
                )
            )
            return
        probe = await self.codex_provider.probe()
        if not probe.binary_available:
            status = HealthStatus.FAILED
        elif not probe.authenticated:
            status = HealthStatus.AUTH_REQUIRED
        else:
            status = HealthStatus.HEALTHY
        checks.append(
            HealthCheck(
                name="agent_provider",
                status=status,
                detail=probe.version or probe.error or "unknown",
            )
        )
