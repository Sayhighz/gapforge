"""Run status, persisted budget, deadline, and in-run concurrency control."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import delete, exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from gapforge.storage.models import ProviderCallLease, ResearchRun, ResearchTask

_RUN_ADMISSION_LOCK_KEY = 0x474150464F524745  # "GAPFORGE", stable signed bigint


@dataclass(frozen=True, slots=True)
class BudgetDecision:
    allowed: bool
    used: int
    limit: int


class RunController:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def start(self, run_id: UUID, *, now: datetime | None = None) -> ResearchRun:
        run = await self._locked_run(run_id)
        start_time = now or datetime.now(UTC)
        if run.status == "QUEUED":
            raw_duration = run.budget_limits.get("max_run_duration_minutes")
            if (
                isinstance(raw_duration, bool)
                or not isinstance(raw_duration, int)
                or raw_duration < 1
            ):
                raise ValueError("persisted max_run_duration_minutes must be a positive integer")
            try:
                deadline = start_time + timedelta(minutes=raw_duration)
            except OverflowError as error:
                raise ValueError("persisted max_run_duration_minutes is too large") from error
            run.status = "RUNNING"
            run.started_at = start_time
            run.deadline_at = deadline
        elif run.status != "RUNNING":
            raise ValueError(f"cannot start run in status {run.status}")
        elif run.deadline_at <= start_time:
            run.status = "BUDGET_EXHAUSTED"
            run.last_checkpoint = {"reason": "run deadline reached before start"}
            run.completed_at = start_time
        await self.session.flush()
        return run

    async def admit_next(
        self,
        *,
        allowed_task_types: frozenset[str],
        now: datetime | None = None,
    ) -> ResearchRun | None:
        """Serialize global admission and start only a supported pending root graph."""

        if not allowed_task_types:
            return None
        admission_time = now or datetime.now(UTC)
        lock_acquired = await self.session.scalar(
            select(func.pg_try_advisory_xact_lock(_RUN_ADMISSION_LOCK_KEY))
        )
        if not lock_acquired:
            return None
        active_run_id = await self.session.scalar(
            select(ResearchRun.id).where(ResearchRun.status == "RUNNING").limit(1)
        )
        if active_run_id is not None:
            return None
        supported_work_exists = exists(
            select(ResearchTask.id).where(
                ResearchTask.run_id == ResearchRun.id,
                ResearchTask.status == "PENDING",
                ResearchTask.available_at <= admission_time,
                ResearchTask.task_type.in_(allowed_task_types),
            )
        )
        candidate = await self.session.scalar(
            select(ResearchRun)
            .where(ResearchRun.status == "QUEUED", supported_work_exists)
            .order_by(ResearchRun.priority, ResearchRun.created_at, ResearchRun.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if candidate is None:
            return None
        return await self.start(candidate.id, now=admission_time)

    async def consume_budget(
        self,
        run_id: UUID,
        *,
        counter: str,
        limit_key: str,
        amount: int = 1,
        now: datetime | None = None,
    ) -> BudgetDecision:
        if amount < 1:
            raise ValueError("budget amount must be positive")
        run = await self._locked_run(run_id)
        current_time = now or datetime.now(UTC)
        if run.status != "RUNNING":
            raise ValueError(f"cannot consume budget for run in status {run.status}")
        limit = int(run.budget_limits[limit_key])
        used = int(run.budget_used.get(counter, 0))
        if run.deadline_at <= current_time or used + amount > limit:
            run.status = "BUDGET_EXHAUSTED"
            run.completed_at = current_time
            run.last_checkpoint = {
                "reason": "deadline" if run.deadline_at <= current_time else "budget",
                "counter": counter,
                "used": used,
                "limit": limit,
            }
            await self.session.flush()
            return BudgetDecision(False, used, limit)
        run.budget_used = {**run.budget_used, counter: used + amount}
        await self.session.flush()
        return BudgetDecision(True, used + amount, limit)

    async def finalize_if_idle(self, run_id: UUID, *, now: datetime | None = None) -> ResearchRun:
        run = await self._locked_run(run_id)
        if run.status not in {"RUNNING", "QUEUED"}:
            return run
        rows = (
            await self.session.execute(
                select(ResearchTask.status, func.count())
                .where(ResearchTask.run_id == run_id)
                .group_by(ResearchTask.status)
            )
        ).tuples()
        task_counts = {status: int(count) for status, count in rows.all()}
        if task_counts.get("PENDING", 0) or task_counts.get("LEASED", 0):
            return run
        failed = task_counts.get("FAILED", 0)
        run.status = "COMPLETED_WITH_WARNINGS" if failed else "COMPLETED"
        if failed:
            run.warnings = [*run.warnings, {"failed_tasks": failed}]
        run.completed_at = now or datetime.now(UTC)
        await self.session.flush()
        return run

    async def _locked_run(self, run_id: UUID) -> ResearchRun:
        run = await self.session.scalar(
            select(ResearchRun).where(ResearchRun.id == run_id).with_for_update()
        )
        if run is None:
            raise LookupError(f"run {run_id} does not exist")
        return run


class AgentCallLimiter:
    """Local optimization; :class:`DurableAgentCallAdmission` is the hard gate."""

    def __init__(self, max_parallel: int = 2) -> None:
        if max_parallel < 1:
            raise ValueError("max_parallel must be positive")
        if max_parallel > 2:
            raise ValueError("max_parallel must be at most 2")
        self._max_parallel = max_parallel
        self._semaphores: defaultdict[UUID, asyncio.Semaphore] = defaultdict(
            lambda: asyncio.Semaphore(self._max_parallel)
        )

    def for_run(self, run_id: UUID) -> asyncio.Semaphore:
        return self._semaphores[run_id]


class DurableAgentCallAdmission:
    """Cross-process provider-call admission backed by expiring PostgreSQL leases."""

    def __init__(self, session: AsyncSession, *, max_parallel: int = 2) -> None:
        if max_parallel < 1:
            raise ValueError("max_parallel must be positive")
        if max_parallel > 2:
            raise ValueError("max_parallel must be at most 2")
        self.session = session
        self.max_parallel = max_parallel

    async def acquire(
        self,
        run_id: UUID,
        *,
        call_key: str,
        lease_owner: str,
        lease_duration: timedelta,
        now: datetime | None = None,
    ) -> ProviderCallLease | None:
        current_time = now or datetime.now(UTC)
        run = await self.session.scalar(
            select(ResearchRun).where(ResearchRun.id == run_id).with_for_update()
        )
        if run is None:
            raise LookupError(f"run {run_id} does not exist")
        if run.status != "RUNNING" or run.deadline_at <= current_time:
            return None
        await self.session.execute(
            delete(ProviderCallLease).where(
                ProviderCallLease.run_id == run_id,
                ProviderCallLease.lease_expires_at <= current_time,
            )
        )
        active_count = await self.session.scalar(
            select(func.count())
            .select_from(ProviderCallLease)
            .where(ProviderCallLease.run_id == run_id)
        )
        if (active_count or 0) >= self.max_parallel:
            return None
        lease = ProviderCallLease(
            run_id=run_id,
            call_key=call_key,
            lease_owner=lease_owner,
            lease_expires_at=min(current_time + lease_duration, run.deadline_at),
        )
        self.session.add(lease)
        await self.session.flush()
        return lease

    async def release(self, lease_id: UUID, *, lease_owner: str) -> None:
        lease = await self.session.scalar(
            select(ProviderCallLease).where(ProviderCallLease.id == lease_id).with_for_update()
        )
        if lease is None:
            return
        if lease.lease_owner != lease_owner:
            raise PermissionError("provider-call lease is not owned by this worker")
        await self.session.delete(lease)
        await self.session.flush()
