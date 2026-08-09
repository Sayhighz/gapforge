"""Transactional assembly of research runs and their durable root work."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from gapforge.queue.repository import DurableQueue
from gapforge.storage.models import ResearchRun, ResearchTask

ROOT_TASK_TYPE = "research.run"
_ROOT_IDEMPOTENCY_KEY = "run-root:v1"


class RunBudgetLimits(BaseModel):
    """Validated immutable budget snapshot persisted with one research run."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    run_interval_hours: int = Field(default=12, ge=1)
    max_research_rounds: int = Field(default=2, ge=1, le=2)
    max_agent_calls_per_run: int = Field(default=6, ge=1)
    max_parallel_agent_calls: int = Field(default=2, ge=1, le=2)
    max_run_duration_minutes: int = Field(default=30, ge=1)
    max_collector_requests_per_run: int = Field(default=60, ge=1)
    max_search_calls_per_run: int = Field(default=20, ge=0)
    max_raw_signals_per_run: int = Field(default=300, ge=1)
    raw_signal_retention_days: int = Field(default=0, ge=0)
    initial_lookback_days: int = Field(default=365, ge=1)
    monitor_overlap_hours: int = Field(default=24, ge=0)
    rejected_reopen_cooldown_days: int = Field(default=30, ge=0)

    @model_validator(mode="after")
    def validate_related_limits(self) -> RunBudgetLimits:
        if self.max_parallel_agent_calls > self.max_agent_calls_per_run:
            raise ValueError("max_parallel_agent_calls cannot exceed max_agent_calls_per_run")
        if self.monitor_overlap_hours > self.initial_lookback_days * 24:
            raise ValueError("monitor_overlap_hours cannot exceed initial_lookback_days")
        return self


class RunScheduleRequest(BaseModel):
    """Typed boundary for scheduling a root task without starting its clock."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    mission_revision_id: UUID
    mode: Literal["HUNT", "MONITOR"]
    priority: int = Field(ge=0, le=1000)
    budget_limits: RunBudgetLimits


@dataclass(frozen=True, slots=True)
class ScheduledRun:
    run: ResearchRun
    task: ResearchTask
    created: bool


class RunScheduler:
    """Create a run and its root task inside the caller's transaction."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def schedule(
        self,
        *,
        request: RunScheduleRequest,
        now: datetime | None = None,
    ) -> ScheduledRun:
        scheduled_at = now or datetime.now(UTC)
        run = ResearchRun(
            mission_revision_id=request.mission_revision_id,
            mode=request.mode,
            status="QUEUED",
            priority=request.priority,
            # The schema currently requires a value before a run starts. For QUEUED
            # rows this is only the enqueue timestamp; no queue path treats it as a
            # deadline. RunController.start atomically replaces it with the hard limit.
            deadline_at=scheduled_at,
            budget_limits=request.budget_limits.model_dump(mode="json"),
            budget_used={},
            warnings=[],
            last_checkpoint={},
        )
        created = True
        try:
            async with self.session.begin_nested():
                self.session.add(run)
                await self.session.flush()
        except IntegrityError:
            existing = await self.session.scalar(
                select(ResearchRun).where(
                    ResearchRun.mission_revision_id == request.mission_revision_id,
                    ResearchRun.status.in_(("QUEUED", "RUNNING")),
                )
            )
            if existing is None:
                raise
            run = existing
            created = False
        task, _ = await DurableQueue(self.session).enqueue(
            run_id=run.id,
            task_type=ROOT_TASK_TYPE,
            idempotency_key=_ROOT_IDEMPOTENCY_KEY,
            priority=request.priority,
            available_at=scheduled_at,
            payload={
                "schema_version": "1.0",
                "run_id": str(run.id),
                "mission_revision_id": str(request.mission_revision_id),
                "mode": request.mode,
            },
        )
        return ScheduledRun(run=run, task=task, created=created)
