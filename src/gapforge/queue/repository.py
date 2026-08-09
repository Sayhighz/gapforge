"""Lease-based durable queue operations."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import and_, or_, select, true
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from gapforge.queue.retry import RetryDecision
from gapforge.storage.models import ResearchRun, ResearchTask


class DurableQueue:
    """Transactional queue facade; callers commit through their unit of work."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def enqueue(
        self,
        *,
        run_id: UUID,
        task_type: str,
        idempotency_key: str,
        payload: dict[str, object],
        priority: int = 100,
        max_attempts: int = 3,
        available_at: datetime | None = None,
    ) -> tuple[ResearchTask, bool]:
        task = ResearchTask(
            id=uuid4(),
            run_id=run_id,
            task_type=task_type,
            status="PENDING",
            priority=priority,
            idempotency_key=idempotency_key,
            payload=payload,
            checkpoint={},
            attempt_count=0,
            max_attempts=max_attempts,
            available_at=available_at or datetime.now(UTC),
        )
        statement = (
            insert(ResearchTask)
            .values(
                id=task.id,
                run_id=task.run_id,
                task_type=task.task_type,
                status=task.status,
                priority=task.priority,
                idempotency_key=task.idempotency_key,
                payload=task.payload,
                checkpoint=task.checkpoint,
                attempt_count=task.attempt_count,
                max_attempts=task.max_attempts,
                available_at=task.available_at,
            )
            .on_conflict_do_nothing(index_elements=["run_id", "idempotency_key"])
            .returning(ResearchTask.id)
        )
        inserted_id = await self.session.scalar(statement)
        if inserted_id is not None:
            persisted = await self.session.get(ResearchTask, inserted_id)
            if persisted is None:
                raise RuntimeError("inserted task was not readable")
            return persisted, True
        existing = await self.session.scalar(
            select(ResearchTask).where(
                ResearchTask.run_id == run_id,
                ResearchTask.idempotency_key == idempotency_key,
            )
        )
        if existing is None:
            raise RuntimeError("idempotent enqueue conflict did not resolve to a task")
        return existing, False

    async def claim(
        self,
        *,
        worker_id: str,
        lease_duration: timedelta,
        now: datetime | None = None,
        allowed_task_types: frozenset[str] | None = None,
    ) -> ResearchTask | None:
        claim_time = now or datetime.now(UTC)
        await self._expire_overdue_runs(claim_time)
        if allowed_task_types == frozenset():
            return None
        task_type_filter = (
            ResearchTask.task_type.in_(allowed_task_types)
            if allowed_task_types is not None
            else true()
        )
        statement = (
            select(ResearchTask)
            .join(ResearchRun, ResearchRun.id == ResearchTask.run_id)
            .where(
                ResearchRun.status == "RUNNING",
                ResearchRun.deadline_at > claim_time,
                ResearchTask.available_at <= claim_time,
                task_type_filter,
                or_(
                    ResearchTask.status == "PENDING",
                    and_(
                        ResearchTask.status == "LEASED",
                        ResearchTask.lease_expires_at < claim_time,
                    ),
                ),
            )
            .order_by(
                ResearchRun.priority.asc(),
                ResearchTask.priority.asc(),
                ResearchTask.created_at.asc(),
            )
            .with_for_update(skip_locked=True, of=ResearchTask)
            .limit(1)
        )
        task = await self.session.scalar(statement)
        if task is None:
            return None
        task.status = "LEASED"
        task.lease_owner = worker_id
        task.lease_expires_at = claim_time + lease_duration
        task.attempt_count += 1
        await self.session.flush()
        return task

    async def _expire_overdue_runs(self, now: datetime) -> None:
        statement = (
            select(ResearchRun)
            .where(ResearchRun.status == "RUNNING", ResearchRun.deadline_at <= now)
            .with_for_update(skip_locked=True)
        )
        for run in (await self.session.scalars(statement)).all():
            run.status = "BUDGET_EXHAUSTED"
            run.completed_at = now
            run.last_checkpoint = {
                **run.last_checkpoint,
                "reason": "deadline",
                "expired_by": "queue_poll",
            }
        await self.session.flush()

    async def checkpoint(
        self,
        task_id: UUID,
        *,
        worker_id: str,
        checkpoint: dict[str, object],
        lease_duration: timedelta,
        now: datetime | None = None,
    ) -> ResearchTask:
        task = await self._leased_by(task_id, worker_id)
        checkpoint_time = now or datetime.now(UTC)
        task.checkpoint = checkpoint
        task.lease_expires_at = checkpoint_time + lease_duration
        await self.session.flush()
        return task

    async def renew_lease(
        self,
        task_id: UUID,
        *,
        worker_id: str,
        lease_duration: timedelta,
        now: datetime | None = None,
    ) -> ResearchTask:
        task = await self._leased_by(task_id, worker_id)
        task.lease_expires_at = (now or datetime.now(UTC)) + lease_duration
        await self.session.flush()
        return task

    async def succeed(
        self,
        task_id: UUID,
        *,
        worker_id: str,
        result: dict[str, object],
        now: datetime | None = None,
    ) -> ResearchTask:
        task = await self._leased_by(task_id, worker_id)
        task.status = "SUCCEEDED"
        task.result = result
        task.completed_at = now or datetime.now(UTC)
        task.lease_owner = None
        task.lease_expires_at = None
        await self.session.flush()
        return task

    async def fail(
        self,
        task_id: UUID,
        *,
        worker_id: str,
        decision: RetryDecision,
        sanitized_error: str,
        now: datetime | None = None,
    ) -> ResearchTask:
        task = await self._leased_by(task_id, worker_id)
        failure_time = now or datetime.now(UTC)
        task.retry_class = decision.kind.value
        task.last_error = sanitized_error
        task.lease_owner = None
        task.lease_expires_at = None
        if decision.retryable and task.attempt_count < task.max_attempts:
            task.status = "PENDING"
            task.available_at = failure_time + timedelta(seconds=decision.delay_seconds)
        else:
            task.status = "FAILED"
            task.completed_at = failure_time
        await self.session.flush()
        return task

    async def _leased_by(self, task_id: UUID, worker_id: str) -> ResearchTask:
        task = await self.session.scalar(
            select(ResearchTask).where(ResearchTask.id == task_id).with_for_update()
        )
        if task is None:
            raise LookupError(f"task {task_id} does not exist")
        if task.status != "LEASED" or task.lease_owner != worker_id:
            raise PermissionError("task lease is not owned by this worker")
        return task
