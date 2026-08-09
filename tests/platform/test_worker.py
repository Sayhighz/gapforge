from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from gapforge.queue.repository import DurableQueue
from gapforge.storage.database import Database
from gapforge.storage.models import ResearchRun, ResearchTask
from gapforge.storage.uow import SqlAlchemyUnitOfWork
from gapforge.worker import Worker


async def _create_worker_task(
    database: Database, *, max_attempts: int = 3
) -> tuple[ResearchRun, ResearchTask]:
    async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
        _, revision = await uow.missions.create_with_revision(
            title=f"Worker mission {uuid4()}",
            mission_text="exercise worker lease",
            original_language="en",
            output_locale="en",
        )
        run = ResearchRun(
            mission_revision_id=revision.id,
            mode="HUNT",
            status="RUNNING",
            priority=1,
            deadline_at=datetime.now(UTC) + timedelta(minutes=5),
            budget_limits={"max_agent_calls_per_run": 6},
            budget_used={},
            warnings=[],
            last_checkpoint={},
        )
        assert uow.session is not None
        uow.session.add(run)
        await uow.session.flush()
        task, _ = await DurableQueue(uow.session).enqueue(
            run_id=run.id,
            task_type="slow",
            idempotency_key="slow:1",
            payload={},
            max_attempts=max_attempts,
        )
        await uow.commit()
        return run, task


async def _cancel_run(database: Database, run_id: object) -> None:
    async with database.session() as session:
        run = await session.get(ResearchRun, run_id)
        if run is not None and run.status == "RUNNING":
            run.status = "CANCELLED"
            run.completed_at = datetime.now(UTC)
            await session.commit()


@pytest.mark.postgres
async def test_worker_heartbeats_work_longer_than_original_lease(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    run, task = await _create_worker_task(database)
    started = asyncio.Event()

    async def slow_handler(_task: ResearchTask) -> dict[str, object]:
        started.set()
        await asyncio.sleep(0.16)
        return {"ok": True}

    worker = Worker(
        database,
        worker_id="worker-a",
        handlers={"slow": slow_handler},
        lease_duration=timedelta(seconds=0.06),
        heartbeat_interval_seconds=0.015,
    )
    try:
        work = asyncio.create_task(worker.run_once())
        await started.wait()
        await asyncio.sleep(0.08)
        async with database.session() as session:
            stolen = await DurableQueue(session).claim(
                worker_id="worker-b",
                lease_duration=timedelta(seconds=1),
                allowed_task_types=frozenset({"slow"}),
            )
        assert stolen is None
        assert await work is True

        async with database.session() as session:
            persisted = await session.get(ResearchTask, task.id)
            assert persisted is not None
            assert persisted.status == "SUCCEEDED"
            assert persisted.attempt_count == 1
    finally:
        await _cancel_run(database, run.id)
        await database.dispose()


@pytest.mark.postgres
async def test_worker_classifies_handler_exception_and_releases_lease(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    run, task = await _create_worker_task(database)

    async def failing_handler(_task: ResearchTask) -> dict[str, object]:
        raise ConnectionError("sensitive upstream detail")

    worker = Worker(
        database,
        worker_id="worker-a",
        handlers={"slow": failing_handler},
    )
    try:
        assert await worker.run_once() is True
        async with database.session() as session:
            persisted = await session.get(ResearchTask, task.id)
            assert persisted is not None
            assert persisted.status == "PENDING"
            assert persisted.retry_class == "TRANSIENT_NETWORK"
            assert persisted.last_error == "ConnectionError"
            assert persisted.lease_owner is None
    finally:
        await _cancel_run(database, run.id)
        await database.dispose()
