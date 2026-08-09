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


@pytest.mark.postgres
async def test_worker_deadline_cancels_handler_and_preserves_pending_task(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    run, task = await _create_worker_task(database)
    async with database.session() as session:
        persisted_run = await session.get(ResearchRun, run.id)
        assert persisted_run is not None
        persisted_run.deadline_at = datetime.now(UTC) + timedelta(seconds=0.08)
        await session.commit()
    cancelled = asyncio.Event()

    async def handler(_task: ResearchTask) -> dict[str, object]:
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return {"unexpected": True}

    worker = Worker(
        database,
        worker_id="worker-deadline",
        handlers={"slow": handler},
        lease_duration=timedelta(seconds=1),
        heartbeat_interval_seconds=0.02,
    )
    try:
        assert await worker.run_once() is True
        assert cancelled.is_set()
        async with database.session() as session:
            persisted_run = await session.get(ResearchRun, run.id)
            persisted_task = await session.get(ResearchTask, task.id)
            assert persisted_run is not None
            assert persisted_task is not None
            assert persisted_run.status == "BUDGET_EXHAUSTED"
            assert persisted_run.last_checkpoint["expired_by"] == "worker_heartbeat"
            assert persisted_task.status == "PENDING"
            assert persisted_task.lease_owner is None
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_worker_lost_lease_does_not_mutate_as_stale_owner(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    run, task = await _create_worker_task(database)
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def handler(_task: ResearchTask) -> dict[str, object]:
        started.set()
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return {"unexpected": True}

    worker = Worker(
        database,
        worker_id="worker-a",
        handlers={"slow": handler},
        lease_duration=timedelta(seconds=1),
        heartbeat_interval_seconds=0.05,
    )
    try:
        work = asyncio.create_task(worker.run_once())
        await started.wait()
        async with database.session() as session:
            persisted = await session.get(ResearchTask, task.id)
            assert persisted is not None
            persisted.lease_owner = "worker-b"
            await session.commit()
        assert await work is True
        assert cancelled.is_set()
        async with database.session() as session:
            persisted = await session.get(ResearchTask, task.id)
            assert persisted is not None
            assert persisted.status == "LEASED"
            assert persisted.lease_owner == "worker-b"
            persisted.status = "PENDING"
            persisted.lease_owner = None
            persisted.lease_expires_at = None
            await session.commit()
    finally:
        await _cancel_run(database, run.id)
        await database.dispose()


@pytest.mark.postgres
async def test_continuous_worker_can_idle_until_stopped(migrated_postgres_url: str) -> None:
    database = Database.from_url(migrated_postgres_url)
    polls = 0

    def stop() -> bool:
        nonlocal polls
        polls += 1
        return polls > 1

    worker = Worker(database, worker_id="idle-worker", handlers={})
    try:
        await worker.run_forever(poll_interval_seconds=0.001, stop=stop)
        assert polls == 2
    finally:
        await database.dispose()
