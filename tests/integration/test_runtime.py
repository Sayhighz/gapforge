from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from gapforge.queue.control import RunController
from gapforge.runtime import (
    ROOT_TASK_TYPE,
    RunBudgetLimits,
    RunScheduler,
    RunScheduleRequest,
    ScheduledRun,
)
from gapforge.storage.database import Database
from gapforge.storage.models import ResearchRun
from gapforge.storage.uow import SqlAlchemyUnitOfWork


async def _cancel_run(database: Database, run_id: UUID | None) -> None:
    if run_id is None:
        return
    async with database.session() as session:
        run = await session.get(ResearchRun, run_id)
        if run is not None and run.status in {"QUEUED", "RUNNING"}:
            run.status = "CANCELLED"
            run.completed_at = datetime.now(UTC)
            await session.commit()


@pytest.mark.parametrize(
    "overrides",
    [
        {"max_research_rounds": 3},
        {"max_parallel_agent_calls": 3},
        {"max_agent_calls_per_run": 1, "max_parallel_agent_calls": 2},
        {"initial_lookback_days": 1, "monitor_overlap_hours": 25},
    ],
)
def test_run_budget_boundary_matches_settings_invariants(overrides: dict[str, int]) -> None:
    with pytest.raises(ValidationError):
        RunBudgetLimits.model_validate(overrides)


@pytest.mark.postgres
async def test_schedule_creates_queued_run_with_atomic_root_task(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    scheduled_at = datetime(2026, 8, 9, 5, tzinfo=UTC)
    run_id: UUID | None = None
    try:
        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            _, revision = await uow.missions.create_with_revision(
                title="Atomic runtime mission",
                mission_text="persist the run and root work together",
                original_language="en",
                output_locale="en",
            )
            assert uow.session is not None
            scheduled = await RunScheduler(uow.session).schedule(
                request=RunScheduleRequest(
                    mission_revision_id=revision.id,
                    mode="HUNT",
                    priority=0,
                    budget_limits={"max_run_duration_minutes": 30},
                ),
                now=scheduled_at,
            )
            await uow.commit()
            run_id = scheduled.run.id

        assert scheduled.created is True
        assert scheduled.run.status == "QUEUED"
        assert scheduled.task.run_id == scheduled.run.id
        assert scheduled.task.task_type == ROOT_TASK_TYPE
        assert scheduled.task.idempotency_key == "run-root:v1"
        assert scheduled.task.payload == {
            "schema_version": "1.0",
            "run_id": str(scheduled.run.id),
            "mission_revision_id": str(revision.id),
            "mode": "HUNT",
        }
    finally:
        await _cancel_run(database, run_id)
        await database.dispose()


@pytest.mark.postgres
async def test_duplicate_schedule_reuses_active_run_and_root_task(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    run_id: UUID | None = None
    try:
        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            _, revision = await uow.missions.create_with_revision(
                title="Idempotent runtime mission",
                mission_text="reuse durable root work",
                original_language="en",
                output_locale="en",
            )
            await uow.commit()

        async with database.session() as session:
            first = await RunScheduler(session).schedule(
                request=RunScheduleRequest(
                    mission_revision_id=revision.id,
                    mode="HUNT",
                    priority=0,
                    budget_limits={"max_run_duration_minutes": 30},
                ),
            )
            await session.commit()
            run_id = first.run.id

        async with database.session() as session:
            duplicate = await RunScheduler(session).schedule(
                request=RunScheduleRequest(
                    mission_revision_id=revision.id,
                    mode="HUNT",
                    priority=0,
                    budget_limits={"max_run_duration_minutes": 30},
                ),
            )
            await session.commit()

        assert duplicate.created is False
        assert duplicate.run.id == first.run.id
        assert duplicate.task.id == first.task.id
    finally:
        await _cancel_run(database, run_id)
        await database.dispose()


@pytest.mark.postgres
async def test_concurrent_schedule_creates_one_run_and_root_task(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    run_id: UUID | None = None
    try:
        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            _, revision = await uow.missions.create_with_revision(
                title="Concurrent scheduler mission",
                mission_text="deduplicate simultaneous schedule requests",
                original_language="en",
                output_locale="en",
            )
            await uow.commit()

        async def schedule_once() -> ScheduledRun:
            async with database.session() as session:
                result = await RunScheduler(session).schedule(
                    request=RunScheduleRequest(
                        mission_revision_id=revision.id,
                        mode="HUNT",
                        priority=0,
                        budget_limits={"max_run_duration_minutes": 30},
                    )
                )
                await session.commit()
                return result

        first, second = await asyncio.gather(schedule_once(), schedule_once())
        run_id = first.run.id
        assert second.run.id == run_id
        assert first.task.id == second.task.id
        assert {first.created, second.created} == {True, False}
    finally:
        await _cancel_run(database, run_id)
        await database.dispose()


@pytest.mark.postgres
async def test_admission_starts_full_duration_after_queue_delay(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    queued_at = datetime(2026, 8, 9, 1, tzinfo=UTC)
    admitted_at = datetime(2026, 8, 9, 3, tzinfo=UTC)
    run_id: UUID | None = None
    try:
        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            _, revision = await uow.missions.create_with_revision(
                title="Delayed admission mission",
                mission_text="the queue delay must not consume runtime",
                original_language="en",
                output_locale="en",
            )
            assert uow.session is not None
            scheduled = await RunScheduler(uow.session).schedule(
                request=RunScheduleRequest(
                    mission_revision_id=revision.id,
                    mode="HUNT",
                    priority=0,
                    budget_limits={"max_run_duration_minutes": 30},
                ),
                now=queued_at,
            )
            await uow.commit()
            run_id = scheduled.run.id

        async with database.session() as session:
            admitted = await RunController(session).admit_next(
                allowed_task_types=frozenset({ROOT_TASK_TYPE}),
                now=admitted_at,
            )
            await session.commit()

        assert admitted is not None
        assert admitted.id == run_id
        assert admitted.status == "RUNNING"
        assert admitted.started_at == admitted_at
        assert admitted.deadline_at == datetime(2026, 8, 9, 3, 30, tzinfo=UTC)
    finally:
        await _cancel_run(database, run_id)
        await database.dispose()


@pytest.mark.postgres
async def test_concurrent_admission_promotes_only_one_global_run(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    run_ids: list[UUID] = []
    admitted = asyncio.Event()
    release = asyncio.Event()
    try:
        for number in range(2):
            async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
                _, revision = await uow.missions.create_with_revision(
                    title=f"Concurrent admission mission {number}",
                    mission_text="serialize the global running slot",
                    original_language="en",
                    output_locale="en",
                )
                assert uow.session is not None
                scheduled = await RunScheduler(uow.session).schedule(
                    request=RunScheduleRequest(
                        mission_revision_id=revision.id,
                        mode="HUNT",
                        priority=number,
                        budget_limits={"max_run_duration_minutes": 30},
                    )
                )
                await uow.commit()
                run_ids.append(scheduled.run.id)

        async def hold_first_admission() -> ResearchRun | None:
            async with database.session() as session:
                result = await RunController(session).admit_next(
                    allowed_task_types=frozenset({ROOT_TASK_TYPE})
                )
                admitted.set()
                await release.wait()
                await session.commit()
                return result

        first_work = asyncio.create_task(hold_first_admission())
        await admitted.wait()
        async with database.session() as session:
            second = await RunController(session).admit_next(
                allowed_task_types=frozenset({ROOT_TASK_TYPE})
            )
            await session.commit()
        release.set()
        first = await first_work

        assert first is not None
        assert second is None
        async with database.session() as session:
            statuses = {
                run.id: run.status
                for run in (
                    await session.scalars(select(ResearchRun).where(ResearchRun.id.in_(run_ids)))
                ).all()
            }
        assert tuple(statuses.values()).count("RUNNING") == 1
        assert tuple(statuses.values()).count("QUEUED") == 1
    finally:
        release.set()
        for run_id in run_ids:
            await _cancel_run(database, run_id)
        await database.dispose()


@pytest.mark.postgres
async def test_admission_does_not_start_or_repair_unsupported_graphs(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    queued_at = datetime(2026, 8, 9, 1, tzinfo=UTC)
    run_ids: list[UUID] = []
    try:
        for number in range(2):
            async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
                _, revision = await uow.missions.create_with_revision(
                    title=f"Invalid graph mission {number}",
                    mission_text="unsupported graphs must remain queued",
                    original_language="en",
                    output_locale="en",
                )
                assert uow.session is not None
                if number == 0:
                    scheduled = await RunScheduler(uow.session).schedule(
                        request=RunScheduleRequest(
                            mission_revision_id=revision.id,
                            mode="HUNT",
                            priority=number,
                            budget_limits={"max_run_duration_minutes": 30},
                        ),
                        now=queued_at,
                    )
                    scheduled.task.status = "SUCCEEDED"
                    scheduled.task.completed_at = queued_at
                    run = scheduled.run
                else:
                    run = ResearchRun(
                        mission_revision_id=revision.id,
                        mode="HUNT",
                        status="QUEUED",
                        priority=number,
                        deadline_at=queued_at,
                        budget_limits=RunBudgetLimits().model_dump(mode="json"),
                        budget_used={},
                        warnings=[],
                        last_checkpoint={},
                    )
                    uow.session.add(run)
                await uow.commit()
                run_ids.append(run.id)

        async with database.session() as session:
            admitted = await RunController(session).admit_next(
                allowed_task_types=frozenset({ROOT_TASK_TYPE}),
                now=queued_at.replace(hour=3),
            )
            await session.commit()

        assert admitted is None
        async with database.session() as session:
            runs = (
                await session.scalars(select(ResearchRun).where(ResearchRun.id.in_(run_ids)))
            ).all()
            assert {run.status for run in runs} == {"QUEUED"}
            assert {run.deadline_at for run in runs} == {queued_at}
    finally:
        for run_id in run_ids:
            await _cancel_run(database, run_id)
        await database.dispose()
