from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from gapforge.domain.contracts import RunWarning
from gapforge.integration.mappers import research_run_from_storage, run_warning_to_storage
from gapforge.queue.control import RunController
from gapforge.queue.repository import DurableQueue
from gapforge.queue.retry import ErrorKind
from gapforge.runtime import (
    ROOT_TASK_TYPE,
    InvalidRunGraphError,
    RunBudgetLimits,
    RunScheduler,
    RunScheduleRequest,
    ScheduledRun,
)
from gapforge.storage.database import Database
from gapforge.storage.models import ResearchRun, ResearchTask
from gapforge.storage.uow import SqlAlchemyUnitOfWork
from gapforge.worker import TaskHandlerError, TaskHandlerRegistry, TaskHandlerResult, Worker


async def _cancel_run(database: Database, run_id: UUID | None) -> None:
    if run_id is None:
        return
    async with database.session() as session:
        run = await session.get(ResearchRun, run_id)
        if run is not None and run.status in {"QUEUED", "RUNNING"}:
            run.status = "CANCELLED"
            run.completed_at = datetime.now(UTC)
            await session.commit()


async def _schedule_run(
    database: Database,
    *,
    title: str,
    now: datetime | None = None,
) -> ScheduledRun:
    async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
        _, revision = await uow.missions.create_with_revision(
            title=title,
            mission_text="exercise the integrated durable runtime",
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
            now=now,
        )
        await uow.commit()
        return scheduled


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


@pytest.mark.parametrize(
    "invalid",
    [
        {"payload": []},
        {"payload": {}, "useful_artifact": "yes"},
        {"payload": {"unsupported": {"set"}}},
        {"payload": {"not_finite": float("nan")}},
        pytest.param({"payload": {"too_large": "x" * 1_000_001}}, id="oversized"),
    ],
)
def test_task_handler_result_enforces_payload_and_useful_signal(
    invalid: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        TaskHandlerResult.model_validate(invalid)


def test_task_handler_registry_rejects_non_callable_handler() -> None:
    with pytest.raises(ValueError, match="must be callable"):
        TaskHandlerRegistry({ROOT_TASK_TYPE: 42})  # type: ignore[dict-item]


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
async def test_duplicate_schedule_does_not_fabricate_missing_root_task(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    run_id: UUID | None = None
    try:
        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            _, revision = await uow.missions.create_with_revision(
                title="Corrupted scheduler graph mission",
                mission_text="duplicate scheduling must fail closed",
                original_language="en",
                output_locale="en",
            )
            assert uow.session is not None
            run = ResearchRun(
                mission_revision_id=revision.id,
                mode="HUNT",
                status="QUEUED",
                priority=0,
                deadline_at=datetime.now(UTC),
                budget_limits=RunBudgetLimits().model_dump(mode="json"),
                budget_used={},
                warnings=[],
                last_checkpoint={},
            )
            uow.session.add(run)
            await uow.commit()
            run_id = run.id

        async with database.session() as session:
            with pytest.raises(InvalidRunGraphError):
                await RunScheduler(session).schedule(
                    request=RunScheduleRequest(
                        mission_revision_id=revision.id,
                        mode="HUNT",
                        priority=0,
                        budget_limits={"max_run_duration_minutes": 30},
                    )
                )
            await session.rollback()

        async with database.session() as session:
            root_count = await session.scalar(
                select(func.count()).select_from(ResearchTask).where(ResearchTask.run_id == run_id)
            )
            assert root_count == 0
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


@pytest.mark.postgres
async def test_worker_with_no_handlers_leaves_supported_work_queued(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    scheduled = await _schedule_run(database, title="No-handler safety mission")
    try:
        worker = Worker(
            database,
            worker_id="empty-worker",
            handlers=TaskHandlerRegistry(),
        )
        assert await worker.run_once() is False

        async with database.session() as session:
            run = await session.get(ResearchRun, scheduled.run.id)
            task = await session.get(ResearchTask, scheduled.task.id)
            assert run is not None
            assert task is not None
            assert run.status == "QUEUED"
            assert task.status == "PENDING"
    finally:
        await _cancel_run(database, scheduled.run.id)
        await database.dispose()


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("existing_runtime", "expected_runtime"),
    [
        ({"stage": "collected"}, {"stage": "collected", "useful_artifact": True}),
        ("invalid-metadata", {"useful_artifact": True}),
    ],
)
async def test_worker_admits_executes_and_finalizes_successful_root(
    migrated_postgres_url: str,
    existing_runtime: object,
    expected_runtime: dict[str, object],
) -> None:
    database = Database.from_url(migrated_postgres_url)
    scheduled = await _schedule_run(database, title="Successful runtime mission")

    async with database.session() as session:
        task = await session.get(ResearchTask, scheduled.task.id)
        assert task is not None
        task.checkpoint = {"runtime": existing_runtime, "collector_cursor": "page-2"}
        await session.commit()

    async def handler(_task: ResearchTask) -> TaskHandlerResult:
        return TaskHandlerResult(payload={"result": "persisted"}, useful_artifact=True)

    try:
        worker = Worker(
            database,
            worker_id="success-worker",
            handlers=TaskHandlerRegistry({ROOT_TASK_TYPE: handler}),
        )
        assert await worker.run_once() is True

        async with database.session() as session:
            run = await session.get(ResearchRun, scheduled.run.id)
            task = await session.get(ResearchTask, scheduled.task.id)
            assert run is not None
            assert task is not None
            assert run.status == "COMPLETED"
            assert run.started_at is not None
            assert task.status == "SUCCEEDED"
            assert task.result == {"result": "persisted"}
            assert task.checkpoint == {
                "runtime": expected_runtime,
                "collector_cursor": "page-2",
            }
    finally:
        await _cancel_run(database, scheduled.run.id)
        await database.dispose()


@pytest.mark.postgres
async def test_worker_retries_transient_failure_then_finalizes_success(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    scheduled = await _schedule_run(database, title="Retry runtime mission")
    calls = 0

    async def handler(_task: ResearchTask) -> dict[str, object]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("do not persist this detail")
        return {"attempt": calls}

    try:
        worker = Worker(
            database,
            worker_id="retry-worker",
            handlers=TaskHandlerRegistry({ROOT_TASK_TYPE: handler}),
        )
        assert await worker.run_once() is True
        async with database.session() as session:
            run = await session.get(ResearchRun, scheduled.run.id)
            task = await session.get(ResearchTask, scheduled.task.id)
            assert run is not None
            assert task is not None
            assert run.status == "RUNNING"
            assert task.status == "PENDING"
            assert task.retry_class == "TRANSIENT_NETWORK"
            task.available_at = datetime.now(UTC) - timedelta(seconds=1)
            await session.commit()

        assert await worker.run_once() is True
        async with database.session() as session:
            run = await session.get(ResearchRun, scheduled.run.id)
            task = await session.get(ResearchTask, scheduled.task.id)
            assert run is not None
            assert task is not None
            assert run.status == "COMPLETED"
            assert task.status == "SUCCEEDED"
            assert task.attempt_count == 2
    finally:
        await _cancel_run(database, scheduled.run.id)
        await database.dispose()


@pytest.mark.postgres
@pytest.mark.parametrize(
    "invalid_result",
    [
        pytest.param(["not", "an", "object"], id="list"),
        pytest.param(object(), id="wrong-object"),
        pytest.param({"unsupported": {"set"}}, id="set"),
        pytest.param({"not_finite": float("nan")}, id="nan"),
    ],
)
async def test_worker_terminalizes_invalid_handler_result_without_stranding_lease(
    migrated_postgres_url: str,
    invalid_result: object,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    scheduled = await _schedule_run(database, title="Invalid handler output mission")

    async def handler(_task: ResearchTask) -> object:
        return invalid_result

    try:
        worker = Worker(
            database,
            worker_id="invalid-output-worker",
            handlers=TaskHandlerRegistry({ROOT_TASK_TYPE: handler}),  # type: ignore[dict-item]
        )
        assert await worker.run_once() is True

        async with database.session() as session:
            run = await session.get(ResearchRun, scheduled.run.id)
            task = await session.get(ResearchTask, scheduled.task.id)
            assert run is not None
            assert task is not None
            assert run.status == "FAILED"
            assert task.status == "FAILED"
            assert task.retry_class == "INVALID_OUTPUT"
            assert task.lease_owner is None
            assert task.lease_expires_at is None
    finally:
        await _cancel_run(database, scheduled.run.id)
        await database.dispose()


@pytest.mark.postgres
async def test_worker_reclaims_expired_lease_after_restart(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    scheduled = await _schedule_run(database, title="Restart recovery mission")
    try:
        async with database.session() as session:
            await RunController(session).admit_next(allowed_task_types=frozenset({ROOT_TASK_TYPE}))
            claimed = await DurableQueue(session).claim(
                worker_id="crashed-worker",
                lease_duration=timedelta(minutes=5),
                allowed_task_types=frozenset({ROOT_TASK_TYPE}),
            )
            assert claimed is not None
            await session.commit()
        async with database.session() as session:
            task = await session.get(ResearchTask, scheduled.task.id)
            assert task is not None
            task.lease_expires_at = datetime.now(UTC) - timedelta(milliseconds=1)
            await session.commit()

        async def handler(_task: ResearchTask) -> dict[str, object]:
            return {"reclaimed": True}

        worker = Worker(
            database,
            worker_id="replacement-worker",
            handlers=TaskHandlerRegistry({ROOT_TASK_TYPE: handler}),
        )
        assert await worker.run_once() is True

        async with database.session() as session:
            run = await session.get(ResearchRun, scheduled.run.id)
            task = await session.get(ResearchTask, scheduled.task.id)
            assert run is not None
            assert task is not None
            assert run.status == "COMPLETED"
            assert task.status == "SUCCEEDED"
            assert task.attempt_count == 2
    finally:
        await _cancel_run(database, scheduled.run.id)
        await database.dispose()


@pytest.mark.postgres
async def test_worker_finalizes_terminal_active_graph_before_next_admission(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    terminal = await _schedule_run(database, title="Terminal graph recovery mission")
    follower_id: UUID | None = None
    try:
        async with database.session() as session:
            await RunController(session).admit_next(allowed_task_types=frozenset({ROOT_TASK_TYPE}))
            task = await DurableQueue(session).claim(
                worker_id="crashed-after-write",
                lease_duration=timedelta(minutes=5),
                allowed_task_types=frozenset({ROOT_TASK_TYPE}),
            )
            assert task is not None
            await DurableQueue(session).succeed(
                task.id,
                worker_id="crashed-after-write",
                result={"persisted": True},
            )
            await session.commit()

        follower = await _schedule_run(database, title="Follower admission mission")
        follower_id = follower.run.id

        async def handler(_task: ResearchTask) -> dict[str, object]:
            return {"follower": "complete"}

        worker = Worker(
            database,
            worker_id="reconciliation-worker",
            handlers=TaskHandlerRegistry({ROOT_TASK_TYPE: handler}),
        )
        assert await worker.run_once() is True

        async with database.session() as session:
            terminal_run = await session.get(ResearchRun, terminal.run.id)
            follower_run = await session.get(ResearchRun, follower.run.id)
            assert terminal_run is not None
            assert follower_run is not None
            assert terminal_run.status == "COMPLETED"
            assert follower_run.status == "COMPLETED"
    finally:
        await _cancel_run(database, terminal.run.id)
        await _cancel_run(database, follower_id)
        await database.dispose()


@pytest.mark.postgres
async def test_worker_fails_missing_active_graph_without_fabricating_work(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    missing_id: UUID | None = None
    follower_id: UUID | None = None
    try:
        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            _, revision = await uow.missions.create_with_revision(
                title="Missing active graph mission",
                mission_text="do not fabricate missing root work",
                original_language="en",
                output_locale="en",
            )
            assert uow.session is not None
            missing = ResearchRun(
                mission_revision_id=revision.id,
                mode="HUNT",
                status="RUNNING",
                priority=0,
                started_at=datetime.now(UTC),
                deadline_at=datetime.now(UTC) + timedelta(minutes=30),
                budget_limits=RunBudgetLimits().model_dump(mode="json"),
                budget_used={},
                warnings=[],
                last_checkpoint={},
            )
            uow.session.add(missing)
            await uow.commit()
            missing_id = missing.id

        follower = await _schedule_run(database, title="Missing graph follower mission")
        follower_id = follower.run.id

        async def handler(_task: ResearchTask) -> dict[str, object]:
            return {"follower": "complete"}

        worker = Worker(
            database,
            worker_id="missing-graph-worker",
            handlers=TaskHandlerRegistry({ROOT_TASK_TYPE: handler}),
        )
        assert await worker.run_once() is True

        async with database.session() as session:
            missing_run = await session.get(ResearchRun, missing_id)
            follower_run = await session.get(ResearchRun, follower.run.id)
            assert missing_run is not None
            assert follower_run is not None
            assert missing_run.status == "FAILED"
            assert missing_run.last_checkpoint["reason"] == "missing_task_graph"
            assert follower_run.status == "COMPLETED"
            fabricated = await session.scalar(
                select(ResearchTask.id).where(ResearchTask.run_id == missing_id)
            )
            assert fabricated is None
    finally:
        await _cancel_run(database, missing_id)
        await _cancel_run(database, follower_id)
        await database.dispose()


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("kind", "expected_status"),
    [
        (ErrorKind.AUTH_REQUIRED, "AUTH_REQUIRED"),
        (ErrorKind.BUDGET_EXHAUSTED, "BUDGET_EXHAUSTED"),
        (ErrorKind.DEADLINE_EXCEEDED, "BUDGET_EXHAUSTED"),
        (ErrorKind.PERMANENT, "FAILED"),
    ],
)
async def test_worker_maps_typed_terminal_failure_to_run_status(
    migrated_postgres_url: str,
    kind: ErrorKind,
    expected_status: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    scheduled = await _schedule_run(database, title=f"Terminal {kind.value} mission")

    async def handler(_task: ResearchTask) -> dict[str, object]:
        raise TaskHandlerError(kind, error_class="source_access_failed")

    try:
        worker = Worker(
            database,
            worker_id=f"terminal-{kind.value.lower()}",
            handlers=TaskHandlerRegistry({ROOT_TASK_TYPE: handler}),
        )
        assert await worker.run_once() is True

        async with database.session() as session:
            run = await session.get(ResearchRun, scheduled.run.id)
            task = await session.get(ResearchTask, scheduled.task.id)
            assert run is not None
            assert task is not None
            assert run.status == expected_status
            assert task.status == "FAILED"
            assert task.retry_class == kind.value
            assert task.last_error == "source_access_failed"
    finally:
        await _cancel_run(database, scheduled.run.id)
        await database.dispose()


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("useful_artifact", "expected_status"),
    [(True, "COMPLETED_WITH_WARNINGS"), (False, "FAILED")],
)
async def test_worker_only_preserves_useful_partial_success_as_warning(
    migrated_postgres_url: str,
    useful_artifact: bool,
    expected_status: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    scheduled = await _schedule_run(database, title="Partial runtime mission")
    try:
        async with database.session() as session:
            _, created = await DurableQueue(session).enqueue(
                run_id=scheduled.run.id,
                task_type="research.followup",
                idempotency_key="followup:v1",
                payload={},
                priority=1,
            )
            assert created is True
            await session.commit()

        async def root_handler(_task: ResearchTask) -> TaskHandlerResult:
            return TaskHandlerResult(
                payload={"root": "complete"},
                useful_artifact=useful_artifact,
            )

        async def failing_handler(_task: ResearchTask) -> dict[str, object]:
            raise TaskHandlerError(ErrorKind.PERMANENT, error_class="followup_failed")

        worker = Worker(
            database,
            worker_id="partial-worker",
            handlers=TaskHandlerRegistry(
                {
                    ROOT_TASK_TYPE: root_handler,
                    "research.followup": failing_handler,
                }
            ),
        )
        assert await worker.run_once() is True
        assert await worker.run_once() is True

        async with database.session() as session:
            run = await session.get(ResearchRun, scheduled.run.id)
            assert run is not None
            assert run.status == expected_status
            if useful_artifact:
                assert run.warnings == [
                    {
                        "code": "PARTIAL_TASK_FAILURE",
                        "details": {
                            "failed_tasks": 1,
                            "failure_classes": ["PERMANENT"],
                            "useful_successes": 1,
                        },
                    }
                ]
                mapped = research_run_from_storage(run)
                assert mapped.warnings == (
                    RunWarning(
                        code="PARTIAL_TASK_FAILURE",
                        details={
                            "failed_tasks": 1,
                            "failure_classes": ["PERMANENT"],
                            "useful_successes": 1,
                        },
                    ),
                )
                assert run_warning_to_storage(mapped.warnings[0]) == run.warnings[0]
            else:
                assert run.warnings == []
                assert run.last_checkpoint["reason"] == "all_tasks_failed"
    finally:
        await _cancel_run(database, scheduled.run.id)
        await database.dispose()
