from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from gapforge.queue.control import AgentCallLimiter, DurableAgentCallAdmission, RunController
from gapforge.queue.repository import DurableQueue
from gapforge.queue.retry import ErrorKind, classify_error, retry_delay_seconds
from gapforge.storage.database import Database
from gapforge.storage.models import ResearchRun, ResearchTask
from gapforge.storage.uow import SqlAlchemyUnitOfWork


async def _create_running_run(
    database: Database,
    *,
    deadline_at: datetime | None = None,
    budget_limit: int = 2,
) -> ResearchRun:
    async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
        _, revision = await uow.missions.create_with_revision(
            title=f"Queue mission {uuid4()}",
            mission_text="test durable work",
            original_language="en",
            output_locale="en",
        )
        run = ResearchRun(
            mission_revision_id=revision.id,
            mode="HUNT",
            status="RUNNING",
            priority=1,
            deadline_at=deadline_at or datetime.now(UTC) + timedelta(minutes=30),
            budget_limits={"max_agent_calls_per_run": budget_limit},
            budget_used={"agent_calls": 0},
            warnings=[],
            last_checkpoint={},
        )
        assert uow.session is not None
        uow.session.add(run)
        await uow.commit()
        return run


async def _finish_run(database: Database, run_id: UUID) -> None:
    async with database.session() as session:
        run = await session.get(ResearchRun, run_id)
        if run is not None and run.status in {"QUEUED", "RUNNING"}:
            run.status = "CANCELLED"
            run.completed_at = datetime.now(UTC)
            await session.commit()


def test_retry_classification_is_bounded_and_deterministic() -> None:
    first = classify_error(ErrorKind.TRANSIENT_NETWORK, attempt=2, seed="task-1")
    same = classify_error(ErrorKind.TRANSIENT_NETWORK, attempt=2, seed="task-1")
    auth = classify_error(ErrorKind.AUTH_REQUIRED, attempt=1, seed="task-1")

    assert first == same
    assert first.retryable is True
    assert 2 <= first.delay_seconds <= 2.5
    assert auth.retryable is False
    assert auth.delay_seconds == 0
    assert retry_delay_seconds(20, "task-1") == 60
    with pytest.raises(ValueError, match="at least 1"):
        retry_delay_seconds(0, "task-1")


@pytest.mark.postgres
async def test_enqueue_suppresses_duplicate_idempotency_key(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    run = await _create_running_run(database)
    try:
        async with database.session() as session:
            queue = DurableQueue(session)
            first, first_created = await queue.enqueue(
                run_id=run.id,
                task_type="collect",
                idempotency_key="source:page:1",
                payload={"page": 1},
            )
            second, second_created = await queue.enqueue(
                run_id=run.id,
                task_type="collect",
                idempotency_key="source:page:1",
                payload={"page": 999},
            )
            await session.commit()

        assert first_created is True
        assert second_created is False
        assert first.id == second.id
        assert second.payload == {"page": 1}
    finally:
        await _finish_run(database, run.id)
        await database.dispose()


@pytest.mark.postgres
async def test_expired_lease_reclaims_checkpoint_without_duplicate_effect(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    run = await _create_running_run(database)
    started = datetime.now(UTC)
    try:
        async with database.session() as session:
            task, _ = await DurableQueue(session).enqueue(
                run_id=run.id,
                task_type="extract",
                idempotency_key="extract:batch:1",
                payload={"batch": 1},
                available_at=started,
            )
            await session.commit()

        async with database.session() as session:
            queue = DurableQueue(session)
            claimed = await queue.claim(
                worker_id="worker-a",
                lease_duration=timedelta(seconds=30),
                now=started,
            )
            assert claimed is not None
            await queue.checkpoint(
                task.id,
                worker_id="worker-a",
                checkpoint={"last_evidence_id": "ev-9"},
                lease_duration=timedelta(seconds=30),
                now=started,
            )
            await session.commit()

        async with database.session() as session:
            assert (
                await DurableQueue(session).claim(
                    worker_id="worker-b",
                    lease_duration=timedelta(seconds=30),
                    now=started + timedelta(seconds=29),
                )
                is None
            )

        async with database.session() as session:
            reclaimed = await DurableQueue(session).claim(
                worker_id="worker-b",
                lease_duration=timedelta(seconds=30),
                now=started + timedelta(seconds=31),
            )
            assert reclaimed is not None
            assert reclaimed.id == task.id
            assert reclaimed.attempt_count == 2
            assert reclaimed.checkpoint == {"last_evidence_id": "ev-9"}
            await DurableQueue(session).succeed(
                task.id,
                worker_id="worker-b",
                result={"persisted": True},
                now=started + timedelta(seconds=32),
            )
            await session.commit()
    finally:
        await _finish_run(database, run.id)
        await database.dispose()


@pytest.mark.postgres
async def test_skip_locked_claims_distinct_tasks(migrated_postgres_url: str) -> None:
    database = Database.from_url(migrated_postgres_url)
    run = await _create_running_run(database)
    try:
        async with database.session() as session:
            queue = DurableQueue(session)
            await queue.enqueue(
                run_id=run.id,
                task_type="collect",
                idempotency_key="first",
                payload={},
            )
            await queue.enqueue(
                run_id=run.id,
                task_type="collect",
                idempotency_key="second",
                payload={},
            )
            await session.commit()

        async with database.session() as first_session, database.session() as second_session:
            first = await DurableQueue(first_session).claim(
                worker_id="worker-a", lease_duration=timedelta(minutes=1)
            )
            second = await DurableQueue(second_session).claim(
                worker_id="worker-b", lease_duration=timedelta(minutes=1)
            )
            assert first is not None
            assert second is not None
            assert first.id != second.id
            await first_session.rollback()
            await second_session.rollback()
    finally:
        await _finish_run(database, run.id)
        await database.dispose()


@pytest.mark.postgres
async def test_budget_exhaustion_checkpoints_without_incrementing(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    run = await _create_running_run(database, budget_limit=1)
    try:
        async with database.session() as session:
            controller = RunController(session)
            first = await controller.consume_budget(
                run.id,
                counter="agent_calls",
                limit_key="max_agent_calls_per_run",
            )
            await session.commit()
        assert first.allowed is True
        assert first.used == 1

        async with database.session() as session:
            second = await RunController(session).consume_budget(
                run.id,
                counter="agent_calls",
                limit_key="max_agent_calls_per_run",
            )
            await session.commit()
        assert second.allowed is False
        assert second.used == 1

        async with database.session() as session:
            persisted = await session.get(ResearchRun, run.id)
            assert persisted is not None
            assert persisted.status == "BUDGET_EXHAUSTED"
            assert persisted.budget_used == {"agent_calls": 1}
            assert persisted.last_checkpoint["reason"] == "budget"
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_budget_consumption_rejects_terminal_run_without_overwriting_status(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    run = await _create_running_run(database)
    terminal_time = datetime.now(UTC)
    async with database.session() as session:
        persisted = await session.get(ResearchRun, run.id)
        assert persisted is not None
        persisted.status = "CANCELLED"
        persisted.completed_at = terminal_time
        persisted.deadline_at = terminal_time - timedelta(seconds=1)
        await session.commit()
    try:
        async with database.session() as session:
            with pytest.raises(ValueError, match="status CANCELLED"):
                await RunController(session).consume_budget(
                    run.id,
                    counter="agent_calls",
                    limit_key="max_agent_calls_per_run",
                    now=terminal_time,
                )
            await session.rollback()
        async with database.session() as session:
            persisted = await session.get(ResearchRun, run.id)
            assert persisted is not None
            assert persisted.status == "CANCELLED"
            assert persisted.budget_used == {"agent_calls": 0}
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_deadline_prevents_claim_and_marks_run_exhausted(
    migrated_postgres_url: str,
) -> None:
    deadline = datetime.now(UTC) + timedelta(minutes=1)
    database = Database.from_url(migrated_postgres_url)
    run = await _create_running_run(database, deadline_at=deadline)
    try:
        async with database.session() as session:
            await DurableQueue(session).enqueue(
                run_id=run.id,
                task_type="collect",
                idempotency_key="late",
                payload={},
            )
            await session.commit()

        async with database.session() as session:
            claimed = await DurableQueue(session).claim(
                worker_id="worker-a",
                lease_duration=timedelta(minutes=1),
                now=deadline + timedelta(seconds=1),
            )
            assert claimed is None
            await session.commit()
        async with database.session() as session:
            persisted = await session.get(ResearchRun, run.id)
            assert persisted is not None
            assert persisted.status == "BUDGET_EXHAUSTED"
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_task_lease_never_extends_past_run_deadline(
    migrated_postgres_url: str,
) -> None:
    started = datetime.now(UTC)
    deadline = started + timedelta(seconds=10)
    database = Database.from_url(migrated_postgres_url)
    run = await _create_running_run(database, deadline_at=deadline)
    try:
        async with database.session() as session:
            await DurableQueue(session).enqueue(
                run_id=run.id,
                task_type="collect",
                idempotency_key="bounded-lease",
                payload={},
                available_at=started,
            )
            await session.commit()
        async with database.session() as session:
            task = await DurableQueue(session).claim(
                worker_id="worker-a",
                lease_duration=timedelta(minutes=5),
                now=started,
            )
            assert task is not None
            assert task.lease_expires_at == deadline
            await session.rollback()
    finally:
        await _finish_run(database, run.id)
        await database.dispose()


@pytest.mark.postgres
async def test_checkpoint_and_completion_after_deadline_preserve_partial_work(
    migrated_postgres_url: str,
) -> None:
    started = datetime.now(UTC)
    deadline = started + timedelta(seconds=10)
    database = Database.from_url(migrated_postgres_url)
    run = await _create_running_run(database, deadline_at=deadline)
    try:
        async with database.session() as session:
            task, _ = await DurableQueue(session).enqueue(
                run_id=run.id,
                task_type="extract",
                idempotency_key="deadline-checkpoint",
                payload={},
                available_at=started,
            )
            await session.commit()
        async with database.session() as session:
            queue = DurableQueue(session)
            claimed = await queue.claim(
                worker_id="worker-a",
                lease_duration=timedelta(minutes=5),
                now=started,
            )
            assert claimed is not None
            expired = await queue.checkpoint(
                task.id,
                worker_id="worker-a",
                checkpoint={"last_evidence_id": "evidence-7"},
                lease_duration=timedelta(minutes=5),
                now=deadline + timedelta(milliseconds=1),
            )
            await session.commit()
            assert expired.status == "PENDING"

        async with database.session() as session:
            persisted_run = await session.get(ResearchRun, run.id)
            persisted_task = await session.get(ResearchTask, task.id)
            assert persisted_run is not None
            assert persisted_task is not None
            assert persisted_run.status == "BUDGET_EXHAUSTED"
            assert persisted_run.last_checkpoint["expired_by"] == "task_checkpoint"
            assert persisted_task.status == "PENDING"
            assert persisted_task.checkpoint == {"last_evidence_id": "evidence-7"}
            assert persisted_task.result is None
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_completion_one_millisecond_after_deadline_cannot_succeed(
    migrated_postgres_url: str,
) -> None:
    started = datetime.now(UTC)
    deadline = started + timedelta(seconds=10)
    database = Database.from_url(migrated_postgres_url)
    run = await _create_running_run(database, deadline_at=deadline)
    try:
        async with database.session() as session:
            task, _ = await DurableQueue(session).enqueue(
                run_id=run.id,
                task_type="extract",
                idempotency_key="late-completion",
                payload={},
                available_at=started,
            )
            await session.commit()
        async with database.session() as session:
            queue = DurableQueue(session)
            claimed = await queue.claim(
                worker_id="worker-a",
                lease_duration=timedelta(minutes=5),
                now=started,
            )
            assert claimed is not None
            completed = await queue.succeed(
                task.id,
                worker_id="worker-a",
                result={"must_not_persist": True},
                now=deadline + timedelta(milliseconds=1),
            )
            await session.commit()
            assert completed.status == "PENDING"

        async with database.session() as session:
            persisted_run = await session.get(ResearchRun, run.id)
            persisted_task = await session.get(ResearchTask, task.id)
            assert persisted_run is not None
            assert persisted_task is not None
            assert persisted_run.status == "BUDGET_EXHAUSTED"
            assert persisted_run.last_checkpoint["expired_by"] == "task_completion"
            assert persisted_task.status == "PENDING"
            assert persisted_task.result is None
            assert persisted_task.completed_at is None
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_cancelled_run_rejects_stale_heartbeat_without_status_overwrite(
    migrated_postgres_url: str,
) -> None:
    started = datetime.now(UTC)
    deadline = started + timedelta(seconds=10)
    database = Database.from_url(migrated_postgres_url)
    run = await _create_running_run(database, deadline_at=deadline)
    try:
        async with database.session() as session:
            task, _ = await DurableQueue(session).enqueue(
                run_id=run.id,
                task_type="extract",
                idempotency_key="cancelled-run-heartbeat",
                payload={},
                available_at=started,
            )
            await session.commit()
        async with database.session() as session:
            claimed = await DurableQueue(session).claim(
                worker_id="worker-a",
                lease_duration=timedelta(minutes=5),
                now=started,
            )
            assert claimed is not None
            await session.commit()
        async with database.session() as session:
            persisted_run = await session.get(ResearchRun, run.id)
            assert persisted_run is not None
            persisted_run.status = "CANCELLED"
            persisted_run.completed_at = started + timedelta(seconds=1)
            await session.commit()
        async with database.session() as session:
            with pytest.raises(PermissionError, match="no longer active"):
                await DurableQueue(session).renew_lease(
                    task.id,
                    worker_id="worker-a",
                    lease_duration=timedelta(minutes=5),
                    now=deadline + timedelta(milliseconds=1),
                )
            await session.rollback()
        async with database.session() as session:
            persisted_run = await session.get(ResearchRun, run.id)
            assert persisted_run is not None
            assert persisted_run.status == "CANCELLED"
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_queue_poll_expires_overdue_run_with_pending_work(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    deadline = datetime.now(UTC) - timedelta(seconds=1)
    run = await _create_running_run(database, deadline_at=deadline)
    try:
        async with database.session() as session:
            task, _ = await DurableQueue(session).enqueue(
                run_id=run.id,
                task_type="collect",
                idempotency_key="preserve-on-deadline",
                payload={"round": 1},
            )
            await session.commit()

        async with database.session() as session:
            claimed = await DurableQueue(session).claim(
                worker_id="worker-a",
                lease_duration=timedelta(minutes=1),
            )
            await session.commit()
        assert claimed is None

        async with database.session() as session:
            persisted_run = await session.get(ResearchRun, run.id)
            persisted_task = await session.get(ResearchTask, task.id)
            assert persisted_run is not None
            assert persisted_task is not None
            assert persisted_run.status == "BUDGET_EXHAUSTED"
            assert persisted_run.last_checkpoint == {
                "reason": "deadline",
                "expired_by": "queue_poll",
            }
            assert persisted_task.status == "PENDING"
            assert persisted_task.payload == {"round": 1}
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_durable_provider_admission_caps_and_reclaims_across_sessions(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    run = await _create_running_run(database)
    started = datetime.now(UTC)
    lease_ids: list[UUID] = []
    try:
        for number in (1, 2):
            async with database.session() as session:
                lease = await DurableAgentCallAdmission(session).acquire(
                    run.id,
                    call_key=f"call-{number}",
                    lease_owner=f"worker-{number}",
                    lease_duration=timedelta(seconds=30),
                    now=started,
                )
                assert lease is not None
                lease_ids.append(lease.id)
                await session.commit()

        async with database.session() as session:
            denied = await DurableAgentCallAdmission(session).acquire(
                run.id,
                call_key="call-3",
                lease_owner="worker-3",
                lease_duration=timedelta(seconds=30),
                now=started,
            )
            assert denied is None

        async with database.session() as session:
            reclaimed = await DurableAgentCallAdmission(session).acquire(
                run.id,
                call_key="call-3",
                lease_owner="worker-3",
                lease_duration=timedelta(seconds=30),
                now=started + timedelta(seconds=31),
            )
            assert reclaimed is not None
            await DurableAgentCallAdmission(session).release(reclaimed.id, lease_owner="worker-3")
            await session.commit()
    finally:
        await _finish_run(database, run.id)
        await database.dispose()


@pytest.mark.postgres
async def test_provider_call_lease_never_extends_past_run_deadline(
    migrated_postgres_url: str,
) -> None:
    started = datetime.now(UTC)
    deadline = started + timedelta(seconds=10)
    database = Database.from_url(migrated_postgres_url)
    run = await _create_running_run(database, deadline_at=deadline)
    try:
        async with database.session() as session:
            lease = await DurableAgentCallAdmission(session).acquire(
                run.id,
                call_key="bounded-call",
                lease_owner="worker-a",
                lease_duration=timedelta(minutes=5),
                now=started,
            )
            assert lease is not None
            assert lease.lease_expires_at == deadline
            await session.rollback()
    finally:
        await _finish_run(database, run.id)
        await database.dispose()


@pytest.mark.postgres
async def test_failed_task_finishes_run_with_warnings(migrated_postgres_url: str) -> None:
    database = Database.from_url(migrated_postgres_url)
    run = await _create_running_run(database)
    try:
        async with database.session() as session:
            queue = DurableQueue(session)
            await queue.enqueue(
                run_id=run.id,
                task_type="collect",
                idempotency_key="unavailable",
                payload={},
                max_attempts=1,
            )
            await session.commit()

        async with database.session() as session:
            queue = DurableQueue(session)
            task = await queue.claim(worker_id="worker-a", lease_duration=timedelta(minutes=1))
            assert task is not None
            await queue.fail(
                task.id,
                worker_id="worker-a",
                decision=classify_error(ErrorKind.SOURCE_UNAVAILABLE, attempt=1, seed=str(task.id)),
                sanitized_error="source unavailable",
            )
            completed = await RunController(session).finalize_if_idle(run.id)
            await session.commit()
        assert completed.status == "COMPLETED_WITH_WARNINGS"
        assert completed.warnings == [{"failed_tasks": 1}]
    finally:
        await database.dispose()


@pytest.mark.asyncio
async def test_agent_call_limiter_allows_at_most_two() -> None:
    limiter = AgentCallLimiter(max_parallel=2)
    run_id = uuid4()
    active = 0
    maximum = 0

    async def work() -> None:
        nonlocal active, maximum
        async with limiter.for_run(run_id):
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0)
            active -= 1

    await asyncio.gather(*(work() for _ in range(5)))

    assert maximum == 2


def test_invalid_agent_call_limit_is_rejected() -> None:
    with pytest.raises(ValueError, match="positive"):
        AgentCallLimiter(max_parallel=0)
