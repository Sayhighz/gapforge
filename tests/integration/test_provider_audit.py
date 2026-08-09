from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4, uuid5

import pytest
from sqlalchemy import func, select

from gapforge.config import AgentProviderName, Settings
from gapforge.domain import contracts as domain
from gapforge.integration.mappers import agent_request_identity, agent_schema_identity
from gapforge.integration.semantic import (
    AuditedSemanticReasoner,
    SemanticAdmissionError,
    SemanticAdmissionKind,
    SemanticCall,
    SemanticContext,
    build_semantic_reasoner,
)
from gapforge.providers import contracts as provider
from gapforge.providers.codex_cli import CodexCliProvider
from gapforge.providers.fake import FakeAgentProvider
from gapforge.queue.retry import ErrorKind
from gapforge.storage.database import Database
from gapforge.storage.models import AgentCall, ProviderCallLease, ResearchRun, ResearchTask
from gapforge.storage.uow import SqlAlchemyUnitOfWork
from gapforge.worker import TaskHandlerError, TaskHandlerRegistry, Worker


class RecordingProvider:
    def __init__(self, responses: list[provider.AgentResult]) -> None:
        self.responses = responses
        self.requests: list[provider.AgentRequest] = []

    async def run(self, request: provider.AgentRequest) -> provider.AgentResult:
        self.requests.append(request)
        return self.responses.pop(0)


class BlockingProvider:
    def __init__(self, response: provider.AgentResult) -> None:
        self.response = response
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.requests: list[provider.AgentRequest] = []

    async def run(self, request: provider.AgentRequest) -> provider.AgentResult:
        self.requests.append(request)
        self.started.set()
        await self.release.wait()
        return self.response


class CountingBlockingProvider(BlockingProvider):
    def __init__(self, response: provider.AgentResult, *, expected_started: int) -> None:
        super().__init__(response)
        self.expected_started = expected_started

    async def run(self, request: provider.AgentRequest) -> provider.AgentResult:
        self.requests.append(request)
        if len(self.requests) >= self.expected_started:
            self.started.set()
        await self.release.wait()
        return self.response


class RaisingProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def run(self, request: provider.AgentRequest) -> provider.AgentResult:
        del request
        self.calls += 1
        raise RuntimeError("untrusted provider exception text must not persist")


class CancellationRecordingProvider(BlockingProvider):
    def __init__(self, response: provider.AgentResult) -> None:
        super().__init__(response)
        self.cancelled = asyncio.Event()

    async def run(self, request: provider.AgentRequest) -> provider.AgentResult:
        try:
            return await super().run(request)
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class PausingAdmissionReasoner(AuditedSemanticReasoner):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.admission_reached = asyncio.Event()
        self.continue_admission = asyncio.Event()

    async def _admit(self, *args: Any, **kwargs: Any) -> Any:
        self.admission_reached.set()
        await self.continue_admission.wait()
        return await super()._admit(*args, **kwargs)


def test_factory_owns_configured_provider_selection_and_secret_boundary() -> None:
    database = Database.from_url("postgresql+asyncpg://gapforge:database-secret@localhost/gapforge")
    fake = build_semantic_reasoner(
        database=database,
        settings=Settings(
            agent_provider=AgentProviderName.FAKE,
            database_url="postgresql+asyncpg://gapforge:database-secret@localhost/gapforge",
            github_token="source-secret",
        ),
        worker_id="factory-worker",
    )
    codex = build_semantic_reasoner(
        database=database,
        settings=Settings(
            agent_provider=AgentProviderName.CODEX_CLI,
            database_url="postgresql+asyncpg://gapforge:database-secret@localhost/gapforge",
            codex_binary="codex-test",
            codex_home=Path("codex-test"),
        ),
        worker_id="factory-worker",
    )

    assert isinstance(fake, AuditedSemanticReasoner)
    assert isinstance(fake._provider, FakeAgentProvider)
    assert isinstance(codex, AuditedSemanticReasoner)
    assert isinstance(codex._provider, CodexCliProvider)
    assert fake._lease_owner == "factory-worker"
    assert "source-secret" in fake._secret_values


async def test_noncanonical_call_id_and_request_secret_are_rejected_before_admission(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    boundary = RecordingProvider([_provider_result({"pain": "must not run"})])
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
        secret_values=frozenset({"request-secret"}),
    )
    schema = {
        "type": "object",
        "properties": {"pain": {"type": "string"}},
        "required": ["pain"],
        "additionalProperties": False,
    }

    try:
        with pytest.raises(ValueError, match="canonical UUID"):
            await reasoner.run(
                context,
                SemanticCall(_domain_request(str(uuid4()).upper()), schema),
            )
        secret_request = _domain_request().model_copy(
            update={"input_json": {"text": "request-secret"}}
        )
        with pytest.raises(SemanticAdmissionError) as captured:
            await reasoner.run(context, SemanticCall(secret_request, schema))
        assert captured.value.kind is SemanticAdmissionKind.TASK_CONTEXT_INVALID
        assert boundary.requests == []
        call_count, lease_count, run = await _counts(database, context.run_id)
        assert call_count == 0
        assert lease_count == 0
        assert run.budget_used == {}
    finally:
        await _finish_context(database, context)
        await database.dispose()


async def _running_context(database: Database, *, budget_limit: int = 4) -> SemanticContext:
    now = datetime.now(UTC)
    async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
        _, revision = await uow.missions.create_with_revision(
            title=f"Provider audit {uuid4()}",
            mission_text="Find recurring evidence-backed operational pain",
            original_language="en",
            output_locale="en",
        )
        run = ResearchRun(
            mission_revision_id=revision.id,
            mode="HUNT",
            status="RUNNING",
            priority=1,
            deadline_at=now + timedelta(minutes=10),
            started_at=now,
            budget_limits={
                "max_agent_calls_per_run": budget_limit,
                "max_parallel_agent_calls": 2,
            },
            budget_used={},
            warnings=[],
            last_checkpoint={},
        )
        assert uow.session is not None
        uow.session.add(run)
        await uow.session.flush()
        task = ResearchTask(
            run_id=run.id,
            task_type="research.run",
            status="LEASED",
            priority=1,
            idempotency_key=f"research:{run.id}",
            payload={},
            checkpoint={},
            attempt_count=1,
            max_attempts=3,
            available_at=now,
            lease_owner="worker-i4",
            lease_expires_at=now + timedelta(minutes=10),
        )
        uow.session.add(task)
        await uow.commit()
        return SemanticContext(run_id=run.id, task_id=task.id)


def _domain_request(call_id: str | None = None) -> domain.AgentRequest:
    return domain.AgentRequest(
        call_id=call_id or str(uuid4()),
        task=domain.SemanticOperation.EXTRACT,
        effort=domain.AgentEffort.LOW,
        input_json={"items": [{"id": "evidence-1", "text": "manual work is slow"}]},
        permitted_evidence_ids=("evidence-1",),
        permitted_urls=(),
        output_schema_name="pain-extraction-v1",
        timeout_seconds=30,
    )


def _provider_result(data: dict[str, Any]) -> provider.AgentResult:
    return provider.AgentResult(
        status=provider.AgentStatus.SUCCESS,
        data=data,
        provider="fake",
        requested_model="",
        resolved_model="fake-deterministic",
        effort=provider.ReasoningEffort.LOW,
        cli_version=None,
        duration_ms=7,
    )


async def _counts(database: Database, run_id: UUID) -> tuple[int, int, ResearchRun]:
    async with database.session() as session:
        calls = await session.scalar(
            select(func.count()).select_from(AgentCall).where(AgentCall.run_id == run_id)
        )
        leases = await session.scalar(
            select(func.count())
            .select_from(ProviderCallLease)
            .where(ProviderCallLease.run_id == run_id)
        )
        run = await session.get(ResearchRun, run_id)
        assert run is not None
        return int(calls or 0), int(leases or 0), run


async def _finish_context(database: Database, context: SemanticContext) -> None:
    async with database.session() as session:
        run = await session.get(ResearchRun, context.run_id)
        if run is not None and run.status == "RUNNING":
            run.status = "CANCELLED"
            run.completed_at = datetime.now(UTC)
            await session.commit()


async def test_success_is_audited_once_and_replayed_without_provider_invocation(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    output = {"pain": "manual work is slow", "evidence_ids": ["evidence-1"]}
    boundary = RecordingProvider([_provider_result(output)])
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    call = SemanticCall(
        request=_domain_request(),
        output_schema={
            "type": "object",
            "properties": {
                "pain": {"type": "string"},
                "evidence_ids": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["pain", "evidence_ids"],
            "additionalProperties": False,
        },
    )

    try:
        first = await reasoner.run(context, call)
        replay = await reasoner.run(context, call)

        assert first == replay
        assert first.call_id == call.request.call_id
        assert first.status is domain.AgentStatus.COMPLETED
        assert first.output_json == output
        assert len(boundary.requests) == 1
        assert boundary.requests[0].operation == "extract"
        assert boundary.requests[0].timeout_seconds <= call.request.timeout_seconds

        call_count, lease_count, run = await _counts(database, context.run_id)
        assert call_count == 1
        assert lease_count == 0
        assert run.budget_used == {"agent_calls": 1}
    finally:
        await _finish_context(database, context)
        await database.dispose()


async def test_canonical_20kb_output_round_trips_despite_jsonb_display_spacing(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    output = {f"k{index:02d}": "x" * 390 for index in range(50)}
    compact_size = len(
        json.dumps(
            output,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )
    display_size = len(json.dumps(output, ensure_ascii=False, sort_keys=True).encode())
    assert compact_size <= 20_000 < display_size
    boundary = RecordingProvider([_provider_result(output)])
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    call = SemanticCall(
        request=_domain_request(),
        output_schema={"type": "object"},
    )

    try:
        result = await reasoner.run(context, call)

        assert result.status is domain.AgentStatus.COMPLETED
        assert result.output_json == output
        async with database.session() as session:
            row = await session.get(AgentCall, UUID(call.request.call_id))
        assert row is not None
        assert row.output_json == output
    finally:
        await _finish_context(database, context)
        await database.dispose()


async def test_invalid_output_gets_one_separately_admitted_audited_repair(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    request = _domain_request()
    repaired_output = {"pain": "supported pain", "evidence_ids": ["evidence-1"]}
    boundary = RecordingProvider(
        [
            provider.AgentResult(
                status=provider.AgentStatus.INVALID_OUTPUT,
                provider="fake",
                effort=provider.ReasoningEffort.LOW,
                duration_ms=3,
                error_class="SchemaValidationError",
                error_message="evidence_ids is required",
            ),
            _provider_result(repaired_output),
        ]
    )
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    call = SemanticCall(
        request=request,
        output_schema={
            "type": "object",
            "properties": {
                "pain": {"type": "string"},
                "evidence_ids": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["pain", "evidence_ids"],
            "additionalProperties": False,
        },
    )

    try:
        result = await reasoner.run(context, call)
        replay = await reasoner.run(context, call)

        assert result == replay
        assert result.call_id == request.call_id
        assert result.status is domain.AgentStatus.COMPLETED
        assert result.output_json == repaired_output
        assert result.repair_attempted is True
        assert len(boundary.requests) == 2
        assert boundary.requests[0].repair_attempt == 0
        assert boundary.requests[1].repair_attempt == 1

        repair_id = uuid5(UUID(request.call_id), "repair:1")
        async with database.session() as session:
            rows = (
                await session.scalars(
                    select(AgentCall)
                    .where(AgentCall.run_id == context.run_id)
                    .order_by(AgentCall.created_at)
                )
            ).all()
            run = await session.get(ResearchRun, context.run_id)
            assert run is not None
        assert [row.id for row in rows] == [UUID(request.call_id), repair_id]
        assert [row.status for row in rows] == ["INVALID_OUTPUT", "COMPLETED"]
        assert rows[0].request_sha256 == rows[1].request_sha256
        assert rows[1].repair_attempts == 1
        assert run.budget_used == {"agent_calls": 2}
    finally:
        await _finish_context(database, context)
        await database.dispose()


async def test_invented_evidence_and_urls_are_rejected_before_repair(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    request = _domain_request().model_copy(
        update={"permitted_urls": ("https://allowed.example/evidence",)}
    )
    valid = {
        "citations": [
            {
                "evidence_id": "evidence-1",
                "source_url": "https://allowed.example/evidence",
            }
        ]
    }
    boundary = RecordingProvider(
        [
            _provider_result(
                {
                    "citations": [
                        {
                            "evidence_id": "invented-evidence",
                            "source_url": "https://attacker.invalid/invented",
                        }
                    ]
                }
            ),
            _provider_result(valid),
        ]
    )
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    call = SemanticCall(
        request=request,
        output_schema={
            "type": "object",
            "properties": {
                "citations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "evidence_id": {"type": "string"},
                            "source_url": {"type": "string", "format": "uri"},
                        },
                        "required": ["evidence_id", "source_url"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["citations"],
            "additionalProperties": False,
        },
    )

    try:
        result = await reasoner.run(context, call)

        assert result.status is domain.AgentStatus.COMPLETED
        assert result.output_json == valid
        assert result.repair_attempted is True
        async with database.session() as session:
            rows = (
                await session.scalars(
                    select(AgentCall)
                    .where(AgentCall.run_id == context.run_id)
                    .order_by(AgentCall.created_at)
                )
            ).all()
        assert rows[0].status == "INVALID_OUTPUT"
        assert rows[0].error_class == "PermittedEvidenceViolation"
        assert rows[0].output_json is None
        assert rows[1].status == "COMPLETED"
    finally:
        await _finish_context(database, context)
        await database.dispose()


async def test_nondefault_repair_policy_is_bounded_and_replay_safe(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    boundary = RecordingProvider(
        [
            provider.AgentResult(
                status=provider.AgentStatus.INVALID_OUTPUT,
                provider="fake",
                effort=provider.ReasoningEffort.LOW,
                duration_ms=1,
                error_class="SchemaValidationError",
            ),
            _provider_result({"pain": "repaired"}),
        ]
    )
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    output_schema = {
        "type": "object",
        "properties": {"pain": {"type": "string"}},
        "required": ["pain"],
        "additionalProperties": False,
    }
    call = SemanticCall(_domain_request(), output_schema, max_output_bytes=4096)

    try:
        first = await reasoner.run(context, call)
        replay = await reasoner.run(context, call)

        assert first == replay
        assert [request.max_output_bytes for request in boundary.requests] == [4096, 4096]
        assert [request.allow_repair for request in boundary.requests] == [True, False]
        async with database.session() as session:
            rows = (
                await session.scalars(
                    select(AgentCall)
                    .where(AgentCall.run_id == context.run_id)
                    .order_by(AgentCall.created_at)
                )
            ).all()
        assert len(rows) == 2
        assert rows[0].request_sha256 != rows[1].request_sha256
        assert len(boundary.requests) == 2

        with pytest.raises(SemanticAdmissionError) as changed_cap:
            await reasoner.run(
                context,
                SemanticCall(call.request, output_schema, max_output_bytes=8192),
            )
        assert changed_cap.value.kind is SemanticAdmissionKind.REPLAY_UNAVAILABLE
        assert len(boundary.requests) == 2
    finally:
        await _finish_context(database, context)
        await database.dispose()


async def test_stale_inflight_attempt_is_terminally_audited_before_new_admission(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    stale_request = _domain_request()
    output_schema = {
        "type": "object",
        "properties": {"pain": {"type": "string"}},
        "required": ["pain"],
        "additionalProperties": False,
    }
    stale_id = UUID(stale_request.call_id)
    schema_hash = agent_schema_identity(
        operation=stale_request.task,
        schema_name=stale_request.output_schema_name,
        output_schema=output_schema,
    )
    async with database.session() as session:
        run = await session.get(ResearchRun, context.run_id)
        assert run is not None
        run.budget_used = {"agent_calls": 1}
        session.add(
            ProviderCallLease(
                run_id=context.run_id,
                task_id=context.task_id,
                call_key=str(stale_id),
                lease_owner="crashed-worker",
                lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
                operation="extract",
                output_schema_name=stale_request.output_schema_name,
                output_schema_sha256=schema_hash,
                request_sha256=agent_request_identity(
                    stale_request,
                    schema_identity=schema_hash,
                ),
                provider="fake",
                requested_model="",
                effort="low",
                repair_attempt=0,
            )
        )
        await session.commit()

    boundary = RecordingProvider([_provider_result({"pain": "new admitted work"})])
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    call = SemanticCall(request=_domain_request(), output_schema=output_schema)

    try:
        result = await reasoner.run(context, call)

        assert result.status is domain.AgentStatus.COMPLETED
        async with database.session() as session:
            stale_audit = await session.get(AgentCall, stale_id)
            run = await session.get(ResearchRun, context.run_id)
            leases = await session.scalar(
                select(func.count())
                .select_from(ProviderCallLease)
                .where(ProviderCallLease.run_id == context.run_id)
            )
            assert run is not None
        assert stale_audit is not None
        assert stale_audit.status == "FAILED"
        assert stale_audit.error_class == "StaleProviderCall"
        assert stale_audit.output_json is None
        assert int(leases or 0) == 0
        assert run.budget_used == {"agent_calls": 2}
    finally:
        await _finish_context(database, context)
        await database.dispose()


async def test_concurrent_same_call_id_is_denied_without_budget_or_duplicate_subprocess(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    boundary = BlockingProvider(_provider_result({"pain": "one subprocess"}))
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    call = SemanticCall(
        request=_domain_request(),
        output_schema={
            "type": "object",
            "properties": {"pain": {"type": "string"}},
            "required": ["pain"],
            "additionalProperties": False,
        },
    )
    first = asyncio.create_task(reasoner.run(context, call))

    try:
        await asyncio.wait_for(boundary.started.wait(), timeout=2)
        with pytest.raises(SemanticAdmissionError) as captured:
            await reasoner.run(context, call)
        assert captured.value.kind is SemanticAdmissionKind.INDETERMINATE_ATTEMPT

        async with database.session() as session:
            run = await session.get(ResearchRun, context.run_id)
            assert run is not None
            assert run.budget_used == {"agent_calls": 1}

        boundary.release.set()
        result = await first
        assert result.status is domain.AgentStatus.COMPLETED
        assert len(boundary.requests) == 1
        call_count, lease_count, _ = await _counts(database, context.run_id)
        assert call_count == 1
        assert lease_count == 0
    finally:
        boundary.release.set()
        if not first.done():
            first.cancel()
        await _finish_context(database, context)
        await database.dispose()


async def test_same_id_rechecks_audit_after_optimistic_lookup_before_admission(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    boundary = RecordingProvider([_provider_result({"pain": "one durable result"})])
    delayed = PausingAdmissionReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    immediate = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    call = SemanticCall(
        _domain_request(),
        {
            "type": "object",
            "properties": {"pain": {"type": "string"}},
            "required": ["pain"],
            "additionalProperties": False,
        },
    )
    delayed_call = asyncio.create_task(delayed.run(context, call))

    try:
        await asyncio.wait_for(delayed.admission_reached.wait(), timeout=2)
        first = await immediate.run(context, call)
        delayed.continue_admission.set()
        replay = await asyncio.wait_for(delayed_call, timeout=2)

        assert replay == first
        assert len(boundary.requests) == 1
        call_count, lease_count, run = await _counts(database, context.run_id)
        assert call_count == 1
        assert lease_count == 0
        assert run.budget_used == {"agent_calls": 1}
    finally:
        delayed.continue_admission.set()
        if not delayed_call.done():
            delayed_call.cancel()
            with suppress(asyncio.CancelledError):
                await delayed_call
        await _finish_context(database, context)
        await database.dispose()


async def test_parallel_cap_denial_does_not_consume_budget(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    boundary = CountingBlockingProvider(
        _provider_result({"pain": "bounded parallel work"}),
        expected_started=2,
    )
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    output_schema = {
        "type": "object",
        "properties": {"pain": {"type": "string"}},
        "required": ["pain"],
        "additionalProperties": False,
    }
    first = asyncio.create_task(
        reasoner.run(context, SemanticCall(_domain_request(), output_schema))
    )
    second = asyncio.create_task(
        reasoner.run(context, SemanticCall(_domain_request(), output_schema))
    )

    try:
        await asyncio.wait_for(boundary.started.wait(), timeout=2)
        with pytest.raises(SemanticAdmissionError) as captured:
            await reasoner.run(context, SemanticCall(_domain_request(), output_schema))
        assert captured.value.kind is SemanticAdmissionKind.PARALLEL_LIMIT

        async with database.session() as session:
            run = await session.get(ResearchRun, context.run_id)
            assert run is not None
            assert run.budget_used == {"agent_calls": 2}

        boundary.release.set()
        results = await asyncio.gather(first, second)
        assert all(result.status is domain.AgentStatus.COMPLETED for result in results)
        assert len(boundary.requests) == 2
    finally:
        boundary.release.set()
        for pending in (first, second):
            if not pending.done():
                pending.cancel()
        await _finish_context(database, context)
        await database.dispose()


async def test_long_provider_call_renews_short_lease_until_audit(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    boundary = BlockingProvider(_provider_result({"pain": "renewed work"}))
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
        lease_duration=timedelta(seconds=0.2),
        heartbeat_interval_seconds=0.05,
    )
    call = SemanticCall(
        _domain_request(),
        {
            "type": "object",
            "properties": {"pain": {"type": "string"}},
            "required": ["pain"],
            "additionalProperties": False,
        },
    )
    pending = asyncio.create_task(reasoner.run(context, call))

    try:
        await asyncio.wait_for(boundary.started.wait(), timeout=2)
        async with database.session() as session:
            initial = await session.scalar(
                select(ProviderCallLease).where(ProviderCallLease.run_id == context.run_id)
            )
            assert initial is not None
            initial_expiry = initial.lease_expires_at

        await asyncio.sleep(0.25)
        async with database.session() as session:
            renewed = await session.scalar(
                select(ProviderCallLease).where(ProviderCallLease.run_id == context.run_id)
            )
            assert renewed is not None
            assert renewed.lease_expires_at > initial_expiry
            assert renewed.lease_expires_at > datetime.now(UTC)

        boundary.release.set()
        result = await pending
        assert result.status is domain.AgentStatus.COMPLETED
        _, lease_count, _ = await _counts(database, context.run_id)
        assert lease_count == 0
    finally:
        boundary.release.set()
        if not pending.done():
            pending.cancel()
        await _finish_context(database, context)
        await database.dispose()


async def test_provider_exception_becomes_sanitized_failed_audit_and_releases_lease(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    boundary = RaisingProvider()
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    call = SemanticCall(
        _domain_request(),
        {
            "type": "object",
            "properties": {"pain": {"type": "string"}},
            "required": ["pain"],
            "additionalProperties": False,
        },
    )

    try:
        result = await reasoner.run(context, call)

        assert result.status is domain.AgentStatus.FAILED
        assert result.error_class == "ProviderExecutionError"
        assert boundary.calls == 1
        async with database.session() as session:
            row = await session.get(AgentCall, UUID(call.request.call_id))
            leases = await session.scalar(
                select(func.count())
                .select_from(ProviderCallLease)
                .where(ProviderCallLease.run_id == context.run_id)
            )
        assert row is not None
        assert row.status == "FAILED"
        assert row.error_class == "ProviderExecutionError"
        assert "untrusted provider exception" not in repr(row.__dict__)
        assert int(leases or 0) == 0
    finally:
        await _finish_context(database, context)
        await database.dispose()


async def test_auth_required_is_audited_once_and_never_repaired_or_retried(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    auth = provider.AgentResult(
        status=provider.AgentStatus.AUTH_REQUIRED,
        provider="fake",
        requested_model="",
        effort=provider.ReasoningEffort.LOW,
        duration_ms=2,
        error_class="AuthenticationRequired",
    )
    boundary = RecordingProvider([auth, _provider_result({"pain": "must not run"})])
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    call = SemanticCall(
        _domain_request(),
        {
            "type": "object",
            "properties": {"pain": {"type": "string"}},
            "required": ["pain"],
            "additionalProperties": False,
        },
    )

    try:
        result = await reasoner.run(context, call)
        replay = await reasoner.run(context, call)

        assert result == replay
        assert result.status is domain.AgentStatus.AUTH_REQUIRED
        assert len(boundary.requests) == 1
        call_count, lease_count, run = await _counts(database, context.run_id)
        assert call_count == 1
        assert lease_count == 0
        assert run.budget_used == {"agent_calls": 1}
    finally:
        await _finish_context(database, context)
        await database.dispose()


async def test_provider_secret_material_is_never_returned_or_persisted(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    secret = "database-password-do-not-leak"
    boundary = RecordingProvider([_provider_result({"pain": f"leaked {secret}"})])
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
        secret_values=frozenset({secret}),
    )
    call = SemanticCall(
        _domain_request(),
        {
            "type": "object",
            "properties": {"pain": {"type": "string"}},
            "required": ["pain"],
            "additionalProperties": False,
        },
    )

    try:
        result = await reasoner.run(context, call)

        assert result.status is domain.AgentStatus.FAILED
        assert result.output_json is None
        assert result.error_class == "SecretMaterialDetected"
        assert secret not in result.model_dump_json()
        async with database.session() as session:
            row = await session.get(AgentCall, UUID(call.request.call_id))
        assert row is not None
        assert secret not in repr(row.__dict__)
        assert row.output_json is None
        assert row.error_class == "SecretMaterialDetected"
    finally:
        await _finish_context(database, context)
        await database.dispose()


async def test_replay_rejects_same_call_id_with_changed_input_or_provider_model(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    boundary = RecordingProvider([_provider_result({"pain": "original"})])
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    output_schema = {
        "type": "object",
        "properties": {"pain": {"type": "string"}},
        "required": ["pain"],
        "additionalProperties": False,
    }
    request = _domain_request()
    call = SemanticCall(request, output_schema)

    try:
        first = await reasoner.run(context, call)
        assert first.status is domain.AgentStatus.COMPLETED

        changed = call.request.model_copy(
            update={"input_json": {"items": [{"id": "evidence-2", "text": "changed"}]}}
        )
        with pytest.raises(SemanticAdmissionError) as changed_input:
            await reasoner.run(context, SemanticCall(changed, output_schema))
        assert changed_input.value.kind is SemanticAdmissionKind.REPLAY_UNAVAILABLE

        changed_model = AuditedSemanticReasoner(
            database.session_factory,
            boundary,
            lease_owner="worker-i4",
            provider_name="fake",
            model="different-model",
        )
        with pytest.raises(SemanticAdmissionError) as model_mismatch:
            await changed_model.run(context, call)
        assert model_mismatch.value.kind is SemanticAdmissionKind.REPLAY_UNAVAILABLE

        for changed_policy in (
            SemanticCall(request, output_schema, max_output_bytes=4096),
            SemanticCall(request, output_schema, allow_repair=False),
        ):
            with pytest.raises(SemanticAdmissionError) as policy_mismatch:
                await reasoner.run(context, changed_policy)
            assert policy_mismatch.value.kind is SemanticAdmissionKind.REPLAY_UNAVAILABLE
        assert len(boundary.requests) == 1
    finally:
        await _finish_context(database, context)
        await database.dispose()


async def test_nondefault_execution_policy_replays_only_with_exact_identity(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    boundary = RecordingProvider([_provider_result({"pain": "bounded"})])
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    output_schema = {
        "type": "object",
        "properties": {"pain": {"type": "string"}},
        "required": ["pain"],
        "additionalProperties": False,
    }
    call = SemanticCall(
        _domain_request(),
        output_schema,
        max_output_bytes=4096,
        allow_repair=False,
    )

    try:
        first = await reasoner.run(context, call)
        replay = await reasoner.run(context, call)

        assert first == replay
        assert len(boundary.requests) == 1
        assert boundary.requests[0].max_output_bytes == 4096
        assert boundary.requests[0].allow_repair is False

        for changed_policy in (
            SemanticCall(
                call.request,
                output_schema,
                max_output_bytes=8192,
                allow_repair=False,
            ),
            SemanticCall(
                call.request,
                output_schema,
                max_output_bytes=4096,
                allow_repair=True,
            ),
        ):
            with pytest.raises(SemanticAdmissionError) as mismatch:
                await reasoner.run(context, changed_policy)
            assert mismatch.value.kind is SemanticAdmissionKind.REPLAY_UNAVAILABLE
        assert len(boundary.requests) == 1
    finally:
        await _finish_context(database, context)
        await database.dispose()


async def test_cancelled_inflight_call_is_stale_audited_and_never_reinvoked(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    crashed_boundary = BlockingProvider(_provider_result({"pain": "lost process result"}))
    crashed_reasoner = AuditedSemanticReasoner(
        database.session_factory,
        crashed_boundary,
        lease_owner="worker-i4",
        provider_name="fake",
        lease_duration=timedelta(seconds=0.2),
        heartbeat_interval_seconds=0.05,
    )
    call = SemanticCall(
        _domain_request(),
        {
            "type": "object",
            "properties": {"pain": {"type": "string"}},
            "required": ["pain"],
            "additionalProperties": False,
        },
    )
    interrupted = asyncio.create_task(crashed_reasoner.run(context, call))

    try:
        await asyncio.wait_for(crashed_boundary.started.wait(), timeout=2)
        interrupted.cancel()
        with pytest.raises(asyncio.CancelledError):
            await interrupted

        async with database.session() as session:
            lease = await session.scalar(
                select(ProviderCallLease).where(ProviderCallLease.run_id == context.run_id)
            )
            assert lease is not None
            lease.lease_expires_at = datetime.now(UTC) - timedelta(milliseconds=1)
            await session.commit()

        retry_boundary = RecordingProvider([_provider_result({"pain": "must not run"})])
        resumed = AuditedSemanticReasoner(
            database.session_factory,
            retry_boundary,
            lease_owner="worker-i4",
            provider_name="fake",
        )
        with pytest.raises(SemanticAdmissionError) as captured:
            await resumed.run(context, call)
        assert captured.value.kind is SemanticAdmissionKind.REPLAY_UNAVAILABLE
        assert retry_boundary.requests == []

        async with database.session() as session:
            row = await session.get(AgentCall, UUID(call.request.call_id))
            run = await session.get(ResearchRun, context.run_id)
            assert run is not None
        assert row is not None
        assert row.status == "FAILED"
        assert row.error_class == "StaleProviderCall"
        assert run.budget_used == {"agent_calls": 1}
    finally:
        crashed_boundary.release.set()
        if not interrupted.done():
            interrupted.cancel()
        await _finish_context(database, context)
        await database.dispose()


async def test_lost_provider_lease_cancels_subprocess_and_forbids_owner_audit(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    boundary = CancellationRecordingProvider(_provider_result({"pain": "must not persist"}))
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
        lease_duration=timedelta(seconds=0.2),
        heartbeat_interval_seconds=0.05,
    )
    call = SemanticCall(
        _domain_request(),
        {
            "type": "object",
            "properties": {"pain": {"type": "string"}},
            "required": ["pain"],
            "additionalProperties": False,
        },
    )
    execution = asyncio.create_task(reasoner.run(context, call))

    try:
        await asyncio.wait_for(boundary.started.wait(), timeout=2)
        async with database.session() as session:
            lease = await session.scalar(
                select(ProviderCallLease).where(ProviderCallLease.run_id == context.run_id)
            )
            assert lease is not None
            await session.delete(lease)
            await session.commit()

        with pytest.raises(SemanticAdmissionError) as captured:
            await asyncio.wait_for(execution, timeout=2)
        assert captured.value.kind is SemanticAdmissionKind.INDETERMINATE_ATTEMPT
        assert boundary.cancelled.is_set()
        async with database.session() as session:
            audit_count = await session.scalar(
                select(func.count())
                .select_from(AgentCall)
                .where(AgentCall.run_id == context.run_id)
            )
            lease_count = await session.scalar(
                select(func.count())
                .select_from(ProviderCallLease)
                .where(ProviderCallLease.run_id == context.run_id)
            )
        assert int(audit_count or 0) == 0
        assert int(lease_count or 0) == 0
    finally:
        boundary.release.set()
        if not execution.done():
            execution.cancel()
            with suppress(asyncio.CancelledError):
                await execution
        await _finish_context(database, context)
        await database.dispose()


async def test_repair_cannot_cross_persisted_call_budget(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database, budget_limit=1)
    boundary = RecordingProvider(
        [
            provider.AgentResult(
                status=provider.AgentStatus.INVALID_OUTPUT,
                provider="fake",
                requested_model="",
                effort=provider.ReasoningEffort.LOW,
                duration_ms=1,
                error_class="SchemaValidationError",
            ),
            _provider_result({"pain": "must not run"}),
        ]
    )
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    call = SemanticCall(
        _domain_request(),
        {
            "type": "object",
            "properties": {"pain": {"type": "string"}},
            "required": ["pain"],
            "additionalProperties": False,
        },
    )

    try:
        with pytest.raises(SemanticAdmissionError) as captured:
            await reasoner.run(context, call)
        assert captured.value.kind is SemanticAdmissionKind.BUDGET_EXHAUSTED
        assert len(boundary.requests) == 1
        async with database.session() as session:
            run = await session.get(ResearchRun, context.run_id)
            calls = await session.scalar(
                select(func.count())
                .select_from(AgentCall)
                .where(AgentCall.run_id == context.run_id)
            )
            leases = await session.scalar(
                select(func.count())
                .select_from(ProviderCallLease)
                .where(ProviderCallLease.run_id == context.run_id)
            )
            assert run is not None
        assert run.status == "RUNNING"
        assert run.budget_used == {"agent_calls": 1}
        assert int(calls or 0) == 1
        assert int(leases or 0) == 0
    finally:
        await _finish_context(database, context)
        await database.dispose()


async def test_worker_budget_exhaustion_does_not_leave_task_leased(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    now = datetime.now(UTC)
    async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
        _, revision = await uow.missions.create_with_revision(
            title=f"Worker provider budget {uuid4()}",
            mission_text="Exercise provider repair budget handling",
            original_language="en",
            output_locale="en",
        )
        run = ResearchRun(
            mission_revision_id=revision.id,
            mode="HUNT",
            status="QUEUED",
            priority=1,
            deadline_at=now,
            budget_limits={
                "max_run_duration_minutes": 10,
                "max_agent_calls_per_run": 1,
                "max_parallel_agent_calls": 2,
            },
            budget_used={},
            warnings=[],
            last_checkpoint={},
        )
        assert uow.session is not None
        uow.session.add(run)
        await uow.session.flush()
        task = ResearchTask(
            run_id=run.id,
            task_type="research.run",
            status="PENDING",
            priority=1,
            idempotency_key=f"research:{run.id}",
            payload={},
            checkpoint={},
            attempt_count=0,
            max_attempts=1,
            available_at=now,
        )
        uow.session.add(task)
        await uow.commit()
        run_id = run.id
        task_id = task.id

    boundary = RecordingProvider(
        [
            provider.AgentResult(
                status=provider.AgentStatus.INVALID_OUTPUT,
                provider="fake",
                requested_model="",
                effort=provider.ReasoningEffort.LOW,
                duration_ms=1,
                error_class="SchemaValidationError",
            )
        ]
    )
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )

    async def handler(claimed: ResearchTask) -> dict[str, object]:
        call = SemanticCall(
            _domain_request(),
            {
                "type": "object",
                "properties": {"pain": {"type": "string"}},
                "required": ["pain"],
                "additionalProperties": False,
            },
        )
        try:
            await reasoner.run(
                SemanticContext(run_id=claimed.run_id, task_id=claimed.id),
                call,
            )
        except SemanticAdmissionError as error:
            assert error.kind is SemanticAdmissionKind.BUDGET_EXHAUSTED
            raise TaskHandlerError(
                ErrorKind.BUDGET_EXHAUSTED,
                error_class="agent_call_budget_exhausted",
            ) from error
        raise AssertionError("repair unexpectedly crossed the call budget")

    try:
        worker = Worker(
            database,
            worker_id="worker-i4",
            handlers=TaskHandlerRegistry({"research.run": handler}),
        )
        assert await worker.run_once() is True

        async with database.session() as session:
            persisted_run = await session.get(ResearchRun, run_id)
            persisted_task = await session.get(ResearchTask, task_id)
            lease_count = await session.scalar(
                select(func.count())
                .select_from(ProviderCallLease)
                .where(ProviderCallLease.run_id == run_id)
            )
            assert persisted_run is not None
            assert persisted_task is not None
        assert persisted_run.status == "BUDGET_EXHAUSTED"
        assert persisted_run.budget_used == {"agent_calls": 1}
        assert persisted_task.status == "FAILED"
        assert persisted_task.retry_class == "BUDGET_EXHAUSTED"
        assert persisted_task.last_error == "agent_call_budget_exhausted"
        assert int(lease_count or 0) == 0
        assert len(boundary.requests) == 1
    finally:
        await database.dispose()


async def test_deadline_denial_does_not_invoke_provider_or_consume_budget(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    async with database.session() as session:
        run = await session.get(ResearchRun, context.run_id)
        assert run is not None
        run.deadline_at = datetime.now(UTC) - timedelta(milliseconds=1)
        await session.commit()
    boundary = RecordingProvider([_provider_result({"pain": "must not run"})])
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    call = SemanticCall(
        _domain_request(),
        {
            "type": "object",
            "properties": {"pain": {"type": "string"}},
            "required": ["pain"],
            "additionalProperties": False,
        },
    )

    try:
        with pytest.raises(SemanticAdmissionError) as captured:
            await reasoner.run(context, call)
        assert captured.value.kind is SemanticAdmissionKind.DEADLINE_EXCEEDED
        assert boundary.requests == []
        async with database.session() as session:
            run = await session.get(ResearchRun, context.run_id)
            assert run is not None
        assert run.budget_used == {}
    finally:
        await _finish_context(database, context)
        await database.dispose()


@pytest.mark.parametrize(
    ("updates", "error_class"),
    [
        ({"provider": "unexpected"}, "ProviderIdentityMismatch"),
        ({"requested_model": "unexpected"}, "ProviderIdentityMismatch"),
        ({"resolved_model": "x" * 161}, "ProviderResultBoundsViolation"),
        ({"error_class": "x" * 81}, "ProviderResultBoundsViolation"),
    ],
)
async def test_untrusted_provider_identity_and_bounds_fail_as_sanitized_audits(
    migrated_postgres_url: str,
    updates: dict[str, object],
    error_class: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context = await _running_context(database)
    malicious = _provider_result({"pain": "must be discarded"}).model_copy(update=updates)
    boundary = RecordingProvider([malicious])
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="worker-i4",
        provider_name="fake",
    )
    call = SemanticCall(
        _domain_request(),
        {
            "type": "object",
            "properties": {"pain": {"type": "string"}},
            "required": ["pain"],
            "additionalProperties": False,
        },
    )

    try:
        result = await reasoner.run(context, call)
        assert result.status is domain.AgentStatus.FAILED
        assert result.error_class == error_class
        assert result.output_json is None
        async with database.session() as session:
            row = await session.get(AgentCall, UUID(call.request.call_id))
        assert row is not None
        assert row.provider == "fake"
        assert row.requested_model == ""
        assert row.resolved_model is None
        assert row.error_class == error_class
    finally:
        await _finish_context(database, context)
        await database.dispose()
