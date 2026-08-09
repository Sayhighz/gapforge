from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4, uuid5

from sqlalchemy import func, select

from gapforge.domain import contracts as domain
from gapforge.integration.mappers import agent_schema_identity
from gapforge.integration.semantic import (
    AuditedSemanticReasoner,
    SemanticCall,
    SemanticContext,
)
from gapforge.providers import contracts as provider
from gapforge.storage.database import Database
from gapforge.storage.models import AgentCall, ProviderCallLease, ResearchRun, ResearchTask
from gapforge.storage.uow import SqlAlchemyUnitOfWork


class RecordingProvider:
    def __init__(self, responses: list[provider.AgentResult]) -> None:
        self.responses = responses
        self.requests: list[provider.AgentRequest] = []

    async def run(self, request: provider.AgentRequest) -> provider.AgentResult:
        self.requests.append(request)
        return self.responses.pop(0)


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
                output_schema_sha256=agent_schema_identity(
                    operation=stale_request.task,
                    schema_name=stale_request.output_schema_name,
                    output_schema=output_schema,
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
