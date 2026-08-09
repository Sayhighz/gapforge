"""Stable semantic-reasoning port shared by research and provider integration."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol, cast
from uuid import UUID, uuid5

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from gapforge.config import AgentProviderName, Settings
from gapforge.domain import contracts as domain
from gapforge.integration.mappers import agent_operation, agent_schema_identity
from gapforge.providers import contracts as provider_contracts
from gapforge.providers.codex_cli import CodexCliProvider
from gapforge.providers.fake import FakeAgentProvider
from gapforge.queue.control import RunController
from gapforge.storage.database import Database
from gapforge.storage.models import AgentCall, ProviderCallLease, ResearchRun, ResearchTask


@dataclass(frozen=True, slots=True)
class SemanticContext:
    """Durable run/task lineage for one logical semantic request."""

    run_id: UUID
    task_id: UUID


@dataclass(frozen=True, slots=True)
class SemanticCall:
    """Canonical domain request paired with its executable JSON Schema."""

    request: domain.AgentRequest
    output_schema: dict[str, Any]


class SemanticReasoner(Protocol):
    """Injected seam consumed by evidence orchestration."""

    async def run(
        self,
        context: SemanticContext,
        call: SemanticCall,
    ) -> domain.AgentResult: ...


class SemanticAdmissionKind(StrEnum):
    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    PARALLEL_LIMIT = "PARALLEL_LIMIT"
    TASK_CONTEXT_INVALID = "TASK_CONTEXT_INVALID"
    REPLAY_UNAVAILABLE = "REPLAY_UNAVAILABLE"
    INDETERMINATE_ATTEMPT = "INDETERMINATE_ATTEMPT"


class SemanticAdmissionError(RuntimeError):
    """Stable hard-gate failure raised before any new provider subprocess."""

    def __init__(self, kind: SemanticAdmissionKind, message: str) -> None:
        super().__init__(message)
        self.kind = kind


@dataclass(frozen=True, slots=True)
class _Admission:
    lease_id: UUID
    remaining_seconds: float


class AuditedSemanticReasoner:
    """Admit, execute, audit, and replay bounded semantic provider calls."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        provider: provider_contracts.AgentProvider,
        *,
        lease_owner: str,
        provider_name: str,
        model: str = "",
    ) -> None:
        if not lease_owner.strip():
            raise ValueError("lease_owner must be non-empty")
        if provider_name not in {"codex_cli", "fake"}:
            raise ValueError("provider_name must be codex_cli or fake")
        self._session_factory = session_factory
        self._provider = provider
        self._lease_owner = lease_owner
        self._provider_name = provider_name
        self._model = model

    async def run(self, context: SemanticContext, call: SemanticCall) -> domain.AgentResult:
        call_id = _canonical_call_uuid(call.request.call_id)
        schema_hash = agent_schema_identity(
            operation=call.request.task,
            schema_name=call.request.output_schema_name,
            output_schema=call.output_schema,
        )
        async with self._session_factory() as session:
            existing = await session.get(AgentCall, call_id)
            if existing is not None:
                replay = self._replay(
                    existing,
                    context=context,
                    call=call,
                    schema_hash=schema_hash,
                )
                if replay.status is not domain.AgentStatus.INVALID_OUTPUT:
                    return replay
                return await self._repair(
                    context,
                    call,
                    invalid_result=self._provider_result_from_audit(existing),
                    schema_hash=schema_hash,
                )

        provider_request = self._provider_request(
            call,
            timeout_seconds=call.request.timeout_seconds,
        )
        provider_result = await self._execute_attempt(
            context,
            call,
            call_id=call_id,
            schema_hash=schema_hash,
            provider_request=provider_request,
            repair_attempt=0,
        )
        if provider_result.status is provider_contracts.AgentStatus.INVALID_OUTPUT:
            return await self._repair(
                context,
                call,
                invalid_result=provider_result,
                schema_hash=schema_hash,
            )
        return _domain_result(call.request.call_id, provider_result)

    async def _execute_attempt(
        self,
        context: SemanticContext,
        call: SemanticCall,
        *,
        call_id: UUID,
        schema_hash: bytes,
        provider_request: provider_contracts.AgentRequest,
        repair_attempt: int,
    ) -> provider_contracts.AgentResult:
        admission = await self._admit(
            context,
            call,
            call_id=call_id,
            schema_hash=schema_hash,
            repair_attempt=repair_attempt,
        )
        provider_request = provider_request.model_copy(
            update={
                "timeout_seconds": max(
                    1.0,
                    min(provider_request.timeout_seconds, admission.remaining_seconds),
                )
            }
        )
        try:
            async with asyncio.timeout(admission.remaining_seconds):
                provider_result = await self._provider.run(provider_request)
        except TimeoutError:
            provider_result = provider_contracts.AgentResult(
                status=provider_contracts.AgentStatus.TIMEOUT,
                provider=self._provider_name,
                requested_model=self._model,
                effort=provider_request.effort,
                duration_ms=max(0, round(admission.remaining_seconds * 1000)),
                error_class="RunDeadlineExceeded",
                error_message="provider call reached the remaining run deadline",
            )
        provider_result = _enforce_permitted_references(call.request, provider_result)
        provider_result = provider_result.model_copy(
            update={"repair_attempts": provider_request.repair_attempt}
        )
        await self._audit_and_release(
            context,
            call,
            call_id=call_id,
            schema_hash=schema_hash,
            lease_id=admission.lease_id,
            result=provider_result,
        )
        return provider_result

    async def _repair(
        self,
        context: SemanticContext,
        call: SemanticCall,
        *,
        invalid_result: provider_contracts.AgentResult,
        schema_hash: bytes,
    ) -> domain.AgentResult:
        original_id = _canonical_call_uuid(call.request.call_id)
        repair_id = _repair_call_uuid(original_id)
        repair_call = SemanticCall(
            request=call.request.model_copy(update={"call_id": str(repair_id)}),
            output_schema=call.output_schema,
        )
        async with self._session_factory() as session:
            existing = await session.get(AgentCall, repair_id)
            if existing is not None:
                replay = self._replay(
                    existing,
                    context=context,
                    call=repair_call,
                    schema_hash=schema_hash,
                )
                return replay.model_copy(
                    update={"call_id": call.request.call_id, "repair_attempted": True}
                )
        original_provider_request = self._provider_request(
            call,
            timeout_seconds=call.request.timeout_seconds,
        )
        repair_request = provider_contracts.build_repair_request(
            original_provider_request,
            invalid_result,
        )
        repaired = await self._execute_attempt(
            context,
            repair_call,
            call_id=repair_id,
            schema_hash=schema_hash,
            provider_request=repair_request,
            repair_attempt=1,
        )
        return _domain_result(call.request.call_id, repaired).model_copy(
            update={"repair_attempted": True}
        )

    async def _admit(
        self,
        context: SemanticContext,
        call: SemanticCall,
        *,
        call_id: UUID,
        schema_hash: bytes,
        repair_attempt: int,
    ) -> _Admission:
        now = datetime.now(UTC)
        denied: SemanticAdmissionError | None = None
        admitted: _Admission | None = None
        async with self._session_factory() as session, session.begin():
            run = await session.scalar(
                select(ResearchRun).where(ResearchRun.id == context.run_id).with_for_update()
            )
            task = await session.scalar(
                select(ResearchTask).where(ResearchTask.id == context.task_id).with_for_update()
            )
            if (
                run is None
                or task is None
                or task.run_id != context.run_id
                or task.status != "LEASED"
                or task.lease_owner != self._lease_owner
            ):
                denied = SemanticAdmissionError(
                    SemanticAdmissionKind.TASK_CONTEXT_INVALID,
                    "semantic call requires the worker-owned leased task",
                )
            else:
                target_was_stale = await self._reconcile_stale_attempts(
                    session,
                    run_id=context.run_id,
                    target_call_id=call_id,
                    now=now,
                )
                if target_was_stale:
                    denied = SemanticAdmissionError(
                        SemanticAdmissionKind.REPLAY_UNAVAILABLE,
                        "stale in-flight call was terminally audited and cannot be re-invoked",
                    )
                elif run.status != "RUNNING" or run.deadline_at <= now:
                    denied = SemanticAdmissionError(
                        SemanticAdmissionKind.DEADLINE_EXCEEDED,
                        "research run has no remaining execution time",
                    )
                else:
                    active_count = await session.scalar(
                        select(func.count())
                        .select_from(ProviderCallLease)
                        .where(
                            ProviderCallLease.run_id == context.run_id,
                            ProviderCallLease.lease_expires_at > now,
                        )
                    )
                    raw_parallel = run.budget_limits.get("max_parallel_agent_calls", 2)
                    if (
                        isinstance(raw_parallel, bool)
                        or not isinstance(raw_parallel, int)
                        or not 1 <= raw_parallel <= 2
                    ):
                        raise ValueError("persisted max_parallel_agent_calls must be 1 or 2")
                    if int(active_count or 0) >= raw_parallel:
                        denied = SemanticAdmissionError(
                            SemanticAdmissionKind.PARALLEL_LIMIT,
                            "durable provider-call parallel limit reached",
                        )
                    else:
                        remaining = (run.deadline_at - now).total_seconds()
                        lease = ProviderCallLease(
                            run_id=context.run_id,
                            task_id=context.task_id,
                            call_key=str(call_id),
                            lease_owner=self._lease_owner,
                            lease_expires_at=run.deadline_at,
                            operation=agent_operation(call.request),
                            output_schema_name=call.request.output_schema_name,
                            output_schema_sha256=schema_hash,
                            provider=self._provider_name,
                            requested_model=self._model,
                            effort=call.request.effort.value,
                            repair_attempt=repair_attempt,
                        )
                        session.add(lease)
                        await session.flush()
                        budget = await RunController(session).consume_budget(
                            context.run_id,
                            counter="agent_calls",
                            limit_key="max_agent_calls_per_run",
                            now=now,
                        )
                        if not budget.allowed:
                            await session.delete(lease)
                            denied = SemanticAdmissionError(
                                SemanticAdmissionKind.BUDGET_EXHAUSTED,
                                "persisted agent-call budget is exhausted",
                            )
                        else:
                            admitted = _Admission(
                                lease_id=lease.id,
                                remaining_seconds=remaining,
                            )
        if denied is not None:
            raise denied
        if admitted is None:  # pragma: no cover - exhaustive transaction guard
            raise RuntimeError("provider admission produced no decision")
        return admitted

    async def _reconcile_stale_attempts(
        self,
        session: AsyncSession,
        *,
        run_id: UUID,
        target_call_id: UUID,
        now: datetime,
    ) -> bool:
        stale = (
            await session.scalars(
                select(ProviderCallLease)
                .where(
                    ProviderCallLease.run_id == run_id,
                    ProviderCallLease.lease_expires_at <= now,
                )
                .with_for_update()
            )
        ).all()
        target_was_stale = False
        for lease in stale:
            stale_id = _canonical_call_uuid(lease.call_key)
            target_was_stale = target_was_stale or stale_id == target_call_id
            if await session.get(AgentCall, stale_id) is None:
                elapsed = max(0, round((lease.lease_expires_at - lease.created_at).total_seconds()))
                session.add(
                    AgentCall(
                        id=stale_id,
                        run_id=lease.run_id,
                        task_id=lease.task_id,
                        provider=lease.provider,
                        operation=lease.operation,
                        output_schema_name=lease.output_schema_name,
                        output_schema_sha256=lease.output_schema_sha256,
                        requested_model=lease.requested_model,
                        resolved_model=None,
                        effort=lease.effort,
                        cli_version=None,
                        status=domain.AgentStatus.FAILED.value,
                        duration_ms=elapsed * 1000,
                        repair_attempts=lease.repair_attempt,
                        usage={
                            "input_tokens": 0,
                            "cached_input_tokens": 0,
                            "output_tokens": 0,
                        },
                        error_class="StaleProviderCall",
                        output_json=None,
                        output_sha256=None,
                    )
                )
            await session.delete(lease)
        await session.flush()
        return target_was_stale

    async def _audit_and_release(
        self,
        context: SemanticContext,
        call: SemanticCall,
        *,
        call_id: UUID,
        schema_hash: bytes,
        lease_id: UUID,
        result: provider_contracts.AgentResult,
    ) -> None:
        mapped = _domain_result(str(call_id), result)
        output_hash = _json_hash(mapped.output_json) if mapped.output_json is not None else None
        async with self._session_factory() as session, session.begin():
            lease = await session.scalar(
                select(ProviderCallLease).where(ProviderCallLease.id == lease_id).with_for_update()
            )
            if lease is None or lease.lease_owner != self._lease_owner:
                raise SemanticAdmissionError(
                    SemanticAdmissionKind.INDETERMINATE_ATTEMPT,
                    "provider-call lease disappeared before audit",
                )
            session.add(
                AgentCall(
                    id=call_id,
                    run_id=context.run_id,
                    task_id=context.task_id,
                    provider=result.provider,
                    operation=agent_operation(call.request),
                    output_schema_name=call.request.output_schema_name,
                    output_schema_sha256=schema_hash,
                    requested_model=result.requested_model,
                    resolved_model=result.resolved_model,
                    effort=result.effort.value,
                    cli_version=result.cli_version,
                    status=mapped.status.value,
                    duration_ms=result.duration_ms,
                    repair_attempts=result.repair_attempts,
                    usage=result.usage.model_dump(mode="json"),
                    error_class=result.error_class,
                    output_json=mapped.output_json,
                    output_sha256=output_hash,
                )
            )
            await session.delete(lease)

    def _provider_request(
        self,
        call: SemanticCall,
        *,
        timeout_seconds: float,
    ) -> provider_contracts.AgentRequest:
        return provider_contracts.AgentRequest(
            operation=agent_operation(call.request),
            instructions=(
                f"Perform only the {call.request.task.value} semantic operation. "
                "Use only the supplied bounded input and permitted evidence."
            ),
            evidence={
                "input": call.request.input_json,
                "permitted_evidence_ids": list(call.request.permitted_evidence_ids),
                "permitted_urls": [str(value) for value in call.request.permitted_urls],
            },
            output_schema=call.output_schema,
            effort=provider_contracts.ReasoningEffort(call.request.effort.value),
            model=self._model,
            timeout_seconds=timeout_seconds,
        )

    @staticmethod
    def _provider_result_from_audit(row: AgentCall) -> provider_contracts.AgentResult:
        status = {
            domain.AgentStatus.COMPLETED: provider_contracts.AgentStatus.SUCCESS,
            domain.AgentStatus.INVALID_OUTPUT: provider_contracts.AgentStatus.INVALID_OUTPUT,
            domain.AgentStatus.AUTH_REQUIRED: provider_contracts.AgentStatus.AUTH_REQUIRED,
            domain.AgentStatus.TIMEOUT: provider_contracts.AgentStatus.TIMEOUT,
            domain.AgentStatus.FAILED: provider_contracts.AgentStatus.ERROR,
        }[domain.AgentStatus(row.status)]
        return provider_contracts.AgentResult(
            status=status,
            data=row.output_json,
            usage=provider_contracts.AgentUsage.model_validate(row.usage),
            provider=row.provider,
            requested_model=row.requested_model,
            resolved_model=row.resolved_model,
            effort=provider_contracts.ReasoningEffort(row.effort),
            cli_version=row.cli_version,
            duration_ms=row.duration_ms,
            repair_attempts=row.repair_attempts,
            error_class=row.error_class,
            error_message="prior output failed the audited semantic boundary",
        )

    def _replay(
        self,
        row: AgentCall,
        *,
        context: SemanticContext,
        call: SemanticCall,
        schema_hash: bytes,
    ) -> domain.AgentResult:
        if (
            row.run_id != context.run_id
            or row.task_id != context.task_id
            or row.operation != agent_operation(call.request)
            or row.output_schema_name != call.request.output_schema_name
            or row.output_schema_sha256 != schema_hash
            or row.effort != call.request.effort.value
        ):
            raise SemanticAdmissionError(
                SemanticAdmissionKind.REPLAY_UNAVAILABLE,
                "call ID already exists with different immutable request identity",
            )
        if row.output_json is not None and row.output_sha256 != _json_hash(row.output_json):
            raise SemanticAdmissionError(
                SemanticAdmissionKind.REPLAY_UNAVAILABLE,
                "persisted replay output failed its audit checksum",
            )
        return domain.AgentResult(
            call_id=call.request.call_id,
            status=domain.AgentStatus(row.status),
            output_json=row.output_json,
            provider=cast(Any, row.provider),
            model_requested=row.requested_model or None,
            effort=domain.AgentEffort(row.effort),
            cli_version=row.cli_version,
            duration_ms=row.duration_ms,
            repair_attempted=bool(row.repair_attempts),
            error_class=row.error_class,
        )


def build_semantic_reasoner(
    *, database: Database, settings: Settings, worker_id: str
) -> SemanticReasoner:
    """Build the configured audited reasoner without exposing provider construction to I3."""

    if settings.agent_provider is AgentProviderName.CODEX_CLI:
        boundary: provider_contracts.AgentProvider = CodexCliProvider(
            binary=settings.codex_binary,
            codex_home=settings.codex_home,
        )
    else:
        boundary = FakeAgentProvider({})
    return AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner=worker_id,
        provider_name=settings.agent_provider.value,
        model=settings.codex_model,
    )


def _canonical_call_uuid(value: str) -> UUID:
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise ValueError("agent call_id must be a canonical UUID string") from exc
    if str(parsed) != value:
        raise ValueError("agent call_id must be a canonical UUID string")
    return parsed


def _repair_call_uuid(original: UUID) -> UUID:
    return uuid5(original, "repair:1")


def _json_hash(value: dict[str, Any]) -> bytes:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).digest()


def _domain_result(call_id: str, result: provider_contracts.AgentResult) -> domain.AgentResult:
    status = {
        provider_contracts.AgentStatus.SUCCESS: domain.AgentStatus.COMPLETED,
        provider_contracts.AgentStatus.INVALID_OUTPUT: domain.AgentStatus.INVALID_OUTPUT,
        provider_contracts.AgentStatus.AUTH_REQUIRED: domain.AgentStatus.AUTH_REQUIRED,
        provider_contracts.AgentStatus.TIMEOUT: domain.AgentStatus.TIMEOUT,
        provider_contracts.AgentStatus.ERROR: domain.AgentStatus.FAILED,
    }[result.status]
    return domain.AgentResult(
        call_id=call_id,
        status=status,
        output_json=result.data,
        provider=cast(Any, result.provider),
        model_requested=result.requested_model or None,
        effort=domain.AgentEffort(result.effort.value),
        cli_version=result.cli_version,
        duration_ms=result.duration_ms,
        repair_attempted=bool(result.repair_attempts),
        error_class=result.error_class,
    )


_EVIDENCE_REFERENCE_KEYS = frozenset(
    {
        "evidence_id",
        "evidence_ids",
        "raw_signal_revision_id",
        "raw_signal_revision_ids",
        "representative_evidence_ids",
        "source_id",
        "source_ids",
    }
)


def _enforce_permitted_references(
    request: domain.AgentRequest,
    result: provider_contracts.AgentResult,
) -> provider_contracts.AgentResult:
    if result.status is not provider_contracts.AgentStatus.SUCCESS or result.data is None:
        return result
    evidence_ids: set[str] = set()
    urls: set[str] = set()
    _collect_references(result.data, evidence_ids=evidence_ids, urls=urls)
    if not evidence_ids <= set(request.permitted_evidence_ids):
        return result.model_copy(
            update={
                "status": provider_contracts.AgentStatus.INVALID_OUTPUT,
                "data": None,
                "error_class": "PermittedEvidenceViolation",
                "error_message": "output referenced evidence outside the permitted allowlist",
            }
        )
    if not urls <= {str(value) for value in request.permitted_urls}:
        return result.model_copy(
            update={
                "status": provider_contracts.AgentStatus.INVALID_OUTPUT,
                "data": None,
                "error_class": "PermittedUrlViolation",
                "error_message": "output referenced a URL outside the permitted allowlist",
            }
        )
    return result


def _collect_references(
    value: Any,
    *,
    evidence_ids: set[str],
    urls: set[str],
) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key in _EVIDENCE_REFERENCE_KEYS or key.endswith(("_evidence_id", "_evidence_ids")):
                _collect_strings(child, evidence_ids)
            if key == "url" or key.endswith(("_url", "_urls")):
                _collect_strings(child, urls)
            _collect_references(child, evidence_ids=evidence_ids, urls=urls)
    elif isinstance(value, list):
        for child in value:
            _collect_references(child, evidence_ids=evidence_ids, urls=urls)


def _collect_strings(value: Any, destination: set[str]) -> None:
    if isinstance(value, str):
        destination.add(value)
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, str):
                destination.add(item)
            else:
                destination.add("<invalid-reference-type>")
    else:
        destination.add("<invalid-reference-type>")
