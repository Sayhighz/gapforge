"""Durable, storage-neutral orchestration of the canonical evidence pipeline."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID, uuid5

from gapforge.collectors.base import Collector, collect_isolated
from gapforge.domain.contracts import (
    AgentEffort,
    AgentRequest,
    AgentStatus,
    Availability,
    CollectRequest,
    CollectResult,
    MissionRevision,
    QueryPlan,
    RunMode,
    SemanticOperation,
    Source,
    SourceCheckpoint,
)
from gapforge.integration.semantic import SemanticCall, SemanticContext, SemanticReasoner
from gapforge.research.query_planning import (
    TimeWindow,
    allocate_signal_budget,
    collection_windows,
    compile_plan,
)
from gapforge.storage.models import ResearchTask
from gapforge.worker import TaskHandlerResult


@dataclass(frozen=True, slots=True)
class ExistingIntelligence:
    opportunity_count: int
    evidence_count: int


@dataclass(frozen=True, slots=True)
class PipelineContext:
    run_id: UUID
    task_id: UUID
    task_attempt: int
    mission_revision: MissionRevision
    mode: RunMode
    budget_limits: dict[str, int]
    collection_until: datetime
    source_checkpoints: Mapping[Source, SourceCheckpoint] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PipelineStageCommit:
    stage: str
    idempotency_key: str
    payload: dict[str, Any]


class EvidencePipelineStore(Protocol):
    async def load_context(self, task: ResearchTask) -> PipelineContext: ...

    async def query_existing(self, context: PipelineContext) -> ExistingIntelligence: ...

    async def load_stage(
        self,
        context: PipelineContext,
        stage: str,
    ) -> dict[str, object] | None: ...

    async def commit_stage(
        self,
        context: PipelineContext,
        commit: PipelineStageCommit,
    ) -> None: ...


class EvidencePipeline:
    """Run bounded pipeline stages behind injected storage and semantic ports."""

    def __init__(
        self,
        *,
        store: EvidencePipelineStore,
        reasoner: SemanticReasoner,
        collectors: Mapping[Source, Collector],
        clock: Callable[[], datetime],
    ) -> None:
        self.store = store
        self.reasoner = reasoner
        self.collectors = dict(collectors)
        self.clock = clock

    async def __call__(self, task: ResearchTask) -> TaskHandlerResult:
        context = await self.store.load_context(task)
        existing_payload = await self.store.load_stage(context, "EXISTING")
        if existing_payload is None:
            existing = await self.store.query_existing(context)
            existing_payload = {
                "opportunity_count": existing.opportunity_count,
                "evidence_count": existing.evidence_count,
            }
            await self._commit(context, "EXISTING", existing_payload)
        existing = ExistingIntelligence(
            opportunity_count=_integer_field(existing_payload, "opportunity_count"),
            evidence_count=_integer_field(existing_payload, "evidence_count"),
        )

        plan_payload = await self.store.load_stage(context, "QUERY_PLAN")
        if plan_payload is None:
            plan = await self._query_plan(context, existing)
            plan_payload = plan.model_dump(mode="json")
            await self._commit(context, "QUERY_PLAN", plan_payload)
        plan = QueryPlan.model_validate(plan_payload)

        collect_payload = await self.store.load_stage(context, "COLLECT")
        if collect_payload is None:
            results = await self._collect(context, plan)
            collect_payload = {
                "results": [result.model_dump(mode="json") for result in results],
                "item_count": sum(len(result.items) for result in results),
            }
            await self._commit(context, "COLLECT", collect_payload)
        result_values = collect_payload.get("results")
        if not isinstance(result_values, list):
            raise ValueError("COLLECT checkpoint results must be a list")
        results = tuple(CollectResult.model_validate(value) for value in result_values)
        warning_codes = _collection_warning_codes(results)
        return TaskHandlerResult(
            payload={
                "opportunities": 0,
                "validated": 0,
                "warnings": warning_codes,
                "completed_stage": "COLLECT",
            },
            useful_artifact=_integer_field(collect_payload, "item_count") > 0,
        )

    async def _query_plan(
        self,
        context: PipelineContext,
        existing: ExistingIntelligence,
    ) -> QueryPlan:
        schema = QueryPlan.model_json_schema()
        schema["title"] = "GapForge QueryPlan v0.1"
        request = AgentRequest(
            call_id=self._call_id(context, SemanticOperation.QUERY_PLAN, 1),
            task=SemanticOperation.QUERY_PLAN,
            effort=AgentEffort.MEDIUM,
            input_json={
                "mission": context.mission_revision.prompt,
                "existing": {
                    "opportunities": existing.opportunity_count,
                    "evidence": existing.evidence_count,
                },
                "round": 1,
            },
            permitted_evidence_ids=(),
            output_schema_name="query-plan-v1",
            timeout_seconds=300,
        )
        result = await self.reasoner.run(
            SemanticContext(run_id=context.run_id, task_id=context.task_id),
            SemanticCall(request=request, output_schema=schema),
        )
        if result.status is not AgentStatus.COMPLETED or result.output_json is None:
            raise RuntimeError(f"query planning failed: {result.status.value}")
        return QueryPlan.model_validate(result.output_json)

    async def _collect(
        self, context: PipelineContext, plan: QueryPlan
    ) -> tuple[CollectResult, ...]:
        compiled = compile_plan(plan)
        sources = tuple(item.source for item in compiled if item.source in self.collectors)
        source_budgets = allocate_signal_budget(
            sources,
            total=context.budget_limits["max_raw_signals_per_run"],
        )
        windows = collection_windows(
            context.mode,
            context.collection_until,
            last_successful_watermark=_last_watermark(context),
            lookback_days=context.budget_limits["initial_lookback_days"],
        )
        candidates: list[tuple[Source, Collector, Any, TimeWindow]] = []
        intent_by_id = {intent.id: intent for intent in plan.intents}
        for query in compiled:
            collector = self.collectors.get(query.source)
            if collector is None:
                continue
            candidates.extend((query.source, collector, query, window) for window in windows)

        signal_limits: dict[int, int] = {}
        for source, source_budget in source_budgets.items():
            source_indexes = [
                index for index, candidate in enumerate(candidates) if candidate[0] is source
            ]
            for index, limit in zip(
                source_indexes,
                _allocate_positive(source_budget, len(source_indexes)),
                strict=True,
            ):
                if limit:
                    signal_limits[index] = limit

        selected_indexes = tuple(signal_limits)[
            : context.budget_limits["max_collector_requests_per_run"]
        ]
        request_limits = _allocate_positive(
            context.budget_limits["max_collector_requests_per_run"],
            len(selected_indexes),
        )
        jobs: list[tuple[Source, Collector, CollectRequest]] = []
        for index, request_limit in zip(selected_indexes, request_limits, strict=True):
            source, collector, query, window = candidates[index]
            jobs.append(
                (
                    source,
                    collector,
                    CollectRequest(
                        mission_revision_id=context.mission_revision.id,
                        intent=intent_by_id[query.intent_id],
                        since=window.since,
                        until=window.until,
                        max_requests=request_limit,
                        max_signals=signal_limits[index],
                        checkpoint=context.source_checkpoints.get(source),
                    ),
                )
            )
        return await collect_isolated(jobs)

    async def _commit(
        self,
        context: PipelineContext,
        stage: str,
        payload: dict[str, Any],
    ) -> None:
        await self.store.commit_stage(
            context,
            PipelineStageCommit(
                stage=stage,
                idempotency_key=str(uuid5(context.run_id, f"pipeline:{stage}:v1")),
                payload=payload,
            ),
        )

    @staticmethod
    def _call_id(
        context: PipelineContext,
        operation: SemanticOperation,
        round_number: int,
    ) -> str:
        return str(
            uuid5(
                context.run_id,
                f"{context.task_id}:{operation.value}:{round_number}:{context.task_attempt}",
            )
        )


def _allocate_positive(total: int, count: int) -> tuple[int, ...]:
    """Allocate integer work without inventing jobs when the budget is smaller."""
    if count == 0:
        return ()
    fair, remainder = divmod(total, count)
    return tuple(fair + (1 if index < remainder else 0) for index in range(count))


def _last_watermark(context: PipelineContext) -> datetime | None:
    watermarks = tuple(
        checkpoint.watermark
        for checkpoint in context.source_checkpoints.values()
        if checkpoint.watermark is not None
    )
    return min(watermarks) if watermarks else None


def _collection_warning_codes(results: tuple[CollectResult, ...]) -> list[str]:
    warnings = {warning.code for result in results for warning in result.warnings}
    warnings.update(
        f"{result.source.value}_{result.availability.value}"
        for result in results
        if result.availability is not Availability.AVAILABLE
    )
    return sorted(warnings)


def _integer_field(payload: Mapping[str, object], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value
