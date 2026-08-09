"""Durable, storage-neutral orchestration of the canonical evidence pipeline."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID, uuid5

from gapforge.analysis.critic import validate_critic_result
from gapforge.analysis.evidence import (
    EvidenceObservation,
    EvidenceRecord,
    build_evidence_card,
    validate_atomic_claim,
    validate_pain_extraction,
)
from gapforge.analysis.hypotheses import (
    validate_alternative_coverage,
    validate_gap_hypothesis,
    validate_problem_hypothesis,
)
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
    Verdict,
)
from gapforge.integration.semantic import SemanticCall, SemanticContext, SemanticReasoner
from gapforge.queue.retry import ErrorKind
from gapforge.research.query_planning import (
    TimeWindow,
    allocate_signal_budget,
    collection_windows,
    compile_plan,
)
from gapforge.runtime.evidence_stages import (
    ClusterBatch,
    CriticBatch,
    GapResearchBatch,
    HypothesisBatch,
    PainExtractionBatch,
)
from gapforge.scoring.engine import ScoringInputs, score_opportunity, validation_decision
from gapforge.storage.models import ResearchTask
from gapforge.worker import TaskHandlerError, TaskHandlerResult


@dataclass(frozen=True, slots=True)
class ExistingCandidate:
    kind: str
    identifier: str
    summary: str


@dataclass(frozen=True, slots=True)
class ExistingIntelligence:
    candidates: tuple[ExistingCandidate, ...] = ()

    @property
    def opportunity_count(self) -> int:
        return sum(candidate.kind == "OPPORTUNITY" for candidate in self.candidates)

    @property
    def evidence_count(self) -> int:
        return sum(candidate.kind == "EVIDENCE" for candidate in self.candidates)


@dataclass(frozen=True, slots=True)
class PipelineContext:
    run_id: UUID
    task_id: UUID
    task_attempt: int
    worker_id: str
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


@dataclass(frozen=True, slots=True)
class CapturedEvidence:
    evidence_id: str
    source: Source
    url: str
    text: str
    observed_at: datetime
    duplicate_group: str
    author_id: str | None
    thread_id: str


class EvidencePipelineStore(Protocol):
    async def load_context(self, task: ResearchTask) -> PipelineContext: ...

    async def query_existing(self, context: PipelineContext) -> ExistingIntelligence: ...

    async def load_stage(
        self,
        context: PipelineContext,
        stage: str,
    ) -> dict[str, object] | None: ...

    async def load_evidence(
        self,
        context: PipelineContext,
        evidence_ids: tuple[str, ...],
    ) -> tuple[CapturedEvidence, ...]: ...

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
                "candidates": [
                    {
                        "kind": candidate.kind,
                        "identifier": candidate.identifier,
                        "summary": candidate.summary,
                    }
                    for candidate in existing.candidates
                ],
            }
            await self._commit(context, "EXISTING", existing_payload)
        candidate_values = existing_payload.get("candidates")
        if not isinstance(candidate_values, list):
            raise ValueError("EXISTING checkpoint candidates must be a list")
        existing = ExistingIntelligence(
            tuple(
                ExistingCandidate(
                    kind=_string_field(value, "kind"),
                    identifier=_string_field(value, "identifier"),
                    summary=_string_field(value, "summary"),
                )
                for value in candidate_values
                if isinstance(value, dict)
            )
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
            try:
                await self._commit(context, "COLLECT", collect_payload)
            except CollectionBudgetExhaustedError as error:
                raise TaskHandlerError(
                    ErrorKind.BUDGET_EXHAUSTED,
                    error_class="CollectionBudgetExhausted",
                ) from error
            durable_collect = await self.store.load_stage(context, "COLLECT")
            if durable_collect is None:
                raise RuntimeError("COLLECT commit did not become durable")
            collect_payload = durable_collect
        result_values = collect_payload.get("results")
        if not isinstance(result_values, list):
            raise ValueError("COLLECT checkpoint results must be a list")
        results = tuple(CollectResult.model_validate(value) for value in result_values)
        warning_codes = _collection_warning_codes(results)
        item_count = _integer_field(collect_payload, "item_count")
        if item_count == 0:
            return TaskHandlerResult(
                payload={
                    "opportunities": 0,
                    "validated": 0,
                    "warnings": warning_codes,
                    "completed_stage": "COLLECT",
                },
                useful_artifact=False,
            )

        evidence_ids = _string_tuple_field(collect_payload, "evidence_ids")
        evidence = await self.store.load_evidence(context, evidence_ids)
        records = {
            item.evidence_id: EvidenceRecord(
                item.evidence_id,
                item.source,
                item.url,
                item.text,
                item.observed_at,
            )
            for item in evidence
        }
        extraction = await self._extract(context, evidence, records)
        if not extraction.pain_signals:
            return TaskHandlerResult(
                payload={
                    "opportunities": 0,
                    "validated": 0,
                    "warnings": warning_codes,
                    "completed_stage": "EXTRACT",
                },
                useful_artifact=True,
            )
        clusters = await self._cluster(context, extraction)
        gap = await self._research_gap(context, evidence, records, clusters)
        cards, scores = self._cards_and_scores(context, evidence, extraction, clusters, gap)
        hypotheses = await self._hypotheses(context, gap, cards, clusters)
        critics = await self._critic(context, gap, cards, hypotheses)
        decisions = []
        score_by_opportunity = {score.opportunity_id: score for score in scores}
        card_by_opportunity = {card.opportunity_id: card for card in cards}
        for result in critics.results:
            decision = validation_decision(
                card=card_by_opportunity.get(result.opportunity_id),
                score=score_by_opportunity[result.opportunity_id],
                competitor_research=gap.competitor_research_status,
                gap_evidence_present=bool(gap.competitor_evidence),
                critic=result,
            )
            decisions.append(
                {
                    "opportunity_id": result.opportunity_id,
                    "verdict": decision.verdict.value,
                    "gates": [asdict(gate) for gate in decision.gates],
                }
            )
        await self._commit(context, "FINAL", {"decisions": decisions})
        validated = sum(item["verdict"] == Verdict.VALIDATE.value for item in decisions)
        return TaskHandlerResult(
            payload={
                "opportunities": len(gap.opportunities),
                "validated": validated,
                "warnings": warning_codes,
                "completed_stage": "FINAL",
            },
            useful_artifact=True,
        )

    async def _extract(
        self,
        context: PipelineContext,
        evidence: tuple[CapturedEvidence, ...],
        records: dict[str, EvidenceRecord],
    ) -> PainExtractionBatch:
        payload = await self._load_or_reason(
            context,
            stage="EXTRACT",
            operation=SemanticOperation.EXTRACT,
            effort=AgentEffort.LOW,
            schema_type=PainExtractionBatch,
            input_json={
                "evidence": [{"id": item.evidence_id, "text": item.text} for item in evidence]
            },
            permitted_evidence_ids=tuple(records),
        )
        batch = PainExtractionBatch.model_validate(payload)
        for signal in batch.pain_signals:
            record = records.get(signal.raw_signal_revision_id)
            if record is None:
                raise ValueError("pain extraction references unknown evidence")
            validate_pain_extraction(signal, record)
        return batch

    async def _cluster(
        self,
        context: PipelineContext,
        extraction: PainExtractionBatch,
    ) -> ClusterBatch:
        payload = await self._load_or_reason(
            context,
            stage="CLUSTER",
            operation=SemanticOperation.CLUSTER,
            effort=AgentEffort.MEDIUM,
            schema_type=ClusterBatch,
            input_json=extraction.model_dump(mode="json"),
            permitted_evidence_ids=tuple(
                signal.raw_signal_revision_id for signal in extraction.pain_signals
            ),
        )
        batch = ClusterBatch.model_validate(payload)
        problem_ids = {problem.id for problem in batch.problems}
        cluster_problem = {cluster.id: cluster.canonical_problem_id for cluster in batch.clusters}
        pain_ids = {signal.id for signal in extraction.pain_signals}
        if not set(cluster_problem.values()) <= problem_ids:
            raise ValueError("cluster references unknown canonical problem")
        if any(
            membership.cluster_id not in cluster_problem
            or membership.pain_signal_id not in pain_ids
            for membership in batch.memberships
        ):
            raise ValueError("cluster membership references unknown artifact")
        return batch

    async def _research_gap(
        self,
        context: PipelineContext,
        evidence: tuple[CapturedEvidence, ...],
        records: dict[str, EvidenceRecord],
        clusters: ClusterBatch,
    ) -> GapResearchBatch:
        payload = await self._load_or_reason(
            context,
            stage="GAP",
            operation=SemanticOperation.GAP,
            effort=AgentEffort.MEDIUM,
            schema_type=GapResearchBatch,
            input_json={
                "problems": [item.model_dump(mode="json") for item in clusters.problems],
                "evidence": [
                    {"id": item.evidence_id, "url": item.url, "text": item.text}
                    for item in evidence
                ],
            },
            permitted_evidence_ids=tuple(records),
            permitted_urls=tuple(item.url for item in evidence),
        )
        batch = GapResearchBatch.model_validate(payload)
        known_claims: set[str] = set()
        for claim in batch.claims:
            validate_atomic_claim(claim, records, known_claim_ids=frozenset(known_claims))
            known_claims.add(claim.id)
        validate_alternative_coverage(batch.competitors)
        competitor_evidence_ids = frozenset(item.id for item in batch.competitor_evidence)
        for hypothesis in batch.gaps:
            validate_gap_hypothesis(
                hypothesis,
                permitted_user_evidence=frozenset(records),
                permitted_competitor_evidence=competitor_evidence_ids,
            )
        gap_ids = {item.id for item in batch.gaps}
        opportunity_ids = {item.id for item in batch.opportunities}
        if any(item.gap_hypothesis_id not in gap_ids for item in batch.opportunities):
            raise ValueError("opportunity references unknown gap hypothesis")
        if {item.opportunity_id for item in batch.score_inputs} != opportunity_ids:
            raise ValueError("every opportunity requires exactly one scoring input")
        return batch

    def _cards_and_scores(
        self,
        context: PipelineContext,
        evidence: tuple[CapturedEvidence, ...],
        extraction: PainExtractionBatch,
        clusters: ClusterBatch,
        gap: GapResearchBatch,
    ) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
        evidence_by_id = {item.evidence_id: item for item in evidence}
        pain_by_id = {item.id: item for item in extraction.pain_signals}
        problem_by_cluster = {item.id: item.canonical_problem_id for item in clusters.clusters}
        pain_by_problem: dict[str, list[Any]] = {}
        for membership in clusters.memberships:
            problem_id = problem_by_cluster[membership.cluster_id]
            pain_by_problem.setdefault(problem_id, []).append(pain_by_id[membership.pain_signal_id])
        gap_by_id = {item.id: item for item in gap.gaps}
        claims_by_evidence: dict[str, list[str]] = {}
        for claim in gap.claims:
            for evidence_id in claim.evidence_ids:
                claims_by_evidence.setdefault(evidence_id, []).append(claim.id)
        cards = []
        scores = []
        inputs_by_opportunity = {item.opportunity_id: item for item in gap.score_inputs}
        for opportunity in gap.opportunities:
            problem_id = gap_by_id[opportunity.gap_hypothesis_id].canonical_problem_id
            observations = []
            for pain in pain_by_problem.get(problem_id, []):
                captured = evidence_by_id[pain.raw_signal_revision_id]
                observations.append(
                    EvidenceObservation(
                        evidence_id=captured.evidence_id,
                        duplicate_group=captured.duplicate_group,
                        author_id=captured.author_id,
                        thread_id=captured.thread_id,
                        source=captured.source,
                        observed_at=captured.observed_at,
                        severity=pain.severity,
                        behavioral_workaround=bool(pain.workaround),
                        paid_or_wtp=pain.payment_signal,
                        supporting_claim_ids=tuple(
                            claims_by_evidence.get(captured.evidence_id, ())
                        ),
                    )
                )
            card = build_evidence_card(
                str(uuid5(context.run_id, f"card:{opportunity.id}")),
                opportunity.id,
                tuple(observations),
            )
            value = inputs_by_opportunity[opportunity.id]
            scoring = ScoringInputs(
                **value.model_dump(exclude={"schema_version", "opportunity_id"}, mode="python")
            )
            score = score_opportunity(
                snapshot_id=str(uuid5(context.run_id, f"score:{opportunity.id}")),
                opportunity_id=opportunity.id,
                mission_revision_id=context.mission_revision.id,
                inputs=scoring,
                evidence_confidence=card.confidence,
                created_at=self.clock(),
            )
            cards.append(card)
            scores.append(score)
        return tuple(cards), tuple(scores)

    async def _hypotheses(
        self,
        context: PipelineContext,
        gap: GapResearchBatch,
        cards: tuple[Any, ...],
        clusters: ClusterBatch,
    ) -> HypothesisBatch:
        payload = await self._load_or_reason(
            context,
            stage="HYPOTHESIS",
            operation=SemanticOperation.HYPOTHESIS,
            effort=AgentEffort.MEDIUM,
            schema_type=HypothesisBatch,
            input_json={
                "problems": [item.model_dump(mode="json") for item in clusters.problems],
                "cards": [item.model_dump(mode="json") for item in cards],
                "claims": [item.model_dump(mode="json") for item in gap.claims],
            },
            permitted_evidence_ids=tuple(
                sorted({value for item in gap.claims for value in item.evidence_ids})
            ),
        )
        batch = HypothesisBatch.model_validate(payload)
        permitted_claims = frozenset(item.id for item in gap.claims)
        for hypothesis in batch.hypotheses:
            validate_problem_hypothesis(hypothesis, permitted_claims)
        return batch

    async def _critic(
        self,
        context: PipelineContext,
        gap: GapResearchBatch,
        cards: tuple[Any, ...],
        hypotheses: HypothesisBatch,
    ) -> CriticBatch:
        payload = await self._load_or_reason(
            context,
            stage="CRITIC",
            operation=SemanticOperation.CRITIC,
            effort=AgentEffort.MEDIUM,
            schema_type=CriticBatch,
            input_json={
                "opportunities": [item.model_dump(mode="json") for item in gap.opportunities],
                "cards": [item.model_dump(mode="json") for item in cards],
                "hypotheses": [item.model_dump(mode="json") for item in hypotheses.hypotheses],
                "gaps": [item.model_dump(mode="json") for item in gap.gaps],
                "claims": [item.model_dump(mode="json") for item in gap.claims],
            },
            permitted_evidence_ids=tuple(
                sorted({value for item in gap.claims for value in item.evidence_ids})
            ),
        )
        batch = CriticBatch.model_validate(payload)
        opportunity_ids = {item.id for item in gap.opportunities}
        if {item.opportunity_id for item in batch.results} != opportunity_ids:
            raise ValueError("critic must return one result per opportunity")
        permitted_claims = frozenset(item.id for item in gap.claims)
        for result in batch.results:
            validate_critic_result(result, permitted_claims)
        return batch

    async def _load_or_reason(
        self,
        context: PipelineContext,
        *,
        stage: str,
        operation: SemanticOperation,
        effort: AgentEffort,
        schema_type: type[Any],
        input_json: dict[str, Any],
        permitted_evidence_ids: tuple[str, ...],
        permitted_urls: tuple[str, ...] = (),
    ) -> dict[str, object]:
        existing = await self.store.load_stage(context, stage)
        if existing is not None:
            return existing
        schema = schema_type.model_json_schema()
        schema["title"] = f"GapForge {stage} v0.1"
        request = AgentRequest(
            call_id=self._call_id(context, operation, 1),
            task=operation,
            effort=effort,
            input_json=input_json,
            permitted_evidence_ids=permitted_evidence_ids,
            permitted_urls=permitted_urls,
            output_schema_name=f"{stage.casefold()}-v1",
            timeout_seconds=300,
        )
        result = await self.reasoner.run(
            SemanticContext(context.run_id, context.task_id),
            SemanticCall(request, schema),
        )
        if result.status is not AgentStatus.COMPLETED or result.output_json is None:
            raise RuntimeError(f"{stage} failed: {result.status.value}")
        await self._commit(context, stage, result.output_json)
        return result.output_json

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
                    "candidates": [
                        {
                            "kind": candidate.kind,
                            "id": candidate.identifier,
                            "summary": candidate.summary,
                        }
                        for candidate in existing.candidates
                    ],
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


def _string_field(payload: Mapping[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a nonempty string")
    return value


def _string_tuple_field(payload: Mapping[str, object], name: str) -> tuple[str, ...]:
    value = payload.get(name)
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError(f"{name} must be a list of strings")
    return tuple(value)


class CollectionBudgetExhaustedError(RuntimeError):
    """Storage refused a collection stage without committing partial effects."""
