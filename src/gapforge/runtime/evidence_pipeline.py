"""Durable, storage-neutral orchestration of the canonical evidence pipeline."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from gapforge.analysis.critic import research_more_intents, validate_critic_result
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
from gapforge.analysis.lifecycle import TrendObservation, classify_trend
from gapforge.analysis.normalization import normalize_text, normalize_url
from gapforge.collectors.base import Collector, collect_isolated
from gapforge.domain.contracts import (
    AgentEffort,
    AgentRequest,
    AgentStatus,
    Availability,
    CanonicalProblem,
    CollectRequest,
    CollectResult,
    CompetitorResearchStatus,
    CriticResult,
    EvidenceCard,
    FetchResult,
    FetchSnapshot,
    MissionRevision,
    OpportunityScoreSnapshot,
    QueryIntent,
    QueryPlan,
    RunMode,
    RunWarning,
    SearchResponse,
    SemanticOperation,
    Source,
    SourceCheckpoint,
    SourceWarning,
    TrendLabel,
    Verdict,
)
from gapforge.integration.semantic import (
    SemanticAdmissionError,
    SemanticAdmissionKind,
    SemanticCall,
    SemanticContext,
    SemanticReasoner,
)
from gapforge.queue.retry import ErrorKind
from gapforge.research.query_planning import (
    TimeWindow,
    allocate_signal_budget,
    collection_windows,
    compile_plan,
)
from gapforge.runtime.evidence_stages import (
    ClusterBatch,
    CompetitorResearchCheckpoint,
    CriticBatch,
    GapResearchBatch,
    HypothesisBatch,
    HypothesisCardLink,
    PainExtractionBatch,
)
from gapforge.scoring.engine import ScoringInputs, score_opportunity, validation_decision
from gapforge.storage.models import ResearchTask
from gapforge.worker import TaskHandlerError, TaskHandlerResult

ARTIFACT_NAMESPACE = uuid5(NAMESPACE_URL, "https://gapforge.dev/v0.1/artifacts")
MAX_SEMANTIC_INPUT_BYTES = 20_000


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
    source_created_at: datetime
    duplicate_group: str
    author_id: str | None
    thread_id: str


@dataclass(slots=True)
class PipelineExecution:
    omissions: dict[str, int] = field(default_factory=dict)

    def record_input(self, stage: str, input_json: Mapping[str, Any]) -> None:
        bounds = input_json.get("input_bounds")
        if not isinstance(bounds, dict):
            return
        counts = bounds.get("omitted_counts")
        if not isinstance(counts, dict):
            return
        for section, count in counts.items():
            if isinstance(section, str) and isinstance(count, int) and count > 0:
                self.omissions[f"{stage}.{section}"] = count

    def record_checkpoint(self, stage: str, payload: Mapping[str, Any]) -> None:
        bounds = payload.get("_input_bounds")
        if isinstance(bounds, dict):
            self.record_input(stage, {"input_bounds": bounds})

    def checkpoint_payload(self, stage: str, artifact: dict[str, Any]) -> dict[str, Any]:
        counts = {
            key.removeprefix(f"{stage}."): value
            for key, value in self.omissions.items()
            if key.startswith(f"{stage}.")
        }
        return {
            **artifact,
            "_input_bounds": {"omitted_counts": counts},
        }


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

    async def remaining_agent_calls(self, context: PipelineContext) -> int: ...

    async def remaining_collection_budget(self, context: PipelineContext) -> tuple[int, int]: ...

    async def reserve_external_budget(
        self,
        context: PipelineContext,
        *,
        stage: str,
        collector_requests: int = 0,
        search_calls: int = 0,
    ) -> bool: ...


class CompetitorSearchPort(Protocol):
    async def search(
        self, query: str, *, max_results: int, max_requests: int
    ) -> SearchResponse: ...


class SafeFetchPort(Protocol):
    async def fetch(self, url: str, *, approved_urls: tuple[str, ...]) -> FetchResult: ...


class EvidencePipeline:
    """Run bounded pipeline stages behind injected storage and semantic ports."""

    def __init__(
        self,
        *,
        store: EvidencePipelineStore,
        reasoner: SemanticReasoner,
        collectors: Mapping[Source, Collector],
        competitor_search: CompetitorSearchPort | None = None,
        safe_fetch: SafeFetchPort | None = None,
        clock: Callable[[], datetime],
    ) -> None:
        self.store = store
        self.reasoner = reasoner
        self.collectors = dict(collectors)
        self.competitor_search = competitor_search
        self.safe_fetch = safe_fetch
        self.clock = clock

    async def __call__(self, task: ResearchTask) -> TaskHandlerResult:
        try:
            return await self._run(task)
        except SemanticAdmissionError as error:
            raise _semantic_task_error(error) from error
        except CollectionBudgetExhaustedError as error:
            raise TaskHandlerError(
                ErrorKind.BUDGET_EXHAUSTED,
                error_class="ExternalRequestBudgetExhausted",
            ) from error
        except ExternalAttemptIndeterminateError as error:
            raise TaskHandlerError(
                ErrorKind.INTEGRITY,
                error_class="ExternalAttemptIndeterminate",
            ) from error

    async def _run(self, task: ResearchTask) -> TaskHandlerResult:
        context = await self.store.load_context(task)
        execution = PipelineExecution()
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
            plan = await self._query_plan(context, existing, execution)
            plan_payload = execution.checkpoint_payload("QUERY_PLAN", plan.model_dump(mode="json"))
            await self._commit(context, "QUERY_PLAN", plan_payload)
        else:
            execution.record_checkpoint("QUERY_PLAN", plan_payload)
        plan = QueryPlan.model_validate(_artifact_payload(plan_payload))

        collect_payload = await self.store.load_stage(context, "COLLECT")
        if collect_payload is None:
            results = await self._collect(context, plan, stage="COLLECT")
            collect_payload = {
                "results": [result.model_dump(mode="json") for result in results],
                "item_count": sum(len(result.items) for result in results),
                "examined_volume": _examined_volume(results, as_of=context.collection_until),
                "examined_items": _examined_items(results),
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
        _append_omission_warning(warning_codes, execution)
        if item_count == 0:
            return TaskHandlerResult(
                payload={
                    "opportunities": 0,
                    "validated": 0,
                    "warnings": warning_codes,
                    "completed_stage": "COLLECT",
                },
                useful_artifact=False,
                warnings=_run_warnings(
                    warning_codes,
                    omissions=execution.omissions,
                ),
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
        extraction = await self._extract(context, evidence, records, execution)
        _append_omission_warning(warning_codes, execution)
        if not extraction.pain_signals:
            return TaskHandlerResult(
                payload={
                    "opportunities": 0,
                    "validated": 0,
                    "warnings": warning_codes,
                    "completed_stage": "EXTRACT",
                },
                useful_artifact=True,
                warnings=_run_warnings(
                    warning_codes,
                    omissions=execution.omissions,
                ),
            )
        clusters = await self._cluster(context, extraction, execution)
        try:
            competitor_research = await self._competitor_research(context, clusters)
        except CollectionBudgetExhaustedError as error:
            raise TaskHandlerError(
                ErrorKind.BUDGET_EXHAUSTED,
                error_class="SearchBudgetExhausted",
            ) from error
        warning_codes.extend(competitor_research.warning_codes)
        warning_codes = sorted(set(warning_codes))
        gap = await self._research_gap(
            context,
            evidence,
            records,
            clusters,
            competitor_research,
            execution,
        )
        cards, scores = await self._cards_and_scores(
            context,
            evidence,
            extraction,
            clusters,
            gap,
            examined_volume=_examined_volume_field(collect_payload),
        )
        hypotheses = await self._hypotheses(context, gap, cards, clusters, execution)
        critics = await self._critic(context, gap, cards, hypotheses, execution)
        gap, cards, scores, hypotheses, critics, research_more_warning = await self._round_two(
            context,
            evidence,
            extraction,
            clusters,
            competitor_research,
            gap,
            cards,
            scores,
            hypotheses,
            critics,
            execution,
            examined_volume=_examined_volume_field(collect_payload),
            examined_items=_examined_items_field(collect_payload),
        )
        if research_more_warning is not None:
            warning_codes.append(research_more_warning)
        _append_omission_warning(warning_codes, execution)
        round_two_result = await self.store.load_stage(context, "RESEARCH_MORE_RESULT")
        final_round = (
            2
            if round_two_result is not None and round_two_result.get("status") == "COMPLETE"
            else 1
        )
        decisions = []
        score_by_opportunity = {score.opportunity_id: score for score in scores}
        card_by_opportunity = {card.opportunity_id: card for card in cards}
        gap_id_by_opportunity = {
            opportunity.id: opportunity.gap_hypothesis_id for opportunity in gap.opportunities
        }
        for result in critics.results:
            decision = validation_decision(
                card=card_by_opportunity.get(result.opportunity_id),
                score=score_by_opportunity[result.opportunity_id],
                competitor_research=competitor_research.status,
                gap_evidence_present=_gap_evidence_present(gap, result.opportunity_id),
                critic=result,
            )
            decisions.append(
                {
                    "opportunity_id": result.opportunity_id,
                    "verdict": decision.verdict.value,
                    "gates": [asdict(gate) for gate in decision.gates],
                    "round_number": final_round,
                    "evidence_card_id": card_by_opportunity[result.opportunity_id].id,
                    "score_snapshot_id": score_by_opportunity[result.opportunity_id].id,
                    "gap_hypothesis_id": gap_id_by_opportunity[result.opportunity_id],
                    "critic_result_id": str(
                        uuid5(
                            context.run_id,
                            f"critic:{final_round}:{result.opportunity_id}",
                        )
                    ),
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
            warnings=_run_warnings(
                warning_codes,
                omissions=execution.omissions,
            ),
        )

    async def _round_two(
        self,
        context: PipelineContext,
        evidence: tuple[CapturedEvidence, ...],
        extraction: PainExtractionBatch,
        clusters: ClusterBatch,
        competitor_research: CompetitorResearchCheckpoint,
        gap: GapResearchBatch,
        cards: tuple[EvidenceCard, ...],
        scores: tuple[OpportunityScoreSnapshot, ...],
        hypotheses: HypothesisBatch,
        critics: CriticBatch,
        execution: PipelineExecution,
        *,
        examined_volume: tuple[int, int],
        examined_items: tuple[tuple[str, str, datetime], ...],
    ) -> tuple[
        GapResearchBatch,
        tuple[EvidenceCard, ...],
        tuple[OpportunityScoreSnapshot, ...],
        HypothesisBatch,
        CriticBatch,
        str | None,
    ]:
        recommendation_pairs = tuple(
            (intent, result.opportunity_id)
            for result in critics.results
            if result.verdict is Verdict.RESEARCH_MORE
            for intent in research_more_intents(
                result,
                completed_rounds=1,
                remaining_agent_calls=4,
            )
        )
        unique = {intent.id: intent for intent, _ in recommendation_pairs}
        intents = tuple(unique[key] for key in sorted(unique))[:4]
        if not intents:
            return gap, cards, scores, hypotheses, critics, None
        selected_intent_ids = {intent.id for intent in intents}
        intent_targets = {
            intent_id: tuple(
                sorted(
                    {
                        opportunity_id
                        for intent, opportunity_id in recommendation_pairs
                        if intent.id == intent_id
                    }
                )
            )
            for intent_id in sorted(selected_intent_ids)
        }
        opportunity_by_id = {item.id: item for item in gap.opportunities}
        gap_by_id = {item.id: item for item in gap.gaps}
        problem_by_id = {item.id: item for item in clusters.problems}
        target_contexts = []
        for intent_id, opportunity_ids in intent_targets.items():
            for opportunity_id in opportunity_ids:
                opportunity = opportunity_by_id.get(opportunity_id)
                if opportunity is None:
                    raise ValueError("research-more intent references an unknown opportunity")
                target_gap = gap_by_id[opportunity.gap_hypothesis_id]
                target_problem = problem_by_id[target_gap.canonical_problem_id]
                target_contexts.append(
                    {
                        "intent_id": intent_id,
                        "opportunity": opportunity.model_dump(mode="json"),
                        "gap_hypothesis": target_gap.model_dump(mode="json"),
                        "canonical_problem": target_problem.model_dump(mode="json"),
                    }
                )
        existing = await self.store.load_stage(context, "RESEARCH_MORE_CONTROL")
        semantic_stages = ("EXTRACT_R2", "CLUSTER_R2", "GAP_R2", "HYPOTHESIS_R2", "CRITIC_R2")
        completed_values: list[str] = []
        for semantic_stage in semantic_stages:
            if await self.store.load_stage(context, semantic_stage) is not None:
                completed_values.append(semantic_stage)
        completed = tuple(completed_values)
        remaining = await self.store.remaining_agent_calls(context)
        required = len(semantic_stages) - len(completed)
        if existing is None:
            if context.budget_limits.get("max_research_rounds", 1) < 2:
                status = "STOPPED_ROUND_LIMIT"
            elif remaining < required:
                status = "STOPPED_AGENT_BUDGET"
            else:
                status = "EXECUTING"
            await self._commit(
                context,
                "RESEARCH_MORE_CONTROL",
                {
                    "status": status,
                    "remaining_agent_calls": remaining,
                    "required_agent_calls": required,
                    "intents": [intent.model_dump(mode="json") for intent in intents],
                    "intent_targets": target_contexts,
                },
            )
        else:
            status = _string_field(existing, "status")
            stored_intents = existing.get("intents")
            if not isinstance(stored_intents, list):
                raise ValueError("research-more checkpoint intents must be a list")
            intents = tuple(QueryIntent.model_validate(item) for item in stored_intents)
            stored_targets = existing.get("intent_targets")
            if not isinstance(stored_targets, list) or not all(
                isinstance(item, dict) for item in stored_targets
            ):
                raise ValueError("research-more checkpoint targets must be a list")
            target_contexts = [dict(item) for item in stored_targets]
        if status.startswith("STOPPED_"):
            warning = (
                "RESEARCH_MORE_STOPPED_AGENT_BUDGET"
                if status == "STOPPED_AGENT_BUDGET"
                else "RESEARCH_MORE_STOPPED_ROUND_LIMIT"
            )
            return gap, cards, scores, hypotheses, critics, warning
        if remaining < required:
            raise TaskHandlerError(
                ErrorKind.BUDGET_EXHAUSTED,
                error_class="ResearchMoreAgentBudgetExhausted",
            )
        request_budget, signal_budget = await self.store.remaining_collection_budget(context)
        if request_budget == 0 or signal_budget == 0:
            result_payload = {
                "status": "STOPPED_COLLECTION_BUDGET",
                "new_evidence_ids": [],
            }
            if await self.store.load_stage(context, "RESEARCH_MORE_RESULT") is None:
                await self._commit(context, "RESEARCH_MORE_RESULT", result_payload)
            return (
                gap,
                cards,
                scores,
                hypotheses,
                critics,
                "RESEARCH_MORE_STOPPED_COLLECTION_BUDGET",
            )
        round_context = replace(
            context,
            budget_limits={
                **context.budget_limits,
                "max_collector_requests_per_run": request_budget,
                "max_raw_signals_per_run": signal_budget,
            },
        )
        collect_payload = await self.store.load_stage(context, "COLLECT_R2")
        if collect_payload is None:
            plan = QueryPlan(round_number=2, intents=intents)
            results = await self._collect(round_context, plan, stage="COLLECT_R2")
            collect_payload = {
                "results": [item.model_dump(mode="json") for item in results],
                "item_count": sum(len(item.items) for item in results),
                "examined_volume": _examined_volume(results, as_of=context.collection_until),
                "examined_items": _examined_items(results),
                "intent_targets": target_contexts,
            }
            await self._commit(context, "COLLECT_R2", collect_payload)
            collect_payload = await self.store.load_stage(context, "COLLECT_R2") or collect_payload
        new_ids = _string_tuple_field(collect_payload, "evidence_ids")
        prior_ids = {item.evidence_id for item in evidence}
        genuinely_new_ids = tuple(sorted(set(new_ids) - prior_ids))
        if not genuinely_new_ids:
            result_payload = {
                "status": "STOPPED_NO_NEW_EVIDENCE",
                "new_evidence_ids": [],
            }
            if await self.store.load_stage(context, "RESEARCH_MORE_RESULT") is None:
                await self._commit(context, "RESEARCH_MORE_RESULT", result_payload)
            return (
                gap,
                cards,
                scores,
                hypotheses,
                critics,
                "RESEARCH_MORE_NO_NEW_EVIDENCE",
            )
        combined_ids = tuple(sorted(prior_ids | set(genuinely_new_ids)))
        combined_evidence = await self.store.load_evidence(context, combined_ids)
        new_evidence = tuple(
            item for item in combined_evidence if item.evidence_id in genuinely_new_ids
        )
        records = {
            item.evidence_id: EvidenceRecord(
                item.evidence_id, item.source, item.url, item.text, item.observed_at
            )
            for item in combined_evidence
        }
        new_extraction = await self._extract(
            context, new_evidence, records, execution, stage="EXTRACT_R2", round_number=2
        )
        merged_pains = {
            item.id: item for item in (*extraction.pain_signals, *new_extraction.pain_signals)
        }
        extraction = PainExtractionBatch(
            pain_signals=tuple(merged_pains[key] for key in sorted(merged_pains))
        )
        round_two_clusters = await self._cluster(
            context,
            extraction,
            execution,
            stage="CLUSTER_R2",
            round_number=2,
            priority_pain_ids=frozenset(item.id for item in new_extraction.pain_signals),
            target_contexts=tuple(target_contexts),
        )
        clusters = _merge_cluster_batches(clusters, round_two_clusters)
        round_two_gap = await self._research_gap(
            context,
            combined_evidence,
            records,
            clusters,
            competitor_research,
            execution,
            stage="GAP_R2",
            round_number=2,
            priority_evidence_ids=frozenset(genuinely_new_ids),
            target_contexts=tuple(target_contexts),
        )
        gap = _merge_gap_batches(gap, round_two_gap)
        round_two_examined_items = _examined_items_field(collect_payload)
        if examined_items or round_two_examined_items:
            volumes = _examined_volume_from_items(
                (*examined_items, *round_two_examined_items),
                as_of=context.collection_until,
            )
        else:
            round_two_volume = _examined_volume_field(collect_payload)
            volumes = (
                examined_volume[0] + round_two_volume[0],
                examined_volume[1] + round_two_volume[1],
            )
        cards, scores = await self._cards_and_scores(
            context,
            combined_evidence,
            extraction,
            clusters,
            gap,
            examined_volume=(volumes[0], volumes[1]),
            stage="CARD_SCORE_R2",
            round_number=2,
        )
        hypotheses = await self._hypotheses(
            context,
            gap,
            cards,
            clusters,
            execution,
            stage="HYPOTHESIS_R2",
            round_number=2,
        )
        critics = await self._critic(
            context,
            gap,
            cards,
            hypotheses,
            execution,
            stage="CRITIC_R2",
            round_number=2,
        )
        result_payload = {
            "status": "COMPLETE",
            "new_evidence_ids": list(genuinely_new_ids),
            "target_opportunity_ids": sorted(
                {
                    opportunity_id
                    for opportunity_ids in intent_targets.values()
                    for opportunity_id in opportunity_ids
                }
            ),
        }
        if await self.store.load_stage(context, "RESEARCH_MORE_RESULT") is None:
            await self._commit(context, "RESEARCH_MORE_RESULT", result_payload)
        return gap, cards, scores, hypotheses, critics, None

    async def _extract(
        self,
        context: PipelineContext,
        evidence: tuple[CapturedEvidence, ...],
        records: dict[str, EvidenceRecord],
        execution: PipelineExecution,
        *,
        stage: str = "EXTRACT",
        round_number: int = 1,
    ) -> PainExtractionBatch:
        evidence_input = _bounded_evidence_input(evidence)
        input_json = _bounded_stage_input(
            (("evidence", evidence_input),),
            total_counts={"evidence": len(evidence)},
        )
        permitted = _selected_ids(input_json, "evidence")
        payload, is_new = await self._load_or_reason(
            context,
            stage=stage,
            operation=SemanticOperation.EXTRACT,
            effort=AgentEffort.LOW,
            schema_type=PainExtractionBatch,
            input_json=input_json,
            permitted_evidence_ids=permitted,
            execution=execution,
            round_number=round_number,
        )
        provider_batch = PainExtractionBatch.model_validate(payload)
        normalized = []
        for signal in provider_batch.pain_signals:
            record = records.get(signal.raw_signal_revision_id)
            if record is None:
                raise ValueError("pain extraction references unknown evidence")
            if signal.raw_signal_revision_id not in permitted:
                raise ValueError("pain extraction references evidence outside its bounded input")
            validate_pain_extraction(signal, record)
            normalized.append(
                signal.model_copy(
                    update={
                        "id": str(
                            uuid5(
                                ARTIFACT_NAMESPACE,
                                "pain:"
                                f"{signal.raw_signal_revision_id}:"
                                f"{normalize_text(signal.pain)}",
                            )
                        )
                    }
                )
            )
        batch = PainExtractionBatch(pain_signals=tuple(normalized))
        if is_new:
            await self._commit(
                context,
                stage,
                execution.checkpoint_payload(stage, batch.model_dump(mode="json")),
            )
        return batch

    async def _competitor_research(
        self,
        context: PipelineContext,
        clusters: ClusterBatch,
    ) -> CompetitorResearchCheckpoint:
        existing = await self.store.load_stage(context, "COMPETITOR_RESEARCH")
        if existing is not None:
            return CompetitorResearchCheckpoint.model_validate(existing)
        remaining_search_calls = context.budget_limits.get("max_search_calls_per_run", 0)
        if self.competitor_search is None or self.safe_fetch is None or remaining_search_calls <= 0:
            checkpoint = CompetitorResearchCheckpoint(
                status=CompetitorResearchStatus.RESEARCH_UNAVAILABLE,
                searches=(),
                snapshots=(),
                warning_codes=("COMPETITOR_RESEARCH_UNAVAILABLE",),
            )
        else:
            query = " OR ".join(problem.summary for problem in clusters.problems[:4])[:500]
            admitted = await self.store.reserve_external_budget(
                context,
                stage="COMPETITOR_RESEARCH",
                search_calls=min(1, remaining_search_calls),
            )
            if not admitted:
                raise ExternalAttemptIndeterminateError(
                    "competitor search reservation is indeterminate after interruption"
                )
            response = await self.competitor_search.search(
                query,
                max_results=10,
                max_requests=min(1, remaining_search_calls),
            )
            if response.request_count > min(1, remaining_search_calls):
                raise ValueError("competitor search exceeded its admitted request budget")
            approved_urls = tuple(str(result.url) for result in response.results)
            snapshots: list[FetchSnapshot] = []
            fetch_warnings: set[str] = {warning.code for warning in response.warnings}
            for result in response.results[:10]:
                fetched = await self.safe_fetch.fetch(str(result.url), approved_urls=approved_urls)
                fetch_warnings.update(warning.code for warning in fetched.warnings)
                if fetched.snapshot is not None:
                    snapshots.append(fetched.snapshot)
            status = (
                CompetitorResearchStatus.COMPLETE
                if snapshots
                else CompetitorResearchStatus.RESEARCH_UNAVAILABLE
            )
            if status is CompetitorResearchStatus.RESEARCH_UNAVAILABLE:
                fetch_warnings.add("COMPETITOR_RESEARCH_UNAVAILABLE")
            checkpoint = CompetitorResearchCheckpoint(
                status=status,
                searches=(response,),
                snapshots=tuple(snapshots),
                warning_codes=tuple(sorted(fetch_warnings)),
            )
        await self._commit(
            context,
            "COMPETITOR_RESEARCH",
            checkpoint.model_dump(mode="json"),
        )
        return checkpoint

    async def _cluster(
        self,
        context: PipelineContext,
        extraction: PainExtractionBatch,
        execution: PipelineExecution,
        *,
        stage: str = "CLUSTER",
        round_number: int = 1,
        priority_pain_ids: frozenset[str] = frozenset(),
        target_contexts: tuple[dict[str, Any], ...] = (),
    ) -> ClusterBatch:
        target_problems = {
            problem.id: problem
            for item in target_contexts
            if isinstance((value := item.get("canonical_problem")), dict)
            for problem in (CanonicalProblem.model_validate(value),)
        }
        input_json = _bounded_stage_input(
            (
                (
                    "pain_signals",
                    [signal.model_dump(mode="json") for signal in extraction.pain_signals],
                ),
            ),
            total_counts={"pain_signals": len(extraction.pain_signals)},
            priority_ids={"pain_signals": priority_pain_ids},
            fixed={
                "target_problems": [
                    target_problems[key].model_dump(mode="json") for key in sorted(target_problems)
                ]
            }
            if target_problems
            else None,
        )
        selected_pain_ids = frozenset(_selected_ids(input_json, "pain_signals"))
        selected_extraction = PainExtractionBatch(
            pain_signals=tuple(
                signal for signal in extraction.pain_signals if signal.id in selected_pain_ids
            )
        )
        payload, is_new = await self._load_or_reason(
            context,
            stage=stage,
            operation=SemanticOperation.CLUSTER,
            effort=AgentEffort.MEDIUM,
            schema_type=ClusterBatch,
            input_json=input_json,
            permitted_evidence_ids=tuple(
                sorted(
                    {signal.raw_signal_revision_id for signal in selected_extraction.pain_signals}
                )
            ),
            execution=execution,
            round_number=round_number,
        )
        provider_batch = ClusterBatch.model_validate(payload)
        if target_problems:
            provider_problem_ids = {item.id for item in provider_batch.problems}
            if provider_problem_ids != set(target_problems):
                raise ValueError("round-two clusters must use exact target problem IDs")
            provider_batch = provider_batch.model_copy(
                update={"problems": tuple(target_problems[key] for key in sorted(target_problems))}
            )
        batch = _normalize_clusters(
            context,
            provider_batch,
            selected_extraction,
        )
        problem_ids = {problem.id for problem in batch.problems}
        cluster_problem = {cluster.id: cluster.canonical_problem_id for cluster in batch.clusters}
        pain_ids = {signal.id for signal in selected_extraction.pain_signals}
        if not set(cluster_problem.values()) <= problem_ids:
            raise ValueError("cluster references unknown canonical problem")
        if any(
            membership.cluster_id not in cluster_problem
            or membership.pain_signal_id not in pain_ids
            for membership in batch.memberships
        ):
            raise ValueError("cluster membership references unknown artifact")
        if is_new:
            await self._commit(
                context,
                stage,
                execution.checkpoint_payload(stage, batch.model_dump(mode="json")),
            )
        return batch

    async def _research_gap(
        self,
        context: PipelineContext,
        evidence: tuple[CapturedEvidence, ...],
        records: dict[str, EvidenceRecord],
        clusters: ClusterBatch,
        competitor_research: CompetitorResearchCheckpoint,
        execution: PipelineExecution,
        *,
        stage: str = "GAP",
        round_number: int = 1,
        priority_evidence_ids: frozenset[str] = frozenset(),
        target_contexts: tuple[dict[str, Any], ...] = (),
    ) -> GapResearchBatch:
        existing = await self.store.load_stage(context, stage)
        if existing is not None:
            execution.record_checkpoint(stage, existing)
            return GapResearchBatch.model_validate(_artifact_payload(existing))
        if competitor_research.status is not CompetitorResearchStatus.COMPLETE:
            batch = GapResearchBatch(
                claims=(),
                competitors=(),
                competitor_evidence=(),
                gaps=(),
                opportunities=(),
                opportunity_fit=(),
            )
            await self._commit(context, stage, batch.model_dump(mode="json"))
            return batch
        evidence_input = _bounded_evidence_input(
            evidence,
            priority_ids=priority_evidence_ids,
        )
        snapshot_input = [
            {
                **item.model_dump(mode="json"),
                "evidence_id": _competitor_snapshot_evidence_id(item),
                "text": item.text[:1_200],
            }
            for item in competitor_research.snapshots[:10]
        ]
        input_json = _bounded_stage_input(
            (
                (
                    "problems",
                    [item.model_dump(mode="json") for item in clusters.problems],
                ),
                ("competitor_snapshots", snapshot_input),
                ("evidence", evidence_input),
            ),
            total_counts={
                "problems": len(clusters.problems),
                "competitor_snapshots": len(competitor_research.snapshots),
                "evidence": len(evidence),
            },
            priority_ids={
                "problems": frozenset(
                    problem["id"]
                    for item in target_contexts
                    if isinstance((problem := item.get("canonical_problem")), dict)
                    and isinstance(problem.get("id"), str)
                ),
                "evidence": priority_evidence_ids,
            },
            fixed={"target_opportunities": list(target_contexts)} if target_contexts else None,
        )
        selected_evidence_ids = _selected_ids(input_json, "evidence")
        selected_problem_ids = _selected_ids(input_json, "problems")
        selected_snapshot_urls = _selected_urls(
            input_json,
            "competitor_snapshots",
        )
        selected_competitor_evidence_ids = _selected_evidence_ids(
            input_json,
            "competitor_snapshots",
        )
        payload, is_new = await self._load_or_reason(
            context,
            stage=stage,
            operation=SemanticOperation.GAP,
            effort=AgentEffort.MEDIUM,
            schema_type=GapResearchBatch,
            input_json=input_json,
            permitted_evidence_ids=tuple(
                sorted({*selected_evidence_ids, *selected_competitor_evidence_ids})
            ),
            permitted_urls=selected_snapshot_urls,
            execution=execution,
            round_number=round_number,
        )
        provider_batch = GapResearchBatch.model_validate(payload)
        if target_contexts:
            provider_batch = _bind_round_two_gap(provider_batch, target_contexts)
        batch = _normalize_gap(context, provider_batch)
        if target_contexts:
            target_opportunity_ids = {
                opportunity["id"]
                for item in target_contexts
                if isinstance((opportunity := item.get("opportunity")), dict)
                and isinstance(opportunity.get("id"), str)
            }
            if {item.id for item in batch.opportunities} != target_opportunity_ids:
                raise ValueError("round-two normalization changed target opportunity identity")
        _require_unique_ids("claims", batch.claims)
        _require_unique_ids("competitors", batch.competitors)
        _require_unique_ids("competitor evidence", batch.competitor_evidence)
        _require_unique_ids("gaps", batch.gaps)
        _require_unique_ids("opportunities", batch.opportunities)
        problem_ids = set(selected_problem_ids)
        competitor_ids = {item.id for item in batch.competitors}
        claim_ids = frozenset(item.id for item in batch.claims)
        snapshots_by_url = {
            normalize_url(str(url)): snapshot
            for snapshot in competitor_research.snapshots
            for url in (snapshot.url, snapshot.final_url)
            if normalize_url(str(url)) in {normalize_url(value) for value in selected_snapshot_urls}
        }
        competitor_records: dict[str, EvidenceRecord] = {}
        if (
            competitor_research.status is not CompetitorResearchStatus.COMPLETE
            and batch.competitor_evidence
        ):
            raise ValueError("unavailable competitor research cannot produce evidence")
        for item in batch.competitor_evidence:
            if item.competitor_id not in competitor_ids or not set(item.claim_ids) <= claim_ids:
                raise ValueError("competitor evidence references unknown artifact")
            snapshot = snapshots_by_url.get(normalize_url(str(item.source_url)))
            if snapshot is None:
                raise ValueError("competitor evidence URL was not captured")
            expected_evidence_id = _competitor_snapshot_evidence_id(snapshot)
            if item.id != expected_evidence_id or item.id not in selected_competitor_evidence_ids:
                raise ValueError("competitor evidence ID was not assigned by Python")
            if (
                normalize_text(item.captured_excerpt) not in normalize_text(snapshot.text)
                or item.content_hash != snapshot.sha256
                or item.observed_at != snapshot.observed_at
            ):
                raise ValueError("competitor evidence does not match captured fetch")
            competitor_records[item.id] = EvidenceRecord(
                item.id,
                Source.STATIC_WEB,
                str(item.source_url),
                snapshot.text,
                snapshot.observed_at,
            )
        permitted_user_records = {
            evidence_id: records[evidence_id] for evidence_id in selected_evidence_ids
        }
        all_records = {**permitted_user_records, **competitor_records}
        for claim in batch.claims:
            validate_atomic_claim(claim, all_records, known_claim_ids=claim_ids)
        validate_alternative_coverage(batch.competitors)
        competitor_evidence_ids = frozenset(item.id for item in batch.competitor_evidence)
        for hypothesis in batch.gaps:
            if hypothesis.canonical_problem_id not in problem_ids:
                raise ValueError("gap references unknown canonical problem")
            validate_gap_hypothesis(
                hypothesis,
                permitted_user_evidence=frozenset(permitted_user_records),
                permitted_competitor_evidence=competitor_evidence_ids,
            )
        gap_ids = {item.id for item in batch.gaps}
        opportunity_ids = {item.id for item in batch.opportunities}
        if any(item.gap_hypothesis_id not in gap_ids for item in batch.opportunities):
            raise ValueError("opportunity references unknown gap hypothesis")
        fit_ids = [item.opportunity_id for item in batch.opportunity_fit]
        if len(fit_ids) != len(set(fit_ids)) or set(fit_ids) != opportunity_ids:
            raise ValueError("every opportunity requires exactly one opportunity-fit input")
        if is_new:
            await self._commit(
                context,
                stage,
                execution.checkpoint_payload(stage, batch.model_dump(mode="json")),
            )
        return batch

    async def _cards_and_scores(
        self,
        context: PipelineContext,
        evidence: tuple[CapturedEvidence, ...],
        extraction: PainExtractionBatch,
        clusters: ClusterBatch,
        gap: GapResearchBatch,
        *,
        examined_volume: tuple[int, int],
        stage: str = "CARD_SCORE",
        round_number: int = 1,
    ) -> tuple[tuple[EvidenceCard, ...], tuple[OpportunityScoreSnapshot, ...]]:
        existing = await self.store.load_stage(context, stage)
        if existing is not None:
            card_values = existing.get("cards")
            score_values = existing.get("scores")
            if not isinstance(card_values, list) or not isinstance(score_values, list):
                raise ValueError("CARD_SCORE checkpoint is invalid")
            return (
                tuple(EvidenceCard.model_validate(item) for item in card_values),
                tuple(OpportunityScoreSnapshot.model_validate(item) for item in score_values),
            )
        evidence_by_id = {item.evidence_id: item for item in evidence}
        pain_by_id = {item.id: item for item in extraction.pain_signals}
        problem_by_cluster = {item.id: item.canonical_problem_id for item in clusters.clusters}
        pain_by_problem: dict[str, list[Any]] = {}
        for membership in clusters.memberships:
            problem_id = problem_by_cluster[membership.cluster_id]
            pain_by_problem.setdefault(problem_id, []).append(pain_by_id[membership.pain_signal_id])
        gap_by_id = {item.id: item for item in gap.gaps}
        claims_by_evidence: dict[str, list[str]] = {}
        contradictions_by_evidence: dict[str, list[str]] = {}
        for claim in gap.claims:
            for evidence_id in claim.evidence_ids:
                claims_by_evidence.setdefault(evidence_id, []).append(claim.id)
                contradictions_by_evidence.setdefault(evidence_id, []).extend(
                    claim.contradicts_claim_ids
                )
        cards = []
        scores = []
        inputs_by_opportunity = {item.opportunity_id: item for item in gap.opportunity_fit}
        snapshot_round = f":{round_number}" if round_number > 1 else ""
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
                        observed_at=captured.source_created_at,
                        severity=pain.severity,
                        behavioral_workaround=bool(pain.workaround),
                        paid_or_wtp=pain.payment_signal,
                        supporting_claim_ids=tuple(
                            claims_by_evidence.get(captured.evidence_id, ())
                        ),
                        contradicting_claim_ids=tuple(
                            contradictions_by_evidence.get(captured.evidence_id, ())
                        ),
                    )
                )
            card = build_evidence_card(
                str(uuid5(context.run_id, f"card{snapshot_round}:{opportunity.id}")),
                opportunity.id,
                tuple(observations),
            )
            value = inputs_by_opportunity[opportunity.id]
            representative_ids = set(card.representative_evidence_ids)
            representative_pains = [
                pain
                for pain in pain_by_problem.get(problem_id, [])
                if pain.raw_signal_revision_id in representative_ids
            ]
            frequency = (
                sum(pain.frequency for pain in representative_pains) / len(representative_pains)
                if representative_pains
                else 0.0
            )
            current_volume, previous_volume = examined_volume
            trend = classify_trend(
                tuple(
                    TrendObservation(
                        signal_id=item.evidence_id,
                        author_id=item.author_id,
                        thread_id=item.thread_id,
                        observed_at=item.observed_at,
                        duplicate_group=item.duplicate_group,
                    )
                    for item in observations
                ),
                as_of=context.collection_until,
                current_examined_volume=current_volume,
                previous_examined_volume=previous_volume,
            )
            trend_score = {
                TrendLabel.RISING: 100.0,
                TrendLabel.FLAT: 50.0,
                TrendLabel.FALLING: 0.0,
                TrendLabel.INSUFFICIENT_DATA: 0.0,
            }[trend.label]
            diversity = 100 * min(
                1.0,
                (
                    min(1.0, len(card.known_author_ids) / 5)
                    + min(1.0, len(card.thread_ids) / 3)
                    + min(1.0, len(card.user_sources) / 2)
                )
                / 3,
            )
            scoring = ScoringInputs(
                severity=card.severity * 100,
                frequency=frequency * 100,
                independent_diversity=diversity,
                behavioral_workaround=100.0 if card.behavioral_workarounds else 0.0,
                wtp_or_spend=100.0 if card.paid_or_wtp_signals else 0.0,
                recency_trend=trend_score,
                **value.model_dump(
                    exclude={"schema_version", "opportunity_id", "explanation"},
                    mode="python",
                ),
            )
            score = score_opportunity(
                snapshot_id=str(uuid5(context.run_id, f"score{snapshot_round}:{opportunity.id}")),
                opportunity_id=opportunity.id,
                mission_revision_id=context.mission_revision.id,
                inputs=scoring,
                evidence_confidence=card.confidence,
                created_at=context.collection_until,
            )
            cards.append(card)
            scores.append(score)
        result = (tuple(cards), tuple(scores))
        await self._commit(
            context,
            stage,
            {
                "cards": [item.model_dump(mode="json") for item in result[0]],
                "scores": [item.model_dump(mode="json") for item in result[1]],
            },
        )
        return result

    async def _hypotheses(
        self,
        context: PipelineContext,
        gap: GapResearchBatch,
        cards: tuple[EvidenceCard, ...],
        clusters: ClusterBatch,
        execution: PipelineExecution,
        *,
        stage: str = "HYPOTHESIS",
        round_number: int = 1,
    ) -> HypothesisBatch:
        existing = await self.store.load_stage(context, stage)
        if existing is not None:
            execution.record_checkpoint(stage, existing)
            return HypothesisBatch.model_validate(_artifact_payload(existing))
        if not gap.opportunities:
            batch = HypothesisBatch(hypotheses=(), hypothesis_card_links=())
            await self._commit(context, stage, batch.model_dump(mode="json"))
            return batch
        gap_by_id = {item.id: item for item in gap.gaps}
        problem_by_id = {item.id: item for item in clusters.problems}
        card_by_opportunity = {item.opportunity_id: item for item in cards}
        claims = [item.model_dump(mode="json") for item in gap.claims]
        claim_ids = [item.id for item in gap.claims]
        cases = []
        for opportunity in sorted(gap.opportunities, key=lambda item: item.id):
            hypothesis_gap = gap_by_id[opportunity.gap_hypothesis_id]
            problem = problem_by_id.get(hypothesis_gap.canonical_problem_id)
            card = card_by_opportunity.get(opportunity.id)
            if problem is None or card is None:
                continue
            cases.append(
                {
                    "opportunity_id": opportunity.id,
                    "problem": problem.model_dump(mode="json"),
                    "evidence_card": _compact_card(card),
                    "competitor_claims": claims,
                    "permitted_claim_ids": claim_ids,
                }
            )
        input_json = _bounded_stage_input(
            (("cases", cases),),
            total_counts={"cases": len(cases)},
        )
        selected_cases = input_json.get("cases")
        if not isinstance(selected_cases, list):
            raise ValueError("hypothesis cases must be a list")
        permitted_claims = frozenset(_selected_case_claim_ids(input_json))
        payload, is_new = await self._load_or_reason(
            context,
            stage=stage,
            operation=SemanticOperation.HYPOTHESIS,
            effort=AgentEffort.MEDIUM,
            schema_type=HypothesisBatch,
            input_json=input_json,
            permitted_evidence_ids=_selected_case_evidence_ids(input_json),
            execution=execution,
            round_number=round_number,
        )
        batch = _normalize_hypotheses(
            HypothesisBatch.model_validate(payload),
            selected_cases=selected_cases,
        )
        selected_problem_ids = {
            problem_id
            for case in selected_cases
            if isinstance(case, dict)
            and isinstance(case.get("problem"), dict)
            and isinstance((problem_id := case["problem"].get("id")), str)
        }
        for hypothesis in batch.hypotheses:
            validate_problem_hypothesis(hypothesis, permitted_claims)
            if hypothesis.canonical_problem_id not in selected_problem_ids:
                raise ValueError("hypothesis references unknown canonical problem")
        if is_new:
            await self._commit(
                context,
                stage,
                execution.checkpoint_payload(stage, batch.model_dump(mode="json")),
            )
        return batch

    async def _critic(
        self,
        context: PipelineContext,
        gap: GapResearchBatch,
        cards: tuple[EvidenceCard, ...],
        hypotheses: HypothesisBatch,
        execution: PipelineExecution,
        *,
        stage: str = "CRITIC",
        round_number: int = 1,
    ) -> CriticBatch:
        existing = await self.store.load_stage(context, stage)
        if existing is not None:
            execution.record_checkpoint(stage, existing)
            return CriticBatch.model_validate(_artifact_payload(existing))
        if not gap.opportunities:
            batch = CriticBatch(results=())
            await self._commit(context, stage, batch.model_dump(mode="json"))
            return batch
        critic_cases = _critic_cases(gap, cards, hypotheses)
        input_json = _bounded_stage_input(
            (("cases", critic_cases),),
            total_counts={"cases": len(gap.opportunities)},
        )
        opportunity_ids = set(_selected_case_opportunity_ids(input_json))
        if opportunity_ids:
            payload, is_new = await self._load_or_reason(
                context,
                stage=stage,
                operation=SemanticOperation.CRITIC,
                effort=AgentEffort.MEDIUM,
                schema_type=CriticBatch,
                input_json=input_json,
                permitted_evidence_ids=_selected_case_evidence_ids(input_json),
                execution=execution,
                round_number=round_number,
            )
            provider_batch = CriticBatch.model_validate(payload)
        else:
            execution.record_input(stage, input_json)
            provider_batch = CriticBatch(results=())
            is_new = True
        result_ids = [item.opportunity_id for item in provider_batch.results]
        if len(result_ids) != len(set(result_ids)) or set(result_ids) != opportunity_ids:
            raise ValueError("critic must return one result per opportunity")
        permitted_claims = frozenset(_selected_case_claim_ids(input_json))
        for result in provider_batch.results:
            validate_critic_result(result, permitted_claims)
        omitted_opportunities = sorted({item.id for item in gap.opportunities} - opportunity_ids)
        batch = CriticBatch(
            results=tuple(provider_batch.results)
            + tuple(
                CriticResult(
                    opportunity_id=opportunity_id,
                    verdict=Verdict.RESEARCH_MORE,
                    confidence=0,
                    missing_evidence=("coherent critic input bundle was omitted",),
                    summary="Critic input omitted; validation is not permitted",
                )
                for opportunity_id in omitted_opportunities
            )
        )
        if is_new:
            await self._commit(
                context,
                stage,
                execution.checkpoint_payload(stage, batch.model_dump(mode="json")),
            )
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
        execution: PipelineExecution,
        permitted_urls: tuple[str, ...] = (),
        round_number: int = 1,
    ) -> tuple[dict[str, object], bool]:
        existing = await self.store.load_stage(context, stage)
        if existing is not None:
            execution.record_checkpoint(stage, existing)
            return {key: value for key, value in existing.items() if key != "_input_bounds"}, False
        execution.record_input(stage, input_json)
        encoded_input = json.dumps(
            input_json,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        if len(encoded_input) > MAX_SEMANTIC_INPUT_BYTES:
            raise ValueError(f"{stage} semantic input exceeds {MAX_SEMANTIC_INPUT_BYTES} bytes")
        schema = schema_type.model_json_schema()
        schema["title"] = f"GapForge {stage} v0.1"
        request = AgentRequest(
            call_id=self._call_id(context, operation, round_number),
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
            raise _agent_result_error(result.status, stage)
        return result.output_json, True

    async def _query_plan(
        self,
        context: PipelineContext,
        existing: ExistingIntelligence,
        execution: PipelineExecution,
    ) -> QueryPlan:
        mission, omitted_mission_characters = _truncate_utf8(
            context.mission_revision.prompt,
            max_bytes=6_000,
        )
        input_json = _bounded_stage_input(
            (
                (
                    "existing_candidates",
                    [
                        {
                            "id": candidate.identifier,
                            "kind": candidate.kind,
                            "summary": candidate.summary[:1_000],
                        }
                        for candidate in existing.candidates
                    ],
                ),
            ),
            total_counts={"existing_candidates": len(existing.candidates)},
            fixed={
                "mission": mission,
                "existing_counts": {
                    "opportunities": existing.opportunity_count,
                    "evidence": existing.evidence_count,
                },
                "round": 1,
                "available_collection_sources": sorted(source.value for source in self.collectors),
            },
        )
        omitted_counts = input_json["input_bounds"]["omitted_counts"]
        if omitted_mission_characters:
            omitted_counts["mission_characters"] = omitted_mission_characters
        execution.record_input("QUERY_PLAN", input_json)
        schema = QueryPlan.model_json_schema()
        schema["title"] = "GapForge QueryPlan v0.1"
        request = AgentRequest(
            call_id=self._call_id(context, SemanticOperation.QUERY_PLAN, 1),
            task=SemanticOperation.QUERY_PLAN,
            effort=AgentEffort.MEDIUM,
            input_json=input_json,
            permitted_evidence_ids=(),
            output_schema_name="query-plan-v1",
            timeout_seconds=300,
        )
        result = await self.reasoner.run(
            SemanticContext(run_id=context.run_id, task_id=context.task_id),
            SemanticCall(request=request, output_schema=schema),
        )
        if result.status is not AgentStatus.COMPLETED or result.output_json is None:
            raise _agent_result_error(result.status, "QUERY_PLAN")
        plan = QueryPlan.model_validate(result.output_json)
        if plan.round_number != 1:
            raise ValueError("initial query plan must have round_number=1")
        available_sources = set(self.collectors)
        unsupported = sorted(
            {
                source.value
                for intent in plan.intents
                for source in intent.sources
                if source not in available_sources
            }
        )
        if unsupported:
            raise ValueError(
                "query plan referenced unavailable collection sources: " + ", ".join(unsupported)
            )
        return plan

    async def _collect(
        self,
        context: PipelineContext,
        plan: QueryPlan,
        *,
        stage: str,
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
        if not jobs:
            return ()
        admitted = await self.store.reserve_external_budget(
            context,
            stage=stage,
            collector_requests=sum(request.max_requests for _, _, request in jobs),
        )
        if not admitted:
            raise ExternalAttemptIndeterminateError(
                f"{stage} collector reservation is indeterminate after interruption"
            )
        results = await collect_isolated(jobs)
        return _validate_collection_contract(tuple(jobs), results)

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


def _examined_volume(
    results: tuple[CollectResult, ...],
    *,
    as_of: datetime,
) -> dict[str, int]:
    current, previous = _examined_volume_from_items(
        tuple(
            (result.source.value, item.external_id, item.source_created_at)
            for result in results
            for item in result.items
        ),
        as_of=as_of,
    )
    return {"current_7d": current, "previous_28d": previous}


def _examined_items(results: tuple[CollectResult, ...]) -> list[dict[str, str]]:
    unique = {
        (result.source.value, item.external_id): {
            "source": result.source.value,
            "external_id": item.external_id,
            "source_created_at": item.source_created_at.isoformat(),
        }
        for result in results
        for item in result.items
    }
    return [unique[key] for key in sorted(unique)]


def _examined_volume_from_items(
    items: tuple[tuple[str, str, datetime], ...],
    *,
    as_of: datetime,
) -> tuple[int, int]:
    current_start = as_of - timedelta(days=7)
    previous_start = as_of - timedelta(days=35)
    unique = {(source, external_id): timestamp for source, external_id, timestamp in items}
    return (
        sum(current_start <= value < as_of for value in unique.values()),
        sum(previous_start <= value < current_start for value in unique.values()),
    )


def _examined_items_field(
    payload: Mapping[str, object],
) -> tuple[tuple[str, str, datetime], ...]:
    values = payload.get("examined_items")
    if values is None:
        return ()
    if not isinstance(values, list):
        raise ValueError("examined_items must be a list")
    parsed: list[tuple[str, str, datetime]] = []
    for value in values:
        if not isinstance(value, dict):
            raise ValueError("examined item must be an object")
        source = value.get("source")
        external_id = value.get("external_id")
        source_created_at = value.get("source_created_at")
        if not isinstance(source, str) or not source:
            raise ValueError("examined item identity must be nonempty")
        if not isinstance(external_id, str) or not external_id:
            raise ValueError("examined item identity must be nonempty")
        if not isinstance(source_created_at, str):
            raise ValueError("examined item timestamp must be a string")
        parsed.append((source, external_id, datetime.fromisoformat(source_created_at)))
    return tuple(sorted(set(parsed)))


def _examined_volume_field(payload: Mapping[str, object]) -> tuple[int, int]:
    value = payload.get("examined_volume")
    if value is None:
        return (0, 0)
    if not isinstance(value, dict):
        raise ValueError("examined_volume must be an object")
    return (
        _integer_field(value, "current_7d"),
        _integer_field(value, "previous_28d"),
    )


def _run_warnings(
    warning_codes: list[str],
    *,
    omissions: Mapping[str, int] | None = None,
) -> tuple[RunWarning, ...]:
    omitted = dict(sorted((omissions or {}).items()))
    return tuple(
        RunWarning(
            code=code,
            details=(
                {
                    "omitted_counts": omitted,
                    "total_omitted": sum(omitted.values()),
                }
                if code == "SEMANTIC_INPUT_OMITTED"
                else {}
            ),
        )
        for code in sorted(set(warning_codes))
    )


def _append_omission_warning(warning_codes: list[str], execution: PipelineExecution) -> None:
    if execution.omissions and "SEMANTIC_INPUT_OMITTED" not in warning_codes:
        warning_codes.append("SEMANTIC_INPUT_OMITTED")


def _artifact_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key != "_input_bounds"}


def _semantic_task_error(error: SemanticAdmissionError) -> TaskHandlerError:
    kind = {
        SemanticAdmissionKind.BUDGET_EXHAUSTED: ErrorKind.BUDGET_EXHAUSTED,
        SemanticAdmissionKind.DEADLINE_EXCEEDED: ErrorKind.DEADLINE_EXCEEDED,
        SemanticAdmissionKind.PARALLEL_LIMIT: ErrorKind.RATE_LIMITED,
        SemanticAdmissionKind.TASK_CONTEXT_INVALID: ErrorKind.INTEGRITY,
        SemanticAdmissionKind.REPLAY_UNAVAILABLE: ErrorKind.INTEGRITY,
        SemanticAdmissionKind.INDETERMINATE_ATTEMPT: ErrorKind.TIMEOUT,
    }[error.kind]
    return TaskHandlerError(kind, error_class=f"Semantic{error.kind.value.title()}")


def _agent_result_error(status: AgentStatus, stage: str) -> TaskHandlerError:
    kind = {
        AgentStatus.AUTH_REQUIRED: ErrorKind.AUTH_REQUIRED,
        AgentStatus.TIMEOUT: ErrorKind.TIMEOUT,
        AgentStatus.INVALID_OUTPUT: ErrorKind.INVALID_OUTPUT,
        AgentStatus.FAILED: ErrorKind.PERMANENT,
        AgentStatus.COMPLETED: ErrorKind.INTEGRITY,
    }[status]
    return TaskHandlerError(kind, error_class=f"{stage.title()}SemanticFailure")


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


def _require_unique_ids(label: str, values: tuple[Any, ...]) -> None:
    identifiers = [item.id for item in values]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError(f"{label} IDs must be unique")


def _merge_cluster_batches(first: ClusterBatch, latest: ClusterBatch) -> ClusterBatch:
    problems = {item.id: item for item in (*first.problems, *latest.problems)}
    clusters = {item.id: item for item in (*first.clusters, *latest.clusters)}
    memberships = {
        (item.cluster_id, item.pain_signal_id): item
        for item in (*first.memberships, *latest.memberships)
    }
    return ClusterBatch(
        problems=tuple(problems[key] for key in sorted(problems)),
        clusters=tuple(clusters[key] for key in sorted(clusters)),
        memberships=tuple(memberships[key] for key in sorted(memberships)),
    )


def _merge_gap_batches(first: GapResearchBatch, latest: GapResearchBatch) -> GapResearchBatch:
    def merged(values: tuple[Any, ...], newer: tuple[Any, ...]) -> tuple[Any, ...]:
        by_id = {item.id: item for item in (*values, *newer)}
        return tuple(by_id[key] for key in sorted(by_id))

    fits = {item.opportunity_id: item for item in (*first.opportunity_fit, *latest.opportunity_fit)}
    return GapResearchBatch(
        claims=merged(first.claims, latest.claims),
        competitors=merged(first.competitors, latest.competitors),
        competitor_evidence=merged(first.competitor_evidence, latest.competitor_evidence),
        gaps=merged(first.gaps, latest.gaps),
        opportunities=merged(first.opportunities, latest.opportunities),
        opportunity_fit=tuple(fits[key] for key in sorted(fits)),
    )


def _stable_id(kind: str, *parts: object) -> str:
    canonical = json.dumps(
        [kind, *parts],
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return str(uuid5(ARTIFACT_NAMESPACE, canonical))


def _competitor_snapshot_evidence_id(snapshot: FetchSnapshot) -> str:
    return _stable_id(
        "competitor-evidence",
        normalize_url(str(snapshot.final_url)),
        snapshot.sha256,
        snapshot.observed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    )


def _normalize_clusters(
    context: PipelineContext,
    batch: ClusterBatch,
    extraction: PainExtractionBatch,
) -> ClusterBatch:
    _require_unique_ids("problems", batch.problems)
    _require_unique_ids("clusters", batch.clusters)
    problem_ids = {
        item.id: _stable_id("problem", normalize_text(item.summary)) for item in batch.problems
    }
    if len(set(problem_ids.values())) != len(problem_ids):
        raise ValueError("canonical problem content must be unique")
    if any(item.canonical_problem_id not in problem_ids for item in batch.clusters):
        raise ValueError("cluster references unknown canonical problem")
    cluster_ids = {
        item.id: _stable_id("cluster", problem_ids[item.canonical_problem_id])
        for item in batch.clusters
    }
    if len(set(cluster_ids.values())) != len(cluster_ids):
        raise ValueError("problem cluster content must be unique")
    known_pains = {item.id for item in extraction.pain_signals}
    if any(item.pain_signal_id not in known_pains for item in batch.memberships):
        raise ValueError("cluster membership references unknown pain signal")
    if any(item.cluster_id not in cluster_ids for item in batch.memberships):
        raise ValueError("cluster membership references unknown cluster")
    return ClusterBatch(
        problems=tuple(
            item.model_copy(update={"id": problem_ids[item.id]}) for item in batch.problems
        ),
        clusters=tuple(
            item.model_copy(
                update={
                    "id": cluster_ids[item.id],
                    "canonical_problem_id": problem_ids[item.canonical_problem_id],
                    "last_growth_at": context.collection_until,
                }
            )
            for item in batch.clusters
        ),
        memberships=tuple(
            item.model_copy(
                update={
                    "cluster_id": cluster_ids[item.cluster_id],
                    "accepted_at": context.collection_until,
                }
            )
            for item in batch.memberships
        ),
    )


def _normalize_gap(context: PipelineContext, batch: GapResearchBatch) -> GapResearchBatch:
    for label, artifacts in (
        ("claims", batch.claims),
        ("competitors", batch.competitors),
        ("competitor evidence", batch.competitor_evidence),
        ("gaps", batch.gaps),
        ("opportunities", batch.opportunities),
    ):
        _require_unique_ids(label, artifacts)
    competitor_ids = {
        item.id: _stable_id(
            "competitor",
            normalize_text(item.name),
            item.kind.value,
            normalize_url(str(item.canonical_url)) if item.canonical_url else None,
        )
        for item in batch.competitors
    }
    competitor_evidence_ids = {item.id: item.id for item in batch.competitor_evidence}
    claim_ids = {
        item.id: _stable_id(
            "claim",
            normalize_text(item.text),
            item.kind.value,
            item.status.value,
            sorted(competitor_evidence_ids.get(value, value) for value in item.evidence_ids),
        )
        for item in batch.claims
    }
    gap_ids = {
        item.id: _stable_id(
            "gap",
            item.canonical_problem_id,
            item.gap_type.value,
            normalize_text(item.statement),
            sorted(item.user_evidence_ids),
            sorted(
                competitor_evidence_ids.get(value, value) for value in item.competitor_evidence_ids
            ),
        )
        for item in batch.gaps
    }
    opportunity_ids = {
        item.id: _stable_id(
            "opportunity",
            next(
                (
                    gap.canonical_problem_id
                    for gap in batch.gaps
                    if gap.id == item.gap_hypothesis_id
                ),
                item.gap_hypothesis_id,
            ),
            normalize_text(item.title),
        )
        for item in batch.opportunities
    }
    for label, id_map in (
        ("claim", claim_ids),
        ("competitor", competitor_ids),
        ("competitor evidence", competitor_evidence_ids),
        ("gap", gap_ids),
        ("opportunity", opportunity_ids),
    ):
        if len(set(id_map.values())) != len(id_map):
            raise ValueError(f"{label} canonical content must be unique")
    normalized_claims = []
    for item in batch.claims:
        citations = tuple(
            citation.model_copy(
                update={
                    "evidence_id": competitor_evidence_ids.get(
                        citation.evidence_id, citation.evidence_id
                    )
                }
            )
            for citation in item.citations
        )
        normalized_claims.append(
            item.model_copy(
                update={
                    "id": claim_ids[item.id],
                    "evidence_ids": tuple(
                        sorted(
                            competitor_evidence_ids.get(value, value) for value in item.evidence_ids
                        )
                    ),
                    "citations": tuple(
                        sorted(
                            citations,
                            key=lambda value: json.dumps(
                                value.model_dump(mode="json"), sort_keys=True
                            ),
                        )
                    ),
                    "contradicts_claim_ids": tuple(
                        sorted(claim_ids.get(value, value) for value in item.contradicts_claim_ids)
                    ),
                }
            )
        )
    normalized_competitors = tuple(
        item.model_copy(update={"id": competitor_ids[item.id]}) for item in batch.competitors
    )
    normalized_competitor_evidence = tuple(
        item.model_copy(
            update={
                "id": competitor_evidence_ids[item.id],
                "competitor_id": competitor_ids.get(item.competitor_id, item.competitor_id),
                "claim_ids": tuple(sorted(claim_ids.get(value, value) for value in item.claim_ids)),
            }
        )
        for item in batch.competitor_evidence
    )
    normalized_gaps = tuple(
        item.model_copy(
            update={
                "id": gap_ids[item.id],
                "user_evidence_ids": tuple(sorted(item.user_evidence_ids)),
                "competitor_evidence_ids": tuple(
                    sorted(
                        competitor_evidence_ids.get(value, value)
                        for value in item.competitor_evidence_ids
                    )
                ),
            }
        )
        for item in batch.gaps
    )
    normalized_opportunities = tuple(
        item.model_copy(
            update={
                "id": opportunity_ids[item.id],
                "gap_hypothesis_id": gap_ids.get(item.gap_hypothesis_id, item.gap_hypothesis_id),
            }
        )
        for item in batch.opportunities
    )
    normalized_fits = tuple(
        item.model_copy(
            update={"opportunity_id": opportunity_ids.get(item.opportunity_id, item.opportunity_id)}
        )
        for item in batch.opportunity_fit
    )
    return GapResearchBatch(
        claims=tuple(sorted(normalized_claims, key=lambda value: value.id)),
        competitors=tuple(sorted(normalized_competitors, key=lambda value: value.id)),
        competitor_evidence=tuple(
            sorted(normalized_competitor_evidence, key=lambda value: value.id)
        ),
        gaps=tuple(sorted(normalized_gaps, key=lambda value: value.id)),
        opportunities=tuple(sorted(normalized_opportunities, key=lambda value: value.id)),
        opportunity_fit=tuple(sorted(normalized_fits, key=lambda value: value.opportunity_id)),
    )


def _bind_round_two_gap(
    batch: GapResearchBatch,
    target_contexts: tuple[dict[str, Any], ...],
) -> GapResearchBatch:
    targets = {
        opportunity["id"]: {
            "opportunity": opportunity,
            "problem_id": problem["id"],
        }
        for item in target_contexts
        if isinstance((opportunity := item.get("opportunity")), dict)
        and isinstance(opportunity.get("id"), str)
        and isinstance((problem := item.get("canonical_problem")), dict)
        and isinstance(problem.get("id"), str)
    }
    returned_ids = {item.id for item in batch.opportunities}
    if not targets or returned_ids != set(targets):
        raise ValueError("round-two gap output must cover exact target opportunities")
    gaps = {item.id: item for item in batch.gaps}
    normalized_opportunities = []
    for opportunity in batch.opportunities:
        target = targets[opportunity.id]
        gap = gaps.get(opportunity.gap_hypothesis_id)
        if gap is None or gap.canonical_problem_id != target["problem_id"]:
            raise ValueError("round-two opportunity does not match its target problem")
        target_opportunity = target["opportunity"]
        normalized_opportunities.append(
            opportunity.model_copy(update={"title": target_opportunity["title"]})
        )
    return batch.model_copy(update={"opportunities": tuple(normalized_opportunities)})


def _normalize_hypotheses(
    batch: HypothesisBatch,
    *,
    selected_cases: list[dict[str, Any]],
) -> HypothesisBatch:
    _require_unique_ids("hypotheses", batch.hypotheses)
    hypotheses_by_id = {item.id: item for item in batch.hypotheses}
    cases_by_opportunity = {
        case["opportunity_id"]: case
        for case in selected_cases
        if isinstance(case.get("opportunity_id"), str)
    }
    links_by_hypothesis = {item.hypothesis_id: item for item in batch.hypothesis_card_links}
    linked_opportunities = [item.opportunity_id for item in batch.hypothesis_card_links]
    if len(links_by_hypothesis) != len(batch.hypothesis_card_links):
        raise ValueError("each hypothesis requires exactly one card link")
    if len(linked_opportunities) != len(set(linked_opportunities)):
        raise ValueError("each opportunity requires exactly one hypothesis")
    if set(links_by_hypothesis) != set(hypotheses_by_id):
        raise ValueError("hypothesis links must cover every hypothesis exactly once")
    if set(linked_opportunities) != set(cases_by_opportunity):
        raise ValueError("hypothesis links must cover every selected opportunity")
    normalized_values = []
    normalized_links = []
    for provider_id in sorted(hypotheses_by_id):
        item = hypotheses_by_id[provider_id]
        link = links_by_hypothesis[provider_id]
        case = cases_by_opportunity[link.opportunity_id]
        problem = case.get("problem")
        card = case.get("evidence_card")
        if not isinstance(problem, dict) or not isinstance(card, dict):
            raise ValueError("hypothesis input case is incomplete")
        if link.evidence_card_id != card.get("id"):
            raise ValueError("hypothesis link references a different evidence card")
        if item.canonical_problem_id != problem.get("id"):
            raise ValueError("hypothesis link references a different canonical problem")
        normalized_id = _stable_id(
            "problem-hypothesis",
            link.opportunity_id,
            link.evidence_card_id,
            item.canonical_problem_id,
            normalize_text(item.icp),
            normalize_text(item.job_to_be_done),
            normalize_text(item.trigger),
            normalize_text(item.current_behavior),
            normalize_text(item.pain),
            normalize_text(item.workflow_failure),
            normalize_text(item.falsification_test),
            sorted(item.supporting_claim_ids),
            sorted(item.contradicting_claim_ids),
        )
        normalized_values.append(
            item.model_copy(
                update={
                    "id": normalized_id,
                    "supporting_claim_ids": tuple(sorted(item.supporting_claim_ids)),
                    "contradicting_claim_ids": tuple(sorted(item.contradicting_claim_ids)),
                }
            )
        )
        normalized_links.append(
            HypothesisCardLink(
                hypothesis_id=normalized_id,
                opportunity_id=link.opportunity_id,
                evidence_card_id=link.evidence_card_id,
            )
        )
    normalized = HypothesisBatch(
        hypotheses=tuple(sorted(normalized_values, key=lambda value: value.id)),
        hypothesis_card_links=tuple(
            sorted(normalized_links, key=lambda value: value.opportunity_id)
        ),
    )
    _require_unique_ids("normalized hypotheses", normalized.hypotheses)
    return normalized


def _bounded_evidence_input(
    evidence: tuple[CapturedEvidence, ...],
    *,
    priority_ids: frozenset[str] = frozenset(),
) -> list[dict[str, str]]:
    values: list[dict[str, str]] = []
    for item in sorted(
        evidence,
        key=lambda value: (value.evidence_id not in priority_ids, value.evidence_id),
    ):
        candidate = {
            "id": item.evidence_id,
            "url": item.url,
            "text": item.text[:1_200],
        }
        if len(values) >= 12:
            break
        proposed = [*values, candidate]
        encoded = json.dumps(
            proposed,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        if len(encoded) > 14_000:
            break
        values.append(candidate)
    return values


def _compact_card(card: EvidenceCard) -> dict[str, Any]:
    value = card.model_dump(mode="json")
    for name in (
        "known_author_ids",
        "thread_ids",
        "observed_days",
        "supporting_claim_ids",
        "contradicting_claim_ids",
        "representative_evidence_ids",
        "missing_evidence",
    ):
        items = value.get(name)
        if isinstance(items, list):
            value[name] = items[:20]
    return value


def _bounded_stage_input(
    sections: tuple[tuple[str, list[dict[str, Any]]], ...],
    *,
    total_counts: dict[str, int],
    fixed: Mapping[str, Any] | None = None,
    priority_ids: Mapping[str, frozenset[str]] | None = None,
) -> dict[str, Any]:
    """Select deterministic partial semantic batches and disclose omissions."""
    payload: dict[str, Any] = dict(fixed or {})
    omitted: dict[str, int] = {}
    for name, raw_values in sections:
        section_priority = (priority_ids or {}).get(name, frozenset())
        values = sorted(
            raw_values,
            key=lambda item: (
                item.get("id") not in section_priority,
                json.dumps(
                    item,
                    allow_nan=False,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ),
        )
        selected: list[dict[str, Any]] = []
        for value in values:
            candidate = {
                **payload,
                name: [*selected, value],
                "input_bounds": {"omitted_counts": omitted},
            }
            if (
                len(
                    json.dumps(
                        candidate,
                        allow_nan=False,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ).encode()
                )
                > 18_000
            ):
                continue
            selected.append(value)
        payload[name] = selected
        omitted[name] = max(0, total_counts.get(name, len(values)) - len(selected))
    payload["input_bounds"] = {"omitted_counts": omitted}
    return payload


def _truncate_utf8(value: str, *, max_bytes: int) -> tuple[str, int]:
    """Truncate text at a UTF-8 boundary and report omitted characters."""
    encoded = value.encode()
    if len(encoded) <= max_bytes:
        return value, 0
    truncated = encoded[:max_bytes]
    while True:
        try:
            selected = truncated.decode()
            return selected, len(value) - len(selected)
        except UnicodeDecodeError:
            truncated = truncated[:-1]


def _selected_ids(payload: Mapping[str, Any], section: str) -> tuple[str, ...]:
    values = payload.get(section)
    if not isinstance(values, list):
        return ()
    return tuple(
        value["id"]
        for value in values
        if isinstance(value, dict) and isinstance(value.get("id"), str)
    )


def _selected_evidence_ids(payload: Mapping[str, Any], section: str) -> tuple[str, ...]:
    values = payload.get(section)
    if not isinstance(values, list):
        return ()
    return tuple(
        value["evidence_id"]
        for value in values
        if isinstance(value, dict) and isinstance(value.get("evidence_id"), str)
    )


def _selected_urls(payload: Mapping[str, Any], section: str) -> tuple[str, ...]:
    values = payload.get(section)
    if not isinstance(values, list):
        return ()
    return tuple(
        sorted(
            {
                url
                for value in values
                if isinstance(value, dict)
                for name in ("url", "final_url")
                if isinstance((url := value.get(name)), str)
            }
        )
    )


def _selected_claim_evidence_ids(payload: Mapping[str, Any]) -> tuple[str, ...]:
    values = payload.get("claims")
    if not isinstance(values, list):
        return ()
    return tuple(
        sorted(
            {
                evidence_id
                for value in values
                if isinstance(value, dict)
                for evidence_id in value.get("evidence_ids", [])
                if isinstance(evidence_id, str)
            }
        )
    )


def _critic_cases(
    gap: GapResearchBatch,
    cards: tuple[EvidenceCard, ...],
    hypotheses: HypothesisBatch,
) -> list[dict[str, Any]]:
    gap_by_id = {item.id: item for item in gap.gaps}
    card_by_opportunity = {item.opportunity_id: item for item in cards}
    hypothesis_by_id = {item.id: item for item in hypotheses.hypotheses}
    link_by_opportunity = {item.opportunity_id: item for item in hypotheses.hypothesis_card_links}
    claims = [item.model_dump(mode="json") for item in gap.claims]
    claim_ids = [item.id for item in gap.claims]
    cases = []
    for opportunity in sorted(gap.opportunities, key=lambda item: item.id):
        hypothesis_gap = gap_by_id[opportunity.gap_hypothesis_id]
        link = link_by_opportunity.get(opportunity.id)
        card = card_by_opportunity.get(opportunity.id)
        if link is None or card is None or link.evidence_card_id != card.id:
            continue
        problem_hypothesis = hypothesis_by_id.get(link.hypothesis_id)
        if (
            problem_hypothesis is None
            or problem_hypothesis.canonical_problem_id != hypothesis_gap.canonical_problem_id
        ):
            continue
        cases.append(
            {
                "opportunity_id": opportunity.id,
                "problem_hypothesis": problem_hypothesis.model_dump(mode="json"),
                "evidence_card": card.model_dump(mode="json"),
                "gap_hypothesis": hypothesis_gap.model_dump(mode="json"),
                "competitor_claims": claims,
                "permitted_claim_ids": claim_ids,
            }
        )
    return cases


def _selected_case_opportunity_ids(payload: Mapping[str, Any]) -> tuple[str, ...]:
    cases = payload.get("cases")
    if not isinstance(cases, list):
        return ()
    return tuple(
        value
        for case in cases
        if isinstance(case, dict)
        if isinstance((value := case.get("opportunity_id")), str)
    )


def _selected_case_claim_ids(payload: Mapping[str, Any]) -> tuple[str, ...]:
    cases = payload.get("cases")
    if not isinstance(cases, list):
        return ()
    return tuple(
        sorted(
            {
                claim_id
                for case in cases
                if isinstance(case, dict)
                for claim_id in case.get("permitted_claim_ids", [])
                if isinstance(claim_id, str)
            }
        )
    )


def _selected_case_evidence_ids(payload: Mapping[str, Any]) -> tuple[str, ...]:
    cases = payload.get("cases")
    if not isinstance(cases, list):
        return ()
    return tuple(
        sorted(
            {
                evidence_id
                for case in cases
                if isinstance(case, dict)
                for claim in case.get("competitor_claims", [])
                if isinstance(claim, dict)
                for evidence_id in claim.get("evidence_ids", [])
                if isinstance(evidence_id, str)
            }
        )
    )


def _gap_evidence_present(batch: GapResearchBatch, opportunity_id: str) -> bool:
    opportunity = next(
        (item for item in batch.opportunities if item.id == opportunity_id),
        None,
    )
    if opportunity is None:
        return False
    gap = next(
        (item for item in batch.gaps if item.id == opportunity.gap_hypothesis_id),
        None,
    )
    known_competitor_evidence = {item.id for item in batch.competitor_evidence}
    return bool(
        gap
        and gap.user_evidence_ids
        and gap.competitor_evidence_ids
        and set(gap.competitor_evidence_ids) <= known_competitor_evidence
    )


def _validate_collection_contract(
    jobs: tuple[tuple[Source, Collector, CollectRequest], ...],
    results: tuple[CollectResult, ...],
) -> tuple[CollectResult, ...]:
    validated: list[CollectResult] = []
    for index, (expected_source, _, request) in enumerate(jobs):
        result = results[index] if index < len(results) else None
        violation = None
        if result is None:
            violation = "result count"
        elif result.source is not expected_source:
            violation = "result source"
        elif any(item.source is not expected_source for item in result.items):
            violation = "item source"
        elif result.checkpoint is not None and result.checkpoint.source is not expected_source:
            violation = "checkpoint source"
        elif result.request_count > request.max_requests:
            violation = "request budget"
        elif len(result.items) > request.max_signals:
            violation = "signal budget"
        if violation is None and result is not None:
            validated.append(result)
            continue
        validated.append(
            CollectResult(
                source=expected_source,
                availability=Availability.SOURCE_UNAVAILABLE,
                request_count=request.max_requests,
                warnings=(
                    SourceWarning(
                        code="COLLECTOR_CONTRACT_VIOLATION",
                        message=f"collector violated its {violation} contract",
                        retryable=False,
                    ),
                ),
            )
        )
    if len(results) > len(jobs) and validated:
        first = validated[0]
        validated[0] = first.model_copy(
            update={
                "warnings": (
                    *first.warnings,
                    SourceWarning(
                        code="COLLECTOR_CONTRACT_VIOLATION",
                        message="collector returned unadmitted extra results",
                        retryable=False,
                    ),
                )
            }
        )
    return tuple(validated)


class CollectionBudgetExhaustedError(RuntimeError):
    """Storage refused a collection stage without committing partial effects."""


class ExternalAttemptIndeterminateError(RuntimeError):
    """A durable external reservation has no corresponding committed result."""
