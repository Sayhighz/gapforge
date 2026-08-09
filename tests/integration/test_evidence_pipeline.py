from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4, uuid5

import pytest
from sqlalchemy import func, select

from gapforge.analysis.lifecycle import TrendObservation, classify_trend
from gapforge.domain.contracts import (
    AgentEffort,
    AgentResult,
    AgentStatus,
    Availability,
    CollectRequest,
    CollectResult,
    FetchResult,
    MissionRevision,
    RunMode,
    SearchResponse,
    SemanticOperation,
    Source,
)
from gapforge.domain.contracts import (
    AgentRequest as DomainAgentRequest,
)
from gapforge.integration.evidence_store import (
    SqlAlchemyEvidencePipelineStore,
    StageConflictError,
)
from gapforge.integration.semantic import (
    AuditedSemanticReasoner,
    SemanticAdmissionError,
    SemanticAdmissionKind,
    SemanticCall,
    SemanticContext,
)
from gapforge.providers import contracts as provider_contracts
from gapforge.queue.retry import ErrorKind
from gapforge.runtime import RunScheduler, RunScheduleRequest
from gapforge.runtime.evidence_pipeline import (
    CapturedEvidence,
    CollectionBudgetExhaustedError,
    EvidencePipeline,
    ExistingCandidate,
    ExistingIntelligence,
    PipelineContext,
    PipelineExecution,
    PipelineStageCommit,
    _bounded_evidence_input,
    _bounded_stage_input,
    _examined_volume,
)
from gapforge.runtime.evidence_stages import PainExtractionBatch
from gapforge.storage.database import Database
from gapforge.storage.models import AgentCall, RawSignalRevision, ResearchRun, ResearchTask
from gapforge.storage.uow import SqlAlchemyUnitOfWork
from gapforge.worker import TaskHandlerError, TaskHandlerRegistry, Worker

NOW = datetime(2026, 8, 9, 12, tzinfo=UTC)
PROVIDER_ALIASES = {
    "pain-1",
    "problem-1",
    "cluster-1",
    "claim-1",
    "claim-2",
    "manual-1",
    "nothing-1",
    "competitor-evidence-1",
    "gap-1",
    "opportunity-1",
    "hypothesis-1",
}


def _rename_provider_aliases(value: object, suffix: str) -> object:
    if isinstance(value, str):
        return f"{value}{suffix}" if value in PROVIDER_ALIASES else value
    if isinstance(value, list):
        return [_rename_provider_aliases(item, suffix) for item in value]
    if isinstance(value, dict):
        return {key: _rename_provider_aliases(item, suffix) for key, item in value.items()}
    return value


class RecordingStore:
    def __init__(self, context: PipelineContext, events: list[str]) -> None:
        self.context = context
        self.events = events
        self.commits: list[PipelineStageCommit] = []
        self.completed: dict[str, dict[str, object]] = {}
        self.crash_after_stage: str | None = None
        self.remaining_calls = context.budget_limits["max_agent_calls_per_run"]

    async def load_context(self, task: ResearchTask) -> PipelineContext:
        assert task.run_id == self.context.run_id
        return self.context

    async def query_existing(self, context: PipelineContext) -> ExistingIntelligence:
        self.events.append("existing")
        return ExistingIntelligence()

    async def load_stage(self, context: PipelineContext, stage: str) -> dict[str, object] | None:
        return self.completed.get(stage)

    async def load_evidence(
        self,
        context: PipelineContext,
        evidence_ids: tuple[str, ...],
    ) -> tuple[CapturedEvidence, ...]:
        raise AssertionError("empty collection must not load evidence")

    async def commit_stage(
        self,
        context: PipelineContext,
        commit: PipelineStageCommit,
    ) -> None:
        self.events.append(f"commit:{commit.stage}")
        self.commits.append(commit)
        self.completed[commit.stage] = commit.payload
        if self.crash_after_stage == commit.stage:
            self.crash_after_stage = None
            raise RuntimeError("simulated process crash after durable commit")

    async def remaining_agent_calls(self, context: PipelineContext) -> int:
        return self.remaining_calls

    async def remaining_collection_budget(self, context: PipelineContext) -> tuple[int, int]:
        return (
            context.budget_limits["max_collector_requests_per_run"],
            context.budget_limits["max_raw_signals_per_run"],
        )


class RecordingReasoner:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.calls: list[tuple[SemanticContext, SemanticCall]] = []

    async def run(self, context: SemanticContext, call: SemanticCall) -> AgentResult:
        self.events.append("semantic:QUERY_PLAN")
        self.calls.append((context, call))
        return AgentResult(
            call_id=call.request.call_id,
            status=AgentStatus.COMPLETED,
            output_json={
                "round_number": 1,
                "intents": [
                    {
                        "id": "intent-accounting",
                        "kind": "BROAD",
                        "concept": "manual invoice reconciliation",
                        "sources": ["HACKER_NEWS"],
                        "rationale": "find repeated manual work",
                    }
                ],
            },
            provider="fake",
            effort=AgentEffort.MEDIUM,
            duration_ms=1,
        )


class EmptyCollector:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.requests: list[CollectRequest] = []

    async def collect(self, request: CollectRequest) -> CollectResult:
        self.events.append(f"collect:{request.source if hasattr(request, 'source') else 'HN'}")
        self.requests.append(request)
        return CollectResult(
            source=Source.HACKER_NEWS,
            availability=Availability.AVAILABLE,
            request_count=1,
        )


class FullCollector:
    def __init__(self) -> None:
        self.requests: list[CollectRequest] = []

    async def collect(self, request: CollectRequest) -> CollectResult:
        self.requests.append(request)
        external_id = "full-2" if request.intent.id == "follow-up-1" else "full-1"
        source = request.intent.sources[0]
        return CollectResult.model_validate(
            {
                "source": source.value,
                "availability": "AVAILABLE",
                "items": [
                    {
                        "source": source.value,
                        "external_id": external_id,
                        "canonical_url": f"https://example.test/{external_id}",
                        "parent_thread_id": "thread-1",
                        "author_identity": "author-1",
                        "title": "Invoice reconciliation takes hours",
                        "body": "We copy every line into a spreadsheet each week.",
                        "source_created_at": NOW,
                    }
                ],
                "request_count": 1,
            }
        )


class UnavailableGithubCollector:
    async def collect(self, request: CollectRequest) -> CollectResult:
        return CollectResult(
            source=Source.GITHUB,
            availability=Availability.SOURCE_UNAVAILABLE,
            request_count=1,
        )


class RepeatEvidenceCollector(FullCollector):
    async def collect(self, request: CollectRequest) -> CollectResult:
        repeated_intent = request.intent.model_copy(
            update={"id": "intent-1", "sources": (Source.HACKER_NEWS,)}
        )
        return await super().collect(request.model_copy(update={"intent": repeated_intent}))


class FullSearch:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int, int]] = []

    async def search(self, query: str, *, max_results: int, max_requests: int) -> SearchResponse:
        self.calls.append((query, max_results, max_requests))
        return SearchResponse.model_validate(
            {
                "availability": "AVAILABLE",
                "query": query,
                "results": [
                    {
                        "id": "search-1",
                        "title": "Manual reconciliation",
                        "url": "https://competitor.test/manual",
                        "snippet": "Spreadsheet copying workaround",
                        "observed_at": NOW,
                        "rank": 1,
                    }
                ],
                "request_count": 1,
            }
        )


class FullFetcher:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def fetch(self, url: str, *, approved_urls: tuple[str, ...]) -> FetchResult:
        assert url in approved_urls
        self.calls.append(url)
        return FetchResult.model_validate(
            {
                "availability": "AVAILABLE",
                "snapshot": {
                    "url": url,
                    "final_url": url,
                    "text": "Manual work requires spreadsheet copying",
                    "content_type": "text/plain",
                    "sha256": "c" * 64,
                    "observed_at": NOW,
                },
            }
        )


class FullStore(RecordingStore):
    async def commit_stage(
        self,
        context: PipelineContext,
        commit: PipelineStageCommit,
    ) -> None:
        self.events.append(f"commit:{commit.stage}")
        self.commits.append(commit)
        if commit.stage in {"COLLECT", "COLLECT_R2"}:
            evidence_ids = sorted(
                {
                    f"{item['source']}:{item['external_id']}:r1"
                    for result in commit.payload["results"]
                    for item in result["items"]
                }
            )
            self.completed[commit.stage] = {
                **commit.payload,
                "evidence_ids": evidence_ids,
            }
        else:
            self.completed[commit.stage] = commit.payload
        if self.crash_after_stage == commit.stage:
            self.crash_after_stage = None
            raise RuntimeError("simulated process crash after durable commit")

    async def load_evidence(
        self,
        context: PipelineContext,
        evidence_ids: tuple[str, ...],
    ) -> tuple[CapturedEvidence, ...]:
        return tuple(
            CapturedEvidence(
                evidence_id=evidence_id,
                source=Source.HACKER_NEWS,
                url=f"https://example.test/{evidence_id.split(':')[1]}",
                text=(
                    "Invoice reconciliation takes hours\n"
                    "We copy every line into a spreadsheet each week."
                ),
                observed_at=NOW,
                duplicate_group=("a" if "full-1" in evidence_id else "b") * 64,
                author_id=f"author-{evidence_id}",
                thread_id=f"thread-{evidence_id}",
            )
            for evidence_id in evidence_ids
        )


class FullReasoner:
    def __init__(
        self,
        *,
        reverse_outputs: bool = False,
        alias_suffix: str = "",
        recommend_more: bool = False,
        critic_verdict: str = "RESEARCH_MORE",
        preserve_extra_opportunity: bool = False,
    ) -> None:
        self.operations: list[SemanticOperation] = []
        self.reverse_outputs = reverse_outputs
        self.alias_suffix = alias_suffix
        self.recommend_more = recommend_more
        self.critic_verdict = critic_verdict
        self.preserve_extra_opportunity = preserve_extra_opportunity
        self.input_sizes: list[int] = []
        self.inputs: list[dict[str, object]] = []

    async def run(self, context: SemanticContext, call: SemanticCall) -> AgentResult:
        operation = call.request.task
        self.operations.append(operation)
        self.inputs.append(call.request.input_json)
        self.input_sizes.append(
            len(
                json.dumps(
                    call.request.input_json,
                    allow_nan=False,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
            )
        )
        outputs = {
            SemanticOperation.QUERY_PLAN: {
                "round_number": 1,
                "intents": [
                    {
                        "id": "intent-1",
                        "kind": "BROAD",
                        "concept": "invoice reconciliation",
                        "sources": ["HACKER_NEWS"],
                        "rationale": "recurring manual work",
                    }
                ],
            },
            SemanticOperation.EXTRACT: {
                "pain_signals": [
                    {
                        "id": "pain-1",
                        "raw_signal_revision_id": "HACKER_NEWS:full-1:r1",
                        "pain": "Invoice reconciliation takes hours",
                        "severity": 0.8,
                        "frequency": 0.7,
                        "workaround": "copy every line into a spreadsheet",
                        "payment_signal": False,
                        "confidence": 0.9,
                        "excerpt": "Invoice reconciliation takes hours",
                    }
                ]
            },
            SemanticOperation.CLUSTER: {
                "problems": [{"id": "problem-1", "summary": "Manual invoice reconciliation"}],
                "clusters": [
                    {
                        "id": "cluster-1",
                        "canonical_problem_id": "problem-1",
                        "state": "PROVISIONAL",
                        "last_growth_at": NOW.isoformat(),
                    }
                ],
                "memberships": [
                    {
                        "cluster_id": "cluster-1",
                        "pain_signal_id": "pain-1",
                        "accepted_at": NOW.isoformat(),
                    }
                ],
            },
            SemanticOperation.GAP: {
                "claims": [
                    {
                        "id": "claim-1",
                        "text": "Users manually reconcile invoice lines",
                        "kind": "USER_PAIN",
                        "status": "SUPPORTED",
                        "evidence_ids": ["HACKER_NEWS:full-1:r1"],
                        "contradicts_claim_ids": ["claim-2"],
                    },
                    {
                        "id": "claim-2",
                        "text": "Manual work requires spreadsheet copying",
                        "kind": "FEATURE",
                        "status": "SUPPORTED",
                        "evidence_ids": ["competitor-evidence-1"],
                        "citations": [
                            {
                                "evidence_id": "competitor-evidence-1",
                                "source_url": "https://competitor.test/manual",
                                "excerpt": "Manual work requires spreadsheet copying",
                                "observed_at": NOW.isoformat(),
                            }
                        ],
                    },
                ],
                "competitors": [
                    {"id": "manual-1", "name": "Manual work", "kind": "MANUAL_WORK"},
                    {"id": "nothing-1", "name": "Do nothing", "kind": "DO_NOTHING"},
                ],
                "competitor_evidence": [
                    {
                        "id": "competitor-evidence-1",
                        "competitor_id": "manual-1",
                        "source_url": "https://competitor.test/manual",
                        "captured_excerpt": "Manual work requires spreadsheet copying",
                        "observed_at": NOW.isoformat(),
                        "content_hash": "c" * 64,
                        "evidence_kind": "WORKAROUND",
                        "claim_ids": ["claim-2"],
                    }
                ],
                "gaps": [
                    {
                        "id": "gap-1",
                        "canonical_problem_id": "problem-1",
                        "gap_type": "WORKFLOW",
                        "statement": "Existing manual reconciliation is slow",
                        "user_evidence_ids": ["HACKER_NEWS:full-1:r1"],
                        "competitor_evidence_ids": ["competitor-evidence-1"],
                    }
                ],
                "opportunities": [
                    {"id": "opportunity-1", "gap_hypothesis_id": "gap-1", "title": "Reconcile"}
                ],
                "opportunity_fit": [
                    {
                        "opportunity_id": "opportunity-1",
                        **{
                            name: 50
                            for name in (
                                "gap_strength",
                                "competitor_dissatisfaction",
                                "reachability",
                                "technical_feasibility",
                                "small_team_feasibility",
                                "inverse_switching_friction",
                                "why_now",
                            )
                        },
                    }
                ],
            },
            SemanticOperation.HYPOTHESIS: {
                "hypotheses": [
                    {
                        "id": "hypothesis-1",
                        "canonical_problem_id": "problem-1",
                        "icp": "Small accounting teams",
                        "job_to_be_done": "Reconcile invoices",
                        "trigger": "Weekly close",
                        "current_behavior": "Copy spreadsheet lines",
                        "pain": "Takes hours",
                        "workflow_failure": "Manual matching",
                        "falsification_test": "Fails if fewer than five teams report this",
                        "supporting_claim_ids": ["claim-1"],
                    }
                ],
                "hypothesis_card_links": [
                    {
                        "hypothesis_id": "hypothesis-1",
                        "opportunity_id": "opportunity-1",
                        "evidence_card_id": "card-1",
                    }
                ],
            },
            SemanticOperation.CRITIC: {
                "results": [
                    {
                        "opportunity_id": "opportunity-1",
                        "verdict": "RESEARCH_MORE",
                        "confidence": 0.8,
                        "summary": "Independent evidence is insufficient",
                    }
                ]
            },
        }
        output = outputs[operation]
        if operation is SemanticOperation.CRITIC:
            output["results"][0]["verdict"] = self.critic_verdict
        if self.alias_suffix:
            output = _rename_provider_aliases(output, self.alias_suffix)
        if operation is SemanticOperation.EXTRACT:
            output["pain_signals"][0]["raw_signal_revision_id"] = call.request.input_json[
                "evidence"
            ][0]["id"]
        elif operation is SemanticOperation.CLUSTER:
            output["memberships"][0]["pain_signal_id"] = call.request.input_json["pain_signals"][0][
                "id"
            ]
        elif operation is SemanticOperation.GAP:
            problem_id = call.request.input_json["problems"][0]["id"]
            output["gaps"][0]["canonical_problem_id"] = problem_id
            selected_evidence_id = call.request.input_json["evidence"][0]["id"]
            output["gaps"][0]["user_evidence_ids"] = [selected_evidence_id]
            user_claim = next(claim for claim in output["claims"] if claim["kind"] == "USER_PAIN")
            user_claim["evidence_ids"] = [selected_evidence_id]
            if self.preserve_extra_opportunity and not selected_evidence_id.startswith("REDDIT:"):
                output["gaps"].append(
                    {
                        **output["gaps"][0],
                        "id": "gap-extra",
                        "statement": "Manual reconciliation blocks exception reviews",
                    }
                )
                output["opportunities"].append(
                    {
                        "id": "opportunity-extra",
                        "gap_hypothesis_id": "gap-extra",
                        "title": "Review reconciliation exceptions",
                    }
                )
                output["opportunity_fit"].append(
                    {
                        **output["opportunity_fit"][0],
                        "opportunity_id": "opportunity-extra",
                    }
                )
        elif operation is SemanticOperation.HYPOTHESIS:
            template = output["hypotheses"][0]
            output["hypotheses"] = []
            output["hypothesis_card_links"] = []
            for index, case in enumerate(call.request.input_json["cases"]):
                hypothesis_id = f"{template['id']}:{index}"
                output["hypotheses"].append(
                    {
                        **template,
                        "id": hypothesis_id,
                        "canonical_problem_id": case["problem"]["id"],
                        "supporting_claim_ids": [case["competitor_claims"][0]["id"]],
                    }
                )
                output["hypothesis_card_links"].append(
                    {
                        "hypothesis_id": hypothesis_id,
                        "opportunity_id": case["opportunity_id"],
                        "evidence_card_id": case["evidence_card"]["id"],
                    }
                )
        elif operation is SemanticOperation.CRITIC:
            template = output["results"][0]
            output["results"] = [
                {**template, "opportunity_id": case["opportunity_id"]}
                for case in call.request.input_json["cases"]
            ]
            if self.recommend_more:
                for item in output["results"]:
                    item["recommended_intents"] = [
                        {
                            "id": "follow-up-1",
                            "kind": "TARGETED",
                            "concept": "reconciliation payment signal",
                            "sources": ["REDDIT"],
                            "rationale": "close missing willingness-to-pay evidence",
                        }
                    ]
        if self.reverse_outputs and operation is SemanticOperation.GAP:
            for section in (
                "claims",
                "competitors",
                "competitor_evidence",
                "gaps",
                "opportunities",
                "opportunity_fit",
            ):
                output[section].reverse()
            for claim in output["claims"]:
                for name in ("evidence_ids", "citations", "contradicts_claim_ids"):
                    claim.setdefault(name, []).reverse()
            for item in output["competitor_evidence"]:
                item["claim_ids"].reverse()
            for item in output["gaps"]:
                item["user_evidence_ids"].reverse()
                item["competitor_evidence_ids"].reverse()
        if self.reverse_outputs and operation is SemanticOperation.HYPOTHESIS:
            output["hypotheses"].reverse()
            output["hypothesis_card_links"].reverse()
            for item in output["hypotheses"]:
                item["supporting_claim_ids"].reverse()
                item.setdefault("contradicting_claim_ids", []).reverse()
        return AgentResult(
            call_id=call.request.call_id,
            status=AgentStatus.COMPLETED,
            output_json=output,
            provider="fake",
            effort=call.request.effort,
            duration_ms=1,
        )


class PartialSourceReasoner(FullReasoner):
    async def run(self, context: SemanticContext, call: SemanticCall) -> AgentResult:
        result = await super().run(context, call)
        if call.request.task is not SemanticOperation.QUERY_PLAN:
            return result
        assert result.output_json is not None
        output = dict(result.output_json)
        intents = [dict(item) for item in output["intents"]]
        intents[0]["sources"] = ["HACKER_NEWS", "GITHUB"]
        return result.model_copy(update={"output_json": {**output, "intents": intents}})


class InvalidClusterReasoner(FullReasoner):
    async def run(self, context: SemanticContext, call: SemanticCall) -> AgentResult:
        result = await super().run(context, call)
        if call.request.task is not SemanticOperation.CLUSTER:
            return result
        assert result.output_json is not None
        output = dict(result.output_json)
        memberships = [dict(item) for item in output["memberships"]]
        memberships[0]["cluster_id"] = "invented-cluster"
        return result.model_copy(update={"output_json": {**output, "memberships": memberships}})


class RepairExhaustingReasoner(FullReasoner):
    async def run(self, context: SemanticContext, call: SemanticCall) -> AgentResult:
        if (
            call.request.task is SemanticOperation.CLUSTER
            and self.operations.count(SemanticOperation.CLUSTER) >= 1
        ):
            raise SemanticAdmissionError(
                SemanticAdmissionKind.BUDGET_EXHAUSTED,
                "repair consumed the final admitted call",
            )
        return await super().run(context, call)


class FullProvider:
    def __init__(self) -> None:
        self.reasoner = FullReasoner(recommend_more=True)

    async def run(self, request: provider_contracts.AgentRequest) -> provider_contracts.AgentResult:
        operation = SemanticOperation(request.operation.upper())
        input_json = request.evidence["input"]
        assert isinstance(input_json, dict)
        call = SemanticCall(
            request=DomainAgentRequest(
                call_id=str(uuid4()),
                task=operation,
                effort=AgentEffort(request.effort.value),
                input_json=input_json,
                permitted_evidence_ids=tuple(request.evidence["permitted_evidence_ids"]),
                permitted_urls=tuple(request.evidence["permitted_urls"]),
                output_schema_name="test-provider-schema",
                timeout_seconds=300,
            ),
            output_schema=request.output_schema,
        )
        result = await self.reasoner.run(SemanticContext(run_id=uuid4(), task_id=uuid4()), call)
        return provider_contracts.AgentResult(
            status=provider_contracts.AgentStatus.SUCCESS,
            data=result.output_json,
            provider="fake",
            effort=request.effort,
            duration_ms=1,
        )


class OverBudgetSearch(FullSearch):
    async def search(self, query: str, *, max_results: int, max_requests: int) -> SearchResponse:
        result = await super().search(
            query,
            max_results=max_results,
            max_requests=max_requests,
        )
        return result.model_copy(update={"request_count": max_requests + 1})


class MismatchedFetcher(FullFetcher):
    async def fetch(self, url: str, *, approved_urls: tuple[str, ...]) -> FetchResult:
        result = await super().fetch(url, approved_urls=approved_urls)
        assert result.snapshot is not None
        return result.model_copy(
            update={"snapshot": result.snapshot.model_copy(update={"sha256": "d" * 64})}
        )


class BudgetStore(RecordingStore):
    async def commit_stage(self, context: PipelineContext, commit: PipelineStageCommit) -> None:
        if commit.stage == "COLLECT":
            raise CollectionBudgetExhaustedError("test budget")
        await super().commit_stage(context, commit)


class NoCollectionBudgetStore(FullStore):
    async def remaining_collection_budget(self, context: PipelineContext) -> tuple[int, int]:
        return (0, 0)


class RecordingArtifactWriter:
    def __init__(self) -> None:
        self.stages: list[str] = []

    async def persist_stage(
        self,
        session: object,
        context: PipelineContext,
        commit: PipelineStageCommit,
    ) -> None:
        assert session is not None
        assert context.run_id is not None
        self.stages.append(commit.stage)


def context(run_id: UUID, task_id: UUID) -> PipelineContext:
    revision_id = uuid4()
    return PipelineContext(
        run_id=run_id,
        task_id=task_id,
        task_attempt=1,
        worker_id="test-worker",
        mission_revision=MissionRevision(
            id=revision_id,
            mission_id=uuid4(),
            revision=1,
            parent_revision_id=None,
            change_reason="initial",
            prompt="Find recurring accounting workflow pain",
            output_locale="en",
            created_at=NOW,
        ),
        mode=RunMode.HUNT,
        budget_limits={
            "max_research_rounds": 2,
            "max_agent_calls_per_run": 6,
            "max_search_calls_per_run": 20,
            "max_collector_requests_per_run": 60,
            "max_raw_signals_per_run": 300,
            "initial_lookback_days": 365,
        },
        collection_until=NOW,
    )


def research_task(run_id: UUID, task_id: UUID) -> ResearchTask:
    return ResearchTask(
        id=task_id,
        run_id=run_id,
        task_type="research.run",
        status="LEASED",
        priority=1,
        idempotency_key="run-root:v1",
        payload={"run_id": str(run_id)},
        checkpoint={},
        attempt_count=1,
        max_attempts=3,
        available_at=NOW,
        lease_owner="test-worker",
        lease_expires_at=datetime(2026, 8, 9, 12, 5, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_hunt_queries_existing_before_collection_and_allows_zero_validate() -> None:
    events: list[str] = []
    run_id = uuid4()
    task_id = uuid4()
    pipeline_context = context(run_id, task_id)
    store = RecordingStore(pipeline_context, events)
    reasoner = RecordingReasoner(events)
    collector = EmptyCollector(events)
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={Source.HACKER_NEWS: collector},
        clock=lambda: NOW,
    )
    task = ResearchTask(
        id=task_id,
        run_id=run_id,
        task_type="research.run",
        status="LEASED",
        priority=1,
        idempotency_key="run-root:v1",
        payload={"run_id": str(run_id)},
        checkpoint={},
        attempt_count=1,
        max_attempts=3,
        available_at=NOW,
        lease_owner="test-worker",
        lease_expires_at=datetime(2026, 8, 9, 12, 5, tzinfo=UTC),
    )

    result = await pipeline(task)

    assert events[0] == "existing"
    assert events.index("semantic:QUERY_PLAN") < next(
        index for index, event in enumerate(events) if event.startswith("collect:")
    )
    assert result.payload == {
        "opportunities": 0,
        "validated": 0,
        "warnings": [],
        "completed_stage": "COLLECT",
    }
    assert result.useful_artifact is False
    semantic_context, semantic_call = reasoner.calls[0]
    assert semantic_context == SemanticContext(run_id=run_id, task_id=task_id)
    assert semantic_call.request.task is SemanticOperation.QUERY_PLAN
    assert semantic_call.request.call_id == str(uuid5(run_id, f"{task_id}:QUERY_PLAN:1:1"))
    assert semantic_call.output_schema["title"] == "GapForge QueryPlan v0.1"
    assert len(collector.requests) == 4
    assert sum(request.max_signals for request in collector.requests) <= 300
    assert [commit.stage for commit in store.commits] == ["EXISTING", "QUERY_PLAN", "COLLECT"]
    assert len({commit.idempotency_key for commit in store.commits}) == 3
    assert all(isinstance(commit.payload, dict) for commit in store.commits)


@pytest.mark.asyncio
async def test_resume_skips_durably_completed_semantic_and_collection_stages() -> None:
    events: list[str] = []
    run_id = uuid4()
    task_id = uuid4()
    pipeline_context = context(run_id, task_id)
    store = RecordingStore(pipeline_context, events)
    store.crash_after_stage = "COLLECT"
    reasoner = RecordingReasoner(events)
    collector = EmptyCollector(events)
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={Source.HACKER_NEWS: collector},
        clock=lambda: NOW,
    )
    task = ResearchTask(
        id=task_id,
        run_id=run_id,
        task_type="research.run",
        status="LEASED",
        priority=1,
        idempotency_key="run-root:v1",
        payload={"run_id": str(run_id)},
        checkpoint={},
        attempt_count=1,
        max_attempts=3,
        available_at=NOW,
        lease_owner="test-worker",
        lease_expires_at=datetime(2026, 8, 9, 12, 5, tzinfo=UTC),
    )

    with pytest.raises(RuntimeError, match="simulated process crash"):
        await pipeline(task)
    result = await pipeline(task)

    assert result.payload["completed_stage"] == "COLLECT"
    assert len(reasoner.calls) == 1
    assert len(collector.requests) == 4
    assert [commit.stage for commit in store.commits] == [
        "EXISTING",
        "QUERY_PLAN",
        "COLLECT",
    ]


@pytest.mark.asyncio
async def test_fake_hunt_runs_complete_lineage_and_valid_zero_validate() -> None:
    events: list[str] = []
    run_id = uuid4()
    task_id = uuid4()
    store = FullStore(context(run_id, task_id), events)
    reasoner = FullReasoner()
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={Source.HACKER_NEWS: FullCollector()},
        competitor_search=FullSearch(),
        safe_fetch=FullFetcher(),
        clock=lambda: NOW,
    )
    task = ResearchTask(
        id=task_id,
        run_id=run_id,
        task_type="research.run",
        status="LEASED",
        priority=1,
        idempotency_key="run-root:v1",
        payload={"run_id": str(run_id)},
        checkpoint={},
        attempt_count=1,
        max_attempts=3,
        available_at=NOW,
        lease_owner="test-worker",
        lease_expires_at=datetime(2026, 8, 9, 12, 5, tzinfo=UTC),
    )

    result = await pipeline(task)

    assert result.payload == {
        "opportunities": 1,
        "validated": 0,
        "warnings": [],
        "completed_stage": "FINAL",
    }
    assert reasoner.operations == [
        SemanticOperation.QUERY_PLAN,
        SemanticOperation.EXTRACT,
        SemanticOperation.CLUSTER,
        SemanticOperation.GAP,
        SemanticOperation.HYPOTHESIS,
        SemanticOperation.CRITIC,
    ]
    assert max(reasoner.input_sizes) < 20_000
    assert [commit.stage for commit in store.commits] == [
        "EXISTING",
        "QUERY_PLAN",
        "COLLECT",
        "EXTRACT",
        "CLUSTER",
        "COMPETITOR_RESEARCH",
        "GAP",
        "CARD_SCORE",
        "HYPOTHESIS",
        "CRITIC",
        "FINAL",
    ]
    decisions = store.completed["FINAL"]["decisions"]
    assert isinstance(decisions, list)
    first_decision = decisions[0]
    assert isinstance(first_decision, dict)
    assert first_decision["verdict"] == "RESEARCH_MORE"
    assert first_decision["round_number"] == 1
    assert first_decision["evidence_card_id"] == store.completed["CARD_SCORE"]["cards"][0]["id"]
    assert first_decision["score_snapshot_id"] == store.completed["CARD_SCORE"]["scores"][0]["id"]
    assert first_decision["critic_result_id"] == str(
        uuid5(run_id, f"critic:1:{first_decision['opportunity_id']}")
    )
    link = store.completed["HYPOTHESIS"]["hypothesis_card_links"][0]
    assert link["opportunity_id"] == first_decision["opportunity_id"]
    assert link["evidence_card_id"] == first_decision["evidence_card_id"]


@pytest.mark.asyncio
async def test_partial_source_failure_preserves_useful_result_and_typed_warning() -> None:
    run_id = uuid4()
    task_id = uuid4()
    pipeline = EvidencePipeline(
        store=FullStore(context(run_id, task_id), []),
        reasoner=PartialSourceReasoner(),
        collectors={
            Source.HACKER_NEWS: FullCollector(),
            Source.GITHUB: UnavailableGithubCollector(),
        },
        competitor_search=FullSearch(),
        safe_fetch=FullFetcher(),
        clock=lambda: NOW,
    )

    result = await pipeline(research_task(run_id, task_id))

    assert result.useful_artifact is True
    assert result.payload["completed_stage"] == "FINAL"
    assert result.payload["warnings"] == ["GITHUB_SOURCE_UNAVAILABLE"]
    assert [warning.code for warning in result.warnings] == ["GITHUB_SOURCE_UNAVAILABLE"]


@pytest.mark.asyncio
async def test_resume_after_card_score_does_not_repeat_reasoning_search_or_fetch() -> None:
    run_id = uuid4()
    task_id = uuid4()
    store = FullStore(context(run_id, task_id), [])
    store.crash_after_stage = "CARD_SCORE"
    reasoner = FullReasoner()
    search = FullSearch()
    fetcher = FullFetcher()
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={Source.HACKER_NEWS: FullCollector()},
        competitor_search=search,
        safe_fetch=fetcher,
        clock=lambda: NOW,
    )
    task = ResearchTask(
        id=task_id,
        run_id=run_id,
        task_type="research.run",
        status="LEASED",
        priority=1,
        idempotency_key="run-root:v1",
        payload={"run_id": str(run_id)},
        checkpoint={},
        attempt_count=1,
        max_attempts=3,
        available_at=NOW,
        lease_owner="test-worker",
        lease_expires_at=datetime(2026, 8, 9, 12, 5, tzinfo=UTC),
    )

    with pytest.raises(RuntimeError, match="simulated process crash"):
        await pipeline(task)
    first_operations = tuple(reasoner.operations)
    first_card_score = store.completed["CARD_SCORE"]

    result = await pipeline(task)

    assert result.payload["completed_stage"] == "FINAL"
    assert tuple(reasoner.operations[: len(first_operations)]) == first_operations
    assert reasoner.operations.count(SemanticOperation.QUERY_PLAN) == 1
    assert reasoner.operations.count(SemanticOperation.GAP) == 1
    assert len(search.calls) == 1
    assert search.calls[0][1:] == (10, 1)
    assert fetcher.calls == ["https://competitor.test/manual"]
    assert store.completed["CARD_SCORE"] == first_card_score


@pytest.mark.asyncio
async def test_research_more_stops_durably_at_exact_persisted_agent_budget() -> None:
    run_id = uuid4()
    task_id = uuid4()
    store = FullStore(context(run_id, task_id), [])
    # Represents six base semantic calls plus any audited repair already charged
    # by I4. The pipeline must query this durable value, not reset a local cap.
    store.remaining_calls = 0
    reasoner = FullReasoner(recommend_more=True)
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={Source.HACKER_NEWS: FullCollector()},
        competitor_search=FullSearch(),
        safe_fetch=FullFetcher(),
        clock=lambda: NOW,
    )

    first = await pipeline(research_task(run_id, task_id))
    second = await pipeline(research_task(run_id, task_id))

    checkpoint = store.completed["RESEARCH_MORE_CONTROL"]
    assert checkpoint["status"] == "STOPPED_AGENT_BUDGET"
    assert checkpoint["remaining_agent_calls"] == 0
    assert checkpoint["required_agent_calls"] == 5
    assert reasoner.operations.count(SemanticOperation.CRITIC) == 1
    assert first.payload["warnings"] == ["RESEARCH_MORE_STOPPED_AGENT_BUDGET"]
    assert second.payload == first.payload


@pytest.mark.asyncio
async def test_exact_eleven_call_budget_executes_coherent_second_round() -> None:
    run_id = uuid4()
    task_id = uuid4()
    store = FullStore(context(run_id, task_id), [])
    store.remaining_calls = 5
    reasoner = FullReasoner(recommend_more=True)
    round_one_collector = FullCollector()
    round_two_collector = FullCollector()
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={
            Source.HACKER_NEWS: round_one_collector,
            Source.REDDIT: round_two_collector,
        },
        competitor_search=FullSearch(),
        safe_fetch=FullFetcher(),
        clock=lambda: NOW,
    )

    result = await pipeline(research_task(run_id, task_id))

    assert result.payload["completed_stage"] == "FINAL"
    assert len(reasoner.operations) == 11
    assert [item.value for item in reasoner.operations[-5:]] == [
        "EXTRACT",
        "CLUSTER",
        "GAP",
        "HYPOTHESIS",
        "CRITIC",
    ]
    for stage in (
        "COLLECT_R2",
        "EXTRACT_R2",
        "CLUSTER_R2",
        "GAP_R2",
        "CARD_SCORE_R2",
        "HYPOTHESIS_R2",
        "CRITIC_R2",
    ):
        assert stage in store.completed
    assert store.completed["RESEARCH_MORE_RESULT"]["status"] == "COMPLETE"
    assert {request.intent.id for request in round_two_collector.requests} == {"follow-up-1"}
    assert store.completed["COLLECT_R2"]["evidence_ids"] == ["REDDIT:full-2:r1"]
    collected_ids = {
        f"{item['source']}:{item['external_id']}:r1"
        for result_item in store.completed["COLLECT_R2"]["results"]
        for item in result_item["items"]
    }
    assert collected_ids == set(store.completed["COLLECT_R2"]["evidence_ids"])
    second_extract = store.completed["EXTRACT_R2"]["pain_signals"]
    assert {item["raw_signal_revision_id"] for item in second_extract} == {"REDDIT:full-2:r1"}
    round_two_cluster_input = reasoner.inputs[
        max(
            index
            for index, operation in enumerate(reasoner.operations)
            if operation.value == "CLUSTER"
        )
    ]
    assert round_two_cluster_input["pain_signals"][0]["raw_signal_revision_id"] == (
        "REDDIT:full-2:r1"
    )
    round_two_gap_input = reasoner.inputs[
        max(
            index for index, operation in enumerate(reasoner.operations) if operation.value == "GAP"
        )
    ]
    assert round_two_gap_input["evidence"][0]["id"] == "REDDIT:full-2:r1"
    assert (
        store.completed["GAP"]["opportunities"][0]["id"]
        == (store.completed["GAP_R2"]["opportunities"][0]["id"])
    )
    assert store.completed["GAP"]["gaps"][0]["id"] != store.completed["GAP_R2"]["gaps"][0]["id"]
    final = store.completed["FINAL"]["decisions"][0]
    assert final["round_number"] == 2
    assert final["gap_hypothesis_id"] == store.completed["GAP_R2"]["gaps"][0]["id"]
    assert final["evidence_card_id"] == store.completed["CARD_SCORE_R2"]["cards"][0]["id"]
    assert final["score_snapshot_id"] == store.completed["CARD_SCORE_R2"]["scores"][0]["id"]
    assert reasoner.inputs[-1]["cases"]


@pytest.mark.asyncio
async def test_exact_ten_call_budget_stops_before_second_round() -> None:
    run_id = uuid4()
    task_id = uuid4()
    store = FullStore(context(run_id, task_id), [])
    store.remaining_calls = 4
    reasoner = FullReasoner(recommend_more=True)
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={Source.HACKER_NEWS: FullCollector(), Source.REDDIT: FullCollector()},
        competitor_search=FullSearch(),
        safe_fetch=FullFetcher(),
        clock=lambda: NOW,
    )

    result = await pipeline(research_task(run_id, task_id))

    assert result.payload["warnings"] == ["RESEARCH_MORE_STOPPED_AGENT_BUDGET"]
    assert store.completed["RESEARCH_MORE_CONTROL"]["status"] == "STOPPED_AGENT_BUDGET"
    assert "COLLECT_R2" not in store.completed
    assert len(reasoner.operations) == 6


@pytest.mark.asyncio
async def test_recommended_intents_do_not_trigger_round_two_for_reject_verdict() -> None:
    run_id = uuid4()
    task_id = uuid4()
    store = FullStore(context(run_id, task_id), [])
    store.remaining_calls = 5
    reasoner = FullReasoner(recommend_more=True, critic_verdict="REJECT")
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={Source.HACKER_NEWS: FullCollector(), Source.REDDIT: FullCollector()},
        competitor_search=FullSearch(),
        safe_fetch=FullFetcher(),
        clock=lambda: NOW,
    )

    await pipeline(research_task(run_id, task_id))

    assert "RESEARCH_MORE_CONTROL" not in store.completed
    assert "COLLECT_R2" not in store.completed
    assert len(reasoner.operations) == 6


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "crash_stage",
    [
        "COLLECT_R2",
        "EXTRACT_R2",
        "CLUSTER_R2",
        "GAP_R2",
        "CARD_SCORE_R2",
        "HYPOTHESIS_R2",
        "CRITIC_R2",
        "RESEARCH_MORE_RESULT",
    ],
)
async def test_second_round_resumes_after_every_durable_boundary(crash_stage: str) -> None:
    run_id = uuid4()
    task_id = uuid4()
    store = FullStore(context(run_id, task_id), [])
    store.remaining_calls = 5
    store.crash_after_stage = crash_stage
    reasoner = FullReasoner(recommend_more=True)
    round_two_collector = FullCollector()
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={
            Source.HACKER_NEWS: FullCollector(),
            Source.REDDIT: round_two_collector,
        },
        competitor_search=FullSearch(),
        safe_fetch=FullFetcher(),
        clock=lambda: NOW,
    )
    task = research_task(run_id, task_id)

    with pytest.raises(RuntimeError, match="simulated process crash"):
        await pipeline(task)
    origin_gap = store.completed["GAP"]["gaps"][0]["id"]
    result = await pipeline(task)

    assert result.payload["completed_stage"] == "FINAL"
    assert len(reasoner.operations) == 11
    assert len([commit for commit in store.commits if commit.stage == crash_stage]) == 1
    assert {request.intent.id for request in round_two_collector.requests} == {"follow-up-1"}
    final_gap = store.completed["FINAL"]["decisions"][0]["gap_hypothesis_id"]
    assert final_gap == store.completed["GAP_R2"]["gaps"][0]["id"]
    assert final_gap != origin_gap


@pytest.mark.asyncio
async def test_second_round_stops_before_reasoning_when_collection_adds_no_evidence() -> None:
    run_id = uuid4()
    task_id = uuid4()
    store = FullStore(context(run_id, task_id), [])
    store.remaining_calls = 5
    reasoner = FullReasoner(recommend_more=True)
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={
            Source.HACKER_NEWS: FullCollector(),
            Source.REDDIT: RepeatEvidenceCollector(),
        },
        competitor_search=FullSearch(),
        safe_fetch=FullFetcher(),
        clock=lambda: NOW,
    )

    result = await pipeline(research_task(run_id, task_id))

    assert result.payload["warnings"] == ["RESEARCH_MORE_NO_NEW_EVIDENCE"]
    assert store.completed["RESEARCH_MORE_RESULT"] == {
        "status": "STOPPED_NO_NEW_EVIDENCE",
        "new_evidence_ids": [],
    }
    assert "EXTRACT_R2" not in store.completed
    assert len(reasoner.operations) == 6


@pytest.mark.asyncio
async def test_second_round_persists_collection_budget_stop_without_fetching() -> None:
    run_id = uuid4()
    task_id = uuid4()
    store = NoCollectionBudgetStore(context(run_id, task_id), [])
    store.remaining_calls = 5
    reasoner = FullReasoner(recommend_more=True)
    round_two_collector = FullCollector()
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={
            Source.HACKER_NEWS: FullCollector(),
            Source.REDDIT: round_two_collector,
        },
        competitor_search=FullSearch(),
        safe_fetch=FullFetcher(),
        clock=lambda: NOW,
    )

    result = await pipeline(research_task(run_id, task_id))

    assert result.payload["warnings"] == ["RESEARCH_MORE_STOPPED_COLLECTION_BUDGET"]
    assert store.completed["RESEARCH_MORE_RESULT"] == {
        "status": "STOPPED_COLLECTION_BUDGET",
        "new_evidence_ids": [],
    }
    assert round_two_collector.requests == []


@pytest.mark.asyncio
async def test_focused_second_round_preserves_untargeted_first_round_opportunity() -> None:
    run_id = uuid4()
    task_id = uuid4()
    store = FullStore(context(run_id, task_id), [])
    store.remaining_calls = 5
    reasoner = FullReasoner(recommend_more=True, preserve_extra_opportunity=True)
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={Source.HACKER_NEWS: FullCollector(), Source.REDDIT: FullCollector()},
        competitor_search=FullSearch(),
        safe_fetch=FullFetcher(),
        clock=lambda: NOW,
    )

    result = await pipeline(research_task(run_id, task_id))

    first_round_ids = {item["id"] for item in store.completed["GAP"]["opportunities"]}
    focused_round_ids = {item["id"] for item in store.completed["GAP_R2"]["opportunities"]}
    final_ids = {item["opportunity_id"] for item in store.completed["FINAL"]["decisions"]}
    assert len(first_round_ids) == 2
    assert len(focused_round_ids) == 1
    assert final_ids == first_round_ids
    assert result.payload["opportunities"] == 2


@pytest.mark.asyncio
async def test_repair_budget_exhaustion_preserves_completed_second_round_checkpoint() -> None:
    run_id = uuid4()
    task_id = uuid4()
    store = FullStore(context(run_id, task_id), [])
    store.remaining_calls = 5
    reasoner = RepairExhaustingReasoner(recommend_more=True)
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={Source.HACKER_NEWS: FullCollector(), Source.REDDIT: FullCollector()},
        competitor_search=FullSearch(),
        safe_fetch=FullFetcher(),
        clock=lambda: NOW,
    )

    with pytest.raises(TaskHandlerError) as failure:
        await pipeline(research_task(run_id, task_id))

    assert failure.value.kind is ErrorKind.BUDGET_EXHAUSTED
    assert "COLLECT_R2" in store.completed
    assert "EXTRACT_R2" in store.completed
    assert "CLUSTER_R2" not in store.completed
    assert "FINAL" not in store.completed


@pytest.mark.asyncio
async def test_zero_search_budget_skips_external_calls_and_returns_zero_opportunities() -> None:
    run_id = uuid4()
    task_id = uuid4()
    pipeline_context = context(run_id, task_id)
    pipeline_context.budget_limits["max_search_calls_per_run"] = 0
    store = FullStore(pipeline_context, [])
    reasoner = FullReasoner()
    search = FullSearch()
    fetcher = FullFetcher()
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={Source.HACKER_NEWS: FullCollector()},
        competitor_search=search,
        safe_fetch=fetcher,
        clock=lambda: NOW,
    )
    task = ResearchTask(
        id=task_id,
        run_id=run_id,
        task_type="research.run",
        status="LEASED",
        priority=1,
        idempotency_key="run-root:v1",
        payload={"run_id": str(run_id)},
        checkpoint={},
        attempt_count=1,
        max_attempts=3,
        available_at=NOW,
        lease_owner="test-worker",
        lease_expires_at=datetime(2026, 8, 9, 12, 5, tzinfo=UTC),
    )

    result = await pipeline(task)

    assert result.payload["opportunities"] == 0
    assert result.payload["warnings"] == ["COMPETITOR_RESEARCH_UNAVAILABLE"]
    assert search.calls == []
    assert fetcher.calls == []
    assert SemanticOperation.GAP not in reasoner.operations


@pytest.mark.asyncio
async def test_canonical_artifact_ids_are_stable_across_runs_and_provider_order() -> None:
    stage_ids: list[dict[str, set[str]]] = []
    canonical_payloads: list[object] = []
    hypothesis_claim_orders: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
    for reverse_outputs in (False, True):
        run_id = uuid4()
        task_id = uuid4()
        store = FullStore(context(run_id, task_id), [])
        pipeline = EvidencePipeline(
            store=store,
            reasoner=FullReasoner(
                reverse_outputs=reverse_outputs,
                alias_suffix="" if not reverse_outputs else "-renamed",
            ),
            collectors={Source.HACKER_NEWS: FullCollector()},
            competitor_search=FullSearch(),
            safe_fetch=FullFetcher(),
            clock=lambda: NOW,
        )
        task = ResearchTask(
            id=task_id,
            run_id=run_id,
            task_type="research.run",
            status="LEASED",
            priority=1,
            idempotency_key="run-root:v1",
            payload={"run_id": str(run_id)},
            checkpoint={},
            attempt_count=1,
            max_attempts=3,
            available_at=NOW,
            lease_owner="test-worker",
            lease_expires_at=datetime(2026, 8, 9, 12, 5, tzinfo=UTC),
        )
        await pipeline(task)
        canonical_payloads.append(store.completed["GAP"])
        normalized_hypothesis = store.completed["HYPOTHESIS"]["hypotheses"][0]
        hypothesis_claim_orders.append(
            (
                tuple(normalized_hypothesis["supporting_claim_ids"]),
                tuple(normalized_hypothesis["contradicting_claim_ids"]),
            )
        )
        stage_ids.append(
            {
                "pains": {item["id"] for item in store.completed["EXTRACT"]["pain_signals"]},
                "problems": {item["id"] for item in store.completed["CLUSTER"]["problems"]},
                "clusters": {item["id"] for item in store.completed["CLUSTER"]["clusters"]},
                "claims": {item["id"] for item in store.completed["GAP"]["claims"]},
                "competitors": {item["id"] for item in store.completed["GAP"]["competitors"]},
                "gaps": {item["id"] for item in store.completed["GAP"]["gaps"]},
                "opportunities": {item["id"] for item in store.completed["GAP"]["opportunities"]},
            }
        )

    assert stage_ids[0] == stage_ids[1]
    assert canonical_payloads[0] == canonical_payloads[1]
    assert hypothesis_claim_orders[0] == hypothesis_claim_orders[1]
    cards = store.completed["CARD_SCORE"]["cards"]
    assert cards != []
    assert cards[0]["contradicting_claim_ids"]


def test_large_evidence_batch_is_deterministically_bounded_with_omission_count() -> None:
    evidence = tuple(
        CapturedEvidence(
            evidence_id=f"HACKER_NEWS:large-{index}:r1",
            source=Source.HACKER_NEWS,
            url=f"https://example.test/large/{index}",
            text="Repeated bounded pain text " * 500,
            observed_at=NOW,
            duplicate_group=f"{index:064x}",
            author_id=f"author-{index}",
            thread_id=f"thread-{index}",
        )
        for index in range(300)
    )

    selected = _bounded_evidence_input(evidence)
    payload = _bounded_stage_input(
        (("evidence", selected),),
        total_counts={"evidence": len(evidence)},
    )

    assert 0 < len(payload["evidence"]) < len(evidence)
    assert payload["input_bounds"]["omitted_counts"]["evidence"] == (
        len(evidence) - len(payload["evidence"])
    )
    assert len(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()) < 20_000


def test_bounded_stage_input_preserves_explicit_new_artifact_priority() -> None:
    values = [{"id": f"pain-a-{index:03d}", "pain": "old pain " * 180} for index in range(30)]
    values.append({"id": "pain-z-new", "pain": "new targeted pain " * 180})

    payload = _bounded_stage_input(
        (("pain_signals", values),),
        total_counts={"pain_signals": len(values)},
        priority_ids={"pain_signals": frozenset({"pain-z-new"})},
    )

    selected_ids = [item["id"] for item in payload["pain_signals"]]
    assert selected_ids[0] == "pain-z-new"
    assert len(selected_ids) < len(values)


@pytest.mark.asyncio
async def test_query_plan_bounds_oversized_mission_and_existing_candidates() -> None:
    run_id = uuid4()
    task_id = uuid4()
    pipeline_context = context(run_id, task_id)
    pipeline_context = replace(
        pipeline_context,
        mission_revision=pipeline_context.mission_revision.model_copy(
            update={"prompt": "บัญชีซ้ำซ้อน " * 4_000}
        ),
    )
    store = RecordingStore(pipeline_context, [])

    async def query_existing(_: PipelineContext) -> ExistingIntelligence:
        return ExistingIntelligence(
            candidates=tuple(
                ExistingCandidate(
                    kind="EVIDENCE",
                    identifier=f"evidence-{index:04d}",
                    summary=("large existing summary " * 200) + str(index),
                )
                for index in range(200)
            )
        )

    store.query_existing = query_existing  # type: ignore[assignment,method-assign]
    reasoner = RecordingReasoner([])
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={Source.HACKER_NEWS: EmptyCollector([])},
        clock=lambda: NOW,
    )

    result = await pipeline(research_task(run_id, task_id))

    assert result.payload["completed_stage"] == "COLLECT"
    input_json = reasoner.calls[0][1].request.input_json
    assert len(json.dumps(input_json, ensure_ascii=False, separators=(",", ":")).encode()) < 20_000
    omitted = input_json["input_bounds"]["omitted_counts"]
    assert omitted["mission_characters"] > 0
    assert omitted["existing_candidates"] > 0
    assert [warning.code for warning in result.warnings] == ["SEMANTIC_INPUT_OMITTED"]
    assert result.warnings[0].details == {
        "omitted_counts": {
            "QUERY_PLAN.existing_candidates": omitted["existing_candidates"],
            "QUERY_PLAN.mission_characters": omitted["mission_characters"],
        },
        "total_omitted": sum(omitted.values()),
    }
    checkpoint_bounds = store.completed["QUERY_PLAN"]["_input_bounds"]
    assert checkpoint_bounds == {"omitted_counts": omitted}


@pytest.mark.asyncio
async def test_cluster_bounds_oversized_extraction_and_uses_selected_lineage() -> None:
    run_id = uuid4()
    task_id = uuid4()
    pipeline_context = context(run_id, task_id)
    store = RecordingStore(pipeline_context, [])
    reasoner = FullReasoner()
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={},
        clock=lambda: NOW,
    )
    extraction = PainExtractionBatch.model_validate(
        {
            "pain_signals": [
                {
                    "id": f"pain-{index:04d}",
                    "raw_signal_revision_id": f"evidence-{index:04d}",
                    "pain": ("manual reconciliation takes hours " * 10) + str(index),
                    "severity": 0.8,
                    "frequency": 0.7,
                    "confidence": 0.9,
                    "excerpt": "manual reconciliation takes hours",
                }
                for index in range(200)
            ]
        }
    )

    await pipeline._cluster(pipeline_context, extraction, PipelineExecution())

    assert reasoner.input_sizes[0] < 20_000
    cluster_input = store.completed["CLUSTER"]
    omitted = cluster_input["_input_bounds"]["omitted_counts"]
    assert omitted["pain_signals"] > 0
    selected_values = reasoner.inputs[0]["pain_signals"]
    assert isinstance(selected_values, list)
    selected_value = selected_values[0]
    assert isinstance(selected_value, dict)
    selected_id = selected_value["id"]
    memberships = cluster_input["memberships"]
    assert isinstance(memberships, list)
    membership = memberships[0]
    assert isinstance(membership, dict)
    assert membership["pain_signal_id"] == selected_id


@pytest.mark.asyncio
async def test_round_two_cluster_keeps_new_pain_that_sorts_after_old_pains() -> None:
    run_id = uuid4()
    task_id = uuid4()
    pipeline_context = context(run_id, task_id)
    store = RecordingStore(pipeline_context, [])
    reasoner = FullReasoner()
    pipeline = EvidencePipeline(
        store=store,
        reasoner=reasoner,
        collectors={},
        clock=lambda: NOW,
    )
    values = [
        {
            "id": f"pain-a-{index:03d}",
            "raw_signal_revision_id": f"evidence-a-{index:03d}",
            "pain": ("old pain " * 45) + str(index),
            "severity": 0.8,
            "frequency": 0.7,
            "confidence": 0.9,
            "excerpt": "old pain",
        }
        for index in range(30)
    ]
    values.append(
        {
            "id": "pain-z-new",
            "raw_signal_revision_id": "evidence-z-new",
            "pain": "new targeted pain " * 25,
            "severity": 0.8,
            "frequency": 0.7,
            "confidence": 0.9,
            "excerpt": "new targeted pain",
        }
    )
    extraction = PainExtractionBatch.model_validate({"pain_signals": values})

    await pipeline._cluster(
        pipeline_context,
        extraction,
        PipelineExecution(),
        stage="CLUSTER_R2",
        round_number=2,
        priority_pain_ids=frozenset({"pain-z-new"}),
    )

    assert reasoner.inputs[0]["pain_signals"][0]["id"] == "pain-z-new"
    assert store.completed["CLUSTER_R2"]["memberships"][0]["pain_signal_id"] == "pain-z-new"


def test_pipeline_trend_volume_is_windowed_and_duplicate_groups_block_viral_growth() -> None:
    items = [
        {
            "source": "HACKER_NEWS",
            "external_id": f"current-{index}",
            "canonical_url": f"https://example.test/current/{index}",
            "title": "Current pain",
            "source_created_at": NOW - timedelta(days=1),
        }
        for index in range(20)
    ] + [
        {
            "source": "HACKER_NEWS",
            "external_id": f"previous-{index}",
            "canonical_url": f"https://example.test/previous/{index}",
            "title": "Previous pain",
            "source_created_at": NOW - timedelta(days=14),
        }
        for index in range(100)
    ]
    results = (
        CollectResult.model_validate(
            {
                "source": "HACKER_NEWS",
                "availability": "AVAILABLE",
                "items": items,
                "request_count": 1,
            }
        ),
    )
    volumes = _examined_volume(results, as_of=NOW)
    copies = tuple(
        TrendObservation(
            signal_id=f"copy-{index}",
            author_id=f"author-{index}",
            thread_id=f"thread-{index}",
            observed_at=NOW - timedelta(days=1),
            duplicate_group="same-cross-post",
        )
        for index in range(10)
    )

    trend = classify_trend(
        copies,
        as_of=NOW,
        current_examined_volume=volumes["current_7d"],
        previous_examined_volume=volumes["previous_28d"],
    )

    assert volumes == {"current_7d": 20, "previous_28d": 100}
    assert trend.current_signals == 1
    assert trend.label.value == "INSUFFICIENT_DATA"


@pytest.mark.asyncio
async def test_unknown_cluster_membership_is_rejected_instead_of_dropped() -> None:
    run_id = uuid4()
    task_id = uuid4()
    pipeline = EvidencePipeline(
        store=FullStore(context(run_id, task_id), []),
        reasoner=InvalidClusterReasoner(),
        collectors={Source.HACKER_NEWS: FullCollector()},
        competitor_search=FullSearch(),
        safe_fetch=FullFetcher(),
        clock=lambda: NOW,
    )

    with pytest.raises(ValueError, match="unknown cluster"):
        await pipeline(research_task(run_id, task_id))


@pytest.mark.asyncio
async def test_search_cannot_report_more_requests_than_admitted() -> None:
    run_id = uuid4()
    task_id = uuid4()
    store = FullStore(context(run_id, task_id), [])
    search = OverBudgetSearch()
    fetcher = FullFetcher()
    pipeline = EvidencePipeline(
        store=store,
        reasoner=FullReasoner(),
        collectors={Source.HACKER_NEWS: FullCollector()},
        competitor_search=search,
        safe_fetch=fetcher,
        clock=lambda: NOW,
    )

    with pytest.raises(ValueError, match="exceeded its admitted request budget"):
        await pipeline(research_task(run_id, task_id))

    assert len(search.calls) == 1
    assert fetcher.calls == []
    assert "COMPETITOR_RESEARCH" not in store.completed


@pytest.mark.asyncio
async def test_competitor_evidence_must_match_the_captured_fetch() -> None:
    run_id = uuid4()
    task_id = uuid4()
    pipeline = EvidencePipeline(
        store=FullStore(context(run_id, task_id), []),
        reasoner=FullReasoner(),
        collectors={Source.HACKER_NEWS: FullCollector()},
        competitor_search=FullSearch(),
        safe_fetch=MismatchedFetcher(),
        clock=lambda: NOW,
    )

    with pytest.raises(ValueError, match="does not match captured fetch"):
        await pipeline(research_task(run_id, task_id))


@pytest.mark.postgres
async def test_collection_commit_persists_lineage_checkpoint_and_budget_atomically(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    started_at = NOW
    try:
        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            _, revision = await uow.missions.create_with_revision(
                title="Pipeline persistence",
                mission_text="Invoice reconciliation takes hours spreadsheet",
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
                now=started_at,
            )
            scheduled.run.status = "RUNNING"
            scheduled.run.started_at = started_at
            scheduled.run.deadline_at = datetime(2026, 8, 9, 12, 30, tzinfo=UTC)
            scheduled.task.status = "LEASED"
            scheduled.task.attempt_count = 1
            scheduled.task.lease_owner = "test-worker"
            scheduled.task.lease_expires_at = datetime(2026, 8, 9, 12, 5, tzinfo=UTC)
            await uow.commit()

        artifact_writer = RecordingArtifactWriter()
        store = SqlAlchemyEvidencePipelineStore(
            database.session_factory,
            author_hmac_secret=b"a" * 32,
            clock=lambda: NOW,
            artifact_writer=artifact_writer,  # type: ignore[arg-type]
        )
        pipeline_context = await store.load_context(scheduled.task)
        item = {
            "source": "HACKER_NEWS",
            "external_id": "hn-42",
            "canonical_url": "https://example.test/posts/42?utm_source=test",
            "parent_thread_id": "thread-42",
            "author_identity": "alice",
            "title": "Invoice reconciliation takes hours",
            "body": "We copy every line into a spreadsheet each week.",
            "source_created_at": NOW.isoformat(),
        }
        payload = {
            "results": [
                {
                    "source": "HACKER_NEWS",
                    "availability": "AVAILABLE",
                    "items": [item],
                    "request_count": 1,
                },
                {
                    "source": "GITHUB",
                    "availability": "SOURCE_UNAVAILABLE",
                    "request_count": 1,
                },
            ],
            "item_count": 1,
        }

        commit = PipelineStageCommit(
            stage="COLLECT",
            idempotency_key=str(uuid5(scheduled.run.id, "pipeline:COLLECT:v1")),
            payload=payload,
        )
        await asyncio.gather(
            store.commit_stage(pipeline_context, commit),
            store.commit_stage(pipeline_context, commit),
        )
        competitor_commit = PipelineStageCommit(
            stage="COMPETITOR_RESEARCH",
            idempotency_key=str(uuid5(scheduled.run.id, "pipeline:COMPETITOR_RESEARCH:v1")),
            payload={
                "status": "RESEARCH_UNAVAILABLE",
                "searches": [
                    {
                        "availability": "AVAILABLE",
                        "query": "invoice reconciliation",
                        "results": [],
                        "request_count": 1,
                    }
                ],
                "snapshots": [],
                "warning_codes": ["NO_RESULTS"],
            },
        )
        await store.commit_stage(pipeline_context, competitor_commit)
        await store.commit_stage(pipeline_context, competitor_commit)
        with pytest.raises(StageConflictError, match="input changed"):
            await store.commit_stage(
                pipeline_context,
                PipelineStageCommit(
                    stage="COLLECT",
                    idempotency_key=commit.idempotency_key,
                    payload={**payload, "item_count": 2},
                ),
            )

        existing = await store.query_existing(pipeline_context)
        assert any(
            candidate.identifier == "HACKER_NEWS:hn-42:r1" for candidate in existing.candidates
        )

        async with database.session() as session:
            first_run = await session.get(ResearchRun, scheduled.run.id)
            first_task = await session.get(ResearchTask, scheduled.task.id)
            assert first_run is not None
            assert first_task is not None
            first_run.status = "COMPLETED"
            first_run.completed_at = NOW
            first_task.status = "SUCCEEDED"
            first_task.completed_at = NOW
            first_task.lease_owner = None
            first_task.lease_expires_at = None
            await session.commit()

        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            _, second_revision = await uow.missions.create_with_revision(
                title="Cross-run deduplication",
                mission_text="Find recurring accounting workflow pain",
                original_language="en",
                output_locale="en",
            )
            assert uow.session is not None
            second = await RunScheduler(uow.session).schedule(
                request=RunScheduleRequest(
                    mission_revision_id=second_revision.id,
                    mode="HUNT",
                    priority=0,
                    budget_limits={"max_run_duration_minutes": 30},
                ),
                now=started_at,
            )
            second.run.status = "RUNNING"
            second.run.started_at = started_at
            second.run.deadline_at = datetime(2026, 8, 9, 12, 30, tzinfo=UTC)
            second.task.status = "LEASED"
            second.task.attempt_count = 1
            second.task.lease_owner = "test-worker"
            second.task.lease_expires_at = datetime(2026, 8, 9, 12, 5, tzinfo=UTC)
            await uow.commit()
        second_context = await store.load_context(second.task)
        await store.commit_stage(
            second_context,
            PipelineStageCommit(
                stage="COLLECT",
                idempotency_key=str(uuid5(second.run.id, "pipeline:COLLECT:v1")),
                payload={
                    "results": [
                        {
                            "source": "GITHUB",
                            "availability": "AVAILABLE",
                            "items": [
                                {
                                    **item,
                                    "source": "GITHUB",
                                    "external_id": "gh-99",
                                    "canonical_url": "https://different.example.test/issues/99",
                                }
                            ],
                            "request_count": 1,
                        }
                    ],
                    "item_count": 1,
                },
            ),
        )

        async with database.session() as session:
            second_run = await session.get(ResearchRun, second.run.id)
            second_task = await session.get(ResearchTask, second.task.id)
            assert second_run is not None
            assert second_task is not None
            second_run.status = "COMPLETED"
            second_run.completed_at = NOW
            second_task.status = "SUCCEEDED"
            second_task.completed_at = NOW
            second_task.lease_owner = None
            second_task.lease_expires_at = None
            await session.commit()

        async with database.session() as session:
            persisted_run = await session.get(ResearchRun, scheduled.run.id)
            persisted_task = await session.get(ResearchTask, scheduled.task.id)
            revisions = (
                await session.scalars(
                    select(RawSignalRevision).where(
                        RawSignalRevision.domain_revision_id.in_(
                            ("HACKER_NEWS:hn-42:r1", "GITHUB:gh-99:r1")
                        )
                    )
                )
            ).all()
        assert persisted_run is not None
        assert persisted_task is not None
        assert persisted_run.budget_used == {
            "collector_requests": 2,
            "raw_signals": 1,
            "search_calls": 1,
        }
        checkpoint = persisted_task.checkpoint["pipeline"]["stages"]["COLLECT"]
        assert checkpoint["idempotency_key"] == str(uuid5(scheduled.run.id, "pipeline:COLLECT:v1"))
        assert len(checkpoint["input_sha256"]) == 64
        assert {row.domain_revision_id for row in revisions} == {
            "HACKER_NEWS:hn-42:r1",
            "GITHUB:gh-99:r1",
        }
        assert len({row.duplicate_group_key for row in revisions}) == 1
        assert artifact_writer.stages == [
            "COLLECT",
            "COMPETITOR_RESEARCH",
            "COLLECT",
        ]
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_collection_budget_refusal_is_atomic_and_terminal(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    try:
        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            _, revision = await uow.missions.create_with_revision(
                title="Atomic collection budget",
                mission_text="Bound collection",
                original_language="en",
                output_locale="en",
            )
            assert uow.session is not None
            scheduled = await RunScheduler(uow.session).schedule(
                request=RunScheduleRequest(
                    mission_revision_id=revision.id,
                    mode="HUNT",
                    priority=0,
                    budget_limits={
                        "max_run_duration_minutes": 30,
                        "max_raw_signals_per_run": 1,
                    },
                ),
                now=NOW,
            )
            scheduled.run.status = "RUNNING"
            scheduled.run.started_at = NOW
            scheduled.run.deadline_at = datetime(2026, 8, 9, 12, 30, tzinfo=UTC)
            scheduled.task.status = "LEASED"
            scheduled.task.attempt_count = 1
            scheduled.task.lease_owner = "test-worker"
            scheduled.task.lease_expires_at = datetime(2026, 8, 9, 12, 5, tzinfo=UTC)
            await uow.commit()
        store = SqlAlchemyEvidencePipelineStore(
            database.session_factory,
            author_hmac_secret=None,
            clock=lambda: NOW,
        )
        pipeline_context = await store.load_context(scheduled.task)
        items = [
            {
                "source": "HACKER_NEWS",
                "external_id": f"budget-{index}",
                "canonical_url": f"https://example.test/budget/{index}",
                "title": f"Pain {index}",
                "source_created_at": NOW.isoformat(),
            }
            for index in range(2)
        ]
        with pytest.raises(CollectionBudgetExhaustedError):
            await store.commit_stage(
                pipeline_context,
                PipelineStageCommit(
                    stage="COLLECT",
                    idempotency_key=str(uuid5(scheduled.run.id, "pipeline:COLLECT:v1")),
                    payload={
                        "results": [
                            {
                                "source": "HACKER_NEWS",
                                "availability": "AVAILABLE",
                                "items": items,
                                "request_count": 1,
                            }
                        ],
                        "item_count": 2,
                    },
                ),
            )
        async with database.session() as session:
            run = await session.get(ResearchRun, scheduled.run.id)
            task = await session.get(ResearchTask, scheduled.task.id)
        assert run is not None
        assert task is not None
        assert run.status == "RUNNING"
        assert run.budget_used == {}
        assert "COLLECT" not in task.checkpoint.get("pipeline", {}).get("stages", {})

        old_context = pipeline_context
        async with database.session() as session:
            current_task = await session.get(ResearchTask, scheduled.task.id)
            assert current_task is not None
            current_task.attempt_count = 2
            current_task.lease_owner = "replacement-worker"
            current_task.lease_expires_at = datetime(2026, 8, 9, 12, 10, tzinfo=UTC)
            await session.commit()
        with pytest.raises(PermissionError, match="lease was lost"):
            await store.commit_stage(
                old_context,
                PipelineStageCommit(
                    stage="STALE",
                    idempotency_key=str(uuid5(scheduled.run.id, "pipeline:STALE:v1")),
                    payload={"attempt": 1},
                ),
            )
        async with database.session() as session:
            run = await session.get(ResearchRun, scheduled.run.id)
            task = await session.get(ResearchTask, scheduled.task.id)
            assert run is not None
            assert task is not None
            run.status = "CANCELLED"
            run.completed_at = NOW
            task.status = "CANCELLED"
            task.completed_at = NOW
            task.lease_owner = None
            task.lease_expires_at = None
            await session.commit()
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_coherent_second_round_uses_exact_durable_agent_call_budget(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    now = datetime.now(UTC)
    try:
        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            _, revision = await uow.missions.create_with_revision(
                title="Audited coherent second round",
                mission_text="Find recurring invoice reconciliation pain",
                original_language="en",
                output_locale="en",
            )
            assert uow.session is not None
            scheduled = await RunScheduler(uow.session).schedule(
                request=RunScheduleRequest(
                    mission_revision_id=revision.id,
                    mode="HUNT",
                    priority=0,
                    budget_limits={
                        "max_run_duration_minutes": 30,
                        "max_research_rounds": 2,
                        "max_agent_calls_per_run": 11,
                        "max_search_calls_per_run": 1,
                        "max_collector_requests_per_run": 20,
                        "max_raw_signals_per_run": 20,
                    },
                ),
                now=now,
            )
            scheduled.run.status = "RUNNING"
            scheduled.run.started_at = now
            scheduled.run.deadline_at = now + timedelta(minutes=30)
            scheduled.task.status = "LEASED"
            scheduled.task.attempt_count = 1
            scheduled.task.lease_owner = "test-worker"
            scheduled.task.lease_expires_at = now + timedelta(minutes=5)
            await uow.commit()

        provider = FullProvider()
        reasoner = AuditedSemanticReasoner(
            database.session_factory,
            provider,
            lease_owner="test-worker",
            provider_name="fake",
        )
        store = SqlAlchemyEvidencePipelineStore(
            database.session_factory,
            author_hmac_secret=b"a" * 32,
            clock=lambda: now,
        )
        pipeline = EvidencePipeline(
            store=store,
            reasoner=reasoner,
            collectors={Source.HACKER_NEWS: FullCollector(), Source.REDDIT: FullCollector()},
            competitor_search=FullSearch(),
            safe_fetch=FullFetcher(),
            clock=lambda: now,
        )

        result = await pipeline(scheduled.task)

        async with database.session() as session:
            run = await session.get(ResearchRun, scheduled.run.id)
            task = await session.get(ResearchTask, scheduled.task.id)
            call_count = await session.scalar(
                select(func.count())
                .select_from(AgentCall)
                .where(AgentCall.run_id == scheduled.run.id)
            )
            assert run is not None
            assert task is not None
            run.status = "COMPLETED"
            run.completed_at = now
            task.status = "SUCCEEDED"
            task.completed_at = now
            task.lease_owner = None
            task.lease_expires_at = None
            await session.commit()
        assert result.payload["completed_stage"] == "FINAL"
        assert result.payload["opportunities"] == 1
        assert run.budget_used["agent_calls"] == 11
        assert call_count == 11
        assert provider.reasoner.operations[-5:] == [
            SemanticOperation.EXTRACT,
            SemanticOperation.CLUSTER,
            SemanticOperation.GAP,
            SemanticOperation.HYPOTHESIS,
            SemanticOperation.CRITIC,
        ]
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_worker_owns_budget_terminal_transition(migrated_postgres_url: str) -> None:
    database = Database.from_url(migrated_postgres_url)
    try:
        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            _, revision = await uow.missions.create_with_revision(
                title="Worker budget ownership",
                mission_text="Bound worker collection",
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
            )
            await uow.commit()
        pipeline_context = context(scheduled.run.id, scheduled.task.id)
        store = BudgetStore(pipeline_context, [])
        pipeline = EvidencePipeline(
            store=store,
            reasoner=RecordingReasoner([]),
            collectors={Source.HACKER_NEWS: FullCollector()},
            clock=lambda: NOW,
        )
        worker = Worker(
            database,
            worker_id="test-worker",
            handlers=TaskHandlerRegistry({"research.run": pipeline}),
        )

        assert await worker.run_once() is True

        async with database.session() as session:
            run = await session.get(ResearchRun, scheduled.run.id)
            task = await session.get(ResearchTask, scheduled.task.id)
        assert run is not None
        assert task is not None
        assert task.status == "FAILED"
        assert task.retry_class == "BUDGET_EXHAUSTED"
        assert run.status == "BUDGET_EXHAUSTED"
    finally:
        await database.dispose()
