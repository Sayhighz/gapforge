from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4, uuid5

import pytest
from sqlalchemy import select

from gapforge.domain.contracts import (
    AgentEffort,
    AgentResult,
    AgentStatus,
    Availability,
    CollectRequest,
    CollectResult,
    MissionRevision,
    RunMode,
    SemanticOperation,
    Source,
)
from gapforge.integration.evidence_store import (
    SqlAlchemyEvidencePipelineStore,
    StageConflictError,
)
from gapforge.integration.semantic import SemanticCall, SemanticContext
from gapforge.runtime import RunScheduler, RunScheduleRequest
from gapforge.runtime.evidence_pipeline import (
    CapturedEvidence,
    CollectionBudgetExhaustedError,
    EvidencePipeline,
    ExistingIntelligence,
    PipelineContext,
    PipelineStageCommit,
)
from gapforge.storage.database import Database
from gapforge.storage.models import RawSignalRevision, ResearchRun, ResearchTask
from gapforge.storage.uow import SqlAlchemyUnitOfWork
from gapforge.worker import TaskHandlerRegistry, Worker

NOW = datetime(2026, 8, 9, 12, tzinfo=UTC)


class RecordingStore:
    def __init__(self, context: PipelineContext, events: list[str]) -> None:
        self.context = context
        self.events = events
        self.commits: list[PipelineStageCommit] = []
        self.completed: dict[str, dict[str, object]] = {}
        self.crash_after_stage: str | None = None

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
    async def collect(self, request: CollectRequest) -> CollectResult:
        return CollectResult.model_validate(
            {
                "source": "HACKER_NEWS",
                "availability": "AVAILABLE",
                "items": [
                    {
                        "source": "HACKER_NEWS",
                        "external_id": "full-1",
                        "canonical_url": "https://example.test/full-1",
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


class FullStore(RecordingStore):
    async def commit_stage(
        self,
        context: PipelineContext,
        commit: PipelineStageCommit,
    ) -> None:
        self.events.append(f"commit:{commit.stage}")
        self.commits.append(commit)
        if commit.stage == "COLLECT":
            self.completed[commit.stage] = {
                **commit.payload,
                "evidence_ids": ["HACKER_NEWS:full-1:r1"],
            }
        else:
            self.completed[commit.stage] = commit.payload

    async def load_evidence(
        self,
        context: PipelineContext,
        evidence_ids: tuple[str, ...],
    ) -> tuple[CapturedEvidence, ...]:
        assert evidence_ids == ("HACKER_NEWS:full-1:r1",)
        return (
            CapturedEvidence(
                evidence_id=evidence_ids[0],
                source=Source.HACKER_NEWS,
                url="https://example.test/full-1",
                text=(
                    "Invoice reconciliation takes hours\n"
                    "We copy every line into a spreadsheet each week."
                ),
                observed_at=NOW,
                duplicate_group="a" * 64,
                author_id="author-pseudonym-1",
                thread_id="thread-1",
            ),
        )


class FullReasoner:
    def __init__(self) -> None:
        self.operations: list[SemanticOperation] = []

    async def run(self, context: SemanticContext, call: SemanticCall) -> AgentResult:
        operation = call.request.task
        self.operations.append(operation)
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
                    }
                ],
                "competitors": [
                    {"id": "manual-1", "name": "Manual work", "kind": "MANUAL_WORK"},
                    {"id": "nothing-1", "name": "Do nothing", "kind": "DO_NOTHING"},
                ],
                "competitor_evidence": [
                    {
                        "id": "competitor-evidence-1",
                        "competitor_id": "manual-1",
                        "source_url": "https://example.test/full-1",
                        "captured_excerpt": "copy every line into a spreadsheet",
                        "observed_at": NOW.isoformat(),
                        "content_hash": "b" * 64,
                        "evidence_kind": "WORKAROUND",
                        "claim_ids": ["claim-1"],
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
                "score_inputs": [
                    {
                        "opportunity_id": "opportunity-1",
                        **{
                            name: 50
                            for name in (
                                "severity",
                                "frequency",
                                "independent_diversity",
                                "behavioral_workaround",
                                "wtp_or_spend",
                                "recency_trend",
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
                "competitor_research_status": "COMPLETE",
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
                ]
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
        return AgentResult(
            call_id=call.request.call_id,
            status=AgentStatus.COMPLETED,
            output_json=outputs[operation],
            provider="fake",
            effort=call.request.effort,
            duration_ms=1,
        )


class BudgetStore(RecordingStore):
    async def commit_stage(self, context: PipelineContext, commit: PipelineStageCommit) -> None:
        if commit.stage == "COLLECT":
            raise CollectionBudgetExhaustedError("test budget")
        await super().commit_stage(context, commit)


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
            "max_collector_requests_per_run": 60,
            "max_raw_signals_per_run": 300,
            "initial_lookback_days": 365,
        },
        collection_until=NOW,
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
    assert [commit.stage for commit in store.commits] == [
        "EXISTING",
        "QUERY_PLAN",
        "COLLECT",
        "EXTRACT",
        "CLUSTER",
        "GAP",
        "HYPOTHESIS",
        "CRITIC",
        "FINAL",
    ]
    decisions = store.completed["FINAL"]["decisions"]
    assert isinstance(decisions, list)
    first_decision = decisions[0]
    assert isinstance(first_decision, dict)
    assert first_decision["verdict"] == "RESEARCH_MORE"


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

        store = SqlAlchemyEvidencePipelineStore(
            database.session_factory,
            author_hmac_secret=b"a" * 32,
            clock=lambda: NOW,
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
        assert persisted_run.budget_used == {"collector_requests": 2, "raw_signals": 1}
        checkpoint = persisted_task.checkpoint["pipeline"]["stages"]["COLLECT"]
        assert checkpoint["idempotency_key"] == str(uuid5(scheduled.run.id, "pipeline:COLLECT:v1"))
        assert len(checkpoint["input_sha256"]) == 64
        assert {row.domain_revision_id for row in revisions} == {
            "HACKER_NEWS:hn-42:r1",
            "GITHUB:gh-99:r1",
        }
        assert len({row.duplicate_group_key for row in revisions}) == 1
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
