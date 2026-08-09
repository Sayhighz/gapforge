from __future__ import annotations

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
from gapforge.integration.evidence_store import SqlAlchemyEvidencePipelineStore
from gapforge.integration.semantic import SemanticCall, SemanticContext
from gapforge.runtime import RunScheduler, RunScheduleRequest
from gapforge.runtime.evidence_pipeline import (
    EvidencePipeline,
    ExistingIntelligence,
    PipelineContext,
    PipelineStageCommit,
)
from gapforge.storage.database import Database
from gapforge.storage.models import RawSignalRevision, ResearchRun, ResearchTask
from gapforge.storage.uow import SqlAlchemyUnitOfWork

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
        return ExistingIntelligence(opportunity_count=0, evidence_count=0)

    async def load_stage(self, context: PipelineContext, stage: str) -> dict[str, object] | None:
        return self.completed.get(stage)

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


def context(run_id: UUID, task_id: UUID) -> PipelineContext:
    revision_id = uuid4()
    return PipelineContext(
        run_id=run_id,
        task_id=task_id,
        task_attempt=1,
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
                mission_text="Find recurring accounting workflow pain",
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

        await store.commit_stage(
            pipeline_context,
            PipelineStageCommit(
                stage="COLLECT",
                idempotency_key=str(uuid5(scheduled.run.id, "pipeline:COLLECT:v1")),
                payload=payload,
            ),
        )

        async with database.session() as session:
            persisted_run = await session.get(ResearchRun, scheduled.run.id)
            persisted_task = await session.get(ResearchTask, scheduled.task.id)
            revisions = (await session.scalars(select(RawSignalRevision))).all()
        assert persisted_run is not None
        assert persisted_task is not None
        assert persisted_run.budget_used == {"collector_requests": 2, "raw_signals": 1}
        assert persisted_task.checkpoint["pipeline"]["stages"]["COLLECT"]["idempotency_key"] == str(
            uuid5(scheduled.run.id, "pipeline:COLLECT:v1")
        )
        assert len(revisions) == 1
        assert revisions[0].domain_revision_id == "HACKER_NEWS:hn-42:r1"
        assert len(revisions[0].duplicate_group_key) == 64
    finally:
        await database.dispose()
