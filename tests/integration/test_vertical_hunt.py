from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

import pytest
from sqlalchemy import func, select

from gapforge.analysis.normalization import normalize_text
from gapforge.config import AgentProviderName, Settings
from gapforge.domain.contracts import (
    Availability,
    CollectRequest,
    CollectResult,
    FetchResult,
    FetchSnapshot,
    SearchResponse,
    Source,
)
from gapforge.integration.semantic import AuditedSemanticReasoner
from gapforge.providers.fake import FakeAgentProvider
from gapforge.runtime import RunScheduler, RunScheduleRequest
from gapforge.runtime import research_handler as research_handler_module
from gapforge.runtime.evidence_pipeline import (
    ARTIFACT_NAMESPACE,
    _competitor_snapshot_evidence_id,
    _stable_id,
)
from gapforge.runtime.research_handler import ResearchRunHandler
from gapforge.storage.database import Database
from gapforge.storage.models import (
    AgentCall,
    AtomicClaim,
    CanonicalProblem,
    CriticResult,
    EvidenceCard,
    FinalAssessmentSnapshot,
    GapHypothesis,
    LifecycleEvent,
    MissionOpportunityAssessment,
    Opportunity,
    OpportunityScoreSnapshot,
    PainSignal,
    ProviderCallLease,
    RawSignalRevision,
    ResearchRun,
    ResearchTask,
)
from gapforge.storage.uow import SqlAlchemyUnitOfWork
from gapforge.worker import TaskHandlerRegistry, Worker

OBSERVED_AT = datetime(2026, 8, 9, 12, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class HuntFixture:
    slug: str
    run_id: UUID
    query_sources: tuple[Source, ...] = (Source.HACKER_NEWS,)
    empty_collection: bool = False
    critic_verdict: str = "RESEARCH_MORE"

    @property
    def external_id(self) -> str:
        return f"vertical-{self.slug}"

    @property
    def raw_revision_id(self) -> str:
        return f"HACKER_NEWS:{self.external_id}:r1"

    @property
    def pain(self) -> str:
        return f"Manual reconciliation {self.slug} takes hours"

    @property
    def problem_summary(self) -> str:
        return f"Manual reconciliation bottleneck {self.slug}"

    @property
    def problem_id(self) -> str:
        return _stable_id("problem", normalize_text(self.problem_summary))

    @property
    def pain_id(self) -> str:
        return str(
            uuid5(
                ARTIFACT_NAMESPACE,
                f"pain:{self.raw_revision_id}:{normalize_text(self.pain)}",
            )
        )

    @property
    def competitor_url(self) -> str:
        return f"https://competitor.test/{self.slug}/manual"

    @property
    def competitor_text(self) -> str:
        return f"Manual reconciliation workaround for {self.slug} uses spreadsheet copying"

    @property
    def snapshot(self) -> FetchSnapshot:
        return FetchSnapshot.model_validate(
            {
                "url": self.competitor_url,
                "final_url": self.competitor_url,
                "text": self.competitor_text,
                "content_type": "text/plain",
                "sha256": "c" * 64,
                "observed_at": OBSERVED_AT,
            }
        )


class FixtureCollector:
    def __init__(self, fixture: HuntFixture, source: Source) -> None:
        self.fixture = fixture
        self.source = source

    async def collect(self, request: CollectRequest) -> CollectResult:
        if self.source is Source.GITHUB:
            return CollectResult(
                source=self.source,
                availability=Availability.SOURCE_UNAVAILABLE,
                request_count=1,
            )
        if self.fixture.empty_collection:
            return CollectResult(
                source=self.source,
                availability=Availability.AVAILABLE,
                request_count=1,
            )
        return CollectResult.model_validate(
            {
                "source": self.source.value,
                "availability": "AVAILABLE",
                "items": [
                    {
                        "source": self.source.value,
                        "external_id": self.fixture.external_id,
                        "canonical_url": (
                            f"https://news.ycombinator.com/item?id={self.fixture.external_id}"
                        ),
                        "parent_thread_id": f"thread-{self.fixture.slug}",
                        "author_identity": f"author-{self.fixture.slug}",
                        "title": self.fixture.pain,
                        "body": "Teams copy every line into a spreadsheet during weekly close.",
                        "source_created_at": OBSERVED_AT,
                    }
                ],
                "request_count": 1,
            }
        )


class FixtureSearch:
    def __init__(self, fixture: HuntFixture) -> None:
        self.fixture = fixture

    async def search(
        self,
        query: str,
        *,
        max_results: int,
        max_requests: int,
    ) -> SearchResponse:
        del max_results, max_requests
        return SearchResponse.model_validate(
            {
                "availability": "AVAILABLE",
                "query": query,
                "results": [
                    {
                        "id": f"search-{self.fixture.slug}",
                        "title": "Manual reconciliation workaround",
                        "url": self.fixture.competitor_url,
                        "snippet": "Spreadsheet copying workaround",
                        "observed_at": OBSERVED_AT,
                        "rank": 1,
                    }
                ],
                "request_count": 1,
            }
        )


class FixtureFetcher:
    def __init__(self, fixture: HuntFixture) -> None:
        self.fixture = fixture

    async def fetch(self, url: str, registry: object) -> FetchResult:
        del registry
        assert url == self.fixture.competitor_url
        return FetchResult(availability=Availability.AVAILABLE, snapshot=self.fixture.snapshot)


def _fake_scripts(fixture: HuntFixture) -> dict[str, list[dict[str, Any]]]:
    if fixture.empty_collection:
        return {"query_plan": [_query_plan(fixture)]}

    competitor_evidence_id = _competitor_snapshot_evidence_id(fixture.snapshot)
    competitor_id = _stable_id(
        "competitor",
        normalize_text(f"Manual work {fixture.slug}"),
        "MANUAL_WORK",
        None,
    )
    user_claim_id = _stable_id(
        "claim",
        normalize_text(f"Users manually reconcile invoice lines for {fixture.slug}"),
        "USER_PAIN",
        "SUPPORTED",
        [fixture.raw_revision_id],
    )
    competitor_claim_id = _stable_id(
        "claim",
        normalize_text(fixture.competitor_text),
        "FEATURE",
        "SUPPORTED",
        [competitor_evidence_id],
    )
    gap_id = _stable_id(
        "gap",
        fixture.problem_id,
        "WORKFLOW",
        normalize_text(f"Existing reconciliation workflow {fixture.slug} is slow"),
        [fixture.raw_revision_id],
        [competitor_evidence_id],
    )
    opportunity_id = _stable_id(
        "opportunity",
        fixture.problem_id,
        normalize_text(f"Reconcile {fixture.slug}"),
    )
    card_id = str(uuid5(fixture.run_id, f"card:{opportunity_id}"))
    fit = {
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
    }
    return {
        "query_plan": [_query_plan(fixture)],
        "extract": [
            {
                "pain_signals": [
                    {
                        "id": "provider-pain",
                        "raw_signal_revision_id": fixture.raw_revision_id,
                        "pain": fixture.pain,
                        "severity": 0.8,
                        "frequency": 0.7,
                        "workaround": "copy every line into a spreadsheet",
                        "payment_signal": False,
                        "confidence": 0.9,
                        "excerpt": fixture.pain,
                    }
                ]
            }
        ],
        "cluster": [
            {
                "problems": [{"id": "provider-problem", "summary": fixture.problem_summary}],
                "clusters": [
                    {
                        "id": "provider-cluster",
                        "canonical_problem_id": "provider-problem",
                        "state": "PROVISIONAL",
                        "last_growth_at": OBSERVED_AT.isoformat(),
                    }
                ],
                "memberships": [
                    {
                        "cluster_id": "provider-cluster",
                        "pain_signal_id": fixture.pain_id,
                        "accepted_at": OBSERVED_AT.isoformat(),
                    }
                ],
            }
        ],
        "gap": [
            {
                "claims": [
                    {
                        "id": user_claim_id,
                        "text": f"Users manually reconcile invoice lines for {fixture.slug}",
                        "kind": "USER_PAIN",
                        "status": "SUPPORTED",
                        "evidence_ids": [fixture.raw_revision_id],
                        "contradicts_claim_ids": [competitor_claim_id],
                    },
                    {
                        "id": competitor_claim_id,
                        "text": fixture.competitor_text,
                        "kind": "FEATURE",
                        "status": "SUPPORTED",
                        "evidence_ids": [competitor_evidence_id],
                        "citations": [
                            {
                                "evidence_id": competitor_evidence_id,
                                "source_url": fixture.competitor_url,
                                "excerpt": fixture.competitor_text,
                                "observed_at": OBSERVED_AT.isoformat(),
                            }
                        ],
                    },
                ],
                "competitors": [
                    {
                        "id": competitor_id,
                        "name": f"Manual work {fixture.slug}",
                        "kind": "MANUAL_WORK",
                    }
                ],
                "competitor_evidence": [
                    {
                        "id": competitor_evidence_id,
                        "competitor_id": competitor_id,
                        "source_url": fixture.competitor_url,
                        "captured_excerpt": fixture.competitor_text,
                        "observed_at": OBSERVED_AT.isoformat(),
                        "content_hash": "c" * 64,
                        "evidence_kind": "WORKAROUND",
                        "claim_ids": [competitor_claim_id],
                    }
                ],
                "gaps": [
                    {
                        "id": gap_id,
                        "canonical_problem_id": fixture.problem_id,
                        "gap_type": "WORKFLOW",
                        "statement": f"Existing reconciliation workflow {fixture.slug} is slow",
                        "user_evidence_ids": [fixture.raw_revision_id],
                        "competitor_evidence_ids": [competitor_evidence_id],
                    }
                ],
                "opportunities": [
                    {
                        "id": opportunity_id,
                        "gap_hypothesis_id": gap_id,
                        "title": f"Reconcile {fixture.slug}",
                    }
                ],
                "opportunity_fit": [{"opportunity_id": opportunity_id, **fit}],
            }
        ],
        "hypothesis": [
            {
                "hypotheses": [
                    {
                        "id": "provider-hypothesis",
                        "canonical_problem_id": fixture.problem_id,
                        "icp": "Small accounting teams",
                        "job_to_be_done": "Reconcile invoices",
                        "trigger": "Weekly close",
                        "current_behavior": "Copy spreadsheet lines",
                        "pain": "Takes hours",
                        "workflow_failure": "Manual matching",
                        "falsification_test": "Fails if fewer than five teams report this",
                        "supporting_claim_ids": [competitor_claim_id],
                    }
                ],
                "hypothesis_card_links": [
                    {
                        "hypothesis_id": "provider-hypothesis",
                        "opportunity_id": opportunity_id,
                        "evidence_card_id": card_id,
                    }
                ],
            }
        ],
        "critic": [
            {
                "results": [
                    {
                        "opportunity_id": opportunity_id,
                        "verdict": fixture.critic_verdict,
                        "confidence": 0.9,
                        "summary": "Apply evidence gates independently of the score.",
                    }
                ]
            }
        ],
    }


def _query_plan(fixture: HuntFixture) -> dict[str, Any]:
    return {
        "round_number": 1,
        "intents": [
            {
                "id": f"intent-{fixture.slug}",
                "kind": "BROAD",
                "concept": f"invoice reconciliation {fixture.slug}",
                "sources": [source.value for source in fixture.query_sources],
                "rationale": "find repeated manual work",
            }
        ],
    }


def _install_external_fixtures(monkeypatch: pytest.MonkeyPatch, fixture: HuntFixture) -> None:
    monkeypatch.setattr(
        research_handler_module,
        "HackerNewsCollector",
        lambda client: FixtureCollector(fixture, Source.HACKER_NEWS),
    )
    monkeypatch.setattr(
        research_handler_module,
        "GitHubCollector",
        lambda client, token: FixtureCollector(fixture, Source.GITHUB),
    )
    monkeypatch.setattr(
        research_handler_module,
        "RedditCollector",
        lambda client, client_id, client_secret: FixtureCollector(fixture, Source.REDDIT),
    )
    monkeypatch.setattr(
        research_handler_module,
        "BraveSearchProvider",
        lambda client, key: FixtureSearch(fixture),
    )
    monkeypatch.setattr(
        research_handler_module,
        "StaticFetcher",
        lambda client: FixtureFetcher(fixture),
    )


async def _schedule_hunt(database: Database, settings: Settings, slug: str) -> tuple[UUID, UUID]:
    async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
        _, revision = await uow.missions.create_with_revision(
            title=f"Vertical HUNT {slug}",
            mission_text=f"Find recurring reconciliation pain for {slug}",
            original_language="en",
            output_locale="en",
        )
        assert uow.session is not None
        scheduled = await RunScheduler(uow.session).schedule(
            request=RunScheduleRequest.model_validate(
                {
                    "mission_revision_id": revision.id,
                    "mode": "HUNT",
                    "priority": 0,
                    "budget_limits": settings.budget_snapshot(),
                }
            )
        )
        await uow.commit()
        return scheduled.run.id, scheduled.task.id


async def _terminalize_if_needed(database: Database, run_id: UUID, task_id: UUID) -> None:
    async with database.session() as session, session.begin():
        run = await session.get(ResearchRun, run_id)
        task = await session.get(ResearchTask, task_id)
        now = datetime.now(UTC)
        if run is not None and run.status in {"QUEUED", "RUNNING"}:
            run.status = "CANCELLED"
            run.completed_at = now
        if task is not None and task.status in {"PENDING", "LEASED"}:
            task.status = "CANCELLED"
            task.completed_at = now
            task.lease_owner = None
            task.lease_expires_at = None


async def _run_cli(
    *arguments: str,
    database_url: str,
    reports_dir: Path,
) -> dict[str, Any]:
    gap = Path(sys.executable).parent / "gap"
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "DATABASE_URL": database_url,
        "AGENT_PROVIDER": "fake",
        "AUTHOR_HMAC_KEY": "vertical-hunt-author-key-0000000000000000",
        "REPORTS_DIR": str(reports_dir),
    }
    process = await asyncio.create_subprocess_exec(
        str(gap),
        *arguments,
        "--json",
        cwd=reports_dir.parent,
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    assert process.returncode == 0, stderr.decode()
    payload = json.loads(stdout)
    assert payload["schema_version"] == "1.0"
    assert payload["error"] is None
    return payload


@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("slug", "empty", "partial", "critic_verdict", "expected_status", "expected_calls"),
    (
        ("baseline", False, False, "RESEARCH_MORE", "COMPLETED", 6),
        ("hard-gate", False, False, "VALIDATE", "COMPLETED", 6),
        ("zero", True, False, "RESEARCH_MORE", "COMPLETED", 1),
        ("partial", False, True, "RESEARCH_MORE", "COMPLETED_WITH_WARNINGS", 6),
    ),
)
async def test_production_hunt_is_durable_queryable_and_fail_closed(
    migrated_postgres_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    slug: str,
    empty: bool,
    partial: bool,
    critic_verdict: str,
    expected_status: str,
    expected_calls: int,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    settings = Settings(
        database_url=migrated_postgres_url,
        agent_provider=AgentProviderName.FAKE,
        author_hmac_key="vertical-hunt-author-key-0000000000000000",
        max_research_rounds=1,
        max_agent_calls_per_run=6,
        max_parallel_agent_calls=1,
        max_collector_requests_per_run=4,
        max_search_calls_per_run=1,
        max_raw_signals_per_run=4,
    )
    run_id, task_id = await _schedule_hunt(database, settings, slug)
    fixture = HuntFixture(
        slug=slug,
        run_id=run_id,
        query_sources=(Source.HACKER_NEWS, Source.GITHUB) if partial else (Source.HACKER_NEWS,),
        empty_collection=empty,
        critic_verdict=critic_verdict,
    )
    _install_external_fixtures(monkeypatch, fixture)
    worker_id = f"vertical-worker-{slug}"
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        FakeAgentProvider(_fake_scripts(fixture)),
        lease_owner=worker_id,
        provider_name="fake",
    )
    handler = ResearchRunHandler(database=database, settings=settings, reasoner=reasoner)

    try:
        assert await Worker(
            database,
            worker_id=worker_id,
            handlers=TaskHandlerRegistry({"research.run": handler}),
        ).run_once()

        async with database.session() as session:
            run = await session.get(ResearchRun, run_id)
            task = await session.get(ResearchTask, task_id)
            call_count = await session.scalar(
                select(func.count()).select_from(AgentCall).where(AgentCall.run_id == run_id)
            )
            lease_count = await session.scalar(
                select(func.count())
                .select_from(ProviderCallLease)
                .where(ProviderCallLease.run_id == run_id)
            )
            final_rows = (
                await session.scalars(
                    select(FinalAssessmentSnapshot).where(FinalAssessmentSnapshot.run_id == run_id)
                )
            ).all()
            assert run is not None and run.status == expected_status
            assert task is not None and task.status == "SUCCEEDED"
            assert task.lease_owner is None and task.lease_expires_at is None
            assert call_count == expected_calls
            assert lease_count == 0

            if empty:
                assert final_rows == []
            else:
                assert len(final_rows) == 1
                final = final_rows[0]
                assessment = await session.get(MissionOpportunityAssessment, final.assessment_id)
                opportunity = (
                    await session.get(Opportunity, assessment.opportunity_id)
                    if assessment is not None
                    else None
                )
                raw = await session.scalar(
                    select(RawSignalRevision).where(
                        RawSignalRevision.domain_revision_id == fixture.raw_revision_id
                    )
                )
                pain = (
                    await session.scalar(
                        select(PainSignal).where(PainSignal.raw_signal_revision_id == raw.id)
                    )
                    if raw is not None
                    else None
                )
                problem = await session.get(CanonicalProblem, UUID(fixture.problem_id))
                gap = await session.get(GapHypothesis, final.gap_hypothesis_id)
                card = await session.get(EvidenceCard, final.evidence_card_id)
                score = await session.get(
                    OpportunityScoreSnapshot,
                    final.score_snapshot_id,
                )
                critic = await session.get(CriticResult, final.critic_result_id)
                lifecycle_count = await session.scalar(
                    select(func.count())
                    .select_from(LifecycleEvent)
                    .where(LifecycleEvent.assessment_id == final.assessment_id)
                )
                claim_ids = (
                    [*card.supporting_claim_ids, *card.contradicting_claim_ids]
                    if card is not None
                    else []
                )
                claim_count = await session.scalar(
                    select(func.count())
                    .select_from(AtomicClaim)
                    .where(AtomicClaim.id.in_(claim_ids))
                )
                assert all(
                    artifact is not None
                    for artifact in (
                        assessment,
                        opportunity,
                        raw,
                        pain,
                        problem,
                        gap,
                        card,
                        score,
                        critic,
                    )
                )
                assert lifecycle_count and lifecycle_count >= 2
                assert claim_count == len(set(claim_ids)) > 0
                if critic_verdict == "VALIDATE":
                    assert final.verdict != "VALIDATE"

        reports_dir = tmp_path / f"reports-{slug}"
        reports_dir.parent.mkdir(parents=True, exist_ok=True)
        first = await _run_cli(
            "report",
            "run",
            str(run_id),
            database_url=migrated_postgres_url,
            reports_dir=reports_dir,
        )
        first_artifact = Path(first["data"]["artifact"]["run_path"])
        first_bytes = await asyncio.to_thread(first_artifact.read_bytes)
        second = await _run_cli(
            "report",
            "run",
            str(run_id),
            database_url=migrated_postgres_url,
            reports_dir=reports_dir,
        )
        second_bytes = await asyncio.to_thread(
            Path(second["data"]["artifact"]["run_path"]).read_bytes
        )
        latest_bytes = await asyncio.to_thread((reports_dir / "latest.md").read_bytes)
        assert second_bytes == first_bytes
        assert latest_bytes == first_bytes
        assert first["data"]["report"]["status"] == expected_status
        assert len(first["data"]["report"]["opportunities"]) == (0 if empty else 1)
        if not empty:
            opportunity_id = first["data"]["report"]["opportunities"][0]["opportunity_id"]
            opportunity_report = await _run_cli(
                "report",
                "opportunity",
                opportunity_id,
                database_url=migrated_postgres_url,
                reports_dir=reports_dir,
            )
            assert (
                opportunity_report["data"]["report"]["opportunity"]["opportunity_id"]
                == opportunity_id
            )
    finally:
        await _terminalize_if_needed(database, run_id, task_id)
        await database.dispose()
