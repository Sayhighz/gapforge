from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from gapforge.analysis.normalization import normalize_text
from gapforge.domain.contracts import MissionRevision as DomainMissionRevision
from gapforge.integration.mappers import storage_uuid_for_identifier
from gapforge.integration.persistence import ResearchArtifactWriter
from gapforge.integration.queries import ResearchQueryService
from gapforge.storage import models
from gapforge.storage.database import Database

NOW = datetime(2026, 8, 9, 12, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class FakeContext:
    run_id: UUID
    task_id: UUID
    task_attempt: int
    mission_revision: DomainMissionRevision
    collection_until: datetime


@dataclass(frozen=True, slots=True)
class FakeCommit:
    stage: str
    payload: dict[str, object]


@pytest.mark.postgres
async def test_writer_persists_complete_typed_lineage_in_stage_transactions(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context, ids = await _seed_pipeline_context(database)
    writer = ResearchArtifactWriter()
    try:
        stages = _stage_payloads(context, ids)
        for stage, payload in stages:
            async with database.session() as session:
                await writer.persist_stage(session, context, FakeCommit(stage, payload))
                await session.commit()

        async with database.session() as session:
            assert await _count(session, models.PainSignal) == 1
            assert await _count(session, models.CanonicalProblem) >= 1
            assert await _count(session, models.ProblemCluster) == 1
            assert await _count(session, models.ProblemClusterMembership) == 1
            assert await _count(session, models.AtomicClaim) == 2
            assert await _count(session, models.Competitor) == 1
            assert await _count(session, models.CompetitorEvidence) == 1
            assert await _count(session, models.GapHypothesis) >= 1
            assert await _count(session, models.Opportunity) == 1
            assert await _count(session, models.EvidenceCard) == 1
            assert await _count(session, models.OpportunityScoreSnapshot) == 1
            assert await _count(session, models.ProblemHypothesis) == 1
            assert await _count(session, models.CriticResult) == 1

            assessment = await session.scalar(
                select(models.MissionOpportunityAssessment).where(
                    models.MissionOpportunityAssessment.opportunity_id == ids["opportunity"]
                )
            )
            assert assessment is not None
            assert assessment.mission_revision_id == context.mission_revision.id
            assert assessment.lifecycle_status == "VALIDATE"
            assert assessment.verdict == "VALIDATE"
            assert assessment.competitor_research_status == "COMPLETE"
            events = tuple(
                await session.scalars(
                    select(models.LifecycleEvent)
                    .where(models.LifecycleEvent.assessment_id == assessment.id)
                    .order_by(models.LifecycleEvent.event_number)
                )
            )
            assert [(item.from_status, item.to_status) for item in events] == [
                (None, "DISCOVERED"),
                ("DISCOVERED", "RESEARCHING"),
                ("RESEARCHING", "VALIDATE"),
            ]
            assert [item.event_number for item in events] == [1, 2, 3]
            assert len(events[-1].details["gates"]) == 14

            stored_claims = tuple(await session.scalars(select(models.AtomicClaim)))
            known_evidence = {
                ids["raw_revision"],
                ids["competitor_evidence"],
            }
            assert all(set(claim.evidence_ids) <= known_evidence for claim in stored_claims)
            card = await session.get(models.EvidenceCard, ids["card"])
            score = await session.get(models.OpportunityScoreSnapshot, ids["score"])
            critic = await session.scalar(
                select(models.CriticResult).where(models.CriticResult.run_id == context.run_id)
            )
            assert card is not None and card.run_id == context.run_id
            assert score is not None and score.run_id == context.run_id
            assert critic is not None and critic.run_id == context.run_id
            assert {score.assessment_id, critic.assessment_id} == {assessment.id}

        queries = ResearchQueryService(database.session_factory)
        evidence = await queries.evidence_item(str(ids["raw_domain"]))
        assert evidence["id"] == ids["raw_domain"]
        assert evidence["kind"] == "raw_signal_revision"
        opportunity = await queries.opportunity(ids["opportunity"])
        assert opportunity["origin_gap_hypothesis_id"] == ids["gap"]
        assert opportunity["assessments"][0]["score_snapshot_id"] == ids["score"]
        changes = await queries.changes(limit=10)
        assert [item["event_number"] for item in reversed(changes)] == [1, 2, 3]
        run_report = await queries.run_report(context.run_id)
        assert run_report.opportunities[0].score.id == str(ids["score"])
        assert run_report.opportunities[0].validation.passed is True
        opportunity_report = await queries.opportunity_report(ids["opportunity"])
        assert opportunity_report.opportunity.evidence_card.id == str(ids["card"])
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_later_monitor_persists_snapshots_without_mutating_terminal_validate(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    initial_context, initial_ids = await _seed_pipeline_context(database)
    writer = ResearchArtifactWriter()
    try:
        for stage, payload in _stage_payloads(initial_context, initial_ids):
            async with database.session() as session:
                await writer.persist_stage(
                    session,
                    initial_context,
                    FakeCommit(stage, payload),
                )
                await session.commit()

        monitor_context = await _seed_followup_context(database, initial_context)
        monitor_ids = dict(initial_ids)
        monitor_ids["gap"] = uuid4()
        monitor_ids["card"] = uuid5(
            monitor_context.run_id,
            f"card:{monitor_ids['opportunity']}",
        )
        monitor_ids["score"] = uuid5(
            monitor_context.run_id,
            f"score:{monitor_ids['opportunity']}",
        )
        monitor_stages = {
            stage: copy.deepcopy(payload)
            for stage, payload in _stage_payloads(monitor_context, monitor_ids)
        }
        monitor_stages["GAP"]["gaps"][0]["statement"] = (
            "Later monitoring confirms the same reconciliation gap"
        )
        for stage in ("GAP", "CARD_SCORE", "HYPOTHESIS", "CRITIC", "FINAL"):
            async with database.session() as session:
                await writer.persist_stage(
                    session,
                    monitor_context,
                    FakeCommit(stage, monitor_stages[stage]),
                )
                await session.commit()

        async with database.session() as session:
            assessment = await session.scalar(
                select(models.MissionOpportunityAssessment).where(
                    models.MissionOpportunityAssessment.opportunity_id == initial_ids["opportunity"]
                )
            )
            assert assessment is not None
            assert assessment.lifecycle_status == "VALIDATE"
            assert assessment.verdict == "VALIDATE"
            events = tuple(
                await session.scalars(
                    select(models.LifecycleEvent)
                    .where(models.LifecycleEvent.assessment_id == assessment.id)
                    .order_by(models.LifecycleEvent.event_number)
                )
            )
            assert [event.to_status for event in events] == [
                "DISCOVERED",
                "RESEARCHING",
                "VALIDATE",
            ]
            assert all(event.run_id != monitor_context.run_id for event in events)
            assert await session.get(models.GapHypothesis, monitor_ids["gap"]) is not None
            assert await session.get(models.EvidenceCard, monitor_ids["card"]) is not None
            assert (
                await session.get(models.OpportunityScoreSnapshot, monitor_ids["score"]) is not None
            )
            assert (
                await session.scalar(
                    select(models.CriticResult).where(
                        models.CriticResult.run_id == monitor_context.run_id
                    )
                )
                is not None
            )
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_writer_rejects_unknown_lineage_atomically(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context, ids = await _seed_pipeline_context(database)
    writer = ResearchArtifactWriter()
    problem_id = uuid4()
    cluster_id = uuid4()
    payload = {
        "problems": [
            {
                "schema_version": "0.1",
                "id": str(problem_id),
                "summary": "Unknown pain lineage",
            }
        ],
        "clusters": [
            {
                "schema_version": "0.1",
                "id": str(cluster_id),
                "canonical_problem_id": str(problem_id),
                "state": "ACTIVE",
                "last_growth_at": NOW.isoformat(),
            }
        ],
        "memberships": [
            {
                "schema_version": "0.1",
                "cluster_id": str(cluster_id),
                "pain_signal_id": str(uuid4()),
                "accepted_at": NOW.isoformat(),
            }
        ],
    }
    try:
        async with database.session() as session:
            with pytest.raises(ValueError, match="unknown pain signal"):
                await writer.persist_stage(session, context, FakeCommit("CLUSTER", payload))
            await session.rollback()
        async with database.session() as session:
            assert await session.get(models.CanonicalProblem, problem_id) is None
            assert await session.get(models.ProblemCluster, cluster_id) is None
            assert await session.get(models.PainSignal, ids["pain"]) is None
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_writer_ignores_only_reserved_input_bounds_and_rejects_unknown_stage_keys(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context, ids = await _seed_pipeline_context(database)
    writer = ResearchArtifactWriter()
    extraction = _stage_payloads(context, ids)[0][1]
    extraction["_input_bounds"] = {"omitted_counts": {"evidence": 2}}
    try:
        async with database.session() as session:
            await writer.persist_stage(session, context, FakeCommit("EXTRACT", extraction))
            await session.commit()
        invalid = {**extraction, "invented": []}
        async with database.session() as session:
            with pytest.raises(ValueError, match="unknown EXTRACT payload keys"):
                await writer.persist_stage(session, context, FakeCommit("EXTRACT", invalid))
        async with database.session() as session:
            with pytest.raises(ValueError, match="unsupported artifact stage"):
                await writer.persist_stage(
                    session,
                    context,
                    FakeCommit("EXTRACT_R3", {"pain_signals": []}),
                )
        async with database.session() as session:
            await writer.persist_stage(
                session,
                context,
                FakeCommit("RESEARCH_MORE_RESULT", {"continue_research": True}),
            )
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_lifecycle_database_rejects_invalid_direct_history_and_projection(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context, ids = await _seed_pipeline_context(database)
    writer = ResearchArtifactWriter()
    try:
        for stage, payload in _stage_payloads(context, ids)[:3]:
            async with database.session() as session:
                await writer.persist_stage(session, context, FakeCommit(stage, payload))
                await session.commit()
        async with database.session() as session:
            assessment = await session.scalar(
                select(models.MissionOpportunityAssessment).where(
                    models.MissionOpportunityAssessment.opportunity_id == ids["opportunity"]
                )
            )
            assert assessment is not None
            assessment_id = assessment.id

        async with database.session() as session:
            with pytest.raises(DBAPIError):
                await session.execute(
                    insert(models.LifecycleEvent).values(
                        id=uuid4(),
                        assessment_id=assessment_id,
                        event_number=3,
                        run_id=context.run_id,
                        gap_hypothesis_id=ids["gap"],
                        from_status=None,
                        to_status="VALIDATE",
                        reason="invalid null transition",
                        details={},
                        created_at=NOW,
                    )
                )
                await session.commit()
            await session.rollback()

        wrong_problem = models.CanonicalProblem(
            id=uuid4(),
            canonical_key=f"wrong-{uuid4()}",
            title="Unrelated problem",
            summary="Unrelated problem",
            status="ACTIVE",
        )
        wrong_gap = models.GapHypothesis(
            id=uuid4(),
            canonical_problem_id=wrong_problem.id,
            gap_type="WORKFLOW",
            statement="Unrelated gap",
            user_evidence_ids=[ids["raw_revision"]],
            competitor_evidence_ids=[ids["competitor_evidence"]],
            contradicting_claim_ids=[],
        )
        async with database.session() as session:
            session.add_all((wrong_problem, wrong_gap))
            await session.commit()
        async with database.session() as session:
            with pytest.raises(DBAPIError):
                await session.execute(
                    insert(models.LifecycleEvent).values(
                        id=uuid4(),
                        assessment_id=assessment_id,
                        event_number=3,
                        run_id=context.run_id,
                        gap_hypothesis_id=wrong_gap.id,
                        from_status="RESEARCHING",
                        to_status="RESEARCH_MORE",
                        reason="cross-problem gap",
                        details={},
                        created_at=NOW,
                    )
                )
                await session.commit()
            await session.rollback()

        async with database.session() as session:
            await session.execute(
                insert(models.LifecycleEvent).values(
                    id=uuid4(),
                    assessment_id=assessment_id,
                    event_number=3,
                    run_id=context.run_id,
                    gap_hypothesis_id=ids["gap"],
                    from_status="RESEARCHING",
                    to_status="RESEARCH_MORE",
                    reason="direct but valid research-more decision",
                    details={},
                    created_at=NOW,
                )
            )
            await session.commit()

        async with database.session() as session:
            assessment = await session.get(models.MissionOpportunityAssessment, assessment_id)
            assert assessment is not None
            assert (assessment.lifecycle_status, assessment.verdict) == (
                "RESEARCH_MORE",
                "RESEARCH_MORE",
            )
            events = tuple(
                await session.scalars(
                    select(models.LifecycleEvent)
                    .where(models.LifecycleEvent.assessment_id == assessment_id)
                    .order_by(models.LifecycleEvent.event_number)
                )
            )
            assert [item.event_number for item in events] == [1, 2, 3]
            event_id = events[-1].id

        async with database.session() as session:
            with pytest.raises(DBAPIError):
                await session.execute(
                    update(models.MissionOpportunityAssessment)
                    .where(models.MissionOpportunityAssessment.id == assessment_id)
                    .values(lifecycle_status="VALIDATE", verdict="VALIDATE")
                )
                await session.commit()
            await session.rollback()
        async with database.session() as session:
            with pytest.raises(DBAPIError):
                await session.execute(
                    update(models.LifecycleEvent)
                    .where(models.LifecycleEvent.id == event_id)
                    .values(reason="rewrite history")
                )
                await session.commit()
            await session.rollback()
        async with database.session() as session:
            with pytest.raises(DBAPIError):
                await session.execute(
                    delete(models.LifecycleEvent).where(models.LifecycleEvent.id == event_id)
                )
                await session.commit()
            await session.rollback()
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_lifecycle_event_numbers_serialize_concurrent_same_timestamp_inserts(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context, ids = await _seed_pipeline_context(database)
    writer = ResearchArtifactWriter()
    try:
        for stage, payload in _stage_payloads(context, ids)[:3]:
            async with database.session() as session:
                await writer.persist_stage(session, context, FakeCommit(stage, payload))
                await session.commit()
        async with database.session() as session:
            assessment_id = await session.scalar(
                select(models.MissionOpportunityAssessment.id).where(
                    models.MissionOpportunityAssessment.opportunity_id == ids["opportunity"]
                )
            )
            assert assessment_id is not None

        async def insert_competing_event() -> bool:
            async with database.session() as session:
                try:
                    await session.execute(
                        insert(models.LifecycleEvent).values(
                            id=uuid4(),
                            assessment_id=assessment_id,
                            event_number=3,
                            run_id=context.run_id,
                            gap_hypothesis_id=ids["gap"],
                            from_status="RESEARCHING",
                            to_status="RESEARCH_MORE",
                            reason="concurrent decision",
                            details={},
                            created_at=NOW,
                        )
                    )
                    await session.commit()
                except DBAPIError:
                    await session.rollback()
                    return False
                return True

        outcomes = await asyncio.gather(insert_competing_event(), insert_competing_event())
        assert sorted(outcomes) == [False, True]
        async with database.session() as session:
            events = tuple(
                await session.scalars(
                    select(models.LifecycleEvent)
                    .where(models.LifecycleEvent.assessment_id == assessment_id)
                    .order_by(models.LifecycleEvent.event_number)
                )
            )
            assert [item.event_number for item in events] == [1, 2, 3]
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_round_two_report_restarts_from_exact_snapshot_without_mutating_origin_gap(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    context, ids = await _seed_pipeline_context(database)
    writer = ResearchArtifactWriter()
    try:
        r1 = {stage: copy.deepcopy(payload) for stage, payload in _stage_payloads(context, ids)}
        r1["CARD_SCORE"]["cards"][0]["known_author_ids"] = ["author-1"]
        r1["CARD_SCORE"]["cards"][0]["thread_ids"] = ["thread-1"]
        r1["CARD_SCORE"]["cards"][0]["user_sources"] = ["HACKER_NEWS"]
        r1["CRITIC"]["results"][0]["verdict"] = "RESEARCH_MORE"
        r1["FINAL"]["decisions"][0]["verdict"] = "RESEARCH_MORE"
        for stage in ("EXTRACT", "CLUSTER", "GAP", "CARD_SCORE", "HYPOTHESIS", "CRITIC", "FINAL"):
            async with database.session() as session:
                await writer.persist_stage(session, context, FakeCommit(stage, r1[stage]))
                await session.commit()

        origin_gap_id = ids["gap"]
        r2_gap_id = uuid4()
        r2_card_id = uuid5(context.run_id, f"card:2:{ids['opportunity']}")
        r2_score_id = uuid5(context.run_id, f"score:2:{ids['opportunity']}")
        r2 = {stage: copy.deepcopy(payload) for stage, payload in _stage_payloads(context, ids)}
        r2["GAP"]["gaps"][0]["id"] = str(r2_gap_id)
        r2["GAP"]["gaps"][0]["statement"] = "New evidence strengthens automation gap"
        r2["GAP"]["opportunities"][0]["gap_hypothesis_id"] = str(r2_gap_id)
        r2["CARD_SCORE"]["cards"][0]["id"] = str(r2_card_id)
        r2["CARD_SCORE"]["scores"][0]["id"] = str(r2_score_id)
        hypothesis = r2["HYPOTHESIS"]["hypotheses"][0]
        r2_hypothesis_id = _hypothesis_id(
            opportunity_id=str(ids["opportunity"]),
            evidence_card_id=str(r2_card_id),
            canonical_problem_id=hypothesis["canonical_problem_id"],
            icp=hypothesis["icp"],
            job_to_be_done=hypothesis["job_to_be_done"],
            trigger=hypothesis["trigger"],
            current_behavior=hypothesis["current_behavior"],
            pain=hypothesis["pain"],
            workflow_failure=hypothesis["workflow_failure"],
            falsification_test=hypothesis["falsification_test"],
            supporting_claim_ids=hypothesis["supporting_claim_ids"],
            contradicting_claim_ids=hypothesis["contradicting_claim_ids"],
        )
        hypothesis["id"] = str(r2_hypothesis_id)
        link = r2["HYPOTHESIS"]["hypothesis_card_links"][0]
        link["hypothesis_id"] = str(r2_hypothesis_id)
        link["evidence_card_id"] = str(r2_card_id)
        critic_id = uuid5(context.run_id, f"critic:2:{ids['opportunity']}")
        final = r2["FINAL"]["decisions"][0]
        final.update(
            {
                "round_number": 2,
                "gap_hypothesis_id": str(r2_gap_id),
                "evidence_card_id": str(r2_card_id),
                "score_snapshot_id": str(r2_score_id),
                "critic_result_id": str(critic_id),
            }
        )
        await _seed_critic_call(database, context, round_number=2)
        for source, target in (
            ("GAP", "GAP_R2"),
            ("CARD_SCORE", "CARD_SCORE_R2"),
            ("HYPOTHESIS", "HYPOTHESIS_R2"),
            ("CRITIC", "CRITIC_R2"),
            ("FINAL", "FINAL"),
        ):
            async with database.session() as session:
                await writer.persist_stage(session, context, FakeCommit(target, r2[source]))
                await session.commit()

        restarted_queries = ResearchQueryService(database.session_factory)
        opportunity = await restarted_queries.opportunity(ids["opportunity"])
        assert opportunity["origin_gap_hypothesis_id"] == origin_gap_id
        report = await restarted_queries.opportunity_report(ids["opportunity"])
        assert report.opportunity.evidence_card.id == str(r2_card_id)
        assert report.opportunity.score.id == str(r2_score_id)
        async with database.session() as session:
            assessment = await session.scalar(
                select(models.MissionOpportunityAssessment).where(
                    models.MissionOpportunityAssessment.opportunity_id == ids["opportunity"]
                )
            )
            assert assessment is not None
            events = tuple(
                await session.scalars(
                    select(models.LifecycleEvent)
                    .where(models.LifecycleEvent.assessment_id == assessment.id)
                    .order_by(models.LifecycleEvent.event_number)
                )
            )
            assert [event.gap_hypothesis_id for event in events] == [
                origin_gap_id,
                origin_gap_id,
                origin_gap_id,
                r2_gap_id,
            ]
    finally:
        await database.dispose()


async def _count(session: AsyncSession, model: type[models.Base]) -> int:
    return int(await session.scalar(select(func.count()).select_from(model)) or 0)


async def _seed_pipeline_context(
    database: Database,
) -> tuple[FakeContext, dict[str, UUID | str]]:
    mission = models.ResearchMission(id=uuid4(), status="DRAFT", title="Accounting pain")
    revision = models.MissionRevision(
        id=uuid4(),
        mission_id=mission.id,
        revision_number=1,
        change_reason="initial",
        mission_text="Find recurring accounting pain",
        original_language="en",
        output_locale="en",
        interpretation={},
    )
    run = models.ResearchRun(
        id=uuid4(),
        mission_revision_id=revision.id,
        mode="HUNT",
        status="COMPLETED",
        priority=1,
        deadline_at=NOW + timedelta(minutes=30),
        started_at=NOW,
        completed_at=NOW,
        budget_limits={"max_agent_calls_per_run": 6},
        budget_used={"agent_calls": 1},
        warnings=[],
        last_checkpoint={},
    )
    task = models.ResearchTask(
        id=uuid4(),
        run_id=run.id,
        task_type="research.run",
        status="LEASED",
        priority=1,
        idempotency_key=f"pipeline-{uuid4()}",
        payload={},
        checkpoint={
            "pipeline": {
                "stages": {
                    "COMPETITOR_RESEARCH": {
                        "payload": {"status": "COMPLETE"},
                    }
                }
            }
        },
        attempt_count=1,
        max_attempts=3,
        available_at=NOW,
        lease_owner="worker-i5",
        lease_expires_at=NOW + timedelta(minutes=5),
    )
    raw_id = storage_uuid_for_identifier("raw-signal", f"HACKER_NEWS:{uuid4()}")
    raw_revision_identifier = f"HACKER_NEWS:{raw_id}:r1"
    raw_revision_id = storage_uuid_for_identifier("raw-signal-revision", raw_revision_identifier)
    raw = models.RawSignal(
        id=raw_id,
        source="HACKER_NEWS",
        external_id=str(raw_id),
        canonical_url="https://news.ycombinator.com/item?id=1",
        parent_external_id=str(raw_id),
        author_pseudonym="author-1",
        author_kind="KNOWN",
        source_created_at=NOW,
        collected_at=NOW,
        is_tombstone=False,
    )
    raw_revision = models.RawSignalRevision(
        id=raw_revision_id,
        raw_signal_id=raw.id,
        revision_number=1,
        domain_revision_id=raw_revision_identifier,
        title="Manual close process",
        body="We manually reconcile spreadsheets every month.",
        original_language="en",
        engagement={},
        source_metadata={},
        content_hash=hashlib.sha256(b"manual close").digest(),
        normalization_version="1",
        observed_at=NOW,
        is_tombstone=False,
        duplicate_group_key=hashlib.sha256(b"group").hexdigest(),
    )
    critic_call_id = uuid5(run.id, f"{task.id}:CRITIC:1:{task.attempt_count}")
    output = {"results": []}
    output_bytes = b'{"results":[]}'
    critic_call = models.AgentCall(
        id=critic_call_id,
        run_id=run.id,
        task_id=task.id,
        provider="fake",
        operation="critic",
        output_schema_name="critic-v1",
        output_schema_sha256=b"s" * 32,
        request_sha256=b"r" * 32,
        requested_model="",
        resolved_model="fake",
        effort="medium",
        cli_version=None,
        status="COMPLETED",
        duration_ms=1,
        repair_attempts=0,
        usage={},
        error_class=None,
        output_json=output,
        output_sha256=hashlib.sha256(output_bytes).digest(),
    )
    async with database.session() as session:
        session.add(mission)
        await session.flush()
        session.add(revision)
        await session.flush()
        session.add(run)
        await session.flush()
        session.add_all((task, raw))
        await session.flush()
        session.add_all((raw_revision, critic_call))
        await session.commit()
    context = FakeContext(
        run_id=run.id,
        task_id=task.id,
        task_attempt=task.attempt_count,
        mission_revision=DomainMissionRevision(
            id=revision.id,
            mission_id=mission.id,
            revision=1,
            change_reason="initial",
            prompt=revision.mission_text,
            output_locale="en",
            created_at=NOW,
        ),
        collection_until=NOW,
    )
    opportunity_id = uuid4()
    return context, {
        "raw_revision": raw_revision_id,
        "pain": uuid4(),
        "problem": uuid4(),
        "cluster": uuid4(),
        "user_claim": uuid4(),
        "competitor": uuid4(),
        "competitor_evidence": uuid4(),
        "competitor_claim": uuid4(),
        "gap": uuid4(),
        "opportunity": opportunity_id,
        "card": uuid5(run.id, f"card:{opportunity_id}"),
        "score": uuid5(run.id, f"score:{opportunity_id}"),
        "hypothesis": uuid4(),
        "raw_domain": raw_revision_identifier,
    }


async def _seed_critic_call(
    database: Database,
    context: FakeContext,
    *,
    round_number: int,
) -> None:
    call_id = uuid5(
        context.run_id,
        f"{context.task_id}:CRITIC:{round_number}:{context.task_attempt}",
    )
    output = {"results": []}
    output_bytes = b'{"results":[]}'
    async with database.session() as session:
        session.add(
            models.AgentCall(
                id=call_id,
                run_id=context.run_id,
                task_id=context.task_id,
                provider="fake",
                operation="critic",
                output_schema_name=f"critic_r{round_number}-v1",
                output_schema_sha256=b"s" * 32,
                request_sha256=b"r" * 32,
                requested_model="",
                resolved_model="fake",
                effort="medium",
                cli_version=None,
                status="COMPLETED",
                duration_ms=1,
                repair_attempts=0,
                usage={},
                error_class=None,
                output_json=output,
                output_sha256=hashlib.sha256(output_bytes).digest(),
            )
        )
        await session.commit()


async def _seed_followup_context(
    database: Database,
    initial_context: FakeContext,
) -> FakeContext:
    collected_at = initial_context.collection_until + timedelta(days=1)
    run = models.ResearchRun(
        id=uuid4(),
        mission_revision_id=initial_context.mission_revision.id,
        mode="MONITOR",
        status="COMPLETED",
        priority=1,
        deadline_at=collected_at + timedelta(minutes=30),
        started_at=collected_at,
        completed_at=collected_at,
        budget_limits={"max_agent_calls_per_run": 6},
        budget_used={"agent_calls": 1},
        warnings=[],
        last_checkpoint={},
    )
    task = models.ResearchTask(
        id=uuid4(),
        run_id=run.id,
        task_type="research.run",
        status="SUCCEEDED",
        priority=1,
        idempotency_key=f"monitor-{uuid4()}",
        payload={},
        checkpoint={
            "pipeline": {
                "stages": {
                    "COMPETITOR_RESEARCH": {
                        "payload": {"status": "COMPLETE"},
                    }
                }
            }
        },
        result={},
        attempt_count=1,
        max_attempts=3,
        available_at=collected_at,
        completed_at=collected_at,
    )
    async with database.session() as session:
        session.add_all((run, task))
        await session.commit()
    context = FakeContext(
        run_id=run.id,
        task_id=task.id,
        task_attempt=task.attempt_count,
        mission_revision=initial_context.mission_revision,
        collection_until=collected_at,
    )
    await _seed_critic_call(database, context, round_number=1)
    return context


def _stage_payloads(
    context: FakeContext, ids: dict[str, UUID | str]
) -> list[tuple[str, dict[str, object]]]:
    raw_domain = ids["raw_domain"]
    assert isinstance(raw_domain, str)
    citation = {
        "schema_version": "0.1",
        "evidence_id": raw_domain,
        "source_url": "https://news.ycombinator.com/item?id=1",
        "excerpt": "manually reconcile spreadsheets",
        "observed_at": NOW.isoformat(),
    }
    competitor_citation = {
        "schema_version": "0.1",
        "evidence_id": str(ids["competitor_evidence"]),
        "source_url": "https://vendor.example/pricing",
        "excerpt": "$99 per month",
        "observed_at": NOW.isoformat(),
    }
    hypothesis_fields = {
        "canonical_problem_id": str(ids["problem"]),
        "icp": "Small finance teams",
        "job_to_be_done": "Close books quickly",
        "trigger": "Month end",
        "current_behavior": "Reconcile spreadsheets",
        "pain": "Slow close",
        "workflow_failure": "Manual matching",
        "falsification_test": "Teams refuse automated matching",
        "supporting_claim_ids": [str(ids["user_claim"])],
        "contradicting_claim_ids": [],
    }
    ids["hypothesis"] = _hypothesis_id(
        opportunity_id=str(ids["opportunity"]),
        evidence_card_id=str(ids["card"]),
        **hypothesis_fields,
    )
    return [
        (
            "EXTRACT",
            {
                "pain_signals": [
                    {
                        "schema_version": "0.1",
                        "id": str(ids["pain"]),
                        "raw_signal_revision_id": raw_domain,
                        "pain": "Manual reconciliation delays month end",
                        "user_context": "Small finance team",
                        "job_to_be_done": "Close the books",
                        "severity": 0.8,
                        "frequency": 0.75,
                        "workaround": "Spreadsheets",
                        "existing_solution": "Manual review",
                        "switching_signal": True,
                        "payment_signal": True,
                        "urgency_signal": True,
                        "emotion_signal": False,
                        "confidence": 0.9,
                        "excerpt": "manually reconcile spreadsheets",
                    }
                ]
            },
        ),
        (
            "CLUSTER",
            {
                "problems": [
                    {
                        "schema_version": "0.1",
                        "id": str(ids["problem"]),
                        "summary": "Manual month-end reconciliation",
                    }
                ],
                "clusters": [
                    {
                        "schema_version": "0.1",
                        "id": str(ids["cluster"]),
                        "canonical_problem_id": str(ids["problem"]),
                        "state": "ACTIVE",
                        "last_growth_at": NOW.isoformat(),
                    }
                ],
                "memberships": [
                    {
                        "schema_version": "0.1",
                        "cluster_id": str(ids["cluster"]),
                        "pain_signal_id": str(ids["pain"]),
                        "accepted_at": NOW.isoformat(),
                    }
                ],
            },
        ),
        (
            "GAP",
            {
                "claims": [
                    {
                        "schema_version": "0.1",
                        "id": str(ids["user_claim"]),
                        "text": "Finance teams manually reconcile spreadsheets",
                        "kind": "USER_PAIN",
                        "status": "SUPPORTED",
                        "evidence_ids": [raw_domain],
                        "citations": [citation],
                        "contradicts_claim_ids": [],
                    },
                    {
                        "schema_version": "0.1",
                        "id": str(ids["competitor_claim"]),
                        "text": "The competitor charges $99 per month",
                        "kind": "PRICE",
                        "status": "SUPPORTED",
                        "evidence_ids": [str(ids["competitor_evidence"])],
                        "citations": [competitor_citation],
                        "contradicts_claim_ids": [],
                    },
                ],
                "competitors": [
                    {
                        "schema_version": "0.1",
                        "id": str(ids["competitor"]),
                        "name": "Ledger Tool",
                        "kind": "SAAS",
                        "canonical_url": "https://vendor.example",
                    }
                ],
                "competitor_evidence": [
                    {
                        "schema_version": "0.1",
                        "id": str(ids["competitor_evidence"]),
                        "competitor_id": str(ids["competitor"]),
                        "source_url": "https://vendor.example/pricing",
                        "captured_excerpt": "$99 per month",
                        "observed_at": NOW.isoformat(),
                        "content_hash": hashlib.sha256(b"$99 per month").hexdigest(),
                        "evidence_kind": "PRICE_PAGE",
                        "metadata": {},
                        "claim_ids": [str(ids["competitor_claim"])],
                    }
                ],
                "gaps": [
                    {
                        "schema_version": "0.1",
                        "id": str(ids["gap"]),
                        "canonical_problem_id": str(ids["problem"]),
                        "gap_type": "WORKFLOW",
                        "statement": "Existing tools do not remove reconciliation work",
                        "user_evidence_ids": [raw_domain],
                        "competitor_evidence_ids": [str(ids["competitor_evidence"])],
                    }
                ],
                "opportunities": [
                    {
                        "schema_version": "0.1",
                        "id": str(ids["opportunity"]),
                        "gap_hypothesis_id": str(ids["gap"]),
                        "title": "Automated close reconciliation",
                    }
                ],
                "opportunity_fit": [
                    {
                        "schema_version": "0.1",
                        "opportunity_id": str(ids["opportunity"]),
                        "gap_strength": 80,
                        "competitor_dissatisfaction": 70,
                        "reachability": 75,
                        "technical_feasibility": 85,
                        "small_team_feasibility": 80,
                        "inverse_switching_friction": 65,
                        "why_now": 70,
                    }
                ],
            },
        ),
        (
            "CARD_SCORE",
            {
                "cards": [
                    {
                        "schema_version": "0.1",
                        "id": str(ids["card"]),
                        "opportunity_id": str(ids["opportunity"]),
                        "known_author_ids": [
                            "author-1",
                            "author-2",
                            "author-3",
                            "author-4",
                            "author-5",
                        ],
                        "thread_ids": ["thread-1", "thread-2", "thread-3"],
                        "user_sources": ["HACKER_NEWS", "REDDIT"],
                        "observed_days": [NOW.isoformat()],
                        "severity": 0.8,
                        "behavioral_workarounds": 1,
                        "paid_or_wtp_signals": 1,
                        "supporting_claim_ids": [str(ids["user_claim"])],
                        "contradicting_claim_ids": [],
                        "representative_evidence_ids": [raw_domain],
                        "confidence": 0.9,
                        "missing_evidence": [],
                    }
                ],
                "scores": [
                    {
                        "schema_version": "0.1",
                        "id": str(ids["score"]),
                        "opportunity_id": str(ids["opportunity"]),
                        "mission_revision_id": str(context.mission_revision.id),
                        "evidence_strength": {
                            "schema_version": "0.1",
                            "values": {"severity": 80},
                            "weights": {"severity": 1},
                        },
                        "opportunity_fit": {
                            "schema_version": "0.1",
                            "values": {"fit": 80},
                            "weights": {"fit": 1},
                        },
                        "raw_metrics": {"authors": 1},
                        "penalties": {},
                        "pre_penalty_score": 80,
                        "final_score": 80,
                        "evidence_confidence": 0.9,
                        "algorithm_version": "gapforge-score-v1",
                        "explanation": ["Strong repeated pain"],
                        "created_at": NOW.isoformat(),
                    }
                ],
            },
        ),
        (
            "HYPOTHESIS",
            {
                "hypotheses": [
                    {
                        "schema_version": "0.1",
                        "id": str(ids["hypothesis"]),
                        **hypothesis_fields,
                    }
                ],
                "hypothesis_card_links": [
                    {
                        "schema_version": "0.1",
                        "hypothesis_id": str(ids["hypothesis"]),
                        "opportunity_id": str(ids["opportunity"]),
                        "evidence_card_id": str(ids["card"]),
                    }
                ],
            },
        ),
        (
            "CRITIC",
            {
                "results": [
                    {
                        "schema_version": "0.1",
                        "opportunity_id": str(ids["opportunity"]),
                        "verdict": "VALIDATE",
                        "confidence": 0.8,
                        "fatal_flags": [],
                        "weak_assumptions": [],
                        "contradictions": [],
                        "missing_evidence": [],
                        "recommended_intents": [],
                        "summary": "Evidence supports validation",
                    }
                ]
            },
        ),
        (
            "FINAL",
            {
                "decisions": [
                    {
                        "opportunity_id": str(ids["opportunity"]),
                        "verdict": "VALIDATE",
                        "gates": [{"name": "score", "passed": True, "actual": 80, "required": 70}],
                        "round_number": 1,
                        "evidence_card_id": str(ids["card"]),
                        "score_snapshot_id": str(ids["score"]),
                        "critic_result_id": str(
                            uuid5(
                                context.run_id,
                                f"critic:1:{ids['opportunity']}",
                            )
                        ),
                        "gap_hypothesis_id": str(ids["gap"]),
                    }
                ]
            },
        ),
    ]


def _hypothesis_id(
    *,
    opportunity_id: str,
    evidence_card_id: str,
    canonical_problem_id: str,
    icp: str,
    job_to_be_done: str,
    trigger: str,
    current_behavior: str,
    pain: str,
    workflow_failure: str,
    falsification_test: str,
    supporting_claim_ids: list[str],
    contradicting_claim_ids: list[str],
) -> UUID:
    namespace = uuid5(NAMESPACE_URL, "https://gapforge.dev/v0.1/artifacts")
    identity = [
        "problem-hypothesis",
        opportunity_id,
        evidence_card_id,
        canonical_problem_id,
        normalize_text(icp),
        normalize_text(job_to_be_done),
        normalize_text(trigger),
        normalize_text(current_behavior),
        normalize_text(pain),
        normalize_text(workflow_failure),
        normalize_text(falsification_test),
        sorted(supporting_claim_ids),
        sorted(contradicting_claim_ids),
    ]
    canonical = json.dumps(
        identity,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return uuid5(namespace, canonical)
