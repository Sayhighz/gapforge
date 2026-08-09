from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from gapforge.integration.persistence import (
    MergeDecisionConflictError,
    MergeDecisionService,
)
from gapforge.integration.queries import ResearchQueryService
from gapforge.storage.database import Database
from gapforge.storage.models import (
    CanonicalProblem,
    GapHypothesis,
    LifecycleEvent,
    MergeCandidate,
    MergeDecisionEvent,
    MissionOpportunityAssessment,
    MissionRevision,
    Opportunity,
    ResearchMission,
    ResearchRun,
)

NOW = datetime(2026, 8, 9, 12, tzinfo=UTC)


@pytest.mark.postgres
async def test_merge_decisions_are_reversible_and_append_only(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    left = CanonicalProblem(
        id=uuid4(),
        canonical_key=f"left-{uuid4()}",
        title="Manual reconciliation",
        summary="Manual reconciliation",
        status="ACTIVE",
    )
    right = CanonicalProblem(
        id=uuid4(),
        canonical_key=f"right-{uuid4()}",
        title="Spreadsheet reconciliation",
        summary="Spreadsheet reconciliation",
        status="ACTIVE",
    )
    candidate = MergeCandidate(
        id=uuid4(),
        left_problem_id=left.id,
        right_problem_id=right.id,
        similarity=0.91,
        rationale="Potentially the same workflow pain",
        status="PENDING",
    )
    left_gap = GapHypothesis(
        id=uuid4(),
        canonical_problem_id=left.id,
        gap_type="WORKFLOW",
        statement="Left gap",
        user_evidence_ids=[uuid4()],
        competitor_evidence_ids=[uuid4()],
        contradicting_claim_ids=[],
    )
    right_gap = GapHypothesis(
        id=uuid4(),
        canonical_problem_id=right.id,
        gap_type="WORKFLOW",
        statement="Right gap",
        user_evidence_ids=[uuid4()],
        competitor_evidence_ids=[uuid4()],
        contradicting_claim_ids=[],
    )
    left_opportunity = Opportunity(
        id=uuid4(),
        canonical_problem_id=left.id,
        gap_hypothesis_id=left_gap.id,
        canonical_key=f"left-opportunity-{uuid4()}",
        title="Left opportunity",
    )
    right_opportunity = Opportunity(
        id=uuid4(),
        canonical_problem_id=right.id,
        gap_hypothesis_id=right_gap.id,
        canonical_key=f"right-opportunity-{uuid4()}",
        title="Right opportunity",
    )
    try:
        async with database.session() as session:
            session.add_all((left, right))
            await session.flush()
            session.add_all((candidate, left_gap, right_gap))
            await session.flush()
            session.add_all((left_opportunity, right_opportunity))
            await session.commit()

        service = MergeDecisionService(database.session_factory, clock=lambda: NOW)
        accepted = await service.decide(
            candidate.id,
            action="ACCEPT",
            actor="local-owner",
            reason="Reviewed the source lineage",
        )
        queries = ResearchQueryService(database.session_factory)
        accepted_opportunities = await queries.opportunities()
        assert {
            tuple(item["equivalent_problem_ids"])
            for item in accepted_opportunities
            if item["canonical_problem_id"] in {left.id, right.id}
        } == {(left.id,), (right.id,)}
        reversed_decision = await service.decide(
            candidate.id,
            action="REVERSE",
            actor="local-owner",
            reason="Later evidence separates the problems",
        )
        assert (accepted.from_status, accepted.to_status) == ("PENDING", "ACCEPTED")
        assert (reversed_decision.from_status, reversed_decision.to_status) == (
            "ACCEPTED",
            "REVERSED",
        )
        assert await queries.merge_history(candidate.id) == [
            {
                "id": accepted.id,
                "candidate_id": candidate.id,
                "decision_number": 1,
                "action": "ACCEPT",
                "from_status": "PENDING",
                "to_status": "ACCEPTED",
                "actor": "local-owner",
                "reason": "Reviewed the source lineage",
                "created_at": NOW,
            },
            {
                "id": reversed_decision.id,
                "candidate_id": candidate.id,
                "decision_number": 2,
                "action": "REVERSE",
                "from_status": "ACCEPTED",
                "to_status": "REVERSED",
                "actor": "local-owner",
                "reason": "Later evidence separates the problems",
                "created_at": NOW,
            },
        ]
        reversed_opportunities = await queries.opportunities()
        assert all(
            item["equivalent_problem_ids"] == []
            for item in reversed_opportunities
            if item["canonical_problem_id"] in {left.id, right.id}
        )

        async with database.session() as session:
            stored = await session.get(MergeCandidate, candidate.id)
            history = tuple(
                (
                    await session.scalars(
                        select(MergeDecisionEvent)
                        .where(MergeDecisionEvent.candidate_id == candidate.id)
                        .order_by(MergeDecisionEvent.decision_number)
                    )
                ).all()
            )
            assert stored is not None
            assert stored.status == "REVERSED"
            assert stored.decided_by == "local-owner"
            assert stored.decision_event_id == reversed_decision.id
            assert [event.action for event in history] == ["ACCEPT", "REVERSE"]
            assert [event.decision_number for event in history] == [1, 2]
            assert [event.reason for event in history] == [
                "Reviewed the source lineage",
                "Later evidence separates the problems",
            ]

            history[0].reason = "rewrite history"
            with pytest.raises(ValueError, match="append-only"):
                await session.flush()
            await session.rollback()
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_merge_decision_state_machine_rejects_invalid_or_duplicate_actions(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    left = CanonicalProblem(
        id=uuid4(),
        canonical_key=f"left-{uuid4()}",
        title="Left",
        summary="Left",
        status="ACTIVE",
    )
    right = CanonicalProblem(
        id=uuid4(),
        canonical_key=f"right-{uuid4()}",
        title="Right",
        summary="Right",
        status="ACTIVE",
    )
    candidate = MergeCandidate(
        id=uuid4(),
        left_problem_id=left.id,
        right_problem_id=right.id,
        similarity=0.75,
        rationale="Review required",
        status="PENDING",
    )
    try:
        async with database.session() as session:
            session.add_all((left, right, candidate))
            await session.commit()
        service = MergeDecisionService(database.session_factory, clock=lambda: NOW)

        with pytest.raises(MergeDecisionConflictError, match="PENDING"):
            await service.decide(
                candidate.id,
                action="REVERSE",
                actor="owner",
                reason="Nothing to reverse",
            )
        await service.decide(
            candidate.id,
            action="REJECT",
            actor="owner",
            reason="Distinct problems",
        )
        with pytest.raises(MergeDecisionConflictError, match="PENDING"):
            await service.decide(
                candidate.id,
                action="ACCEPT",
                actor="owner",
                reason="Changed mind without reversal",
            )
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_concurrent_merge_decisions_serialize_on_candidate_version(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    left, right, candidate = _candidate_rows()
    try:
        async with database.session() as session:
            session.add_all((left, right, candidate))
            await session.commit()
        service = MergeDecisionService(database.session_factory, clock=lambda: NOW)

        results = await asyncio.gather(
            service.decide(
                candidate.id,
                action="ACCEPT",
                actor="owner-a",
                reason="First review",
            ),
            service.decide(
                candidate.id,
                action="REJECT",
                actor="owner-b",
                reason="Concurrent review",
            ),
            return_exceptions=True,
        )

        assert sum(isinstance(item, MergeDecisionEvent) for item in results) == 1
        assert sum(isinstance(item, MergeDecisionConflictError) for item in results) == 1
        async with database.session() as session:
            versions = tuple(
                await session.scalars(
                    select(MergeDecisionEvent.decision_number).where(
                        MergeDecisionEvent.candidate_id == candidate.id
                    )
                )
            )
            assert versions == (1,)
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_merge_decision_database_constraints_reject_invalid_audit_rows_and_links(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    left, right, candidate = _candidate_rows()
    other_left, other_right, other_candidate = _candidate_rows()
    try:
        async with database.session() as session:
            session.add_all((left, right, candidate, other_left, other_right, other_candidate))
            await session.commit()
        async with database.engine.connect() as connection:
            constraints = set(
                await connection.scalars(
                    text(
                        "SELECT conname FROM pg_constraint "
                        "WHERE connamespace = current_schema()::regnamespace"
                    )
                )
            )
        assert {
            "ck_merge_candidates_valid_status",
            "ck_merge_decision_events_valid_action",
            "ck_merge_decision_events_valid_from_status",
            "ck_merge_decision_events_valid_to_status",
            "ck_merge_decision_events_valid_transition",
            "ck_merge_decision_events_bounded_actor",
            "ck_merge_decision_events_bounded_reason",
        } <= constraints

        invalid_rows = (
            ("UNKNOWN", "PENDING", "ACCEPTED", "actor", "reason"),
            ("ACCEPT", "ACCEPTED", "ACCEPTED", "actor", "reason"),
            ("REVERSE", "ACCEPTED", "REVERSED", " ", "reason"),
            ("REJECT", "PENDING", "REJECTED", "actor", "x" * 501),
        )
        for action, from_status, to_status, actor, reason in invalid_rows:
            async with database.session() as session:
                with pytest.raises(IntegrityError):
                    await session.execute(
                        text(
                            "INSERT INTO merge_decision_events "
                            "(id, candidate_id, decision_number, action, from_status, "
                            "to_status, actor, reason) VALUES "
                            "(:id, :candidate_id, 1, :action, :from_status, "
                            ":to_status, :actor, :reason)"
                        ),
                        {
                            "id": uuid4(),
                            "candidate_id": candidate.id,
                            "action": action,
                            "from_status": from_status,
                            "to_status": to_status,
                            "actor": actor,
                            "reason": reason,
                        },
                    )
                    await session.commit()

        async with database.session() as session:
            with pytest.raises(DBAPIError, match="from_status does not match candidate"):
                await session.execute(
                    text(
                        "INSERT INTO merge_decision_events "
                        "(id, candidate_id, decision_number, action, from_status, "
                        "to_status, actor, reason) VALUES "
                        "(:id, :candidate_id, 1, 'REVERSE', 'ACCEPTED', "
                        "'REVERSED', 'actor', 'orphan reverse')"
                    ),
                    {"id": uuid4(), "candidate_id": candidate.id},
                )
                await session.commit()
        async with database.session() as session:
            with pytest.raises(DBAPIError, match="next candidate version"):
                await session.execute(
                    text(
                        "INSERT INTO merge_decision_events "
                        "(id, candidate_id, decision_number, action, from_status, "
                        "to_status, actor, reason) VALUES "
                        "(:id, :candidate_id, 2, 'ACCEPT', 'PENDING', "
                        "'ACCEPTED', 'actor', 'skipped version')"
                    ),
                    {"id": uuid4(), "candidate_id": candidate.id},
                )
                await session.commit()

        direct_left, direct_right, direct_candidate = _candidate_rows()
        direct_event_id = uuid4()
        async with database.session() as session:
            session.add_all((direct_left, direct_right, direct_candidate))
            await session.commit()
        async with database.session() as session:
            await session.execute(
                text(
                    "INSERT INTO merge_decision_events "
                    "(id, candidate_id, decision_number, action, from_status, "
                    "to_status, actor, reason) VALUES "
                    "(:id, :candidate_id, 1, 'ACCEPT', 'PENDING', "
                    "'ACCEPTED', 'direct-owner', 'direct review')"
                ),
                {"id": direct_event_id, "candidate_id": direct_candidate.id},
            )
            await session.commit()
        async with database.session() as session:
            projected = await session.get(MergeCandidate, direct_candidate.id)
            assert projected is not None
            assert projected.status == "ACCEPTED"
            assert projected.decision_event_id == direct_event_id
            with pytest.raises(DBAPIError, match="from_status does not match candidate"):
                await session.execute(
                    text(
                        "INSERT INTO merge_decision_events "
                        "(id, candidate_id, decision_number, action, from_status, "
                        "to_status, actor, reason) VALUES "
                        "(:id, :candidate_id, 2, 'ACCEPT', 'PENDING', "
                        "'ACCEPTED', 'direct-owner', 'duplicate accept')"
                    ),
                    {"id": uuid4(), "candidate_id": direct_candidate.id},
                )
                await session.commit()

        service = MergeDecisionService(database.session_factory, clock=lambda: NOW)
        event = await service.decide(
            candidate.id,
            action="ACCEPT",
            actor="owner",
            reason="Reviewed",
        )
        async with database.session() as session:
            with pytest.raises(DBAPIError, match="does not match candidate"):
                await session.execute(
                    text(
                        "UPDATE merge_candidates SET decision_event_id = :event_id "
                        "WHERE id = :candidate_id"
                    ),
                    {"event_id": event.id, "candidate_id": other_candidate.id},
                )
                await session.commit()
        async with database.session() as session:
            with pytest.raises(DBAPIError, match="projection"):
                await session.execute(
                    text(
                        "UPDATE merge_candidates SET status = 'REVERSED' WHERE id = :candidate_id"
                    ),
                    {"candidate_id": candidate.id},
                )
                await session.commit()
        async with database.session() as session:
            with pytest.raises(DBAPIError, match="append-only"):
                await session.execute(
                    text(
                        "UPDATE merge_decision_events SET reason = 'rewritten' WHERE id = :event_id"
                    ),
                    {"event_id": event.id},
                )
                await session.commit()
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_migration_preserves_populated_legacy_lifecycle_link_on_upgrade_and_downgrade(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    try:
        async with database.engine.begin() as connection:
            await connection.execute(
                text("TRUNCATE merge_decision_events, merge_candidates CASCADE")
            )
    finally:
        await database.dispose()
    candidate_id, lifecycle_event_id = await _seed_legacy_link(migrated_postgres_url)
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", migrated_postgres_url.replace("%", "%%"))

    await asyncio.to_thread(command.downgrade, config, "8f6c2b4d9a10")
    try:
        database = Database.from_url(migrated_postgres_url)
        try:
            async with database.engine.connect() as connection:
                assert (
                    await connection.scalar(
                        text("SELECT lifecycle_event_id FROM merge_candidates WHERE id = :id"),
                        {"id": candidate_id},
                    )
                    == lifecycle_event_id
                )
        finally:
            await database.dispose()

        await asyncio.to_thread(command.upgrade, config, "head")
        database = Database.from_url(migrated_postgres_url)
        try:
            async with database.engine.connect() as connection:
                row = (
                    await connection.execute(
                        text(
                            "SELECT lifecycle_event_id, decision_event_id "
                            "FROM merge_candidates WHERE id = :id"
                        ),
                        {"id": candidate_id},
                    )
                ).one()
                assert row.lifecycle_event_id == lifecycle_event_id
                assert row.decision_event_id is None
        finally:
            await database.dispose()

        await asyncio.to_thread(command.downgrade, config, "8f6c2b4d9a10")
        database = Database.from_url(migrated_postgres_url)
        try:
            async with database.engine.connect() as connection:
                assert (
                    await connection.scalar(
                        text("SELECT lifecycle_event_id FROM merge_candidates WHERE id = :id"),
                        {"id": candidate_id},
                    )
                    == lifecycle_event_id
                )
        finally:
            await database.dispose()
    finally:
        await asyncio.to_thread(command.upgrade, config, "head")


def _candidate_rows() -> tuple[CanonicalProblem, CanonicalProblem, MergeCandidate]:
    left = CanonicalProblem(
        id=uuid4(),
        canonical_key=f"left-{uuid4()}",
        title="Left",
        summary="Left",
        status="ACTIVE",
    )
    right = CanonicalProblem(
        id=uuid4(),
        canonical_key=f"right-{uuid4()}",
        title="Right",
        summary="Right",
        status="ACTIVE",
    )
    return (
        left,
        right,
        MergeCandidate(
            id=uuid4(),
            left_problem_id=left.id,
            right_problem_id=right.id,
            similarity=0.75,
            rationale="Review required",
            status="PENDING",
        ),
    )


async def _seed_legacy_link(database_url: str) -> tuple[UUID, UUID]:
    database = Database.from_url(database_url)
    mission = ResearchMission(id=uuid4(), status="DRAFT", title="Legacy merge")
    revision = MissionRevision(
        id=uuid4(),
        mission_id=mission.id,
        revision_number=1,
        change_reason="initial",
        mission_text="Legacy merge",
        original_language="en",
        output_locale="en",
        interpretation={},
    )
    run = ResearchRun(
        id=uuid4(),
        mission_revision_id=revision.id,
        mode="HUNT",
        status="COMPLETED",
        priority=1,
        deadline_at=NOW,
        started_at=NOW,
        completed_at=NOW,
        budget_limits={},
        budget_used={},
        warnings=[],
        last_checkpoint={},
    )
    left, right, candidate = _candidate_rows()
    gap = GapHypothesis(
        id=uuid4(),
        canonical_problem_id=left.id,
        gap_type="WORKFLOW",
        statement="Legacy gap",
        user_evidence_ids=[uuid4()],
        competitor_evidence_ids=[uuid4()],
        contradicting_claim_ids=[],
    )
    opportunity = Opportunity(
        id=uuid4(),
        canonical_problem_id=left.id,
        gap_hypothesis_id=gap.id,
        canonical_key=f"opportunity-{uuid4()}",
        title="Legacy opportunity",
    )
    assessment = MissionOpportunityAssessment(
        id=uuid4(),
        mission_revision_id=revision.id,
        opportunity_id=opportunity.id,
        lifecycle_status="DISCOVERED",
        relevance=0.8,
        verdict=None,
        competitor_research_status="INCOMPLETE",
    )
    lifecycle = LifecycleEvent(
        id=uuid4(),
        assessment_id=assessment.id,
        event_number=1,
        run_id=run.id,
        gap_hypothesis_id=gap.id,
        from_status=None,
        to_status="DISCOVERED",
        reason="legacy merge review",
        details={},
    )
    candidate.lifecycle_event_id = lifecycle.id
    try:
        async with database.session() as session:
            session.add_all((mission, left, right))
            await session.flush()
            session.add(revision)
            await session.flush()
            session.add_all((run, gap))
            await session.flush()
            session.add(opportunity)
            await session.flush()
            session.add(assessment)
            await session.flush()
            session.add(lifecycle)
            await session.flush()
            session.add(candidate)
            await session.commit()
        return candidate.id, lifecycle.id
    finally:
        await database.dispose()
