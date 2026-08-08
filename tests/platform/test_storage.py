from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError, IntegrityError

from gapforge.storage.database import Database
from gapforge.storage.models import MissionRevision, ResearchRun
from gapforge.storage.uow import SqlAlchemyUnitOfWork


@pytest.mark.postgres
async def test_mission_revisions_are_immutable_and_linked(migrated_postgres_url: str) -> None:
    database = Database.from_url(migrated_postgres_url)
    try:
        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            mission, first = await uow.missions.create_with_revision(
                title="Thai accounting pain",
                mission_text="ค้นหาปัญหาบัญชีของธุรกิจขนาดเล็ก",
                original_language="th",
                output_locale="th",
            )
            await uow.commit()

        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            second = await uow.missions.revise(
                mission.id,
                mission_text="ค้นหาปัญหาบัญชีและภาษีของธุรกิจขนาดเล็ก",
                change_reason="include tax workflows",
                original_language="th",
                output_locale="th",
            )
            await uow.commit()

        assert second.revision_number == 2
        assert second.parent_revision_id == first.id

        async with database.session() as session:
            persisted = await session.get(MissionRevision, first.id)
            assert persisted is not None
            persisted.change_reason = "mutated"
            with pytest.raises(ValueError, match="append-only"):
                await session.commit()
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_one_global_active_run_is_database_enforced(migrated_postgres_url: str) -> None:
    database = Database.from_url(migrated_postgres_url)
    first_run_id = None
    try:
        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            _, revision_a = await uow.missions.create_with_revision(
                title="Mission A",
                mission_text="mission a",
                original_language="en",
                output_locale="en",
            )
            _, revision_b = await uow.missions.create_with_revision(
                title="Mission B",
                mission_text="mission b",
                original_language="en",
                output_locale="en",
            )
            await uow.commit()

        async with database.session() as session:
            first_run = ResearchRun(
                mission_revision_id=revision_a.id,
                mode="HUNT",
                status="RUNNING",
                priority=1,
                deadline_at=datetime.now(UTC) + timedelta(minutes=30),
                budget_limits={"max_agent_calls_per_run": 6},
                budget_used={"agent_calls": 0},
            )
            session.add(first_run)
            await session.commit()
            first_run_id = first_run.id

        async with database.session() as session:
            session.add(
                ResearchRun(
                    mission_revision_id=revision_b.id,
                    mode="HUNT",
                    status="QUEUED",
                    priority=1,
                    deadline_at=datetime.now(UTC) + timedelta(minutes=30),
                    budget_limits={"max_agent_calls_per_run": 6},
                    budget_used={"agent_calls": 0},
                )
            )
            with pytest.raises(IntegrityError):
                await session.commit()
    finally:
        if first_run_id is not None:
            async with database.session() as session:
                run = await session.get(ResearchRun, first_run_id)
                assert run is not None
                run.status = "COMPLETED"
                run.completed_at = datetime.now(UTC)
                await session.commit()
        await database.dispose()


@pytest.mark.postgres
async def test_database_append_only_trigger_blocks_direct_update(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    try:
        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            _, revision = await uow.missions.create_with_revision(
                title="Immutable mission",
                mission_text="immutable",
                original_language="en",
                output_locale="en",
            )
            await uow.commit()

        with pytest.raises(DBAPIError, match="append-only"):
            async with database.engine.begin() as connection:
                await connection.execute(
                    MissionRevision.__table__.update()
                    .where(MissionRevision.id == revision.id)
                    .values(change_reason="illegal")
                )
    finally:
        await database.dispose()


def test_model_contains_complete_required_entity_set() -> None:
    assert {
        "research_missions",
        "mission_revisions",
        "research_runs",
        "research_tasks",
        "source_checkpoints",
        "raw_signals",
        "raw_signal_revisions",
        "pain_signals",
        "canonical_problems",
        "problem_clusters",
        "problem_cluster_memberships",
        "merge_candidates",
        "evidence_cards",
        "atomic_claims",
        "problem_hypotheses",
        "competitors",
        "competitor_evidence",
        "gap_hypotheses",
        "opportunities",
        "mission_opportunity_assessments",
        "opportunity_score_snapshots",
        "critic_results",
        "product_hypotheses",
        "lifecycle_events",
        "agent_calls",
    } == set(MissionRevision.metadata.tables)


@pytest.mark.postgres
async def test_repository_round_trip_without_handwritten_sql(migrated_postgres_url: str) -> None:
    database = Database.from_url(migrated_postgres_url)
    try:
        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            mission, revision = await uow.missions.create_with_revision(
                title="Repository mission",
                mission_text="find persistent operational pains",
                original_language="en",
                output_locale="en",
                interpretation={"market": "SMB"},
            )
            await uow.commit()

        async with database.session() as session:
            rows = (
                await session.scalars(
                    select(MissionRevision).where(MissionRevision.mission_id == mission.id)
                )
            ).all()
        assert len(rows) == 1
        assert rows[0].id == revision.id
        assert rows[0].interpretation == {"market": "SMB"}
    finally:
        await database.dispose()
