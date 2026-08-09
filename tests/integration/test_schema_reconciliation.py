from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import Numeric, inspect, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from gapforge.domain.contracts import (
    AtomicClaim as DomainAtomicClaim,
)
from gapforge.domain.contracts import (
    Citation,
    ClaimKind,
    EpistemicStatus,
    ScoreComponents,
    Source,
)
from gapforge.domain.contracts import (
    CompetitorEvidence as DomainCompetitorEvidence,
)
from gapforge.domain.contracts import (
    EvidenceCard as DomainEvidenceCard,
)
from gapforge.domain.contracts import (
    MissionOpportunityAssessment as DomainMissionOpportunityAssessment,
)
from gapforge.domain.contracts import (
    OpportunityScoreSnapshot as DomainScoreSnapshot,
)
from gapforge.integration.mappers import (
    assessment_from_storage,
    assessment_to_storage_values,
    atomic_claim_from_storage,
    atomic_claim_to_storage_values,
    competitor_evidence_from_storage,
    competitor_evidence_to_storage_values,
    evidence_card_from_storage,
    evidence_card_to_storage_values,
    score_snapshot_from_storage,
    score_snapshot_to_storage_values,
    storage_uuid_for_identifier,
)
from gapforge.storage.database import Database
from gapforge.storage.models import (
    AgentCall,
    AtomicClaim,
    CanonicalProblem,
    Competitor,
    CompetitorEvidence,
    EvidenceCard,
    GapHypothesis,
    MissionOpportunityAssessment,
    MissionRevision,
    Opportunity,
    OpportunityScoreSnapshot,
    PainSignal,
    RawSignalRevision,
    ResearchMission,
    ResearchRun,
)

NOW = datetime(2026, 8, 9, 12, tzinfo=UTC)


def test_reconciled_models_use_exact_and_lineage_preserving_columns() -> None:
    for column in (
        PainSignal.__table__.c.severity,
        PainSignal.__table__.c.frequency,
        OpportunityScoreSnapshot.__table__.c.evidence_strength,
        OpportunityScoreSnapshot.__table__.c.opportunity_fit,
        OpportunityScoreSnapshot.__table__.c.pre_penalty_score,
        OpportunityScoreSnapshot.__table__.c.final_score,
    ):
        assert isinstance(column.type, Numeric)
    assert PainSignal.__table__.c.severity.nullable is False
    assert PainSignal.__table__.c.frequency.nullable is False
    assert RawSignalRevision.__table__.c.domain_revision_id.nullable is False
    assert RawSignalRevision.__table__.c.duplicate_group_key.nullable is False
    assert EvidenceCard.__table__.c.opportunity_id.nullable is False
    assert MissionOpportunityAssessment.__table__.c.competitor_research_status.nullable is False
    assert AtomicClaim.__table__.c.citations.nullable is False
    assert AtomicClaim.__table__.c.contradicts_claim_ids.nullable is False
    assert CompetitorEvidence.__table__.c.claim_ids.nullable is False
    assert OpportunityScoreSnapshot.__table__.c.evidence_components.nullable is False
    assert OpportunityScoreSnapshot.__table__.c.opportunity_fit_components.nullable is False
    assert AgentCall.__table__.c.operation.nullable is False
    assert AgentCall.__table__.c.output_schema_name.nullable is False
    assert AgentCall.__table__.c.output_schema_sha256.nullable is False


@pytest.mark.postgres
async def test_reconciled_schema_constraints_and_candidate_indexes_exist(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    try:
        async with database.engine.connect() as connection:
            extension = await connection.scalar(
                text("SELECT extname FROM pg_extension WHERE extname = 'pg_trgm'")
            )
            constraints = set(
                await connection.scalars(
                    text(
                        "SELECT conname FROM pg_constraint "
                        "WHERE connamespace = current_schema()::regnamespace"
                    )
                )
            )
            constraint_rows = await connection.execute(
                text(
                    "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint "
                    "WHERE connamespace = current_schema()::regnamespace"
                )
            )
            constraint_definitions = dict(constraint_rows.tuples().all())
            indexes = set(
                await connection.scalars(
                    text("SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()")
                )
            )
            columns = await connection.run_sync(
                lambda sync: {
                    table: {column["name"]: column for column in inspect(sync).get_columns(table)}
                    for table in (
                        "pain_signals",
                        "raw_signal_revisions",
                        "opportunity_score_snapshots",
                    )
                }
            )

        assert extension == "pg_trgm"
        assert {
            "ck_pain_signals_severity_range",
            "ck_pain_signals_frequency_range",
            "ck_mission_opportunity_assessments_competitor_research_status",
            "ck_mission_opportunity_assessments_valid_verdict",
            "ck_agent_calls_output_schema_sha256_length",
            "ck_agent_calls_valid_operation",
            "ck_agent_calls_nonempty_output_schema_name",
            "ck_agent_calls_request_sha256_length",
            "ck_agent_calls_completed_output_sha256_length",
            "ck_provider_call_leases_schema_hash_length",
            "ck_provider_call_leases_request_hash_length",
            "ck_provider_call_leases_canonical_call_key",
            "ck_raw_signal_revisions_duplicate_group_key_format",
            "ck_opportunity_score_snapshots_evidence_strength_range",
            "ck_opportunity_score_snapshots_opportunity_fit_range",
        } <= constraints
        assert {
            "ix_raw_signals_canonical_url_trgm",
            "ix_raw_signal_revisions_search_document",
            "ix_raw_signal_revisions_search_text_trgm",
            "ix_raw_signal_revisions_duplicate_group",
        } <= indexes
        assert (
            "FOREIGN KEY (opportunity_id, canonical_problem_id) "
            "REFERENCES opportunities(id, canonical_problem_id) ON DELETE RESTRICT"
            in constraint_definitions["fk_evidence_cards_opportunity_id_opportunities"]
        )
        output_hash_constraint = constraint_definitions[
            "ck_agent_calls_completed_output_sha256_length"
        ]
        assert "(output_json IS NULL) = (output_sha256 IS NULL)" in output_hash_constraint
        assert "octet_length(output_sha256) = 32" in output_hash_constraint
        bounded_output_constraint = constraint_definitions["ck_agent_calls_bounded_object_output"]
        assert "octet_length((output_json)::text) <= 32768" in bounded_output_constraint
        assert columns["pain_signals"]["severity"]["nullable"] is False
        assert columns["pain_signals"]["frequency"]["nullable"] is False
        assert columns["raw_signal_revisions"]["domain_revision_id"]["nullable"] is False
        assert "search_document" in columns["raw_signal_revisions"]
        assert columns["raw_signal_revisions"]["search_document"]["nullable"] is False
        assert columns["raw_signal_revisions"]["search_text"]["nullable"] is False
        assert columns["opportunity_score_snapshots"]["pre_penalty_score"]["nullable"] is False
    finally:
        await database.dispose()


def test_exact_numeric_model_values_do_not_round_through_binary_float() -> None:
    pain = PainSignal(
        raw_signal_revision_id="00000000-0000-0000-0000-000000000001",
        extraction_version="extract-v1",
        cluster_status="UNCLUSTERED",
        pain="Manual work",
        severity=Decimal("0.12345678901234567"),
        frequency=Decimal("0.98765432109876543"),
        signals={},
        confidence=Decimal("0.87654321098765432"),
        excerpt="manual work",
    )

    assert pain.severity == Decimal("0.12345678901234567")
    assert pain.frequency == Decimal("0.98765432109876543")


@pytest.mark.postgres
async def test_research_evidence_claim_and_score_round_trip_with_typed_mappers(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    mission = ResearchMission(id=uuid4(), status="DRAFT", title="Accounting pain")
    revision = MissionRevision(
        id=uuid4(),
        mission_id=mission.id,
        revision_number=1,
        change_reason="initial mission",
        mission_text="Find recurring accounting pain",
        original_language="en",
        output_locale="en",
        interpretation={},
    )
    run = ResearchRun(
        id=uuid4(),
        mission_revision_id=revision.id,
        mode="HUNT",
        status="RUNNING",
        priority=1,
        deadline_at=NOW + timedelta(minutes=30),
        started_at=NOW,
        budget_limits={"max_agent_calls_per_run": 6},
        budget_used={"agent_calls": 0},
    )
    problem = CanonicalProblem(
        id=uuid4(),
        canonical_key="manual-reconciliation",
        title="Manual reconciliation",
        summary="Recurring reconciliation work",
        status="ACTIVE",
    )
    gap = GapHypothesis(
        id=uuid4(),
        canonical_problem_id=problem.id,
        gap_type="WORKFLOW",
        statement="Current tools leave a manual workflow gap",
        user_evidence_ids=[uuid4()],
        competitor_evidence_ids=[uuid4()],
        contradicting_claim_ids=[],
    )
    opportunity = Opportunity(
        id=uuid4(),
        canonical_problem_id=problem.id,
        gap_hypothesis_id=gap.id,
        canonical_key="manual-reconciliation-workflow",
        title="Automate reconciliation",
    )
    domain_assessment = DomainMissionOpportunityAssessment(
        id=str(uuid4()),
        mission_revision_id=revision.id,
        opportunity_id=str(opportunity.id),
        lifecycle_state="RESEARCHING",
        relevance=0.8123456789012345,
        verdict="RESEARCH_MORE",
        competitor_research_status="COMPLETE",
        assessed_at=NOW,
    )
    assessment = MissionOpportunityAssessment(
        **assessment_to_storage_values(domain_assessment),
        updated_at=NOW,
    )
    revision_storage_id = storage_uuid_for_identifier("raw-signal-revision", "raw-1:r1")
    claim_id = uuid4()
    domain_card = DomainEvidenceCard(
        id=str(uuid4()),
        opportunity_id=str(opportunity.id),
        known_author_ids=("author-1", "author-2"),
        thread_ids=("thread-1", "thread-2"),
        user_sources=(Source.HACKER_NEWS, Source.REDDIT),
        observed_days=(NOW,),
        severity=0.81234,
        behavioral_workarounds=2,
        paid_or_wtp_signals=1,
        supporting_claim_ids=(str(claim_id),),
        representative_evidence_ids=("raw-1:r1",),
        confidence=0.7654321098765432,
        missing_evidence=("more recent evidence",),
    )
    domain_claim = DomainAtomicClaim(
        id=str(claim_id),
        text="Captured page lists $20 per month",
        kind=ClaimKind.PRICE,
        status=EpistemicStatus.SUPPORTED,
        evidence_ids=("raw-1:r1",),
        citations=(
            Citation(
                evidence_id="raw-1:r1",
                source_url="https://example.com/pricing",
                excerpt="$20 per month",
                observed_at=NOW,
            ),
        ),
    )
    competitor = Competitor(
        id=uuid4(),
        name="LegacyBooks",
        normalized_name="legacybooks",
        canonical_url="https://example.com",
        alternative_type="SAAS",
    )
    domain_competitor_evidence = DomainCompetitorEvidence(
        id=str(uuid4()),
        competitor_id=str(competitor.id),
        source_url="https://example.com/pricing",
        captured_excerpt="$20 per month",
        observed_at=NOW,
        content_hash="b" * 64,
        evidence_kind="PRICE_PAGE",
        metadata={"currency": "USD"},
        claim_ids=(str(claim_id),),
    )
    domain_score = DomainScoreSnapshot(
        id=str(uuid4()),
        opportunity_id=str(opportunity.id),
        mission_revision_id=revision.id,
        evidence_strength=ScoreComponents(
            values={"severity": 0.12345678901234567}, weights={"severity": 1.0}
        ),
        opportunity_fit=ScoreComponents(
            values={"gap_strength": 71.875}, weights={"gap_strength": 1.0}
        ),
        raw_metrics={"authors": 5.0},
        penalties={"concentration": 2.125},
        pre_penalty_score=0.23456789012345678,
        final_score=0.12345678901234567,
        evidence_confidence=0.8765432109876543,
        explanation=("independent evidence",),
        created_at=NOW,
    )
    stored_card = EvidenceCard(
        **evidence_card_to_storage_values(
            domain_card,
            canonical_problem_id=problem.id,
            run_id=run.id,
            algorithm_version="evidence-v1",
            revision_ids={"raw-1:r1": revision_storage_id},
            claim_ids={str(claim_id): claim_id},
        )
    )
    stored_claim = AtomicClaim(
        **atomic_claim_to_storage_values(
            domain_claim,
            subject_type="OPPORTUNITY",
            subject_id=opportunity.id,
            revision_ids={"raw-1:r1": revision_storage_id},
        )
    )
    stored_score = OpportunityScoreSnapshot(
        **score_snapshot_to_storage_values(domain_score, assessment_id=assessment.id, run_id=run.id)
    )
    stored_competitor_evidence = CompetitorEvidence(
        **competitor_evidence_to_storage_values(domain_competitor_evidence)
    )
    try:
        async with database.session() as session:
            session.add(mission)
            await session.flush()
            session.add(revision)
            await session.flush()
            session.add_all((run, problem, competitor))
            await session.flush()
            session.add(gap)
            await session.flush()
            session.add(opportunity)
            await session.flush()
            session.add(assessment)
            await session.flush()
            session.add_all((stored_card, stored_claim, stored_score, stored_competitor_evidence))
            await session.commit()

        async with database.session() as session:
            card_row = await session.get(EvidenceCard, stored_card.id)
            claim_row = await session.get(AtomicClaim, stored_claim.id)
            score_row = await session.get(OpportunityScoreSnapshot, stored_score.id)
            assessment_row = await session.get(MissionOpportunityAssessment, assessment.id)
            competitor_evidence_row = await session.get(
                CompetitorEvidence, stored_competitor_evidence.id
            )
            assert card_row is not None
            assert claim_row is not None
            assert score_row is not None
            assert assessment_row is not None
            assert competitor_evidence_row is not None
            revisions = {revision_storage_id: "raw-1:r1"}
            assert (
                evidence_card_from_storage(
                    card_row,
                    revision_identifiers=revisions,
                    claim_identifiers={claim_id: str(claim_id)},
                )
                == domain_card
            )
            assert (
                atomic_claim_from_storage(claim_row, revision_identifiers=revisions) == domain_claim
            )
            assert (
                score_snapshot_from_storage(
                    score_row,
                    opportunity_id=opportunity.id,
                    mission_revision_id=revision.id,
                )
                == domain_score
            )
            assert assessment_from_storage(assessment_row) == domain_assessment
            assert (
                competitor_evidence_from_storage(competitor_evidence_row)
                == domain_competitor_evidence
            )

            for operation, schema_name in (("UNKNOWN", "schema-v1"), ("query_plan", " ")):
                with pytest.raises(IntegrityError):
                    async with session.begin_nested():
                        session.add(
                            AgentCall(
                                run_id=run.id,
                                provider="fake",
                                operation=operation,
                                output_schema_name=schema_name,
                                output_schema_sha256=b"s" * 32,
                                request_sha256=b"r" * 32,
                                requested_model="",
                                effort="medium",
                                status="COMPLETED",
                                duration_ms=1,
                                repair_attempts=0,
                                usage={},
                                output_json={"ok": True},
                                output_sha256=b"o" * 32,
                            )
                        )
                        await session.flush()

            for status, output_json, output_sha256 in (
                ("FAILED", None, b"x" * 32),
                ("COMPLETED", {"ok": True}, None),
                ("COMPLETED", {"ok": True}, b"short"),
            ):
                with pytest.raises(IntegrityError):
                    async with session.begin_nested():
                        session.add(
                            AgentCall(
                                run_id=run.id,
                                provider="fake",
                                operation="extract",
                                output_schema_name="schema-v1",
                                output_schema_sha256=b"s" * 32,
                                request_sha256=b"r" * 32,
                                requested_model="",
                                effort="medium",
                                status=status,
                                duration_ms=1,
                                repair_attempts=0,
                                usage={},
                                output_json=output_json,
                                output_sha256=output_sha256,
                            )
                        )
                        await session.flush()

            invalid_score_values = score_snapshot_to_storage_values(
                domain_score,
                assessment_id=assessment.id,
                run_id=run.id,
            )
            invalid_score_values["id"] = uuid4()
            invalid_score_values["evidence_strength"] = Decimal("-0.1")
            with pytest.raises(IntegrityError):
                async with session.begin_nested():
                    session.add(OpportunityScoreSnapshot(**invalid_score_values))
                    await session.flush()
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_fts_and_trigram_candidate_queries_use_revision_indexes(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    try:
        async with database.engine.connect() as connection:
            await connection.execute(text("SET enable_seqscan = off"))
            trigram_plan = "\n".join(
                str(row[0])
                for row in await connection.execute(
                    text(
                        "EXPLAIN SELECT id FROM raw_signal_revisions "
                        "WHERE search_text % 'manual reconciliation'"
                    )
                )
            )
            fts_plan = "\n".join(
                str(row[0])
                for row in await connection.execute(
                    text(
                        "EXPLAIN SELECT id FROM raw_signal_revisions "
                        "WHERE search_document @@ plainto_tsquery('simple', 'reconciliation')"
                    )
                )
            )
        assert "ix_raw_signal_revisions_search_text_trgm" in trigram_plan
        assert "ix_raw_signal_revisions_search_document" in fts_plan
    finally:
        await database.dispose()


def _migration_config(postgres_url: str) -> Config:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url.replace("%", "%%"))
    return config


@pytest.mark.postgres
async def test_reconciliation_migration_upgrades_downgrades_and_reupgrades(
    postgres_url: str,
) -> None:
    config = _migration_config(postgres_url)

    await asyncio.to_thread(command.downgrade, config, "base")
    await asyncio.to_thread(command.upgrade, config, "head")
    await asyncio.to_thread(command.downgrade, config, "132931969d6b")
    await asyncio.to_thread(command.upgrade, config, "head")

    database = Database.from_url(postgres_url)
    try:
        async with database.engine.connect() as connection:
            revision = await connection.scalar(text("SELECT version_num FROM alembic_version"))
        assert revision == ScriptDirectory.from_config(config).get_current_head()
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_reconciliation_migration_transforms_supported_legacy_rows(
    postgres_url: str,
) -> None:
    config = _migration_config(postgres_url)
    await asyncio.to_thread(command.downgrade, config, "base")
    await asyncio.to_thread(command.upgrade, config, "132931969d6b")
    names = (
        "mission",
        "revision",
        "run",
        "problem",
        "gap",
        "opportunity",
        "assessment",
        "card",
        "score",
        "call",
    )
    ids = {name: uuid4() for name in names}
    database = Database.from_url(postgres_url)
    try:
        async with database.engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO research_missions (id, status, title) "
                    "VALUES (:id, 'DRAFT', 'Legacy mission')"
                ),
                {"id": ids["mission"]},
            )
            await connection.execute(
                text(
                    "INSERT INTO mission_revisions "
                    "(id, mission_id, revision_number, change_reason, mission_text, "
                    "original_language, output_locale, interpretation) VALUES "
                    "(:id, :mission_id, 1, 'initial', 'legacy mission', 'en', 'en', '{}')"
                ),
                {"id": ids["revision"], "mission_id": ids["mission"]},
            )
            await connection.execute(
                text(
                    "INSERT INTO research_runs "
                    "(id, mission_revision_id, mode, status, priority, deadline_at, "
                    "budget_limits, budget_used, warnings, last_checkpoint) VALUES "
                    "(:id, :revision_id, 'HUNT', 'COMPLETED', 1, :deadline, "
                    "'{}', '{}', '[]', '{}')"
                ),
                {"id": ids["run"], "revision_id": ids["revision"], "deadline": NOW},
            )
            await connection.execute(
                text(
                    "INSERT INTO canonical_problems "
                    "(id, canonical_key, title, summary, status) "
                    "VALUES (:id, 'legacy-problem', 'Legacy', 'Legacy problem', 'ACTIVE')"
                ),
                {"id": ids["problem"]},
            )
            await connection.execute(
                text(
                    "INSERT INTO gap_hypotheses "
                    "(id, canonical_problem_id, gap_type, statement, user_evidence_ids, "
                    "competitor_evidence_ids, contradicting_claim_ids) VALUES "
                    "(:id, :problem_id, 'WORKFLOW', 'Legacy gap', "
                    "ARRAY[:evidence_id]::uuid[], ARRAY[:competitor_id]::uuid[], "
                    "ARRAY[]::uuid[])"
                ),
                {
                    "id": ids["gap"],
                    "problem_id": ids["problem"],
                    "evidence_id": uuid4(),
                    "competitor_id": uuid4(),
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO opportunities "
                    "(id, canonical_problem_id, gap_hypothesis_id, canonical_key, title) "
                    "VALUES (:id, :problem_id, :gap_id, 'legacy-opportunity', 'Legacy')"
                ),
                {
                    "id": ids["opportunity"],
                    "problem_id": ids["problem"],
                    "gap_id": ids["gap"],
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO mission_opportunity_assessments "
                    "(id, mission_revision_id, opportunity_id, lifecycle_status, "
                    "relevance, verdict) "
                    "VALUES (:id, :revision_id, :opportunity_id, 'DISCOVERED', 0.8, NULL)"
                ),
                {
                    "id": ids["assessment"],
                    "revision_id": ids["revision"],
                    "opportunity_id": ids["opportunity"],
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO evidence_cards "
                    "(id, canonical_problem_id, run_id, algorithm_version, independent_authors, "
                    "independent_threads, source_count, metrics, supporting_claim_ids, "
                    "contradicting_claim_ids, representative_signal_ids, confidence, "
                    "missing_evidence) VALUES "
                    "(:id, :problem_id, :run_id, 'legacy-v1', 5, 3, 2, '{}', "
                    "ARRAY[]::uuid[], ARRAY[]::uuid[], ARRAY[]::uuid[], 0.8, '[]')"
                ),
                {
                    "id": ids["card"],
                    "problem_id": ids["problem"],
                    "run_id": ids["run"],
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO opportunity_score_snapshots "
                    "(id, assessment_id, run_id, algorithm_version, raw_metrics, "
                    "evidence_strength, opportunity_fit, weights, penalties, final_score, "
                    "confidence, explanation) VALUES "
                    "(:id, :assessment_id, :run_id, 'legacy-v1', '{}', 80, 75, "
                    "'{\"legacy_axis\": 1}', '{\"concentration\": 2}', 75.45, 0.8, '[]')"
                ),
                {
                    "id": ids["score"],
                    "assessment_id": ids["assessment"],
                    "run_id": ids["run"],
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO agent_calls "
                    "(id, run_id, provider, requested_model, effort, status, duration_ms, "
                    "repair_attempts, usage) VALUES "
                    "(:id, :run_id, 'fake', '', 'medium', 'SUCCESS', 10, 0, '{}')"
                ),
                {"id": ids["call"], "run_id": ids["run"]},
            )
        await database.dispose()

        await asyncio.to_thread(command.upgrade, config, "4d8f8a2c7b31")

        database = Database.from_url(postgres_url)
        async with database.engine.connect() as connection:
            score = (
                (
                    await connection.execute(
                        text(
                            "SELECT evidence_components, opportunity_fit_components, weights, "
                            "pre_penalty_score FROM opportunity_score_snapshots WHERE id = :id"
                        ),
                        {"id": ids["score"]},
                    )
                )
                .mappings()
                .one()
            )
            call = (
                (
                    await connection.execute(
                        text(
                            "SELECT operation, output_schema_name, output_schema_sha256 "
                            "FROM agent_calls WHERE id = :id"
                        ),
                        {"id": ids["call"]},
                    )
                )
                .mappings()
                .one()
            )
            assessment = (
                (
                    await connection.execute(
                        text(
                            "SELECT competitor_research_status "
                            "FROM mission_opportunity_assessments "
                            "WHERE id = :id"
                        ),
                        {"id": ids["assessment"]},
                    )
                )
                .mappings()
                .one()
            )
            card_opportunity = await connection.scalar(
                text("SELECT opportunity_id FROM evidence_cards WHERE id = :id"),
                {"id": ids["card"]},
            )
        assert score["evidence_components"] == {
            "values": {"legacy_axis": 80},
            "weights": {"legacy_axis": 1},
        }
        assert score["opportunity_fit_components"]["values"] == {"legacy_axis": 75}
        assert score["weights"] == {
            "evidence": {"legacy_axis": 1},
            "opportunity_fit": {"legacy_axis": 1},
        }
        assert score["pre_penalty_score"] == Decimal("77.45966692414830000")
        assert call["operation"] == "legacy_unknown"
        assert call["output_schema_name"] == "legacy-unknown-v0"
        assert len(call["output_schema_sha256"]) == 32
        assert assessment["competitor_research_status"] == "INCOMPLETE"
        assert card_opportunity == ids["opportunity"]
        await database.dispose()

        with pytest.raises(DBAPIError, match="legacy agent calls lack replay output"):
            await asyncio.to_thread(command.upgrade, config, "head")
        database = Database.from_url(postgres_url)
        async with database.engine.begin() as connection:
            await connection.execute(text("TRUNCATE agent_calls CASCADE"))
        await database.dispose()
        await asyncio.to_thread(command.upgrade, config, "head")

        await asyncio.to_thread(command.downgrade, config, "132931969d6b")
        await asyncio.to_thread(command.upgrade, config, "head")
        database = Database.from_url(postgres_url)
        async with database.engine.connect() as connection:
            remigrated_weights = await connection.scalar(
                text("SELECT weights FROM opportunity_score_snapshots WHERE id = :id"),
                {"id": ids["score"]},
            )
        assert remigrated_weights == {
            "evidence": {"legacy_axis": 1},
            "opportunity_fit": {"legacy_axis": 1},
        }
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_reconciliation_migration_refuses_invented_raw_revision_lineage(
    postgres_url: str,
) -> None:
    config = _migration_config(postgres_url)
    await asyncio.to_thread(command.downgrade, config, "base")
    await asyncio.to_thread(command.upgrade, config, "132931969d6b")
    raw_id = uuid4()
    revision_id = uuid4()
    database = Database.from_url(postgres_url)
    try:
        async with database.engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO raw_signals "
                    "(id, source, external_id, canonical_url, author_kind, collected_at, "
                    "is_tombstone) VALUES "
                    "(:id, 'HACKER_NEWS', 'legacy-42', 'https://example.com/42', 'KNOWN', "
                    ":observed, false)"
                ),
                {"id": raw_id, "observed": NOW},
            )
            await connection.execute(
                text(
                    "INSERT INTO raw_signal_revisions "
                    "(id, raw_signal_id, revision_number, title, body, original_language, "
                    "engagement, source_metadata, content_hash, normalization_version, "
                    "observed_at, is_tombstone) VALUES "
                    "(:id, :raw_id, 1, 'Legacy', 'Legacy body', 'en', '{}', '{}', "
                    ":content_hash, '1', :observed, false)"
                ),
                {
                    "id": revision_id,
                    "raw_id": raw_id,
                    "content_hash": b"a" * 32,
                    "observed": NOW,
                },
            )
        await database.dispose()

        with pytest.raises(DBAPIError, match="migration refused"):
            await asyncio.to_thread(command.upgrade, config, "head")

        database = Database.from_url(postgres_url)
        async with database.engine.connect() as connection:
            version = await connection.scalar(text("SELECT version_num FROM alembic_version"))
            columns = await connection.run_sync(
                lambda sync: {
                    column["name"] for column in inspect(sync).get_columns("raw_signal_revisions")
                }
            )
        assert version == "132931969d6b"
        assert "domain_revision_id" not in columns
        assert "duplicate_group_key" not in columns

        await database.dispose()
        await asyncio.to_thread(command.downgrade, config, "base")
        await asyncio.to_thread(command.upgrade, config, "head")
    finally:
        await database.dispose()
