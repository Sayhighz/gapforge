from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from collections.abc import Iterator
from pathlib import Path
from uuid import UUID, uuid4, uuid5

import pytest
from sqlalchemy import delete, insert, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql import Executable
from typer.testing import CliRunner

from gapforge.cli import app
from gapforge.integration.persistence import ResearchArtifactWriter
from gapforge.integration.product_hypotheses import ProductHypothesisService
from gapforge.storage import models
from gapforge.storage.database import Database
from tests.integration.test_artifact_persistence import (
    FakeContext,
    _commit_stage,
    _seed_followup_context,
    _seed_pipeline_context,
    _stage_payloads,
)

runner = CliRunner()


@pytest.fixture(autouse=True)
def _isolate_application_rows(migrated_postgres_url: str) -> Iterator[None]:
    async def cleanup() -> None:
        database = Database.from_url(migrated_postgres_url)
        try:
            async with database.session() as session:
                existing = set(
                    await session.scalars(
                        text(
                            "SELECT tablename FROM pg_tables "
                            "WHERE schemaname = 'public' AND tablename <> 'alembic_version'"
                        )
                    )
                )
                tables = sorted(existing & set(models.Base.metadata.tables))
                if tables:
                    quoted = ", ".join(f'"{table}"' for table in tables)
                    await session.execute(text(f"TRUNCATE TABLE {quoted} RESTART IDENTITY CASCADE"))
                    await session.commit()
        finally:
            await database.dispose()

    asyncio.run(cleanup())
    yield
    asyncio.run(cleanup())


async def _seed_validated_run(
    database_url: str,
) -> tuple[FakeContext, dict[str, UUID | str], UUID, UUID]:
    database = Database.from_url(database_url)
    try:
        context, identifiers = await _seed_pipeline_context(database, output_locale="th")
        writer = ResearchArtifactWriter()
        for stage, payload in _stage_payloads(context, identifiers):
            await _commit_stage(database, writer, context, stage, payload)
        async with database.session() as session:
            assessment = await session.scalar(
                select(models.MissionOpportunityAssessment).where(
                    models.MissionOpportunityAssessment.opportunity_id == identifiers["opportunity"]
                )
            )
            assert assessment is not None
            snapshot = await session.scalar(
                select(models.FinalAssessmentSnapshot).where(
                    models.FinalAssessmentSnapshot.assessment_id == assessment.id
                )
            )
            assert snapshot is not None
            return context, identifiers, assessment.id, snapshot.evidence_card_id
    finally:
        await database.dispose()


async def _seed_monitor_snapshot(
    database_url: str,
    initial_context: FakeContext,
    identifiers: dict[str, UUID | str],
) -> UUID:
    database = Database.from_url(database_url)
    try:
        context = await _seed_followup_context(database, initial_context)
        monitor_ids = dict(identifiers)
        monitor_ids["gap"] = uuid4()
        monitor_ids["card"] = uuid5(
            context.run_id,
            f"card:{monitor_ids['opportunity']}",
        )
        monitor_ids["score"] = uuid5(
            context.run_id,
            f"score:{monitor_ids['opportunity']}",
        )
        stages = {
            stage: copy.deepcopy(payload)
            for stage, payload in _stage_payloads(context, monitor_ids)
        }
        stages["GAP"]["gaps"][0]["statement"] = (
            "Later monitoring confirms this exact validated opportunity"
        )
        writer = ResearchArtifactWriter()
        for stage in ("GAP", "CARD_SCORE", "HYPOTHESIS", "CRITIC", "FINAL"):
            await _commit_stage(database, writer, context, stage, stages[stage])
        card_id = monitor_ids["card"]
        assert isinstance(card_id, UUID)
        return card_id
    finally:
        await database.dispose()


async def _seed_researching_assessment(database_url: str) -> UUID:
    database = Database.from_url(database_url)
    try:
        context, identifiers = await _seed_pipeline_context(database, output_locale="th")
        writer = ResearchArtifactWriter()
        for stage, payload in _stage_payloads(context, identifiers):
            if stage not in {"EXTRACT", "CLUSTER", "GAP"}:
                continue
            await _commit_stage(database, writer, context, stage, payload)
        async with database.session() as session:
            assessment = await session.scalar(
                select(models.MissionOpportunityAssessment).where(
                    models.MissionOpportunityAssessment.opportunity_id == identifiers["opportunity"]
                )
            )
            assert assessment is not None
            assert assessment.lifecycle_status == "RESEARCHING"
            return assessment.id
    finally:
        await database.dispose()


def _invoke_json(arguments: list[str]) -> tuple[dict[str, object], int, str]:
    result = runner.invoke(app, [*arguments, "--json"])
    assert result.stdout.count("\n") == 1
    return json.loads(result.stdout), result.exit_code, result.stderr


@pytest.mark.postgres
def test_report_cli_persists_terminal_run_and_renders_opportunity_on_demand(
    migrated_postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    context, identifiers, _, _ = asyncio.run(_seed_validated_run(migrated_postgres_url))
    reports_dir = tmp_path / "reports"
    monkeypatch.setenv("DATABASE_URL", migrated_postgres_url)
    monkeypatch.setenv("AGENT_PROVIDER", "fake")
    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))

    envelope, exit_code, stderr = _invoke_json(["report", "run", str(context.run_id)])

    assert exit_code == 0
    assert stderr == ""
    data = envelope["data"]
    assert isinstance(data, dict)
    report = data["report"]
    artifact = data["artifact"]
    assert report["run_id"] == str(context.run_id)
    assert report["output_locale"] == "th"
    expected_path = reports_dir / "2026-08-09" / f"run-{context.run_id}.md"
    assert artifact["run_path"] == str(expected_path)
    assert artifact["latest_path"] == str(reports_dir / "latest.md")
    assert artifact["is_latest"] is True
    content = expected_path.read_text(encoding="utf-8")
    assert content.startswith("# รายงานการวิจัย GapForge\n")
    assert hashlib.sha256(content.encode()).hexdigest() == artifact["content_sha256"]
    latest_before = (reports_dir / "latest.md").read_bytes()
    latest_mtime = (reports_dir / "latest.md").stat().st_mtime_ns

    detail, exit_code, stderr = _invoke_json(
        ["report", "opportunity", str(identifiers["opportunity"])]
    )

    assert exit_code == 0
    assert stderr == ""
    detail_data = detail["data"]
    assert isinstance(detail_data, dict)
    assert detail_data["report"]["opportunity"]["opportunity_id"] == str(identifiers["opportunity"])
    assert "## เกณฑ์การตรวจสอบ" in detail_data["markdown"]
    assert (reports_dir / "latest.md").read_bytes() == latest_before
    assert (reports_dir / "latest.md").stat().st_mtime_ns == latest_mtime


@pytest.mark.postgres
def test_product_hypothesis_cli_is_explicit_idempotent_and_queryable(
    migrated_postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, assessment_id, evidence_card_id = asyncio.run(_seed_validated_run(migrated_postgres_url))
    monkeypatch.setenv("DATABASE_URL", migrated_postgres_url)
    monkeypatch.setenv("AGENT_PROVIDER", "fake")
    arguments = [
        "product-hypothesis",
        "create",
        str(assessment_id),
        "--request-id",
        "request-001",
        "--proposition",
        "Automate month-end reconciliation with reviewed exception queues",
    ]

    created, exit_code, stderr = _invoke_json(arguments)
    repeated, repeated_exit, _ = _invoke_json(arguments)

    assert exit_code == repeated_exit == 0
    assert stderr == ""
    assert created["data"] == repeated["data"]
    expected_id = uuid5(assessment_id, "product-hypothesis:request-001")
    assert created["data"]["id"] == str(expected_id)
    assert created["data"]["assessment_id"] == str(assessment_id)
    assert created["data"]["explicit_request_id"] == "request-001"
    assert created["data"]["proposition"].startswith("Automate month-end")

    shown, show_exit, _ = _invoke_json(["product-hypothesis", "show", str(expected_id)])
    assert show_exit == 0
    assert shown["data"] == created["data"]

    conflict, conflict_exit, conflict_stderr = _invoke_json(
        [*arguments[:-1], "A changed proposition reusing the request identity"]
    )
    assert conflict_exit == 4
    assert conflict_stderr == ""
    assert conflict["error"]["code"] == "CONFLICT"

    async def assert_storage() -> None:
        database = Database.from_url(migrated_postgres_url)
        try:
            async with database.session() as session:
                row = await session.get(models.ProductHypothesis, expected_id)
                assert row is not None
                assert row.evidence_card_id == evidence_card_id
                assert row.requested_by == "request-001"
                assert row.content == {
                    "schema_version": "0.1",
                    "proposition": created["data"]["proposition"],
                }
        finally:
            await database.dispose()

    asyncio.run(assert_storage())


@pytest.mark.postgres
def test_product_hypothesis_cli_rejects_non_validate_assessment(
    migrated_postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context, identifiers, assessment_id, evidence_card_id = asyncio.run(
        _seed_validated_run(migrated_postgres_url)
    )
    monkeypatch.setenv("DATABASE_URL", migrated_postgres_url)
    original_arguments = [
        "product-hypothesis",
        "create",
        str(assessment_id),
        "--request-id",
        "request-before-monitor",
        "--proposition",
        "Preserve this exact validated snapshot",
    ]
    original, original_exit, _ = _invoke_json(original_arguments)
    assert original_exit == 0

    later_card_id = asyncio.run(_seed_monitor_snapshot(migrated_postgres_url, context, identifiers))
    assert later_card_id != evidence_card_id
    replayed, replay_exit, _ = _invoke_json(original_arguments)
    assert replay_exit == 0
    assert replayed["data"] == original["data"]

    conflict, conflict_exit, _ = _invoke_json(
        [*original_arguments[:-1], "Changed content after MONITOR"]
    )
    assert conflict_exit == 4
    assert conflict["error"]["code"] == "CONFLICT"

    researching_assessment_id = asyncio.run(_seed_researching_assessment(migrated_postgres_url))
    result, exit_code, stderr = _invoke_json(
        [
            "product-hypothesis",
            "create",
            str(researching_assessment_id),
            "--request-id",
            "request-blocked",
            "--proposition",
            "This must not persist",
        ]
    )

    assert exit_code == 4
    assert stderr == ""
    assert result["error"]["code"] == "INVALID_STATE"

    async def assert_original_card() -> None:
        database = Database.from_url(migrated_postgres_url)
        try:
            async with database.session() as session:
                row = await session.get(
                    models.ProductHypothesis,
                    UUID(str(original["data"]["id"])),
                )
                assert row is not None
                assert row.evidence_card_id == evidence_card_id
        finally:
            await database.dispose()

    asyncio.run(assert_original_card())


@pytest.mark.postgres
async def test_product_hypothesis_database_enforces_lineage_bounds_and_append_only(
    migrated_postgres_url: str,
) -> None:
    first = await _seed_validated_run(migrated_postgres_url)
    second = await _seed_validated_run(migrated_postgres_url)
    _, _, assessment_id, evidence_card_id = first
    _, _, _, other_card_id = second
    database = Database.from_url(migrated_postgres_url)
    try:
        service = ProductHypothesisService(database.session_factory)
        product = await service.create(
            assessment_id,
            request_id="database-guard",
            proposition="Persist only against the exact VALIDATE snapshot",
        )
        product_id = UUID(product.id)
        unicode_product = await service.create(
            assessment_id,
            request_id="unicode-boundary",
            proposition="🧾" * 20_000,
        )
        assert len(unicode_product.proposition) == 20_000
        with pytest.raises(ValueError, match="20000"):
            await service.create(
                assessment_id,
                request_id="unicode-over-limit",
                proposition="🧾" * 20_001,
            )
        async with database.session() as session:
            constraints = set(
                await session.scalars(
                    text(
                        "SELECT conname FROM pg_constraint "
                        "WHERE conrelid = 'product_hypotheses'::regclass"
                    )
                )
            )
            triggers = set(
                await session.scalars(
                    text(
                        "SELECT tgname FROM pg_trigger "
                        "WHERE tgrelid = 'product_hypotheses'::regclass "
                        "AND NOT tgisinternal"
                    )
                )
            )
            assert {
                "ck_product_hypotheses_bounded_requested_by",
                "ck_product_hypotheses_bounded_content",
                "uq_product_hypotheses_assessment_request",
            } <= constraints
            assert {
                "trg_product_hypotheses_validate_insert",
                "trg_product_hypotheses_append_only",
            } <= triggers

        async def rejected(statement: Executable) -> None:
            async with database.session() as session:
                with pytest.raises(DBAPIError):
                    async with session.begin_nested():
                        await session.execute(statement)

        await rejected(
            insert(models.ProductHypothesis).values(
                id=uuid4(),
                assessment_id=assessment_id,
                requested_by="mismatched-card",
                content={"schema_version": "0.1", "proposition": "Wrong card"},
                evidence_card_id=other_card_id,
            )
        )
        await rejected(
            insert(models.ProductHypothesis).values(
                id=uuid4(),
                assessment_id=assessment_id,
                requested_by="",
                content={"schema_version": "0.1", "proposition": "Empty request"},
                evidence_card_id=evidence_card_id,
            )
        )
        await rejected(
            insert(models.ProductHypothesis).values(
                id=uuid4(),
                assessment_id=assessment_id,
                requested_by="database-guard",
                content={
                    "schema_version": "0.1",
                    "proposition": "A conflicting duplicate request identity",
                },
                evidence_card_id=evidence_card_id,
            )
        )
        await rejected(
            insert(models.ProductHypothesis).values(
                id=uuid4(),
                assessment_id=assessment_id,
                requested_by="extra-content-key",
                content={
                    "schema_version": "0.1",
                    "proposition": "Bounded content",
                    "invented": True,
                },
                evidence_card_id=evidence_card_id,
            )
        )
        await rejected(
            update(models.ProductHypothesis)
            .where(models.ProductHypothesis.id == product_id)
            .values(content={"schema_version": "0.1", "proposition": "Mutated"})
        )
        await rejected(
            delete(models.ProductHypothesis).where(models.ProductHypothesis.id == product_id)
        )

        async with database.session() as session:
            stored = await session.get(models.ProductHypothesis, product_id)
            assert stored is not None
            assert stored.content["proposition"] == product.proposition
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_product_hypothesis_insert_serializes_against_assessment_lock(
    migrated_postgres_url: str,
) -> None:
    _, _, assessment_id, evidence_card_id = await _seed_validated_run(migrated_postgres_url)
    database = Database.from_url(migrated_postgres_url)
    first = database.session_factory()
    second = database.session_factory()
    try:
        locked_assessment = await first.scalar(
            select(models.MissionOpportunityAssessment)
            .where(models.MissionOpportunityAssessment.id == assessment_id)
            .with_for_update()
        )
        assert locked_assessment is not None
        product_id = uuid4()

        async def direct_insert() -> None:
            await second.execute(
                insert(models.ProductHypothesis).values(
                    id=product_id,
                    assessment_id=assessment_id,
                    requested_by="racing-request",
                    content={
                        "schema_version": "0.1",
                        "proposition": "Must recheck after the assessment lock releases",
                    },
                    evidence_card_id=evidence_card_id,
                )
            )
            await second.commit()

        insertion = asyncio.create_task(direct_insert())
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(insertion), timeout=0.1)
        await first.commit()
        await insertion
        async with database.session() as session:
            stored = await session.get(models.ProductHypothesis, product_id)
            assert stored is not None
            assert stored.evidence_card_id == evidence_card_id
    finally:
        await first.close()
        await second.close()
        await database.dispose()
