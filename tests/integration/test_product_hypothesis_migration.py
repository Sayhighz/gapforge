from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, insert, select, text
from sqlalchemy.exc import DBAPIError

from gapforge.integration.persistence import ResearchArtifactWriter
from gapforge.integration.product_hypotheses import ProductHypothesisService
from gapforge.storage import models
from gapforge.storage.database import Database
from tests.integration.test_artifact_persistence import (
    _commit_stage,
    _seed_pipeline_context,
    _stage_payloads,
)

GUARD_REVISION = "e5b1a6c02f9d"
PRE_GUARD_REVISION = "d93e04b7a621"


def _config(url: str) -> Config:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return config


async def _clear_guarded_rows(url: str) -> None:
    database = Database.from_url(url)
    try:
        async with database.session() as session:
            exists = await session.scalar(
                text("SELECT to_regclass('product_hypotheses') IS NOT NULL")
            )
            if exists:
                await session.execute(text("ALTER TABLE product_hypotheses DISABLE TRIGGER USER"))
                await session.execute(text("DELETE FROM product_hypotheses"))
                await session.execute(text("ALTER TABLE product_hypotheses ENABLE TRIGGER USER"))
                await session.commit()
    finally:
        await database.dispose()


async def _seed_validated(
    url: str,
) -> tuple[models.MissionOpportunityAssessment, models.EvidenceCard]:
    database = Database.from_url(url)
    try:
        context, identifiers = await _seed_pipeline_context(database)
        writer = ResearchArtifactWriter()
        for stage, payload in _stage_payloads(context, identifiers):
            await _commit_stage(database, writer, context, stage, payload)
        async with database.session() as session:
            assessment = await session.scalar(
                select(models.MissionOpportunityAssessment).where(
                    models.MissionOpportunityAssessment.opportunity_id == identifiers["opportunity"]
                )
            )
            card = await session.get(models.EvidenceCard, identifiers["card"])
            assert assessment is not None and card is not None
            session.expunge(assessment)
            session.expunge(card)
            return assessment, card
    finally:
        await database.dispose()


async def _revision_and_product_count(url: str) -> tuple[str, int]:
    database = Database.from_url(url)
    try:
        async with database.session() as session:
            revision = await session.scalar(text("SELECT version_num FROM alembic_version"))
            count = await session.scalar(select(func.count()).select_from(models.ProductHypothesis))
            return str(revision), int(count or 0)
    finally:
        await database.dispose()


async def _product_hypothesis_triggers(url: str) -> set[str]:
    database = Database.from_url(url)
    try:
        async with database.session() as session:
            return set(
                await session.scalars(
                    text(
                        "SELECT tgname FROM pg_trigger "
                        "WHERE tgrelid = 'product_hypotheses'::regclass "
                        "AND NOT tgisinternal"
                    )
                )
            )
    finally:
        await database.dispose()


@pytest.mark.postgres
def test_empty_product_hypothesis_guard_upgrade_has_exact_catalog(postgres_url: str) -> None:
    config = _config(postgres_url)
    asyncio.run(_clear_guarded_rows(postgres_url))
    command.downgrade(config, "base")
    command.upgrade(config, PRE_GUARD_REVISION)
    command.upgrade(config, "head")

    async def inspect_catalog() -> tuple[set[str], set[str]]:
        database = Database.from_url(postgres_url)
        try:
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
                return constraints, triggers
        finally:
            await database.dispose()

    constraints, triggers = asyncio.run(inspect_catalog())
    assert {
        "ck_product_hypotheses_bounded_requested_by",
        "ck_product_hypotheses_bounded_content",
        "uq_product_hypotheses_assessment_request",
    } <= constraints
    assert triggers == {
        "trg_product_hypotheses_validate_insert",
        "trg_product_hypotheses_append_only",
    }


@pytest.mark.postgres
def test_guard_upgrade_refuses_legacy_rows_without_data_loss(postgres_url: str) -> None:
    config = _config(postgres_url)
    asyncio.run(_clear_guarded_rows(postgres_url))
    command.downgrade(config, "base")
    command.upgrade(config, PRE_GUARD_REVISION)
    assessment, card = asyncio.run(_seed_validated(postgres_url))

    async def insert_legacy() -> None:
        database = Database.from_url(postgres_url)
        try:
            async with database.session() as session:
                await session.execute(
                    insert(models.ProductHypothesis).values(
                        id=uuid4(),
                        assessment_id=assessment.id,
                        requested_by="legacy-request",
                        content={"proposition": "Legacy content without canonical schema"},
                        evidence_card_id=card.id,
                    )
                )
                await session.commit()
        finally:
            await database.dispose()

    asyncio.run(insert_legacy())
    with pytest.raises(DBAPIError, match="legacy Product Hypotheses"):
        command.upgrade(config, "head")
    assert asyncio.run(_revision_and_product_count(postgres_url)) == (PRE_GUARD_REVISION, 1)

    asyncio.run(_clear_guarded_rows(postgres_url))
    command.upgrade(config, "head")


@pytest.mark.postgres
def test_guard_downgrade_refuses_rows_without_data_loss(postgres_url: str) -> None:
    config = _config(postgres_url)
    asyncio.run(_clear_guarded_rows(postgres_url))
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    assessment, _ = asyncio.run(_seed_validated(postgres_url))

    async def create() -> None:
        database = Database.from_url(postgres_url)
        try:
            await ProductHypothesisService(database.session_factory).create(
                assessment.id,
                request_id="downgrade-guard",
                proposition="Preserve this append-only hypothesis",
            )
        finally:
            await database.dispose()

    asyncio.run(create())
    with pytest.raises(DBAPIError, match="cannot downgrade"):
        command.downgrade(config, PRE_GUARD_REVISION)
    assert asyncio.run(_revision_and_product_count(postgres_url)) == (GUARD_REVISION, 1)

    asyncio.run(_clear_guarded_rows(postgres_url))
    command.downgrade(config, PRE_GUARD_REVISION)
    assert asyncio.run(_product_hypothesis_triggers(postgres_url)) == {
        "trg_product_hypotheses_append_only"
    }
    command.upgrade(config, "head")
