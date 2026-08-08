from __future__ import annotations

import asyncio

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text

from gapforge.storage.database import Database
from gapforge.storage.models import Base


async def _inspect_database(url: str) -> tuple[set[str], str]:
    database = Database.from_url(url)
    try:
        async with database.engine.connect() as connection:
            tables = await connection.run_sync(
                lambda sync_connection: inspect(sync_connection).get_table_names()
            )
            revision = await connection.scalar(text("SELECT version_num FROM alembic_version"))
        return set(tables), str(revision)
    finally:
        await database.dispose()


def test_clean_upgrade_has_every_model_table(postgres_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url.replace("%", "%%"))

    command.downgrade(config, "base")
    command.upgrade(config, "head")

    tables, revision = asyncio.run(_inspect_database(postgres_url))
    assert set(Base.metadata.tables) <= tables
    assert revision == "369e6194e3cb"


def test_migration_round_trip(postgres_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url.replace("%", "%%"))

    command.downgrade(config, "base")
    command.upgrade(config, "head")

    tables, _ = asyncio.run(_inspect_database(postgres_url))
    assert "research_runs" in tables
