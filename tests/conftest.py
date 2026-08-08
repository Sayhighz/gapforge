from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from alembic import command
from alembic.config import Config


@pytest.fixture(scope="session")
def postgres_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
    return url


@pytest.fixture(scope="session")
def migrated_postgres_url(postgres_url: str) -> Iterator[str]:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url.replace("%", "%%"))
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    yield postgres_url
