from __future__ import annotations

import pytest

from gapforge.storage.admin import execute_read_only_sql, validate_read_only_sql
from gapforge.storage.database import Database


@pytest.mark.parametrize(
    "query",
    [
        "DELETE FROM research_missions",
        "SELECT 1; SELECT 2",
        "SELECT 1 -- hidden mutation",
        "SELECT pg_terminate_backend(pg_backend_pid())",
        "SELECT nextval('dangerous_sequence')",
        "SELECT lo_unlink(1)",
        "SELECT public.user_defined_function()",
        "SELECT * FROM research_missions FOR UPDATE",
    ],
)
def test_read_only_sql_rejects_mutation_and_non_allowlisted_functions(query: str) -> None:
    with pytest.raises(ValueError):
        validate_read_only_sql(query)


@pytest.mark.postgres
async def test_read_only_sql_executes_with_timeout_and_row_bound(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    try:
        async with database.session() as session:
            result = await execute_read_only_sql(
                session,
                "SELECT 'a' AS value, upper('a') AS upper_value "
                "UNION ALL SELECT 'b', upper('b') "
                "UNION ALL SELECT 'c', upper('c')",
                row_limit=2,
                timeout_ms=500,
            )
        assert result.columns == ("value", "upper_value")
        assert result.rows == (
            {"value": "a", "upper_value": "A"},
            {"value": "b", "upper_value": "B"},
        )
        assert result.truncated is True
    finally:
        await database.dispose()


@pytest.mark.postgres
async def test_read_only_transaction_blocks_sequence_side_effect_if_validator_is_bypassed(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    try:
        async with database.session() as session:
            with pytest.raises(Exception, match="read-only"):
                await session.execute(__import__("sqlalchemy").text("SET TRANSACTION READ ONLY"))
                await session.execute(
                    __import__("sqlalchemy").text("CREATE TEMP TABLE should_fail(id int)")
                )
    finally:
        await database.dispose()
