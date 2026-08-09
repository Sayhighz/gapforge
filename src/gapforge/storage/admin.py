"""Strictly bounded read-only SQL escape hatch for administrators."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_FORBIDDEN = re.compile(
    r"\b(?:alter|analyze|call|comment|copy|create|delete|do|drop|execute|grant|insert|"
    r"listen|load|lock|merge|notify|prepare|refresh|reindex|reset|revoke|set|truncate|"
    r"unlisten|update|vacuum)\b",
    re.IGNORECASE,
)
_DANGEROUS_FUNCTIONS = re.compile(
    r"\b(?:dblink|lo_export|lo_import|pg_ls_dir|pg_read_binary_file|pg_read_file|"
    r"pg_sleep|set_config)\s*\(",
    re.IGNORECASE,
)
_FUNCTION_CALL = re.compile(r"(?<!\w)(?:[a-z_][a-z0-9_]*\.)?([a-z_][a-z0-9_]*)\s*\(", re.IGNORECASE)
_NON_FUNCTION_CALL_WORDS = frozenset({"as", "exists", "from", "in", "over", "values"})
_SAFE_FUNCTIONS = frozenset(
    {
        "abs",
        "avg",
        "cast",
        "char_length",
        "coalesce",
        "count",
        "current_setting",
        "date_trunc",
        "greatest",
        "least",
        "length",
        "lower",
        "max",
        "min",
        "nullif",
        "round",
        "substring",
        "sum",
        "upper",
    }
)


@dataclass(frozen=True, slots=True)
class QueryResult:
    columns: tuple[str, ...]
    rows: tuple[dict[str, Any], ...]
    truncated: bool


def validate_read_only_sql(query: str) -> str:
    normalized = query.strip()
    if not normalized:
        raise ValueError("query cannot be empty")
    if len(normalized.encode()) > 10_000:
        raise ValueError("query exceeds 10 KB")
    if ";" in normalized or "--" in normalized or "/*" in normalized or "*/" in normalized:
        raise ValueError("comments and multiple statements are not allowed")
    if not re.match(r"^select\b", normalized, re.IGNORECASE):
        raise ValueError("only a SELECT statement is allowed")
    if _FORBIDDEN.search(normalized) or _DANGEROUS_FUNCTIONS.search(normalized):
        raise ValueError("query contains a prohibited operation")
    called_functions = {
        match.casefold()
        for match in _FUNCTION_CALL.findall(normalized)
        if match.casefold() not in _NON_FUNCTION_CALL_WORDS
    }
    if unsupported := called_functions - _SAFE_FUNCTIONS:
        names = ", ".join(sorted(unsupported))
        raise ValueError(f"query calls a non-allowlisted function: {names}")
    if re.search(r"\bfor\s+(?:update|share)\b", normalized, re.IGNORECASE):
        raise ValueError("locking SELECT statements are not allowed")
    return normalized


async def execute_read_only_sql(
    session: AsyncSession,
    query: str,
    *,
    row_limit: int = 100,
    timeout_ms: int = 2000,
) -> QueryResult:
    if not 1 <= row_limit <= 1000:
        raise ValueError("row_limit must be between 1 and 1000")
    if not 100 <= timeout_ms <= 5000:
        raise ValueError("timeout_ms must be between 100 and 5000")
    validated = validate_read_only_sql(query)
    await session.execute(text("SET TRANSACTION READ ONLY"))
    await session.execute(
        text("SELECT set_config('statement_timeout', :timeout, true)"),
        {"timeout": f"{timeout_ms}ms"},
    )
    # The interpolation is safe because validate_read_only_sql applies a strict single-SELECT
    # grammar and function allowlist; values remain bound parameters.
    bounded_query = f"SELECT * FROM ({validated}) AS gapforge_bounded_query LIMIT :row_limit"  # noqa: S608
    result = await session.execute(text(bounded_query), {"row_limit": row_limit + 1})
    mappings = result.mappings().all()
    rows = tuple(
        {str(key): _json_value(value) for key, value in mapping.items()}
        for mapping in mappings[:row_limit]
    )
    return QueryResult(
        columns=tuple(str(key) for key in result.keys()),
        rows=rows,
        truncated=len(mappings) > row_limit,
    )


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (date, datetime, Decimal, UUID)):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (list, dict)):
        return value
    return str(value)
