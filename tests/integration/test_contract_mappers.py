from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from gapforge.domain.contracts import (
    AgentEffort,
    AgentRequest,
    Source,
    SourceCheckpoint,
    TaskStatus,
)
from gapforge.integration.mappers import (
    MappingError,
    agent_operation,
    agent_schema_identity,
    checkpoint_from_storage,
    checkpoint_to_storage,
    domain_identifier_from_storage,
    normalize_output_locale,
    storage_uuid_for_identifier,
    task_status_from_storage,
    task_status_to_storage,
)

NOW = datetime(2026, 8, 9, 12, tzinfo=UTC)


def test_task_status_mapping_is_explicit_and_bijective() -> None:
    pairs = {
        TaskStatus.QUEUED: "PENDING",
        TaskStatus.LEASED: "LEASED",
        TaskStatus.COMPLETED: "SUCCEEDED",
        TaskStatus.FAILED: "FAILED",
        TaskStatus.CANCELLED: "CANCELLED",
    }

    assert {status: task_status_to_storage(status) for status in TaskStatus} == pairs
    assert {stored: task_status_from_storage(stored) for stored in pairs.values()} == {
        stored: domain for domain, stored in pairs.items()
    }
    with pytest.raises(MappingError, match="unknown storage task status"):
        task_status_from_storage("COMPLETED")


def test_query_plan_is_a_bounded_medium_effort_operation() -> None:
    request = AgentRequest(
        call_id="call-query-plan",
        task="QUERY_PLAN",
        effort=AgentEffort.MEDIUM,
        input_json={"mission": "find recurring accounting pain"},
        permitted_evidence_ids=(),
        output_schema_name="query-plan-v1",
        timeout_seconds=30,
    )

    assert agent_operation(request) == "query_plan"
    with pytest.raises(ValidationError, match="effort"):
        request.model_copy(update={"effort": AgentEffort.HIGH}).model_validate(
            {**request.model_dump(), "effort": AgentEffort.HIGH}
        )


def test_checkpoint_mapping_preserves_none_cursor_and_watermark() -> None:
    checkpoint = SourceCheckpoint(source=Source.REDDIT, cursor=None, watermark=NOW)

    stored = checkpoint_to_storage(checkpoint)

    assert stored.cursor == {"kind": "opaque", "value": None}
    assert stored.watermark_at == NOW
    assert checkpoint_from_storage(Source.REDDIT, stored.cursor, stored.watermark_at) == checkpoint
    with pytest.raises(MappingError, match="checkpoint cursor"):
        checkpoint_from_storage(Source.REDDIT, {"value": "next", "extra": True}, NOW)


@pytest.mark.parametrize(
    ("raw", "canonical"),
    [("th", "th"), ("EN", "en"), ("en-us", "en-US"), ("TH-th", "th-TH")],
)
def test_output_locale_mapping_is_canonical(raw: str, canonical: str) -> None:
    assert normalize_output_locale(raw) == canonical


@pytest.mark.parametrize("raw", ["eng", "zh-Hant", "en-US-posix", "th_TH", ""])
def test_output_locale_mapping_rejects_lossy_locales(raw: str) -> None:
    with pytest.raises(MappingError, match="output locale"):
        normalize_output_locale(raw)


def test_string_identifiers_map_to_stable_uuid_without_losing_original() -> None:
    storage_id = storage_uuid_for_identifier("raw-signal-revision", "raw-1:r2")

    assert isinstance(storage_id, UUID)
    assert storage_id == storage_uuid_for_identifier("raw-signal-revision", "raw-1:r2")
    assert storage_id != storage_uuid_for_identifier("raw-signal", "raw-1:r2")
    assert (
        domain_identifier_from_storage("raw-signal-revision", storage_id, "raw-1:r2") == "raw-1:r2"
    )
    with pytest.raises(MappingError, match="does not match"):
        domain_identifier_from_storage("raw-signal-revision", uuid4(), "raw-1:r2")


def test_agent_schema_identity_is_canonical_and_operation_scoped() -> None:
    first = agent_schema_identity(
        operation="query_plan",
        schema_name="query-plan-v1",
        output_schema={"required": ["intents"], "type": "object"},
    )
    reordered = agent_schema_identity(
        operation="query_plan",
        schema_name="query-plan-v1",
        output_schema={"type": "object", "required": ["intents"]},
    )

    assert first == reordered
    assert len(first) == 32
    assert first != agent_schema_identity(
        operation="critic",
        schema_name="query-plan-v1",
        output_schema={"type": "object", "required": ["intents"]},
    )
