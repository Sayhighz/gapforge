"""Explicit, lossless mappings between research and platform contracts."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid5

from gapforge.domain.contracts import (
    AgentRequest,
    SemanticOperation,
    Source,
    SourceCheckpoint,
    TaskStatus,
)

_IDENTIFIER_NAMESPACE = UUID("de9d3b28-a802-5ce6-86bb-1864f13e59c4")
_LOCALE_PATTERN = re.compile(r"^(?P<language>[A-Za-z]{2})(?:-(?P<region>[A-Za-z]{2}))?$")

_TASK_STATUS_TO_STORAGE = {
    TaskStatus.QUEUED: "PENDING",
    TaskStatus.LEASED: "LEASED",
    TaskStatus.COMPLETED: "SUCCEEDED",
    TaskStatus.FAILED: "FAILED",
    TaskStatus.CANCELLED: "CANCELLED",
}
_TASK_STATUS_FROM_STORAGE = {value: key for key, value in _TASK_STATUS_TO_STORAGE.items()}


class MappingError(ValueError):
    """A platform value cannot be represented by the canonical domain contract."""


@dataclass(frozen=True, slots=True)
class StorageCheckpoint:
    """Persistence-shaped checkpoint values without a database dependency."""

    cursor: dict[str, Any]
    watermark_at: datetime | None


def task_status_to_storage(status: TaskStatus) -> str:
    """Translate canonical task lifecycle names to queue persistence names."""

    try:
        return _TASK_STATUS_TO_STORAGE[status]
    except KeyError as exc:  # pragma: no cover - exhaustive enum guard
        raise MappingError(f"unknown domain task status: {status!r}") from exc


def task_status_from_storage(status: str) -> TaskStatus:
    """Translate a persisted queue status without silently accepting aliases."""

    try:
        return _TASK_STATUS_FROM_STORAGE[status]
    except KeyError as exc:
        raise MappingError(f"unknown storage task status: {status!r}") from exc


def agent_operation(request: AgentRequest) -> str:
    """Return the provider/storage operation identifier for a semantic request."""

    return request.task.value.lower()


def checkpoint_to_storage(checkpoint: SourceCheckpoint) -> StorageCheckpoint:
    """Wrap an opaque source cursor so `None` does not collapse into an empty object."""

    return StorageCheckpoint(
        cursor={"kind": "opaque", "value": checkpoint.cursor},
        watermark_at=checkpoint.watermark,
    )


def checkpoint_from_storage(
    source: Source,
    cursor: dict[str, Any],
    watermark_at: datetime | None,
) -> SourceCheckpoint:
    """Rebuild a domain checkpoint, rejecting unknown or structurally lossy JSON."""

    if set(cursor) != {"kind", "value"} or cursor.get("kind") != "opaque":
        raise MappingError("checkpoint cursor must be an opaque cursor envelope")
    value = cursor["value"]
    if value is not None and not isinstance(value, str):
        raise MappingError("checkpoint cursor value must be a string or null")
    return SourceCheckpoint(source=source, cursor=value, watermark=watermark_at)


def normalize_output_locale(value: str) -> str:
    """Canonicalize the deliberately bounded v0.1 language/region locale form."""

    match = _LOCALE_PATTERN.fullmatch(value)
    if match is None:
        raise MappingError("output locale must be a two-letter language and optional region")
    language = match.group("language").lower()
    region = match.group("region")
    return language if region is None else f"{language}-{region.upper()}"


def storage_uuid_for_identifier(kind: str, identifier: str) -> UUID:
    """Derive a stable namespaced UUID while the original ID is persisted alongside it."""

    if not kind or not identifier:
        raise MappingError("identifier kind and value must be non-empty")
    return uuid5(_IDENTIFIER_NAMESPACE, f"{kind}\0{identifier}")


def domain_identifier_from_storage(kind: str, storage_id: UUID, identifier: str) -> str:
    """Validate the stored original identifier against its deterministic UUID."""

    expected = storage_uuid_for_identifier(kind, identifier)
    if storage_id != expected:
        raise MappingError("storage UUID does not match the persisted domain identifier")
    return identifier


def agent_schema_identity(
    *, operation: SemanticOperation, schema_name: str, output_schema: dict[str, Any]
) -> bytes:
    """Hash operation, versioned schema name, and canonical JSON into one audit identity."""

    if not schema_name:
        raise MappingError("agent schema name must be non-empty")
    try:
        payload = json.dumps(
            {
                "operation": operation.value,
                "output_schema": output_schema,
                "schema_name": schema_name,
            },
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    except (TypeError, ValueError) as exc:
        raise MappingError("output schema must contain finite JSON values") from exc
    return hashlib.sha256(payload).digest()
