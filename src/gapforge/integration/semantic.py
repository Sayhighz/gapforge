"""Stable semantic-reasoning port shared by research and provider integration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from gapforge.domain import contracts as domain


@dataclass(frozen=True, slots=True)
class SemanticContext:
    """Durable run/task lineage for one logical semantic request."""

    run_id: UUID
    task_id: UUID


@dataclass(frozen=True, slots=True)
class SemanticCall:
    """Canonical domain request paired with its executable JSON Schema."""

    request: domain.AgentRequest
    output_schema: dict[str, Any]


class SemanticReasoner(Protocol):
    """Injected seam consumed by evidence orchestration."""

    async def run(
        self,
        context: SemanticContext,
        call: SemanticCall,
    ) -> domain.AgentResult: ...
