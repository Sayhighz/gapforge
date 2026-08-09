"""Credential-free deterministic provider for tests and local workflows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from time import monotonic
from typing import Any

from jsonschema import Draft202012Validator

from gapforge.providers.contracts import (
    AgentRequest,
    AgentResult,
    AgentStatus,
    AgentUsage,
)


class FakeAgentProvider:
    """Return operation-keyed fixtures without network or subprocess access."""

    def __init__(self, responses: Mapping[str, Sequence[dict[str, Any]]]) -> None:
        self._responses = {operation: tuple(items) for operation, items in responses.items()}
        self._indexes: dict[str, int] = {}

    async def run(self, request: AgentRequest) -> AgentResult:
        started = monotonic()
        responses = self._responses.get(request.operation, ())
        index = self._indexes.get(request.operation, 0)
        if not responses:
            return self._result(
                request,
                started,
                status=AgentStatus.ERROR,
                error_class="FakeResponseMissing",
                error_message=f"no fake response for operation {request.operation}",
            )
        response = responses[min(index, len(responses) - 1)]
        self._indexes[request.operation] = index + 1
        errors = list(Draft202012Validator(request.output_schema).iter_errors(response))
        if errors:
            return self._result(
                request,
                started,
                status=AgentStatus.INVALID_OUTPUT,
                error_class="SchemaValidationError",
                error_message=errors[0].message,
            )
        return self._result(
            request,
            started,
            status=AgentStatus.SUCCESS,
            data=response,
        )

    @staticmethod
    def _result(
        request: AgentRequest,
        started: float,
        *,
        status: AgentStatus,
        data: dict[str, Any] | None = None,
        error_class: str | None = None,
        error_message: str | None = None,
    ) -> AgentResult:
        return AgentResult(
            status=status,
            data=data,
            usage=AgentUsage(),
            provider="fake",
            requested_model=request.model,
            resolved_model="fake-deterministic",
            effort=request.effort,
            duration_ms=max(0, round((monotonic() - started) * 1000)),
            error_class=error_class,
            error_message=error_message,
        )
