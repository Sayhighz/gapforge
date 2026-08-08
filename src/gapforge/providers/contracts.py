"""Versioned provider request/result contracts."""

from __future__ import annotations

import json
from enum import StrEnum
from typing import Any, Literal, Protocol

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ReasoningEffort(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class AgentStatus(StrEnum):
    SUCCESS = "SUCCESS"
    AUTH_REQUIRED = "AUTH_REQUIRED"
    INVALID_OUTPUT = "INVALID_OUTPUT"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"


class AgentUsage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    input_tokens: int = Field(default=0, ge=0)
    cached_input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)


class AgentRequest(BaseModel):
    """One bounded semantic operation; Python owns call/round budgets."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["1.0"] = "1.0"
    operation: str = Field(min_length=1, max_length=80, pattern=r"^[a-z][a-z0-9_]*$")
    instructions: str = Field(min_length=1, max_length=20_000)
    evidence: dict[str, Any] = Field(default_factory=dict)
    output_schema: dict[str, Any]
    effort: ReasoningEffort = ReasoningEffort.MEDIUM
    model: str = Field(default="", max_length=160)
    timeout_seconds: float = Field(default=300, ge=1, le=1800)
    max_output_bytes: int = Field(default=1_048_576, ge=1024, le=8_388_608)
    allow_repair: bool = True
    repair_attempt: int = Field(default=0, ge=0, le=1)
    repair_context: str | None = Field(default=None, max_length=2000)

    @field_validator("evidence")
    @classmethod
    def bound_evidence(cls, value: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(value, ensure_ascii=False, default=str).encode()) > 1_048_576:
            raise ValueError("evidence payload exceeds 1 MiB")
        return value

    @field_validator("output_schema")
    @classmethod
    def validate_output_schema(cls, value: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(value, ensure_ascii=False, default=str).encode()) > 262_144:
            raise ValueError("output schema exceeds 256 KiB")
        stack: list[tuple[Any, int]] = [(value, 1)]
        key_count = 0
        while stack:
            item, depth = stack.pop()
            if depth > 32:
                raise ValueError("output schema exceeds maximum nesting depth")
            if isinstance(item, dict):
                key_count += len(item)
                if key_count > 5000:
                    raise ValueError("output schema exceeds maximum key count")
                stack.extend((child, depth + 1) for child in item.values())
            elif isinstance(item, list):
                stack.extend((child, depth + 1) for child in item)
        try:
            Draft202012Validator.check_schema(value)
        except SchemaError as error:
            raise ValueError(f"invalid Draft 2020-12 output schema: {error.message}") from error
        return value

    @model_validator(mode="after")
    def validate_repair_state(self) -> AgentRequest:
        if self.repair_attempt == 0 and self.repair_context is not None:
            raise ValueError("repair_context requires a repair attempt")
        if self.repair_attempt == 1 and not self.repair_context:
            raise ValueError("repair attempt requires repair_context")
        return self


class AgentResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["1.0"] = "1.0"
    status: AgentStatus
    data: dict[str, Any] | None = None
    usage: AgentUsage = Field(default_factory=AgentUsage)
    provider: str
    requested_model: str = ""
    resolved_model: str | None = None
    effort: ReasoningEffort
    cli_version: str | None = None
    duration_ms: int = Field(ge=0)
    repair_attempts: int = Field(default=0, ge=0, le=1)
    error_class: str | None = None
    error_message: str | None = None
    events: tuple[dict[str, Any], ...] = ()
    stdout_truncated: bool = False
    stderr_truncated: bool = False


class AgentProvider(Protocol):
    async def run(self, request: AgentRequest) -> AgentResult: ...


def build_repair_request(request: AgentRequest, invalid_result: AgentResult) -> AgentRequest:
    """Create the one allowed repair call for separate budget/admission/audit handling."""

    if invalid_result.status is not AgentStatus.INVALID_OUTPUT:
        raise ValueError("only invalid output can be repaired")
    if not request.allow_repair or request.repair_attempt != 0:
        raise ValueError("repair limit reached or repair disabled")
    return request.model_copy(
        update={
            "allow_repair": False,
            "repair_attempt": 1,
            "repair_context": invalid_result.error_message or "output failed validation",
        }
    )
