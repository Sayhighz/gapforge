"""Validated persistence-boundary inputs owned by the platform lane."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator

_LOCALE_PATTERN = r"^[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*$"


class MissionRevisionInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    title: str | None = Field(default=None, min_length=1, max_length=240)
    mission_text: str = Field(min_length=1, max_length=20_000)
    original_language: str = Field(min_length=2, max_length=32, pattern=_LOCALE_PATTERN)
    output_locale: str = Field(min_length=2, max_length=32, pattern=_LOCALE_PATTERN)
    change_reason: str = Field(min_length=1, max_length=1000)
    interpretation: dict[str, object] = Field(default_factory=dict)

    @field_validator("mission_text", "change_reason")
    @classmethod
    def prohibit_nul(cls, value: str) -> str:
        if "\x00" in value:
            raise ValueError("text cannot contain NUL bytes")
        return value
