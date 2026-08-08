"""Validated application configuration.

Settings are deliberately centralized so hard budgets can be persisted with a run and
validated before any network, database, or subprocess work begins.
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AgentProviderName(StrEnum):
    CODEX_CLI = "codex_cli"
    FAKE = "fake"


class Settings(BaseSettings):
    """GapForge settings loaded from environment variables or ``.env``."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    database_url: SecretStr = SecretStr(
        "postgresql+asyncpg://gapforge:gapforge@postgres:5432/gapforge"
    )
    agent_provider: AgentProviderName = AgentProviderName.CODEX_CLI
    codex_model: str = ""
    codex_home: Path = Path("/var/lib/gapforge/codex")
    codex_binary: str = "codex"

    run_interval_hours: int = Field(default=12, ge=1)
    max_research_rounds: int = Field(default=2, ge=1)
    max_agent_calls_per_run: int = Field(default=6, ge=1)
    max_parallel_agent_calls: int = Field(default=2, ge=1)
    max_run_duration_minutes: int = Field(default=30, ge=1)
    max_collector_requests_per_run: int = Field(default=60, ge=1)
    max_search_calls_per_run: int = Field(default=20, ge=0)
    max_raw_signals_per_run: int = Field(default=300, ge=1)
    raw_signal_retention_days: int = Field(default=0, ge=0)
    initial_lookback_days: int = Field(default=365, ge=1)
    monitor_overlap_hours: int = Field(default=24, ge=0)
    rejected_reopen_cooldown_days: int = Field(default=30, ge=0)

    github_token: SecretStr | None = None
    reddit_client_id: SecretStr | None = None
    reddit_client_secret: SecretStr | None = None
    brave_api_key: SecretStr | None = None

    log_level: str = "INFO"
    log_json: bool = True
    reports_dir: Path = Path("reports")
    backups_dir: Path = Path("backups")

    @model_validator(mode="after")
    def validate_related_limits(self) -> Settings:
        if self.max_parallel_agent_calls > self.max_agent_calls_per_run:
            raise ValueError("MAX_PARALLEL_AGENT_CALLS cannot exceed MAX_AGENT_CALLS_PER_RUN")
        if self.monitor_overlap_hours > self.initial_lookback_days * 24:
            raise ValueError("MONITOR_OVERLAP_HOURS cannot exceed INITIAL_LOOKBACK_DAYS")
        return self

    def budget_snapshot(self) -> dict[str, int]:
        """Return the immutable hard-limit values persisted on each run."""

        return {
            "run_interval_hours": self.run_interval_hours,
            "max_research_rounds": self.max_research_rounds,
            "max_agent_calls_per_run": self.max_agent_calls_per_run,
            "max_parallel_agent_calls": self.max_parallel_agent_calls,
            "max_run_duration_minutes": self.max_run_duration_minutes,
            "max_collector_requests_per_run": self.max_collector_requests_per_run,
            "max_search_calls_per_run": self.max_search_calls_per_run,
            "max_raw_signals_per_run": self.max_raw_signals_per_run,
            "raw_signal_retention_days": self.raw_signal_retention_days,
            "initial_lookback_days": self.initial_lookback_days,
            "monitor_overlap_hours": self.monitor_overlap_hours,
            "rejected_reopen_cooldown_days": self.rejected_reopen_cooldown_days,
        }

    def secret_values(self) -> frozenset[str]:
        """Return configured non-empty secrets for exact-value log redaction."""

        values: set[str] = {self.database_url.get_secret_value()}
        for value in (
            self.github_token,
            self.reddit_client_id,
            self.reddit_client_secret,
            self.brave_api_key,
        ):
            if value is not None and value.get_secret_value():
                values.add(value.get_secret_value())
        return frozenset(values)
