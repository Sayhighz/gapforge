from pathlib import Path

import pytest
from pydantic import ValidationError

from gapforge.config import AgentProviderName, Settings


def test_settings_defaults_match_specification() -> None:
    settings = Settings(_env_file=None)

    assert settings.agent_provider is AgentProviderName.CODEX_CLI
    assert settings.codex_model == ""
    assert settings.budget_snapshot() == {
        "run_interval_hours": 12,
        "max_research_rounds": 2,
        "max_agent_calls_per_run": 6,
        "max_parallel_agent_calls": 2,
        "max_run_duration_minutes": 30,
        "max_collector_requests_per_run": 60,
        "max_search_calls_per_run": 20,
        "max_raw_signals_per_run": 300,
        "raw_signal_retention_days": 0,
        "initial_lookback_days": 365,
        "monitor_overlap_hours": 24,
        "rejected_reopen_cooldown_days": 30,
    }


def test_settings_load_environment_and_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MAX_AGENT_CALLS_PER_RUN", "4")
    monkeypatch.setenv("MAX_PARALLEL_AGENT_CALLS", "1")
    monkeypatch.setenv("CODEX_HOME", "/var/run/gapforge-test/codex")

    settings = Settings(_env_file=None)

    assert settings.max_agent_calls_per_run == 4
    assert settings.max_parallel_agent_calls == 1
    assert settings.codex_home == Path("/var/run/gapforge-test/codex")


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({"max_research_rounds": 0}, "greater than or equal to 1"),
        (
            {"max_agent_calls_per_run": 1, "max_parallel_agent_calls": 2},
            "MAX_PARALLEL_AGENT_CALLS",
        ),
        ({"initial_lookback_days": 1, "monitor_overlap_hours": 25}, "MONITOR_OVERLAP_HOURS"),
    ],
)
def test_invalid_settings_fail_early(values: dict[str, int], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        Settings(_env_file=None, **values)


def test_secret_values_excludes_missing_credentials() -> None:
    settings = Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://user:db-secret@localhost/gapforge",
        github_token="github-secret",
    )

    assert settings.secret_values() == frozenset(
        {"postgresql+asyncpg://user:db-secret@localhost/gapforge", "github-secret"}
    )
