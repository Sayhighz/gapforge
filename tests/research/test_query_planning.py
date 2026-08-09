from datetime import UTC, datetime, timedelta

import pytest

from gapforge.domain.contracts import (
    QueryIntent,
    QueryIntentKind,
    QueryPlan,
    RunMode,
    Source,
)
from gapforge.research.query_planning import (
    allocate_signal_budget,
    collection_windows,
    compile_intent,
    compile_plan,
    stratified_windows,
)

NOW = datetime(2026, 8, 9, tzinfo=UTC)


def make_intent(identifier: str, *, prior_yield: float | None = None) -> QueryIntent:
    return QueryIntent(
        id=identifier,
        kind=QueryIntentKind.BROAD,
        concept='billing "pain"\n',
        audience="freelancers",
        negatives=("job posting",),
        sources=(Source.GITHUB, Source.HACKER_NEWS),
        rationale="manual billing",
        prior_yield=prior_yield,
    )


def test_compiler_adds_behavior_negatives_and_source_syntax() -> None:
    compiled = compile_intent(make_intent("intent-1"), Source.GITHUB)
    assert '"billing pain"' in compiled.query
    assert '"workaround"' in compiled.query
    assert "-job-posting" in compiled.query
    assert "is:issue is:closed" in compiled.query
    assert "\n" not in compiled.query


def test_compilation_deduplicates_and_low_yield_loses_priority() -> None:
    plan = QueryPlan(
        round_number=1,
        intents=(make_intent("high", prior_yield=1), make_intent("low", prior_yield=0)),
    )
    compiled = compile_plan(plan)
    assert len(compiled) == 2
    assert {item.intent_id for item in compiled} == {"high"}


def test_time_stratification_and_monitor_overlap() -> None:
    windows = stratified_windows(NOW)
    assert [item.allocation for item in windows] == [0.35, 0.25, 0.2, 0.2]
    assert windows[0].since == NOW - timedelta(days=30)
    assert windows[-1].since == NOW - timedelta(days=365)
    monitor = collection_windows(
        RunMode.MONITOR, NOW, last_successful_watermark=NOW - timedelta(hours=48)
    )
    assert monitor[0].since == NOW - timedelta(hours=72)
    assert monitor[0].allocation == 1


def test_emerging_window_rebalances_and_missing_watermark_fails() -> None:
    windows = stratified_windows(NOW, 90)
    assert len(windows) == 2
    assert sum(item.allocation for item in windows) == pytest.approx(1)
    with pytest.raises(ValueError, match="watermark"):
        collection_windows(RunMode.MONITOR, NOW)


def test_source_budget_is_fair_and_capped() -> None:
    budget = allocate_signal_budget((Source.HACKER_NEWS, Source.GITHUB, Source.REDDIT))
    assert budget == {Source.GITHUB: 100, Source.HACKER_NEWS: 100, Source.REDDIT: 100}
    two_sources = allocate_signal_budget((Source.HACKER_NEWS, Source.GITHUB))
    assert max(two_sources.values()) <= 150
    tiny = allocate_signal_budget((Source.HACKER_NEWS, Source.GITHUB, Source.REDDIT), total=2)
    assert sum(tiny.values()) == 2
    assert all(value > 0 for value in tiny.values())
