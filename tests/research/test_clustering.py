from datetime import UTC, datetime, timedelta

import pytest

from gapforge.analysis.clustering import (
    Observation,
    apply_merge_decision,
    can_form_provisional_cluster,
    cluster_state,
    independence_metrics,
    opportunity_entry_verdict,
)
from gapforge.domain.contracts import MergeCandidate, MergeDecision, Verdict

NOW = datetime(2026, 8, 9, tzinfo=UTC)


def observation(index: int, *, author: str | None, thread: str, duplicate: str, spend: bool = False) -> Observation:
    return Observation(f"s-{index}", author, thread, duplicate, explicit_spend=spend)


def test_repeat_author_duplicate_and_viral_thread_cannot_fake_independence() -> None:
    repeats = tuple(observation(index, author="same", thread="viral", duplicate=f"d-{index}") for index in range(20))
    metrics = independence_metrics(repeats)
    assert metrics.unique_known_authors == 1
    assert metrics.independent_threads == 1
    assert opportunity_entry_verdict(repeats) is None
    duplicates = tuple(observation(index, author=f"a-{index}", thread=f"t-{index}", duplicate="same") for index in range(5))
    assert independence_metrics(duplicates).independent_signals == 1
    assert not can_form_provisional_cluster(duplicates)


def test_cluster_and_opportunity_thresholds() -> None:
    two = (observation(1, author="a", thread="t1", duplicate="d1"), observation(2, author="b", thread="t1", duplicate="d2"))
    assert can_form_provisional_cluster(two)
    three = (*two, observation(3, author="c", thread="t2", duplicate="d3"))
    assert opportunity_entry_verdict(three) is Verdict.RESEARCH_MORE
    assert opportunity_entry_verdict((observation(4, author="a", thread="t1", duplicate="d4", spend=True),)) is Verdict.RESEARCH_MORE


def test_dormancy_and_merge_reversal_are_deterministic() -> None:
    assert cluster_state(NOW - timedelta(days=90), NOW) == "DORMANT"
    candidate = MergeCandidate(id="m-1", left_problem_id="p-1", right_problem_id="p-2", similarity=0.8, rationale="lexical overlap")
    accept = MergeDecision(candidate_id="m-1", action="ACCEPT", actor="operator", reason="same workflow", decided_at=NOW)
    accepted = apply_merge_decision(candidate, accept)
    reverse = MergeDecision(candidate_id="m-1", action="REVERSE", actor="operator", reason="different ICP", decided_at=NOW)
    assert apply_merge_decision(accepted, reverse).status == "REVERSED"
    with pytest.raises(ValueError, match="invalid"):
        apply_merge_decision(candidate, reverse)
