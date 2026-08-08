"""Independence-aware cluster and opportunity gates plus audited merge transitions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from gapforge.domain.contracts import MergeCandidate, MergeDecision, Verdict


@dataclass(frozen=True, slots=True)
class Observation:
    signal_id: str
    author_id: str | None
    thread_id: str
    duplicate_group: str
    behavioral_specificity: float = 0.0
    explicit_spend: bool = False


@dataclass(frozen=True, slots=True)
class IndependenceMetrics:
    unique_known_authors: int
    independent_threads: int
    independent_signals: int
    has_specific_behavior_or_spend: bool


def independence_metrics(observations: tuple[Observation, ...]) -> IndependenceMetrics:
    independent: dict[str, Observation] = {}
    for observation in sorted(
        observations,
        key=lambda item: (
            item.duplicate_group,
            item.author_id is None,
            item.author_id or "",
            item.thread_id,
            item.signal_id,
        ),
    ):
        independent.setdefault(observation.duplicate_group, observation)
    values = tuple(independent.values())
    return IndependenceMetrics(
        unique_known_authors=len({item.author_id for item in values if item.author_id}),
        independent_threads=len({item.thread_id for item in values}),
        independent_signals=len(values),
        has_specific_behavior_or_spend=any(item.behavioral_specificity >= 0.8 or item.explicit_spend for item in values),
    )


def can_form_provisional_cluster(observations: tuple[Observation, ...]) -> bool:
    metrics = independence_metrics(observations)
    return (
        metrics.independent_signals >= 2 and metrics.unique_known_authors >= 2
    ) or metrics.has_specific_behavior_or_spend


def opportunity_entry_verdict(observations: tuple[Observation, ...]) -> Verdict | None:
    metrics = independence_metrics(observations)
    if metrics.unique_known_authors >= 3 and metrics.independent_threads >= 2:
        return Verdict.RESEARCH_MORE
    if any(item.explicit_spend for item in observations):
        return Verdict.RESEARCH_MORE
    return None


def cluster_state(last_growth_at: datetime, now: datetime) -> str:
    if now < last_growth_at:
        raise ValueError("now cannot precede cluster growth")
    return "DORMANT" if now - last_growth_at >= timedelta(days=90) else "ACTIVE"


def apply_merge_decision(candidate: MergeCandidate, decision: MergeDecision) -> MergeCandidate:
    if decision.candidate_id != candidate.id:
        raise ValueError("decision does not match merge candidate")
    transitions = {
        ("PENDING", "ACCEPT"): "ACCEPTED",
        ("PENDING", "REJECT"): "REJECTED",
        ("ACCEPTED", "REVERSE"): "REVERSED",
    }
    next_status = transitions.get((candidate.status, decision.action))
    if next_status is None:
        raise ValueError("invalid merge decision transition")
    return candidate.model_copy(update={"status": next_status})
