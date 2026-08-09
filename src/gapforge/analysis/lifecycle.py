"""Opportunity lifecycle, trend, and rejected-opportunity reopen gates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from gapforge.domain.contracts import LifecycleEvent, LifecycleState, TrendLabel


@dataclass(frozen=True, slots=True)
class TrendObservation:
    signal_id: str
    author_id: str | None
    thread_id: str
    observed_at: datetime
    duplicate_group: str | None = None


@dataclass(frozen=True, slots=True)
class TrendResult:
    label: TrendLabel
    current_signals: int
    current_known_authors: int
    current_threads: int
    current_rate: float
    previous_rate: float


def classify_trend(
    observations: tuple[TrendObservation, ...],
    *,
    as_of: datetime,
    current_examined_volume: int,
    previous_examined_volume: int,
) -> TrendResult:
    if current_examined_volume < 0 or previous_examined_volume < 0:
        raise ValueError("examined source volume cannot be negative")
    current_start, previous_start = (
        as_of - timedelta(days=7),
        as_of - timedelta(days=35),
    )

    def independent(
        values: tuple[TrendObservation, ...],
    ) -> tuple[TrendObservation, ...]:
        selected: dict[str, TrendObservation] = {}
        for item in sorted(
            values,
            key=lambda value: (
                value.duplicate_group or value.signal_id,
                value.author_id is None,
                value.author_id or "",
                value.thread_id,
                value.signal_id,
            ),
        ):
            selected.setdefault(item.duplicate_group or item.signal_id, item)
        return tuple(selected.values())

    current = independent(
        tuple(
            item for item in observations if current_start <= item.observed_at < as_of
        )
    )
    previous = independent(
        tuple(
            item
            for item in observations
            if previous_start <= item.observed_at < current_start
        )
    )
    authors = len({item.author_id for item in current if item.author_id})
    threads = len({item.thread_id for item in current})
    current_rate = (len(current) + 1) / (current_examined_volume + 2)
    previous_rate = (len(previous) + 1) / (previous_examined_volume + 2)
    sufficient = (
        len(current) >= 5
        and authors >= 3
        and threads >= 2
        and current_examined_volume > 0
        and previous_examined_volume > 0
    )
    if not sufficient:
        label = TrendLabel.INSUFFICIENT_DATA
    elif current_rate >= previous_rate * 1.5:
        label = TrendLabel.RISING
    elif current_rate <= previous_rate / 1.5:
        label = TrendLabel.FALLING
    else:
        label = TrendLabel.FLAT
    return TrendResult(
        label, len(current), authors, threads, current_rate, previous_rate
    )


@dataclass(frozen=True, slots=True)
class ReopenEvidence:
    new_independent_users: int = 0
    new_source: bool = False
    first_wtp_or_spend: bool = False
    material_competitor_change: bool = False
    trend: TrendLabel = TrendLabel.INSUFFICIENT_DATA


def can_reopen_rejected(
    *,
    rejected_at: datetime,
    now: datetime,
    evidence: ReopenEvidence,
    cooldown_days: int = 30,
) -> bool:
    if cooldown_days < 0 or now < rejected_at:
        raise ValueError("invalid reopen timing")
    triggered = (
        evidence.new_independent_users >= 3
        or evidence.new_source
        or evidence.first_wtp_or_spend
        or evidence.material_competitor_change
        or evidence.trend is TrendLabel.RISING
    )
    cooldown_met = now - rejected_at >= timedelta(days=cooldown_days)
    return triggered and (cooldown_met or evidence.first_wtp_or_spend)


def transition_lifecycle(
    current: LifecycleState,
    target: LifecycleState,
    *,
    reopen_allowed: bool = False,
    validation_passed: bool = False,
) -> LifecycleState:
    allowed = {
        LifecycleState.DISCOVERED: {LifecycleState.RESEARCHING},
        LifecycleState.RESEARCHING: {
            LifecycleState.RESEARCH_MORE,
            LifecycleState.VALIDATE,
            LifecycleState.REJECTED,
        },
        LifecycleState.RESEARCH_MORE: {
            LifecycleState.RESEARCHING,
            LifecycleState.VALIDATE,
            LifecycleState.REJECTED,
        },
        LifecycleState.REJECTED: {LifecycleState.RESEARCH_MORE}
        if reopen_allowed
        else set(),
        LifecycleState.VALIDATE: set(),
    }
    if target is LifecycleState.VALIDATE and not validation_passed:
        raise ValueError("VALIDATE transition requires all validation gates")
    if target not in allowed[current]:
        raise ValueError(
            f"invalid lifecycle transition {current.value}->{target.value}"
        )
    return target


def append_lifecycle_event(
    history: tuple[LifecycleEvent, ...],
    event: LifecycleEvent,
) -> tuple[LifecycleEvent, ...]:
    if any(existing.id == event.id for existing in history):
        raise ValueError("lifecycle events are append-only and IDs cannot be reused")
    if history:
        prior = history[-1]
        if prior.assessment_id != event.assessment_id:
            raise ValueError("lifecycle history cannot mix assessments")
        if event.created_at < prior.created_at:
            raise ValueError("lifecycle events must be chronological")
        if event.from_state is not prior.to_state:
            raise ValueError("lifecycle event does not continue prior state")
    elif event.from_state is not None:
        raise ValueError("first lifecycle event must start without a prior state")
    return (*history, event)
