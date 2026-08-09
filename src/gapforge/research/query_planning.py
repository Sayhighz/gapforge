"""Deterministic compilation and scheduling of semantic query intents."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta

from gapforge.domain.contracts import (
    QueryIntent,
    QueryIntentKind,
    QueryPlan,
    RunMode,
    Source,
)

BEHAVIOR_PATTERNS = (
    '"workaround"',
    '"manual process"',
    '"paying for"',
    '"switched from"',
    '"takes hours"',
)
NEGATIVE_FILTERS = ("-job", "-hiring", "-course", "-giveaway")


@dataclass(frozen=True, slots=True)
class TimeWindow:
    label: str
    since: datetime
    until: datetime
    allocation: float


@dataclass(frozen=True, slots=True)
class CompiledQuery:
    intent_id: str
    source: Source
    query: str
    rationale: str
    priority: float


def _quoted(value: str) -> str:
    clean = re.sub(r"[\x00-\x1f\x7f]", " ", value).replace('"', " ")
    clean = " ".join(clean.split())
    return f'"{clean[:200]}"'


def compile_intent(intent: QueryIntent, source: Source) -> CompiledQuery:
    """Compile semantic intent to bounded source syntax; never accept raw model syntax."""
    if source not in intent.sources:
        raise ValueError(f"intent {intent.id} does not permit source {source}")

    tokens = [_quoted(intent.concept)]
    if intent.audience:
        tokens.append(_quoted(intent.audience))
    if intent.behavior:
        tokens.append(_quoted(intent.behavior))
    elif intent.kind is QueryIntentKind.BROAD:
        tokens.append("(" + " OR ".join(BEHAVIOR_PATTERNS[:3]) + ")")

    negatives = list(NEGATIVE_FILTERS)
    for term in intent.negatives[:8]:
        safe = re.sub(r"[^\w -]", " ", term).strip().replace(" ", "-")
        if safe:
            negatives.append(f"-{safe[:80]}")

    if source is Source.GITHUB:
        tokens.extend(("is:issue", "is:closed", "-label:template"))
    elif source is Source.REDDIT:
        tokens.append("self:yes")
    query = " ".join((*tokens, *negatives))
    return CompiledQuery(
        intent_id=intent.id,
        source=source,
        query=query[:500],
        rationale=intent.rationale,
        priority=yield_priority(intent),
    )


def yield_priority(intent: QueryIntent) -> float:
    """Prioritize unknown/high-yield intents while deterministically demoting low yield."""
    baseline = 1.0 if intent.kind is QueryIntentKind.TARGETED else 0.8
    if intent.prior_yield is None:
        return baseline
    return round(baseline * (0.25 + 0.75 * float(intent.prior_yield)), 6)


def compile_plan(plan: QueryPlan) -> tuple[CompiledQuery, ...]:
    compiled: dict[tuple[Source, str], CompiledQuery] = {}
    for intent in plan.intents:
        for source in intent.sources:
            candidate = compile_intent(intent, source)
            key = (candidate.source, candidate.query)
            existing = compiled.get(key)
            if existing is None or (candidate.priority, candidate.intent_id) > (
                existing.priority,
                existing.intent_id,
            ):
                compiled[key] = candidate
    return tuple(
        sorted(
            compiled.values(),
            key=lambda item: (-item.priority, item.intent_id, item.source.value),
        )
    )


def stratified_windows(until: datetime, lookback_days: int = 365) -> tuple[TimeWindow, ...]:
    """Return the required four initial-HUNT strata, clipped for emerging missions."""
    if not 1 <= lookback_days <= 365:
        raise ValueError("lookback_days must be between 1 and 365")
    bands = (
        ("0-30", 0, 30, 0.35),
        ("31-90", 30, 90, 0.25),
        ("91-180", 90, 180, 0.20),
        ("181-365", 180, 365, 0.20),
    )
    windows = []
    for label, recent, old, allocation in bands:
        if recent >= lookback_days:
            continue
        windows.append(
            TimeWindow(
                label=label,
                since=until - timedelta(days=min(old, lookback_days)),
                until=until - timedelta(days=recent),
                allocation=allocation,
            )
        )
    total = sum(item.allocation for item in windows)
    return tuple(
        TimeWindow(item.label, item.since, item.until, item.allocation / total) for item in windows
    )


def collection_windows(
    mode: RunMode,
    until: datetime,
    *,
    last_successful_watermark: datetime | None = None,
    lookback_days: int = 365,
) -> tuple[TimeWindow, ...]:
    if mode is RunMode.HUNT:
        return stratified_windows(until, lookback_days)
    if last_successful_watermark is None:
        raise ValueError("MONITOR requires a last successful watermark")
    since = last_successful_watermark - timedelta(hours=24)
    if since >= until:
        raise ValueError("monitor watermark must precede collection end")
    return (TimeWindow("monitor-overlap", since, until, 1.0),)


def allocate_signal_budget(
    sources: Iterable[Source],
    total: int = 300,
) -> dict[Source, int]:
    """Allocate fairly; no source can consume more than half the run budget."""
    unique = tuple(sorted(set(sources), key=lambda source: source.value))
    if not unique:
        return {}
    if not 1 <= total <= 300:
        raise ValueError("signal budget must be between 1 and 300")
    selected = unique[:total]
    fair, remainder = divmod(total, len(selected))
    cap = max(1, total // 2)
    return {
        source: min(fair + (1 if index < remainder else 0), 100, cap)
        for index, source in enumerate(selected)
    }
