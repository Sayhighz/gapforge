"""Deterministic Evidence Cards and strict extraction/claim provenance validation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from gapforge.analysis.normalization import normalize_text, normalize_url
from gapforge.domain.contracts import (
    AtomicClaim,
    ClaimKind,
    EpistemicStatus,
    EvidenceCard,
    PainSignal,
    RepairRequest,
    Source,
)

USER_SOURCES = {Source.HACKER_NEWS, Source.GITHUB, Source.REDDIT, Source.USER}


class EvidenceValidationError(ValueError):
    def __init__(self, errors: tuple[str, ...]) -> None:
        self.errors = errors
        super().__init__("; ".join(errors))


@dataclass(frozen=True, slots=True)
class EvidenceRecord:
    evidence_id: str
    source: Source
    url: str
    text: str
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class EvidenceObservation:
    evidence_id: str
    duplicate_group: str
    author_id: str | None
    thread_id: str
    source: Source
    observed_at: datetime
    severity: float
    behavioral_workaround: bool = False
    paid_or_wtp: bool = False
    supporting_claim_ids: tuple[str, ...] = ()
    contradicting_claim_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not 0 <= self.severity <= 1:
            raise ValueError("severity must be between 0 and 1")


def validate_pain_extraction(signal: PainSignal, evidence: EvidenceRecord) -> None:
    if signal.raw_signal_revision_id != evidence.evidence_id:
        raise EvidenceValidationError(("pain signal references an unpermitted evidence ID",))
    haystack = normalize_text(evidence.text)
    errors = []
    if normalize_text(signal.excerpt) not in haystack:
        errors.append("excerpt is not present in source evidence")
    for name, value in (
        ("user_context", signal.user_context),
        ("workaround", signal.workaround),
        ("existing_solution", signal.existing_solution),
    ):
        if value and normalize_text(value) not in haystack:
            errors.append(f"{name} is not grounded in source evidence")
    if errors:
        raise EvidenceValidationError(tuple(errors))


def build_evidence_card(
    card_id: str,
    opportunity_id: str,
    observations: tuple[EvidenceObservation, ...],
) -> EvidenceCard:
    """Build metrics from one deterministic representative per duplicate group."""
    independent: dict[str, EvidenceObservation] = {}
    for item in sorted(observations, key=lambda value: (value.duplicate_group, value.evidence_id)):
        independent.setdefault(item.duplicate_group, item)
    values = tuple(independent.values())
    authors = tuple(sorted({item.author_id for item in values if item.author_id}))
    threads = tuple(sorted({item.thread_id for item in values}))
    sources = tuple(
        sorted(
            {item.source for item in values if item.source in USER_SOURCES},
            key=lambda value: value.value,
        )
    )
    days = tuple(
        datetime.combine(day, datetime.min.time(), tzinfo=UTC)
        for day in sorted({item.observed_at.astimezone(UTC).date() for item in values})
    )
    severity = sum(item.severity for item in values) / len(values) if values else 0.0
    workarounds = sum(item.behavioral_workaround for item in values)
    paid = sum(item.paid_or_wtp for item in values)
    confidence = min(
        1.0,
        0.25 * severity
        + 0.25 * min(1, len(authors) / 5)
        + 0.20 * min(1, len(threads) / 3)
        + 0.15 * min(1, len(sources) / 2)
        + 0.15 * min(1, len(days) / 3),
    )
    missing = []
    if len(authors) < 5:
        missing.append("five independent known authors")
    if len(threads) < 3:
        missing.append("three independent threads")
    if len(sources) < 2:
        missing.append("two user-evidence sources")
    if not workarounds:
        missing.append("behavioral workaround")
    if not paid:
        missing.append("willingness-to-pay or existing spend")
    return EvidenceCard(
        id=card_id,
        opportunity_id=opportunity_id,
        known_author_ids=authors,
        thread_ids=threads,
        user_sources=sources,
        observed_days=days,
        severity=severity,
        behavioral_workarounds=workarounds,
        paid_or_wtp_signals=paid,
        supporting_claim_ids=tuple(
            sorted({claim for item in values for claim in item.supporting_claim_ids})
        ),
        contradicting_claim_ids=tuple(
            sorted({claim for item in values for claim in item.contradicting_claim_ids})
        ),
        representative_evidence_ids=tuple(item.evidence_id for item in values[:50]),
        confidence=round(confidence, 6),
        missing_evidence=tuple(missing),
    )


def validate_atomic_claim(
    claim: AtomicClaim,
    evidence: dict[str, EvidenceRecord],
    *,
    known_claim_ids: frozenset[str] = frozenset(),
) -> None:
    errors: list[str] = []
    if claim.id in claim.contradicts_claim_ids:
        errors.append("claim cannot contradict itself")
    invented_contradictions = set(claim.contradicts_claim_ids) - known_claim_ids
    if invented_contradictions:
        errors.append("claim references unknown contradiction IDs")
    if claim.status is EpistemicStatus.SUPPORTED:
        invented = set(claim.evidence_ids) - evidence.keys()
        if invented:
            errors.append("claim references invented evidence IDs")
    for citation in claim.citations:
        if citation.evidence_id not in claim.evidence_ids:
            errors.append("citation evidence ID is not declared by the claim")
        record = evidence.get(citation.evidence_id)
        if record is None:
            errors.append("citation references invented evidence ID")
            continue
        if normalize_url(str(citation.source_url)) != normalize_url(record.url):
            errors.append("citation URL does not match captured evidence")
        if citation.observed_at != record.observed_at:
            errors.append("citation observation time does not match captured evidence")
        if normalize_text(citation.excerpt) not in normalize_text(record.text):
            errors.append("citation excerpt is not present in captured evidence")
    if (
        claim.kind in {ClaimKind.PRICE, ClaimKind.FEATURE}
        and claim.status is EpistemicStatus.SUPPORTED
    ):
        if not claim.citations:
            errors.append("supported price/feature claim lacks a captured citation")
    if errors:
        raise EvidenceValidationError(tuple(dict.fromkeys(errors)))


def repair_request(
    call_id: str, error: EvidenceValidationError, *, prior_attempts: int
) -> RepairRequest:
    if prior_attempts != 0:
        raise ValueError("invalid output receives only one repair attempt")
    return RepairRequest(original_call_id=call_id, validation_errors=error.errors[:20])
