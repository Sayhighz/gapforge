from datetime import UTC, datetime, timedelta

import pytest

from gapforge.analysis.evidence import (
    EvidenceObservation,
    EvidenceRecord,
    EvidenceValidationError,
    build_evidence_card,
    repair_request,
    validate_atomic_claim,
    validate_pain_extraction,
)
from gapforge.domain.contracts import (
    AtomicClaim,
    Citation,
    ClaimKind,
    EpistemicStatus,
    PainSignal,
    Source,
)

NOW = datetime(2026, 8, 9, tzinfo=UTC)


def record() -> EvidenceRecord:
    return EvidenceRecord(
        "e-1",
        Source.STATIC_WEB,
        "https://acme.example/pricing",
        "The Pro plan costs $20 per month and includes exports.",
        NOW,
    )


def test_pain_extraction_requires_source_id_excerpt_and_grounded_context() -> None:
    source = EvidenceRecord(
        "raw-1:r1",
        Source.GITHUB,
        "https://github.com/acme/app/issues/1",
        "As an accountant I export CSV files manually every Friday.",
        NOW,
    )
    valid = PainSignal(
        id="p-1",
        raw_signal_revision_id="raw-1:r1",
        pain="Reconciliation is manual",
        user_context="accountant",
        workaround="export CSV files manually",
        severity=0.8,
        frequency=0.7,
        confidence=0.9,
        excerpt="I export CSV files manually every Friday",
    )
    validate_pain_extraction(valid, source)
    fabricated = valid.model_copy(update={"workaround": "uses an AI agent"})
    with pytest.raises(EvidenceValidationError, match="not grounded"):
        validate_pain_extraction(fabricated, source)


def test_evidence_card_deduplicates_before_independence_metrics() -> None:
    observations = tuple(
        EvidenceObservation(
            evidence_id=f"e-{index}",
            duplicate_group="viral-copy",
            author_id=f"a-{index}",
            thread_id=f"t-{index}",
            source=Source.GITHUB,
            observed_at=NOW,
            severity=1,
            behavioral_workaround=True,
            paid_or_wtp=True,
        )
        for index in range(10)
    )
    card = build_evidence_card("card-1", "o-1", observations)
    assert len(card.known_author_ids) == 1
    assert len(card.thread_ids) == 1
    assert card.behavioral_workarounds == 1
    assert card.paid_or_wtp_signals == 1
    assert "five independent known authors" in card.missing_evidence


def test_evidence_card_captures_time_source_and_contradiction_diversity() -> None:
    observations = (
        EvidenceObservation(
            "e-1",
            "d-1",
            "a-1",
            "t-1",
            Source.GITHUB,
            NOW,
            0.8,
            supporting_claim_ids=("c-1",),
        ),
        EvidenceObservation(
            "e-2",
            "d-2",
            "a-2",
            "t-2",
            Source.REDDIT,
            NOW - timedelta(days=20),
            0.4,
            contradicting_claim_ids=("c-2",),
        ),
    )
    card = build_evidence_card("card-1", "o-1", observations)
    assert card.user_sources == (Source.GITHUB, Source.REDDIT)
    assert len(card.observed_days) == 2
    assert card.supporting_claim_ids == ("c-1",)
    assert card.contradicting_claim_ids == ("c-2",)


def test_claim_validation_rejects_invented_id_url_excerpt_time_and_contradiction() -> (
    None
):
    citation = Citation(
        evidence_id="e-1",
        source_url="https://attacker.example/",
        excerpt="$99 forever",
        observed_at=NOW - timedelta(days=1),
    )
    claim = AtomicClaim(
        id="c-1",
        text="Acme costs $20",
        kind=ClaimKind.PRICE,
        status=EpistemicStatus.SUPPORTED,
        evidence_ids=("e-1", "invented"),
        citations=(citation,),
        contradicts_claim_ids=("missing",),
    )
    with pytest.raises(EvidenceValidationError) as exc:
        validate_atomic_claim(
            claim, {"e-1": record()}, known_claim_ids=frozenset({"c-2"})
        )
    message = str(exc.value)
    assert "invented evidence" in message
    assert "URL does not match" in message
    assert "observation time" in message
    assert "excerpt is not present" in message
    assert "unknown contradiction" in message


def test_valid_captured_price_claim_and_single_repair_marker() -> None:
    source = record()
    claim = AtomicClaim(
        id="c-1",
        text="Acme Pro costs $20 monthly",
        kind=ClaimKind.PRICE,
        status=EpistemicStatus.SUPPORTED,
        evidence_ids=("e-1",),
        citations=(
            Citation(
                evidence_id="e-1",
                source_url=source.url,
                excerpt="Pro plan costs $20 per month",
                observed_at=NOW,
            ),
        ),
    )
    validate_atomic_claim(claim, {"e-1": source})
    error = EvidenceValidationError(("bad evidence",))
    assert repair_request("call-1", error, prior_attempts=0).repair_attempt == 1
    with pytest.raises(ValueError, match="only one"):
        repair_request("call-1", error, prior_attempts=1)


def test_supported_price_citation_must_be_declared_by_claim() -> None:
    source = record()
    with pytest.raises(ValueError, match="declared by the claim"):
        AtomicClaim(
            id="c-1",
            text="Acme costs $20",
            kind=ClaimKind.PRICE,
            status=EpistemicStatus.SUPPORTED,
            evidence_ids=("e-other",),
            citations=(
                Citation(
                    evidence_id="e-1",
                    source_url=source.url,
                    excerpt="$20 per month",
                    observed_at=NOW,
                ),
            ),
        )
