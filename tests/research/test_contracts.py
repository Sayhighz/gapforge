from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from gapforge.domain.contracts import (
    AtomicClaim,
    ClaimKind,
    EpistemicStatus,
    MissionRevision,
    QueryIntent,
    QueryIntentKind,
    QueryPlan,
    RawSignal,
    Source,
)

NOW = datetime(2026, 8, 9, tzinfo=UTC)


def intent(identifier: str, kind: QueryIntentKind = QueryIntentKind.BROAD) -> QueryIntent:
    return QueryIntent(
        id=identifier,
        kind=kind,
        concept="expense reconciliation",
        sources=(Source.HACKER_NEWS,),
        rationale="Find repeated manual workflow pain",
    )


def test_contracts_are_strict_versioned_and_serializable() -> None:
    value = intent("q-1")
    payload = value.model_dump(mode="json")
    assert payload["schema_version"] == "0.1"
    assert QueryIntent.model_validate_json(value.model_dump_json()) == value
    with pytest.raises(ValidationError, match="extra_forbidden"):
        QueryIntent.model_validate({**payload, "unexpected": True})


def test_mission_revision_parent_invariant() -> None:
    with pytest.raises(ValidationError, match="only revision 1"):
        MissionRevision(
            id=uuid4(), mission_id=uuid4(), revision=2, change_reason="refine ICP", prompt="Research pain", output_locale="th", created_at=NOW
        )


def test_query_plan_rejects_cap_and_duplicate_bypass() -> None:
    with pytest.raises(ValidationError, match="8 broad"):
        QueryPlan(round_number=1, intents=tuple(intent(f"q-{index}") for index in range(9)))
    with pytest.raises(ValidationError, match="must be unique"):
        QueryPlan(round_number=1, intents=(intent("same"), intent("same")))
    with pytest.raises(ValidationError):
        QueryPlan(round_number=3, intents=())


def test_supported_claim_requires_evidence_and_captured_price_citation() -> None:
    with pytest.raises(ValidationError, match="require evidence"):
        AtomicClaim(id="claim-1", text="Users pay $10", kind=ClaimKind.PRICE, status=EpistemicStatus.SUPPORTED)
    with pytest.raises(ValidationError, match="captured citations"):
        AtomicClaim(
            id="claim-1", text="Users pay $10", kind=ClaimKind.PRICE,
            status=EpistemicStatus.SUPPORTED, evidence_ids=("e-1",),
        )


def test_raw_signal_rejects_unbounded_or_contentless_input() -> None:
    base = dict(
        id="raw-1", source=Source.HACKER_NEWS, external_id="1",
        canonical_url="https://news.ycombinator.com/item?id=1", source_created_at=NOW,
        collected_at=NOW, content_hash="a" * 64,
    )
    with pytest.raises(ValidationError, match="requires title or body"):
        RawSignal(**base)
    with pytest.raises(ValidationError):
        RawSignal(**base, body="x" * 20_001)

