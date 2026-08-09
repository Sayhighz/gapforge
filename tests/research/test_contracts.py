from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from gapforge.domain.contracts import (
    AgentEffort,
    AgentRequest,
    AgentResult,
    AgentStatus,
    AtomicClaim,
    ClaimKind,
    EpistemicStatus,
    MissionRevision,
    MissionOpportunityAssessment,
    QueryIntent,
    QueryIntentKind,
    QueryPlan,
    RawSignal,
    Source,
    Verdict,
)

NOW = datetime(2026, 8, 9, tzinfo=UTC)


def intent(
    identifier: str, kind: QueryIntentKind = QueryIntentKind.BROAD
) -> QueryIntent:
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
            id=uuid4(),
            mission_id=uuid4(),
            revision=2,
            change_reason="refine ICP",
            prompt="Research pain",
            output_locale="th",
            created_at=NOW,
        )


def test_query_plan_rejects_cap_and_duplicate_bypass() -> None:
    with pytest.raises(ValidationError, match="8 broad"):
        QueryPlan(
            round_number=1, intents=tuple(intent(f"q-{index}") for index in range(9))
        )
    with pytest.raises(ValidationError, match="must be unique"):
        QueryPlan(round_number=1, intents=(intent("same"), intent("same")))
    with pytest.raises(ValidationError):
        QueryPlan(round_number=3, intents=())


def test_supported_claim_requires_evidence_and_captured_price_citation() -> None:
    with pytest.raises(ValidationError, match="require evidence"):
        AtomicClaim(
            id="claim-1",
            text="Users pay $10",
            kind=ClaimKind.PRICE,
            status=EpistemicStatus.SUPPORTED,
        )
    with pytest.raises(ValidationError, match="captured citations"):
        AtomicClaim(
            id="claim-1",
            text="Users pay $10",
            kind=ClaimKind.PRICE,
            status=EpistemicStatus.SUPPORTED,
            evidence_ids=("e-1",),
        )


def test_raw_signal_rejects_unbounded_or_contentless_input() -> None:
    base = dict(
        id="raw-1",
        source=Source.HACKER_NEWS,
        external_id="1",
        canonical_url="https://news.ycombinator.com/item?id=1",
        source_created_at=NOW,
        collected_at=NOW,
        content_hash="a" * 64,
    )
    with pytest.raises(ValidationError, match="requires title or body"):
        RawSignal(**base)
    with pytest.raises(ValidationError):
        RawSignal(**base, body="x" * 20_001)


def test_provider_contract_round_trip_and_bounded_payload() -> None:
    request = AgentRequest(
        call_id="call-1",
        task="EXTRACT",
        effort=AgentEffort.LOW,
        input_json={"evidence": [{"id": "e-1", "text": "manual work"}]},
        permitted_evidence_ids=("e-1",),
        output_schema_name="pain-signals-v1",
        timeout_seconds=30,
    )
    result = AgentResult(
        call_id="call-1",
        status=AgentStatus.COMPLETED,
        output_json={"pain": "manual work"},
        provider="fake",
        effort=AgentEffort.LOW,
        duration_ms=10,
    )
    assert AgentRequest.model_validate_json(request.model_dump_json()) == request
    assert AgentResult.model_validate_json(result.model_dump_json()) == result
    with pytest.raises(ValidationError, match="20,000 bytes"):
        AgentRequest.model_validate(
            {**request.model_dump(), "input_json": {"payload": "x" * 20_001}}
        )
    with pytest.raises(ValidationError, match="effort"):
        AgentRequest(
            call_id="call-2",
            task="CRITIC",
            effort=AgentEffort.LOW,
            input_json={},
            permitted_evidence_ids=(),
            output_schema_name="critic-v1",
            timeout_seconds=30,
        )


def test_validate_assessment_requires_evidence_card_and_score() -> None:
    with pytest.raises(ValidationError, match="Evidence Card"):
        MissionOpportunityAssessment(
            id="a-1",
            mission_revision_id=uuid4(),
            opportunity_id="o-1",
            lifecycle_state="VALIDATE",
            verdict=Verdict.VALIDATE,
            assessed_at=NOW,
        )
