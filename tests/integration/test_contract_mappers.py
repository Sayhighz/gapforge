from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from gapforge.domain.contracts import (
    AgentEffort,
    AgentRequest,
    AgentStatus,
    AtomicClaim,
    Citation,
    ClaimKind,
    CompetitorEvidence,
    CompetitorResearchStatus,
    EpistemicStatus,
    EvidenceCard,
    LifecycleState,
    MissionOpportunityAssessment,
    OpportunityScoreSnapshot,
    RunWarning,
    ScoreComponents,
    SemanticOperation,
    Source,
    SourceCheckpoint,
    TaskStatus,
    Verdict,
)
from gapforge.integration.mappers import (
    PERSISTED_ENTITY_MAPPINGS,
    MappingError,
    agent_operation,
    agent_request_identity,
    agent_schema_identity,
    agent_status_from_provider,
    agent_status_to_provider,
    assessment_from_storage,
    assessment_to_storage_values,
    atomic_claim_from_storage,
    atomic_claim_to_storage_values,
    checkpoint_from_storage,
    checkpoint_to_storage,
    competitor_evidence_from_storage,
    competitor_evidence_to_storage_values,
    domain_identifier_from_storage,
    evidence_card_from_storage,
    evidence_card_to_storage_values,
    mission_revision_from_storage,
    normalize_output_locale,
    research_run_from_storage,
    research_task_from_storage,
    run_warning_to_storage,
    score_snapshot_from_storage,
    score_snapshot_to_storage_values,
    storage_uuid_for_identifier,
    task_status_from_storage,
    task_status_to_storage,
    validate_entity_mapping,
)
from gapforge.providers.contracts import AgentStatus as ProviderAgentStatus
from gapforge.storage.models import (
    AtomicClaim as StoredAtomicClaim,
)
from gapforge.storage.models import CompetitorEvidence as StoredCompetitorEvidence
from gapforge.storage.models import (
    EvidenceCard as StoredEvidenceCard,
)
from gapforge.storage.models import (
    MissionOpportunityAssessment as StoredMissionOpportunityAssessment,
)
from gapforge.storage.models import MissionRevision as StoredMissionRevision
from gapforge.storage.models import (
    OpportunityScoreSnapshot as StoredOpportunityScoreSnapshot,
)
from gapforge.storage.models import ResearchRun as StoredResearchRun
from gapforge.storage.models import ResearchTask as StoredResearchTask

NOW = datetime(2026, 8, 9, 12, tzinfo=UTC)


def test_task_status_mapping_is_explicit_and_bijective() -> None:
    pairs = {
        TaskStatus.QUEUED: "PENDING",
        TaskStatus.LEASED: "LEASED",
        TaskStatus.COMPLETED: "SUCCEEDED",
        TaskStatus.FAILED: "FAILED",
        TaskStatus.CANCELLED: "CANCELLED",
    }

    assert {status: task_status_to_storage(status) for status in TaskStatus} == pairs
    assert {stored: task_status_from_storage(stored) for stored in pairs.values()} == {
        stored: domain for domain, stored in pairs.items()
    }
    with pytest.raises(MappingError, match="unknown storage task status"):
        task_status_from_storage("COMPLETED")


def test_query_plan_is_a_bounded_medium_effort_operation() -> None:
    request = AgentRequest(
        call_id="call-query-plan",
        task="QUERY_PLAN",
        effort=AgentEffort.MEDIUM,
        input_json={"mission": "find recurring accounting pain"},
        permitted_evidence_ids=(),
        output_schema_name="query-plan-v1",
        timeout_seconds=30,
    )

    assert agent_operation(request) == "query_plan"
    with pytest.raises(ValidationError, match="effort"):
        request.model_copy(update={"effort": AgentEffort.HIGH}).model_validate(
            {**request.model_dump(), "effort": AgentEffort.HIGH}
        )


def test_provider_status_mapping_is_explicit_and_bijective() -> None:
    expected = {
        AgentStatus.COMPLETED: ProviderAgentStatus.SUCCESS,
        AgentStatus.INVALID_OUTPUT: ProviderAgentStatus.INVALID_OUTPUT,
        AgentStatus.AUTH_REQUIRED: ProviderAgentStatus.AUTH_REQUIRED,
        AgentStatus.TIMEOUT: ProviderAgentStatus.TIMEOUT,
        AgentStatus.FAILED: ProviderAgentStatus.ERROR,
    }

    assert {status: agent_status_to_provider(status) for status in AgentStatus} == expected
    assert {status: agent_status_from_provider(status) for status in ProviderAgentStatus} == {
        value: key for key, value in expected.items()
    }


def test_mission_run_and_task_storage_names_map_to_domain_contracts() -> None:
    mission_id = uuid4()
    revision_id = uuid4()
    run_id = uuid4()
    mission = StoredMissionRevision(
        id=revision_id,
        mission_id=mission_id,
        revision_number=1,
        change_reason="initial",
        mission_text="Research recurring pain",
        original_language="en",
        output_locale="EN-us",
        interpretation={},
        created_at=NOW,
    )
    run = StoredResearchRun(
        id=run_id,
        mission_revision_id=revision_id,
        mode="HUNT",
        status="COMPLETED_WITH_WARNINGS",
        priority=1,
        deadline_at=NOW,
        started_at=NOW,
        completed_at=NOW,
        budget_limits={},
        budget_used={},
        warnings=[
            {
                "code": "PARTIAL_TASK_FAILURE",
                "details": {
                    "failed_tasks": 1,
                    "failure_classes": ["SOURCE_UNAVAILABLE"],
                    "useful_successes": 2,
                },
            }
        ],
        last_checkpoint={},
        created_at=NOW,
    )
    task = StoredResearchTask(
        id=uuid4(),
        run_id=run_id,
        task_type="collect",
        status="SUCCEEDED",
        priority=1,
        idempotency_key="collect:1",
        payload={},
        checkpoint={"page": 2},
        attempt_count=1,
        max_attempts=3,
        available_at=NOW,
        completed_at=NOW,
    )

    assert mission_revision_from_storage(mission).output_locale == "en-US"
    mapped_run = research_run_from_storage(run)
    assert mapped_run.finished_at == NOW
    assert mapped_run.warnings == (
        RunWarning(
            code="PARTIAL_TASK_FAILURE",
            details={
                "failed_tasks": 1,
                "failure_classes": ["SOURCE_UNAVAILABLE"],
                "useful_successes": 2,
            },
        ),
    )
    assert run_warning_to_storage(mapped_run.warnings[0]) == run.warnings[0]
    assert research_task_from_storage(task).status is TaskStatus.COMPLETED


@pytest.mark.parametrize(
    "warning",
    [
        {"details": {}},
        {"code": "PARTIAL_TASK_FAILURE"},
        {"code": "PARTIAL_TASK_FAILURE", "details": {}, "extra": True},
        {"code": "PARTIAL_TASK_FAILURE", "details": "not-an-object"},
        {"code": "PARTIAL_TASK_FAILURE", "details": {str(index): index for index in range(51)}},
        {"code": "PARTIAL_TASK_FAILURE", "details": {"ratio": float("nan")}},
    ],
)
def test_research_run_mapping_rejects_noncanonical_structured_warnings(
    warning: dict[str, object],
) -> None:
    row = StoredResearchRun(
        id=uuid4(),
        mission_revision_id=uuid4(),
        mode="HUNT",
        status="COMPLETED_WITH_WARNINGS",
        priority=1,
        deadline_at=NOW,
        started_at=NOW,
        completed_at=NOW,
        budget_limits={},
        budget_used={},
        warnings=[warning],
        last_checkpoint={},
        created_at=NOW,
    )

    with pytest.raises(MappingError, match="run warning"):
        research_run_from_storage(row)


def test_checkpoint_mapping_preserves_none_cursor_and_watermark() -> None:
    checkpoint = SourceCheckpoint(source=Source.REDDIT, cursor=None, watermark=NOW)

    stored = checkpoint_to_storage(checkpoint)

    assert stored.cursor == {"kind": "opaque", "value": None}
    assert stored.watermark_at == NOW
    assert checkpoint_from_storage(Source.REDDIT, stored.cursor, stored.watermark_at) == checkpoint
    with pytest.raises(MappingError, match="checkpoint cursor"):
        checkpoint_from_storage(Source.REDDIT, {"value": "next", "extra": True}, NOW)


@pytest.mark.parametrize(
    ("raw", "canonical"),
    [("th", "th"), ("EN", "en"), ("en-us", "en-US"), ("TH-th", "th-TH")],
)
def test_output_locale_mapping_is_canonical(raw: str, canonical: str) -> None:
    assert normalize_output_locale(raw) == canonical


@pytest.mark.parametrize("raw", ["eng", "zh-Hant", "en-US-posix", "th_TH", ""])
def test_output_locale_mapping_rejects_lossy_locales(raw: str) -> None:
    with pytest.raises(MappingError, match="output locale"):
        normalize_output_locale(raw)


def test_string_identifiers_map_to_stable_uuid_without_losing_original() -> None:
    storage_id = storage_uuid_for_identifier("raw-signal-revision", "raw-1:r2")

    assert isinstance(storage_id, UUID)
    assert storage_id == storage_uuid_for_identifier("raw-signal-revision", "raw-1:r2")
    assert storage_id != storage_uuid_for_identifier("raw-signal", "raw-1:r2")
    assert (
        domain_identifier_from_storage("raw-signal-revision", storage_id, "raw-1:r2") == "raw-1:r2"
    )
    with pytest.raises(MappingError, match="does not match"):
        domain_identifier_from_storage("raw-signal-revision", uuid4(), "raw-1:r2")


def test_uuid_storage_mapping_rejects_lexically_noncanonical_ids() -> None:
    score = OpportunityScoreSnapshot(
        id=str(uuid4()).upper(),
        opportunity_id=str(uuid4()),
        mission_revision_id=uuid4(),
        evidence_strength=ScoreComponents(values={"severity": 80}, weights={"severity": 1}),
        opportunity_fit=ScoreComponents(values={"fit": 70}, weights={"fit": 1}),
        pre_penalty_score=75,
        final_score=75,
        evidence_confidence=0.8,
        created_at=NOW,
    )

    with pytest.raises(MappingError, match="canonical UUID"):
        score_snapshot_to_storage_values(score, assessment_id=uuid4(), run_id=uuid4())


def test_agent_schema_identity_is_canonical_and_operation_scoped() -> None:
    first = agent_schema_identity(
        operation=SemanticOperation.QUERY_PLAN,
        schema_name="query-plan-v1",
        output_schema={"required": ["intents"], "type": "object"},
    )
    reordered = agent_schema_identity(
        operation=SemanticOperation.QUERY_PLAN,
        schema_name="query-plan-v1",
        output_schema={"type": "object", "required": ["intents"]},
    )

    assert first == reordered
    assert len(first) == 32
    assert first != agent_schema_identity(
        operation=SemanticOperation.CRITIC,
        schema_name="query-plan-v1",
        output_schema={"type": "object", "required": ["intents"]},
    )


def test_agent_request_identity_is_canonical_and_excludes_runtime_fields() -> None:
    schema_identity = b"s" * 32
    request = AgentRequest(
        call_id=str(uuid4()),
        task=SemanticOperation.EXTRACT,
        effort=AgentEffort.LOW,
        input_json={"z": 1, "items": [{"id": "evidence-1"}]},
        permitted_evidence_ids=("evidence-2", "evidence-1"),
        permitted_urls=("https://example.com/b", "https://example.com/a"),
        output_schema_name="pain-v1",
        timeout_seconds=30,
    )
    same_semantics = request.model_copy(
        update={
            "call_id": str(uuid4()),
            "timeout_seconds": 1,
            "input_json": {"items": [{"id": "evidence-1"}], "z": 1},
            "permitted_evidence_ids": ("evidence-1", "evidence-2"),
            "permitted_urls": ("https://example.com/a", "https://example.com/b"),
        }
    )

    identity = agent_request_identity(request, schema_identity=schema_identity)

    assert len(identity) == 32
    assert identity == agent_request_identity(
        same_semantics,
        schema_identity=schema_identity,
    )
    assert identity != agent_request_identity(
        request.model_copy(update={"input_json": {"items": []}}),
        schema_identity=schema_identity,
    )
    assert identity != agent_request_identity(request, schema_identity=b"t" * 32)
    with pytest.raises(MappingError, match="32 bytes"):
        agent_request_identity(request, schema_identity=b"short")


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), float("-inf")])
def test_agent_schema_identity_rejects_noncanonical_numbers(invalid: float) -> None:
    with pytest.raises(MappingError, match="finite JSON"):
        agent_schema_identity(
            operation=SemanticOperation.QUERY_PLAN,
            schema_name="query-plan-v1",
            output_schema={"const": invalid},
        )


def test_every_persisted_research_entity_has_an_explicit_complete_mapping() -> None:
    expected = {
        "ResearchMission",
        "MissionRevision",
        "ResearchRun",
        "ResearchTask",
        "SourceCheckpoint",
        "RawSignal",
        "RawSignalRevision",
        "PainSignal",
        "CanonicalProblem",
        "ProblemCluster",
        "ProblemClusterMembership",
        "MergeCandidate",
        "EvidenceCard",
        "AtomicClaim",
        "ProblemHypothesis",
        "Competitor",
        "CompetitorEvidence",
        "GapHypothesis",
        "Opportunity",
        "MissionOpportunityAssessment",
        "OpportunityScoreSnapshot",
        "CriticResult",
        "ProductHypothesis",
        "LifecycleEvent",
        "AgentCall",
    }

    assert set(PERSISTED_ENTITY_MAPPINGS) == expected
    for name, mapping in PERSISTED_ENTITY_MAPPINGS.items():
        assert mapping.domain_type.__name__ == name
        assert mapping.storage_type.__name__ == name
        assert mapping.mapped_domain_fields == frozenset(mapping.domain_type.model_fields) - {
            "schema_version"
        }
        assert all(route.rationale for route in mapping.field_routes.values())
        assert all(route.rationale for route in mapping.storage_only_routes.values())
        for field, route in mapping.field_routes.items():
            if field == "id" or field.endswith("_id") or field.endswith("_ids"):
                assert route.transform != "direct"

    task_mapping = PERSISTED_ENTITY_MAPPINGS["ResearchTask"]
    assert task_mapping.field_routes["status"].transform == "task_status"
    agent_call_mapping = PERSISTED_ENTITY_MAPPINGS["AgentCall"]
    for field in ("request", "result"):
        assert agent_call_mapping.field_routes[field].executable is False
        assert agent_call_mapping.field_routes[field].transform == "provider_audit_projection"
    mission_mapping = PERSISTED_ENTITY_MAPPINGS["ResearchMission"]
    with pytest.raises(RuntimeError, match="required storage columns"):
        validate_entity_mapping(
            replace(
                mission_mapping,
                storage_only_routes={
                    key: route
                    for key, route in mission_mapping.storage_only_routes.items()
                    if key != "updated_at"
                },
            )
        )


def test_evidence_card_typed_mapping_round_trips_all_metrics() -> None:
    opportunity_id = uuid4()
    canonical_problem_id = uuid4()
    run_id = uuid4()
    revision_storage_id = storage_uuid_for_identifier("raw-signal-revision", "raw-1:r1")
    card = EvidenceCard(
        id=str(uuid4()),
        opportunity_id=str(opportunity_id),
        known_author_ids=("author-1", "author-2"),
        thread_ids=("thread-1", "thread-2"),
        user_sources=(Source.HACKER_NEWS, Source.REDDIT),
        observed_days=(NOW,),
        severity=0.81234,
        behavioral_workarounds=2,
        paid_or_wtp_signals=1,
        supporting_claim_ids=(str(uuid4()),),
        contradicting_claim_ids=(str(uuid4()),),
        representative_evidence_ids=("raw-1:r1",),
        confidence=0.76543,
        missing_evidence=("more recent evidence",),
    )

    stored = StoredEvidenceCard(
        **evidence_card_to_storage_values(
            card,
            canonical_problem_id=canonical_problem_id,
            run_id=run_id,
            algorithm_version="evidence-v1",
            revision_ids={"raw-1:r1": revision_storage_id},
            claim_ids={
                identifier: UUID(identifier)
                for identifier in (
                    *card.supporting_claim_ids,
                    *card.contradicting_claim_ids,
                )
            },
        )
    )

    assert stored.metrics["severity"] == "0.81234"
    claim_identifiers = {
        UUID(identifier): identifier
        for identifier in (*card.supporting_claim_ids, *card.contradicting_claim_ids)
    }
    assert (
        evidence_card_from_storage(
            stored,
            revision_identifiers={revision_storage_id: "raw-1:r1"},
            claim_identifiers=claim_identifiers,
        )
        == card
    )
    with pytest.raises(MappingError, match="unknown persisted claim"):
        evidence_card_from_storage(
            stored,
            revision_identifiers={revision_storage_id: "raw-1:r1"},
            claim_identifiers={},
        )

    with pytest.raises(MappingError, match="unknown raw revision"):
        evidence_card_to_storage_values(
            card,
            canonical_problem_id=canonical_problem_id,
            run_id=run_id,
            algorithm_version="evidence-v1",
            revision_ids={},
            claim_ids={identifier: UUID(identifier) for identifier in card.supporting_claim_ids},
        )


def test_assessment_mapping_preserves_nullable_verdict_and_competitor_status() -> None:
    assessment = MissionOpportunityAssessment(
        id=str(uuid4()),
        mission_revision_id=uuid4(),
        opportunity_id=str(uuid4()),
        lifecycle_state=LifecycleState.RESEARCHING,
        relevance=0.8123456789012345,
        verdict=None,
        competitor_research_status=CompetitorResearchStatus.RESEARCH_UNAVAILABLE,
        assessed_at=NOW,
    )
    row = StoredMissionOpportunityAssessment(
        **assessment_to_storage_values(assessment),
        updated_at=NOW,
    )

    assert assessment_from_storage(row) == assessment

    completed = assessment.model_copy(
        update={
            "lifecycle_state": LifecycleState.RESEARCH_MORE,
            "verdict": Verdict.RESEARCH_MORE,
        }
    )
    completed_row = StoredMissionOpportunityAssessment(
        **assessment_to_storage_values(completed),
        updated_at=NOW,
    )
    assert assessment_from_storage(completed_row) == completed


def test_score_snapshot_typed_mapping_round_trips_components_without_float_storage() -> None:
    opportunity_id = uuid4()
    mission_revision_id = uuid4()
    score = OpportunityScoreSnapshot(
        id=str(uuid4()),
        opportunity_id=str(opportunity_id),
        mission_revision_id=mission_revision_id,
        evidence_strength=ScoreComponents(values={"severity": 83.125}, weights={"severity": 1.0}),
        opportunity_fit=ScoreComponents(
            values={"gap_strength": 71.875}, weights={"gap_strength": 1.0}
        ),
        raw_metrics={"authors": 5.0},
        penalties={"concentration": 2.125},
        pre_penalty_score=77.28125,
        final_score=75.15625,
        evidence_confidence=0.8125,
        explanation=("independent evidence",),
        created_at=NOW,
    )

    stored = StoredOpportunityScoreSnapshot(
        **score_snapshot_to_storage_values(score, assessment_id=uuid4(), run_id=uuid4())
    )

    assert stored.pre_penalty_score == Decimal("77.28125")
    assert stored.evidence_components["values"]["severity"] == "83.125"
    assert (
        score_snapshot_from_storage(
            stored,
            opportunity_id=opportunity_id,
            mission_revision_id=mission_revision_id,
        )
        == score
    )

    stored.weights = {
        "evidence": {"severity": "0.5"},
        "opportunity_fit": {"gap_strength": "1.0"},
    }
    with pytest.raises(MappingError, match="weights disagree"):
        score_snapshot_from_storage(
            stored,
            opportunity_id=opportunity_id,
            mission_revision_id=mission_revision_id,
        )


def test_atomic_claim_typed_mapping_round_trips_and_rejects_bad_lineage() -> None:
    evidence_id = storage_uuid_for_identifier("raw-signal-revision", "raw-1:r1")
    claim = AtomicClaim(
        id=str(uuid4()),
        text="Captured page lists $20 per month",
        kind=ClaimKind.PRICE,
        status=EpistemicStatus.SUPPORTED,
        evidence_ids=("raw-1:r1",),
        citations=(
            Citation(
                evidence_id="raw-1:r1",
                source_url="https://example.com/pricing",
                excerpt="$20 per month",
                observed_at=NOW,
            ),
        ),
        contradicts_claim_ids=(str(uuid4()),),
    )
    stored = StoredAtomicClaim(
        **atomic_claim_to_storage_values(
            claim,
            subject_type="OPPORTUNITY",
            subject_id=uuid4(),
            revision_ids={"raw-1:r1": evidence_id},
        )
    )

    assert (
        atomic_claim_from_storage(stored, revision_identifiers={evidence_id: "raw-1:r1"}) == claim
    )
    with pytest.raises(MappingError, match="evidence ID"):
        atomic_claim_from_storage(stored, revision_identifiers={})

    captured_id = uuid4()
    stored.evidence_ids = [captured_id]
    stored.citations = [
        {
            "evidence_id": str(captured_id),
            "source_url": "https://example.com/pricing",
            "excerpt": "$20 per month",
            "observed_at": NOW.isoformat(),
        }
    ]
    captured_claim = atomic_claim_from_storage(
        stored,
        revision_identifiers={},
        captured_evidence_identifiers={captured_id: str(captured_id)},
    )
    assert captured_claim.evidence_ids == (str(captured_id),)
    with pytest.raises(MappingError, match="captured evidence ID"):
        atomic_claim_from_storage(
            stored,
            revision_identifiers={},
            captured_evidence_identifiers={captured_id: str(uuid4())},
        )


def test_competitor_evidence_mapping_preserves_capture_and_claim_semantics() -> None:
    evidence = CompetitorEvidence(
        id=str(uuid4()),
        competitor_id=str(uuid4()),
        source_url="https://example.com/pricing",
        captured_excerpt="$20 per month",
        observed_at=NOW,
        content_hash="a" * 64,
        evidence_kind="PRICE_PAGE",
        metadata={"currency": "USD"},
        claim_ids=(str(uuid4()),),
    )

    stored = StoredCompetitorEvidence(**competitor_evidence_to_storage_values(evidence))

    assert stored.content_hash == b"\xaa" * 32
    assert competitor_evidence_from_storage(stored) == evidence
    stored.content_hash = b"short"
    with pytest.raises(MappingError, match="32 bytes"):
        competitor_evidence_from_storage(stored)
