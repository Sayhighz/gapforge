"""Explicit, lossless mappings between research and platform contracts."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, TypedDict
from uuid import UUID, uuid5

from gapforge.domain import contracts as domain
from gapforge.domain.contracts import (
    AgentRequest,
    SemanticOperation,
    Source,
    SourceCheckpoint,
    TaskStatus,
)
from gapforge.providers import contracts as provider
from gapforge.storage import models as storage

_IDENTIFIER_NAMESPACE = UUID("de9d3b28-a802-5ce6-86bb-1864f13e59c4")
_LOCALE_PATTERN = re.compile(r"^(?P<language>[A-Za-z]{2})(?:-(?P<region>[A-Za-z]{2}))?$")

_TASK_STATUS_TO_STORAGE = {
    TaskStatus.QUEUED: "PENDING",
    TaskStatus.LEASED: "LEASED",
    TaskStatus.COMPLETED: "SUCCEEDED",
    TaskStatus.FAILED: "FAILED",
    TaskStatus.CANCELLED: "CANCELLED",
}
_TASK_STATUS_FROM_STORAGE = {value: key for key, value in _TASK_STATUS_TO_STORAGE.items()}
_AGENT_STATUS_TO_PROVIDER = {
    domain.AgentStatus.COMPLETED: provider.AgentStatus.SUCCESS,
    domain.AgentStatus.INVALID_OUTPUT: provider.AgentStatus.INVALID_OUTPUT,
    domain.AgentStatus.AUTH_REQUIRED: provider.AgentStatus.AUTH_REQUIRED,
    domain.AgentStatus.TIMEOUT: provider.AgentStatus.TIMEOUT,
    domain.AgentStatus.FAILED: provider.AgentStatus.ERROR,
}
_AGENT_STATUS_FROM_PROVIDER = {value: key for key, value in _AGENT_STATUS_TO_PROVIDER.items()}


class MappingError(ValueError):
    """A platform value cannot be represented by the canonical domain contract."""


@dataclass(frozen=True, slots=True)
class StorageCheckpoint:
    """Persistence-shaped checkpoint values without a database dependency."""

    cursor: dict[str, Any]
    watermark_at: datetime | None


@dataclass(frozen=True, slots=True)
class FieldRoute:
    destination: str
    transform: str
    executable: bool
    rationale: str


@dataclass(frozen=True, slots=True)
class StorageOnlyRoute:
    source: str
    executable: bool
    rationale: str


@dataclass(frozen=True, slots=True)
class EntityMapping:
    """Auditable domain and injected routes for one ORM persistence boundary."""

    domain_type: type[domain.Contract]
    storage_type: type[storage.Base]
    field_routes: dict[str, FieldRoute]
    storage_only_routes: dict[str, StorageOnlyRoute]

    @property
    def mapped_domain_fields(self) -> frozenset[str]:
        return frozenset(self.field_routes)

    @property
    def covered_storage_columns(self) -> frozenset[str]:
        columns = frozenset(self.storage_type.__table__.c.keys())
        routed = {
            route.destination.split(".", 1)[0].split(":", 1)[0]
            for route in self.field_routes.values()
            if route.destination.split(".", 1)[0].split(":", 1)[0] in columns
        }
        return frozenset(routed) | frozenset(self.storage_only_routes)


class EvidenceCardValues(TypedDict):
    id: UUID
    canonical_problem_id: UUID
    opportunity_id: UUID
    run_id: UUID
    algorithm_version: str
    independent_authors: int
    independent_threads: int
    source_count: int
    metrics: dict[str, Any]
    supporting_claim_ids: list[UUID]
    contradicting_claim_ids: list[UUID]
    representative_signal_ids: list[UUID]
    confidence: Decimal
    missing_evidence: list[str]


class AtomicClaimValues(TypedDict):
    id: UUID
    subject_type: str
    subject_id: UUID
    claim_type: str
    text: str
    status: str
    evidence_ids: list[UUID]
    citations: list[dict[str, str]]
    contradicts_claim_ids: list[UUID]
    observed_at: datetime | None


class CompetitorEvidenceValues(TypedDict):
    id: UUID
    competitor_id: UUID
    source_url: str
    captured_excerpt: str
    observed_at: datetime
    content_hash: bytes
    evidence_kind: str
    metadata_json: dict[str, Any]
    claim_ids: list[UUID]


class ScoreSnapshotValues(TypedDict):
    id: UUID
    assessment_id: UUID
    run_id: UUID
    algorithm_version: str
    raw_metrics: dict[str, str]
    evidence_strength: Decimal
    opportunity_fit: Decimal
    evidence_components: dict[str, dict[str, str]]
    opportunity_fit_components: dict[str, dict[str, str]]
    weights: dict[str, dict[str, str]]
    penalties: dict[str, str]
    pre_penalty_score: Decimal
    final_score: Decimal
    confidence: Decimal
    explanation: list[str]
    created_at: datetime


class AssessmentValues(TypedDict):
    id: UUID
    mission_revision_id: UUID
    opportunity_id: UUID
    lifecycle_status: str
    relevance: Decimal
    verdict: str | None
    competitor_research_status: str
    rejected_at: datetime | None


def task_status_to_storage(status: TaskStatus) -> str:
    """Translate canonical task lifecycle names to queue persistence names."""

    try:
        return _TASK_STATUS_TO_STORAGE[status]
    except KeyError as exc:  # pragma: no cover - exhaustive enum guard
        raise MappingError(f"unknown domain task status: {status!r}") from exc


def task_status_from_storage(status: str) -> TaskStatus:
    """Translate a persisted queue status without silently accepting aliases."""

    try:
        return _TASK_STATUS_FROM_STORAGE[status]
    except KeyError as exc:
        raise MappingError(f"unknown storage task status: {status!r}") from exc


def agent_operation(request: AgentRequest) -> str:
    """Return the provider/storage operation identifier for a semantic request."""

    return request.task.value.lower()


def agent_status_to_provider(status: domain.AgentStatus) -> provider.AgentStatus:
    """Translate research result statuses to the stable provider boundary."""

    return _AGENT_STATUS_TO_PROVIDER[status]


def agent_status_from_provider(status: provider.AgentStatus) -> domain.AgentStatus:
    """Translate provider statuses without accepting ambiguous aliases."""

    return _AGENT_STATUS_FROM_PROVIDER[status]


def mission_revision_from_storage(row: storage.MissionRevision) -> domain.MissionRevision:
    """Restore the immutable mission contract with canonical locale naming."""

    return domain.MissionRevision(
        id=row.id,
        mission_id=row.mission_id,
        revision=row.revision_number,
        parent_revision_id=row.parent_revision_id,
        change_reason=row.change_reason,
        prompt=row.mission_text,
        output_locale=normalize_output_locale(row.output_locale),
        created_at=row.created_at,
    )


def research_run_from_storage(row: storage.ResearchRun) -> domain.ResearchRun:
    """Map completed_at and lossless structured warnings to the run contract."""

    return domain.ResearchRun(
        id=row.id,
        mission_revision_id=row.mission_revision_id,
        mode=row.mode,
        status=row.status,
        created_at=row.created_at,
        started_at=row.started_at,
        finished_at=row.completed_at,
        warnings=tuple(_run_warning_from_storage(value) for value in row.warnings),
    )


def _run_warning_from_storage(value: object) -> domain.RunWarning:
    if not isinstance(value, dict) or set(value) != {"code", "details"}:
        raise MappingError("persisted run warning must contain exactly code and details")
    try:
        return domain.RunWarning.model_validate(value)
    except ValueError as exc:
        raise MappingError("persisted run warning violates the canonical contract") from exc


def run_warning_to_storage(warning: domain.RunWarning) -> dict[str, Any]:
    """Return the exact JSON object accepted by the durable run controller."""

    return {"code": warning.code, "details": warning.details}


def research_task_from_storage(row: storage.ResearchTask) -> domain.ResearchTask:
    """Restore a durable queue task using the explicit status vocabulary bridge."""

    return domain.ResearchTask(
        id=row.id,
        run_id=row.run_id,
        task_type=row.task_type,
        status=task_status_from_storage(row.status),
        attempt=row.attempt_count,
        lease_owner=row.lease_owner,
        lease_expires_at=row.lease_expires_at,
        checkpoint=row.checkpoint,
    )


def checkpoint_to_storage(checkpoint: SourceCheckpoint) -> StorageCheckpoint:
    """Wrap an opaque source cursor so `None` does not collapse into an empty object."""

    return StorageCheckpoint(
        cursor={"kind": "opaque", "value": checkpoint.cursor},
        watermark_at=checkpoint.watermark,
    )


def checkpoint_from_storage(
    source: Source,
    cursor: dict[str, Any],
    watermark_at: datetime | None,
) -> SourceCheckpoint:
    """Rebuild a domain checkpoint, rejecting unknown or structurally lossy JSON."""

    if set(cursor) != {"kind", "value"} or cursor.get("kind") != "opaque":
        raise MappingError("checkpoint cursor must be an opaque cursor envelope")
    value = cursor["value"]
    if value is not None and not isinstance(value, str):
        raise MappingError("checkpoint cursor value must be a string or null")
    return SourceCheckpoint(source=source, cursor=value, watermark=watermark_at)


def normalize_output_locale(value: str) -> str:
    """Canonicalize the deliberately bounded v0.1 language/region locale form."""

    match = _LOCALE_PATTERN.fullmatch(value)
    if match is None:
        raise MappingError("output locale must be a two-letter language and optional region")
    language = match.group("language").lower()
    region = match.group("region")
    return language if region is None else f"{language}-{region.upper()}"


def storage_uuid_for_identifier(kind: str, identifier: str) -> UUID:
    """Derive a stable namespaced UUID while the original ID is persisted alongside it."""

    if not kind or not identifier:
        raise MappingError("identifier kind and value must be non-empty")
    return uuid5(_IDENTIFIER_NAMESPACE, f"{kind}\0{identifier}")


def domain_identifier_from_storage(kind: str, storage_id: UUID, identifier: str) -> str:
    """Validate the stored original identifier against its deterministic UUID."""

    expected = storage_uuid_for_identifier(kind, identifier)
    if storage_id != expected:
        raise MappingError("storage UUID does not match the persisted domain identifier")
    return identifier


def agent_schema_identity(
    *, operation: SemanticOperation, schema_name: str, output_schema: dict[str, Any]
) -> bytes:
    """Hash operation, versioned schema name, and canonical JSON into one audit identity."""

    if not schema_name:
        raise MappingError("agent schema name must be non-empty")
    try:
        payload = json.dumps(
            {
                "operation": operation.value,
                "output_schema": output_schema,
                "schema_name": schema_name,
            },
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    except (TypeError, ValueError) as exc:
        raise MappingError("output schema must contain finite JSON values") from exc
    return hashlib.sha256(payload).digest()


def evidence_card_to_storage_values(
    card: domain.EvidenceCard,
    *,
    canonical_problem_id: UUID,
    run_id: UUID,
    algorithm_version: str,
    revision_ids: dict[str, UUID],
    claim_ids: dict[str, UUID],
) -> EvidenceCardValues:
    """Map a complete Evidence Card without collapsing its independence metrics."""

    representative_ids = [
        _known_revision_id(identifier, revision_ids)
        for identifier in card.representative_evidence_ids
    ]
    return EvidenceCardValues(
        id=_uuid_text(card.id, "Evidence Card ID"),
        canonical_problem_id=canonical_problem_id,
        opportunity_id=_uuid_text(card.opportunity_id, "opportunity ID"),
        run_id=run_id,
        algorithm_version=algorithm_version,
        independent_authors=len(card.known_author_ids),
        independent_threads=len(card.thread_ids),
        source_count=len(card.user_sources),
        metrics={
            "behavioral_workarounds": card.behavioral_workarounds,
            "known_author_ids": list(card.known_author_ids),
            "observed_days": [value.isoformat() for value in card.observed_days],
            "paid_or_wtp_signals": card.paid_or_wtp_signals,
            "severity": _decimal_text(card.severity),
            "thread_ids": list(card.thread_ids),
            "user_sources": [source.value for source in card.user_sources],
        },
        supporting_claim_ids=[
            _known_identifier(identifier, claim_ids, "supporting claim ID")
            for identifier in card.supporting_claim_ids
        ],
        contradicting_claim_ids=[
            _known_identifier(identifier, claim_ids, "contradicting claim ID")
            for identifier in card.contradicting_claim_ids
        ],
        representative_signal_ids=representative_ids,
        confidence=_decimal(card.confidence),
        missing_evidence=list(card.missing_evidence),
    )


def evidence_card_from_storage(
    row: storage.EvidenceCard,
    *,
    revision_identifiers: dict[UUID, str],
    claim_identifiers: dict[UUID, str],
) -> domain.EvidenceCard:
    """Restore and validate every Evidence Card metric from one persisted row."""

    metrics = row.metrics
    required = {
        "behavioral_workarounds",
        "known_author_ids",
        "observed_days",
        "paid_or_wtp_signals",
        "severity",
        "thread_ids",
        "user_sources",
    }
    if set(metrics) != required:
        raise MappingError("Evidence Card metrics are incomplete or contain unknown fields")
    representatives = tuple(
        _revision_identifier(identifier, revision_identifiers)
        for identifier in row.representative_signal_ids
    )
    result = domain.EvidenceCard(
        id=str(row.id),
        opportunity_id=str(row.opportunity_id),
        known_author_ids=tuple(metrics["known_author_ids"]),
        thread_ids=tuple(metrics["thread_ids"]),
        user_sources=tuple(metrics["user_sources"]),
        observed_days=tuple(metrics["observed_days"]),
        severity=float(metrics["severity"]),
        behavioral_workarounds=metrics["behavioral_workarounds"],
        paid_or_wtp_signals=metrics["paid_or_wtp_signals"],
        supporting_claim_ids=tuple(
            _claim_identifier(value, claim_identifiers) for value in row.supporting_claim_ids
        ),
        contradicting_claim_ids=tuple(
            _claim_identifier(value, claim_identifiers) for value in row.contradicting_claim_ids
        ),
        representative_evidence_ids=representatives,
        confidence=float(row.confidence),
        missing_evidence=tuple(row.missing_evidence),
    )
    if row.independent_authors != len(result.known_author_ids):
        raise MappingError("Evidence Card author count disagrees with typed metrics")
    if row.independent_threads != len(result.thread_ids):
        raise MappingError("Evidence Card thread count disagrees with typed metrics")
    if row.source_count != len(result.user_sources):
        raise MappingError("Evidence Card source count disagrees with typed metrics")
    return result


def atomic_claim_to_storage_values(
    claim: domain.AtomicClaim,
    *,
    subject_type: str,
    subject_id: UUID,
    revision_ids: dict[str, UUID],
) -> AtomicClaimValues:
    """Map claim citations and contradiction lineage with validated revision IDs."""

    evidence_ids = [_known_revision_id(value, revision_ids) for value in claim.evidence_ids]
    citations = [
        {
            "evidence_id": citation.evidence_id,
            "excerpt": citation.excerpt,
            "observed_at": citation.observed_at.isoformat(),
            "source_url": str(citation.source_url),
        }
        for citation in claim.citations
    ]
    return AtomicClaimValues(
        id=_uuid_text(claim.id, "claim ID"),
        subject_type=subject_type,
        subject_id=subject_id,
        claim_type=claim.kind.value,
        text=claim.text,
        status=claim.status.value,
        evidence_ids=evidence_ids,
        citations=citations,
        contradicts_claim_ids=[
            _uuid_text(value, "contradicted claim ID") for value in claim.contradicts_claim_ids
        ],
        observed_at=max((citation.observed_at for citation in claim.citations), default=None),
    )


def atomic_claim_from_storage(
    row: storage.AtomicClaim,
    *,
    revision_identifiers: dict[UUID, str],
) -> domain.AtomicClaim:
    """Restore a claim only when citations resolve to its persisted evidence lineage."""

    evidence_ids = tuple(
        _revision_identifier(identifier, revision_identifiers) for identifier in row.evidence_ids
    )
    citations: list[domain.Citation] = []
    for raw in row.citations:
        if set(raw) != {"evidence_id", "excerpt", "observed_at", "source_url"}:
            raise MappingError("claim citation is incomplete or contains unknown fields")
        evidence_id = raw["evidence_id"]
        if evidence_id not in evidence_ids:
            raise MappingError("claim citation does not resolve to persisted evidence")
        citations.append(domain.Citation.model_validate(raw))
    return domain.AtomicClaim(
        id=str(row.id),
        text=row.text,
        kind=row.claim_type,
        status=row.status,
        evidence_ids=evidence_ids,
        citations=tuple(citations),
        contradicts_claim_ids=tuple(str(value) for value in row.contradicts_claim_ids),
    )


def competitor_evidence_to_storage_values(
    evidence: domain.CompetitorEvidence,
) -> CompetitorEvidenceValues:
    """Persist captured competitor evidence and its atomic-claim links together."""

    return CompetitorEvidenceValues(
        id=_uuid_text(evidence.id, "competitor evidence ID"),
        competitor_id=_uuid_text(evidence.competitor_id, "competitor ID"),
        source_url=str(evidence.source_url),
        captured_excerpt=evidence.captured_excerpt,
        observed_at=evidence.observed_at,
        content_hash=bytes.fromhex(evidence.content_hash),
        evidence_kind=evidence.evidence_kind,
        metadata_json=evidence.metadata,
        claim_ids=[_uuid_text(value, "competitor claim ID") for value in evidence.claim_ids],
    )


def competitor_evidence_from_storage(
    row: storage.CompetitorEvidence,
) -> domain.CompetitorEvidence:
    """Restore competitor evidence without confusing captured evidence and claim linkage."""

    if len(row.content_hash) != 32:
        raise MappingError("competitor evidence content hash must contain 32 bytes")
    return domain.CompetitorEvidence(
        id=str(row.id),
        competitor_id=str(row.competitor_id),
        source_url=row.source_url,
        captured_excerpt=row.captured_excerpt,
        observed_at=row.observed_at,
        content_hash=row.content_hash.hex(),
        evidence_kind=row.evidence_kind,
        metadata=row.metadata_json,
        claim_ids=tuple(str(value) for value in row.claim_ids),
    )


def score_snapshot_to_storage_values(
    score: domain.OpportunityScoreSnapshot,
    *,
    assessment_id: UUID,
    run_id: UUID,
) -> ScoreSnapshotValues:
    """Persist every score input as canonical decimal text plus exact numeric axes."""

    evidence = _components_to_json(score.evidence_strength)
    fit = _components_to_json(score.opportunity_fit)
    return ScoreSnapshotValues(
        id=_uuid_text(score.id, "score snapshot ID"),
        assessment_id=assessment_id,
        run_id=run_id,
        algorithm_version=score.algorithm_version,
        raw_metrics={key: _decimal_text(value) for key, value in score.raw_metrics.items()},
        evidence_strength=_score_decimal(_weighted_score(evidence)),
        opportunity_fit=_score_decimal(_weighted_score(fit)),
        evidence_components=evidence,
        opportunity_fit_components=fit,
        weights={"evidence": evidence["weights"], "opportunity_fit": fit["weights"]},
        penalties={key: _decimal_text(value) for key, value in score.penalties.items()},
        pre_penalty_score=_score_decimal(score.pre_penalty_score),
        final_score=_score_decimal(score.final_score),
        confidence=_score_decimal(score.evidence_confidence),
        explanation=list(score.explanation),
        created_at=score.created_at,
    )


def assessment_to_storage_values(
    assessment: domain.MissionOpportunityAssessment,
) -> AssessmentValues:
    """Persist the assessment-owned state; artifact IDs remain normalized child lookups."""

    return AssessmentValues(
        id=_uuid_text(assessment.id, "assessment ID"),
        mission_revision_id=assessment.mission_revision_id,
        opportunity_id=_uuid_text(assessment.opportunity_id, "opportunity ID"),
        lifecycle_status=assessment.lifecycle_state.value,
        relevance=_score_decimal(assessment.relevance),
        verdict=assessment.verdict.value if assessment.verdict is not None else None,
        competitor_research_status=assessment.competitor_research_status.value,
        rejected_at=assessment.rejected_at,
    )


def assessment_from_storage(
    row: storage.MissionOpportunityAssessment,
    *,
    score_snapshot_id: UUID | None = None,
    evidence_card_id: UUID | None = None,
) -> domain.MissionOpportunityAssessment:
    """Restore assessment state plus explicitly resolved normalized artifact lineage."""

    return domain.MissionOpportunityAssessment(
        id=str(row.id),
        mission_revision_id=row.mission_revision_id,
        opportunity_id=str(row.opportunity_id),
        lifecycle_state=row.lifecycle_status,
        relevance=float(row.relevance),
        verdict=row.verdict,
        competitor_research_status=row.competitor_research_status,
        score_snapshot_id=str(score_snapshot_id) if score_snapshot_id is not None else None,
        evidence_card_id=str(evidence_card_id) if evidence_card_id is not None else None,
        rejected_at=row.rejected_at,
        assessed_at=row.updated_at,
    )


def score_snapshot_from_storage(
    row: storage.OpportunityScoreSnapshot,
    *,
    opportunity_id: UUID,
    mission_revision_id: UUID,
) -> domain.OpportunityScoreSnapshot:
    """Restore a score snapshot and reject scalar/component disagreement."""

    evidence = _components_from_json(row.evidence_components)
    fit = _components_from_json(row.opportunity_fit_components)
    expected_weights = {
        "evidence": {key: _decimal(value) for key, value in evidence.weights.items()},
        "opportunity_fit": {key: _decimal(value) for key, value in fit.weights.items()},
    }
    if not isinstance(row.weights, dict) or set(row.weights) != set(expected_weights):
        raise MappingError("stored score weights disagree with component maps")
    try:
        stored_weights = {
            axis: {key: _decimal(value) for key, value in values.items()}
            for axis, values in row.weights.items()
            if isinstance(values, dict)
        }
    except (AttributeError, TypeError, ValueError) as exc:
        raise MappingError("stored score weights disagree with component maps") from exc
    if stored_weights != expected_weights:
        raise MappingError("stored score weights disagree with component maps")
    if _decimal(row.evidence_strength) != _score_decimal(_weighted_score(row.evidence_components)):
        raise MappingError("evidence strength scalar disagrees with stored components")
    if _decimal(row.opportunity_fit) != _score_decimal(
        _weighted_score(row.opportunity_fit_components)
    ):
        raise MappingError("opportunity fit scalar disagrees with stored components")
    return domain.OpportunityScoreSnapshot(
        id=str(row.id),
        opportunity_id=str(opportunity_id),
        mission_revision_id=mission_revision_id,
        evidence_strength=evidence,
        opportunity_fit=fit,
        raw_metrics={key: float(value) for key, value in row.raw_metrics.items()},
        penalties={key: float(value) for key, value in row.penalties.items()},
        pre_penalty_score=float(row.pre_penalty_score),
        final_score=float(row.final_score),
        evidence_confidence=float(row.confidence),
        algorithm_version=row.algorithm_version,
        explanation=tuple(row.explanation),
        created_at=row.created_at,
    )


def _uuid_text(value: str, label: str) -> UUID:
    try:
        parsed = UUID(value)
    except (TypeError, ValueError) as exc:
        raise MappingError(f"{label} must be a canonical UUID string") from exc
    if value != str(parsed):
        raise MappingError(f"{label} must be a canonical UUID string")
    return parsed


def _decimal(value: float | Decimal) -> Decimal:
    converted = Decimal(str(value))
    if not converted.is_finite():
        raise MappingError("numeric persistence values must be finite")
    return converted


def _decimal_text(value: float | Decimal) -> str:
    return format(_decimal(value), "f")


def _score_decimal(value: float | Decimal) -> Decimal:
    """Quantize bounded score scalars to the schema's 17-decimal persistence policy."""

    return _decimal(value).quantize(Decimal("0.00000000000000001"))


def _known_revision_id(value: str, revision_ids: dict[str, UUID]) -> UUID:
    try:
        identifier = revision_ids[value]
    except KeyError as exc:
        raise MappingError(f"unknown raw revision ID: {value}") from exc
    domain_identifier_from_storage("raw-signal-revision", identifier, value)
    return identifier


def _known_identifier(value: str, identifiers: dict[str, UUID], label: str) -> UUID:
    try:
        identifier = identifiers[value]
    except KeyError as exc:
        raise MappingError(f"unknown {label}: {value}") from exc
    if value != str(identifier):
        raise MappingError(f"{label} mapping is not canonical")
    return identifier


def _revision_identifier(identifier: UUID, revision_identifiers: dict[UUID, str]) -> str:
    try:
        domain_id = revision_identifiers[identifier]
    except KeyError as exc:
        raise MappingError(f"unknown persisted raw revision ID: {identifier}") from exc
    return domain_identifier_from_storage("raw-signal-revision", identifier, domain_id)


def _claim_identifier(identifier: UUID, claim_identifiers: dict[UUID, str]) -> str:
    try:
        domain_id = claim_identifiers[identifier]
    except KeyError as exc:
        raise MappingError(f"unknown persisted claim ID: {identifier}") from exc
    if _uuid_text(domain_id, "claim ID") != identifier:
        raise MappingError("persisted claim ID does not match its domain identifier")
    return domain_id


def _components_to_json(components: domain.ScoreComponents) -> dict[str, dict[str, str]]:
    return {
        "values": {key: _decimal_text(value) for key, value in components.values.items()},
        "weights": {key: _decimal_text(value) for key, value in components.weights.items()},
    }


def _components_from_json(value: dict[str, Any]) -> domain.ScoreComponents:
    if set(value) != {"values", "weights"}:
        raise MappingError("score components require only values and weights")
    values = value["values"]
    weights = value["weights"]
    if not isinstance(values, dict) or not isinstance(weights, dict):
        raise MappingError("score component values and weights must be objects")
    return domain.ScoreComponents(
        values={key: float(item) for key, item in values.items()},
        weights={key: float(item) for key, item in weights.items()},
    )


def _weighted_score(value: dict[str, Any]) -> Decimal:
    components = _components_from_json(value)
    return sum(
        (
            _decimal(components.values[key]) * _decimal(weight)
            for key, weight in components.weights.items()
        ),
        start=Decimal(0),
    )


def _entity_mapping(
    domain_type: type[domain.Contract],
    storage_type: type[storage.Base],
    *,
    routes: dict[str, str],
    storage_only: dict[str, str] | None = None,
) -> EntityMapping:
    domain_fields = frozenset(domain_type.model_fields) - {"schema_version"}
    storage_columns = frozenset(storage_type.__table__.c.keys())
    direct = domain_fields & storage_columns
    raw_routes = {name: name for name in direct} | routes
    field_routes = {
        name: _field_route(name, destination, storage_columns)
        for name, destination in raw_routes.items()
    }
    if domain_type is domain.ResearchTask:
        status_route = field_routes["status"]
        field_routes["status"] = FieldRoute(
            status_route.destination,
            "task_status",
            status_route.executable,
            "explicit QUEUED/COMPLETED to PENDING/SUCCEEDED translation",
        )
    if domain_type is domain.AgentCall:
        for field in ("request", "result"):
            route = field_routes[field]
            field_routes[field] = FieldRoute(
                route.destination,
                "provider_audit_projection",
                False,
                "deferred to the provider audit adapter, which projects the validated "
                "request/result into the explicitly routed AgentCall audit columns",
            )
    missing = domain_fields - frozenset(field_routes)
    extra = frozenset(field_routes) - domain_fields
    if missing or extra:
        raise RuntimeError(
            f"invalid {domain_type.__name__} mapping; missing={sorted(missing)}, "
            f"extra={sorted(extra)}"
        )
    injected = {
        column: StorageOnlyRoute(
            source="integration context",
            executable=True,
            rationale=rationale,
        )
        for column, rationale in (storage_only or {}).items()
    }
    unknown_storage = frozenset(injected) - storage_columns
    if unknown_storage:
        raise RuntimeError(
            f"invalid {domain_type.__name__} storage-only routes: {sorted(unknown_storage)}"
        )
    provisional = EntityMapping(domain_type, storage_type, field_routes, injected)
    validate_entity_mapping(provisional)
    return provisional


def validate_entity_mapping(mapping: EntityMapping) -> None:
    """Reject incomplete domain coverage and every implicit storage column."""

    domain_fields = frozenset(mapping.domain_type.model_fields) - {"schema_version"}
    if mapping.mapped_domain_fields != domain_fields:
        raise RuntimeError(f"{mapping.domain_type.__name__} has incomplete domain field routes")
    storage_columns = frozenset(mapping.storage_type.__table__.c.keys())
    uncovered = storage_columns - mapping.covered_storage_columns
    if uncovered:
        raise RuntimeError(
            f"{mapping.domain_type.__name__} has required storage columns without routes: "
            f"{sorted(uncovered)}"
        )


def _field_route(
    name: str,
    destination: str,
    storage_columns: frozenset[str],
) -> FieldRoute:
    root = destination.split(".", 1)[0].split(":", 1)[0]
    executable = root in storage_columns
    transform = _field_transform(name, destination)
    rationale = (
        "same-record executable conversion"
        if executable
        else "deferred normalized relation resolved by the I3/I5 persistence adapter"
    )
    return FieldRoute(destination, transform, executable, rationale)


def _field_transform(name: str, destination: str) -> str:
    if name == "warnings":
        return "typed_run_warning_json"
    if name == "status" and destination == "status":
        return "enum_value"
    if name == "status" and destination != "status":
        return "task_status" if destination == "status" else "enum_value"
    if name in {
        "kind",
        "mode",
        "source",
        "state",
        "verdict",
        "lifecycle_state",
        "from_state",
        "to_state",
    }:
        return "enum_value"
    if name == "id" or name.endswith("_id") or name.endswith("_ids"):
        if name == "id" and destination == "domain_revision_id":
            return "namespaced_uuid_with_original"
        return "uuid_text_or_typed_lineage"
    if name in {
        "severity",
        "frequency",
        "confidence",
        "evidence_confidence",
        "pre_penalty_score",
        "final_score",
        "evidence_strength",
        "opportunity_fit",
        "relevance",
    }:
        return "canonical_decimal"
    if ":hex-bytes" in destination:
        return "sha256_hex_bytes"
    return "direct"


PERSISTED_ENTITY_MAPPINGS = {
    item.domain_type.__name__: item
    for item in (
        _entity_mapping(
            domain.ResearchMission,
            storage.ResearchMission,
            routes={},
            storage_only={
                "title": "mission title is supplied by mission admission context",
                "activated_at": "mission lifecycle transition supplies activation time",
                "paused_at": "mission lifecycle transition supplies pause time",
                "archived_at": "mission lifecycle transition supplies archive time",
                "updated_at": "database-managed mutable-row audit timestamp",
            },
        ),
        _entity_mapping(
            domain.MissionRevision,
            storage.MissionRevision,
            routes={"revision": "revision_number", "prompt": "mission_text"},
            storage_only={
                "original_language": "detected mission language is supplied by admission context",
                "interpretation": "mission admission stores bounded interpretation metadata",
            },
        ),
        _entity_mapping(
            domain.ResearchRun,
            storage.ResearchRun,
            routes={"finished_at": "completed_at"},
            storage_only={
                "deadline_at": "run controller derives the persisted deadline",
                "budget_limits": "validated run budgets are supplied by admission context",
                "priority": "run admission supplies bounded scheduling priority",
                "budget_used": "run controller owns durable budget accounting",
                "last_checkpoint": "run controller owns durable workflow checkpoint state",
                "updated_at": "database-managed mutable-row audit timestamp",
            },
        ),
        _entity_mapping(
            domain.ResearchTask,
            storage.ResearchTask,
            routes={"attempt": "attempt_count"},
            storage_only={
                "idempotency_key": "durable task graph supplies the deterministic key",
                "priority": "task graph supplies scheduling priority",
                "payload": "task graph supplies bounded operation payload",
                "checkpoint": "worker owns resumable task checkpoint state",
                "max_attempts": "retry policy supplies the attempt bound",
                "available_at": "queue policy supplies retry availability",
                "retry_class": "retry classifier supplies the stable failure class",
                "last_error": "worker supplies sanitized diagnostic text",
                "result": "worker supplies bounded task result metadata",
                "completed_at": "queue transition supplies completion time",
                "created_at": "database-managed creation timestamp",
                "updated_at": "database-managed mutable-row audit timestamp",
            },
        ),
        _entity_mapping(
            domain.SourceCheckpoint,
            storage.SourceCheckpoint,
            routes={"watermark": "watermark_at"},
            storage_only={
                "id": "checkpoint repository supplies the storage UUID",
                "mission_revision_id": "checkpoint ownership is supplied by collection context",
                "last_successful_run_id": "collector success context supplies run lineage",
                "created_at": "database-managed creation timestamp",
                "updated_at": "database-managed mutable-row audit timestamp",
            },
        ),
        _entity_mapping(
            domain.RawSignal,
            storage.RawSignal,
            routes={
                "parent_thread_id": "parent_external_id",
                "author_identity": "author_pseudonym",
                "title": "raw_signal_revisions.title",
                "body": "raw_signal_revisions.body",
                "original_language": "raw_signal_revisions.original_language",
                "engagement": "raw_signal_revisions.engagement",
                "metadata": "raw_signal_revisions.source_metadata",
                "content_hash": "raw_signal_revisions.content_hash",
                "normalization_version": "raw_signal_revisions.normalization_version",
            },
            storage_only={
                "author_kind": "normalizer classifies known versus unknown author identity",
                "is_tombstone": "collector deletion semantics supply tombstone state",
                "created_at": "database-managed creation timestamp",
                "updated_at": "database-managed mutable-row audit timestamp",
            },
        ),
        _entity_mapping(
            domain.RawSignalRevision,
            storage.RawSignalRevision,
            routes={
                "id": "domain_revision_id",
                "revision": "revision_number",
                "tombstone": "is_tombstone",
                "content_hash": "content_hash:hex-bytes",
            },
            storage_only={
                "id": "deterministic UUIDv5 is derived from domain_revision_id",
                "original_language": "copied from the normalized raw signal",
                "normalization_version": "copied from the normalized raw signal",
                "duplicate_group_key": "deduplication supplies a deterministic SHA-256 group key",
                "engagement": "copied from the normalized raw signal revision payload",
                "source_metadata": "copied from bounded normalized source metadata",
                "search_text": "database-generated title/body similarity document",
                "search_document": "database-generated full-text search vector",
                "created_at": "database-managed creation timestamp",
            },
        ),
        _entity_mapping(
            domain.PainSignal,
            storage.PainSignal,
            routes={
                "job_to_be_done": "jtbd",
                "switching_signal": "signals.switching",
                "payment_signal": "signals.payment",
                "urgency_signal": "signals.urgency",
                "emotion_signal": "signals.emotion",
            },
            storage_only={
                "extraction_version": "versioned extraction stage supplies its algorithm identity",
                "cluster_status": "clustering workflow owns persisted membership state",
                "created_at": "database-managed creation timestamp",
            },
        ),
        _entity_mapping(
            domain.CanonicalProblem,
            storage.CanonicalProblem,
            routes={},
            storage_only={
                "canonical_key": "deterministic clustering identity is supplied by clustering",
                "title": "canonical presentation title is supplied by clustering",
                "status": "canonicalization workflow owns global problem status",
                "created_at": "database-managed creation timestamp",
                "updated_at": "database-managed mutable-row audit timestamp",
            },
        ),
        _entity_mapping(
            domain.ProblemCluster,
            storage.ProblemCluster,
            routes={"state": "status"},
            storage_only={
                "summary": "cluster summary is supplied by clustering output",
                "created_at": "database-managed creation timestamp",
                "updated_at": "database-managed mutable-row audit timestamp",
            },
        ),
        _entity_mapping(
            domain.ProblemClusterMembership,
            storage.ProblemClusterMembership,
            routes={"cluster_id": "problem_cluster_id", "accepted_at": "created_at"},
            storage_only={
                "similarity": "clustering supplies the bounded similarity",
                "rationale": "clustering supplies the evidence-linked rationale",
                "id": "membership repository supplies the storage UUID",
                "removed_at": "membership transition supplies removal time",
                "removed_reason": "membership transition supplies removal rationale",
            },
        ),
        _entity_mapping(
            domain.MergeCandidate,
            storage.MergeCandidate,
            routes={},
            storage_only={
                "decided_at": "merge decision workflow supplies decision time",
                "decision_reason": "merge decision workflow supplies rationale",
                "lifecycle_event_id": "merge decision workflow supplies audit event lineage",
                "created_at": "database-managed creation timestamp",
                "updated_at": "database-managed mutable-row audit timestamp",
            },
        ),
        _entity_mapping(
            domain.EvidenceCard,
            storage.EvidenceCard,
            routes={
                "known_author_ids": "metrics.known_author_ids",
                "thread_ids": "metrics.thread_ids",
                "user_sources": "metrics.user_sources",
                "observed_days": "metrics.observed_days",
                "severity": "metrics.severity",
                "behavioral_workarounds": "metrics.behavioral_workarounds",
                "paid_or_wtp_signals": "metrics.paid_or_wtp_signals",
                "representative_evidence_ids": "representative_signal_ids",
            },
            storage_only={
                "canonical_problem_id": "opportunity lineage resolves the canonical problem",
                "run_id": "pipeline stage context supplies the research run",
                "algorithm_version": "Evidence Card builder supplies its version",
                "independent_authors": "derived from typed known_author_ids",
                "independent_threads": "derived from typed thread_ids",
                "source_count": "derived from typed user_sources",
                "created_at": "database-managed creation timestamp",
            },
        ),
        _entity_mapping(
            domain.AtomicClaim,
            storage.AtomicClaim,
            routes={"kind": "claim_type"},
            storage_only={
                "subject_type": "claim persistence context supplies the typed subject kind",
                "subject_id": "claim persistence context supplies the subject UUID",
                "observed_at": "claim extraction context supplies evidence observation time",
                "created_at": "database-managed creation timestamp",
            },
        ),
        _entity_mapping(
            domain.ProblemHypothesis,
            storage.ProblemHypothesis,
            routes={"job_to_be_done": "jtbd", "falsification_test": "falsifier"},
            storage_only={
                "evidence_card_id": "hypothesis stage supplies the validated Evidence Card",
                "created_at": "database-managed creation timestamp",
            },
        ),
        _entity_mapping(
            domain.Competitor,
            storage.Competitor,
            routes={"kind": "alternative_type"},
            storage_only={
                "normalized_name": "normalizer derives the stable competitor name",
                "created_at": "database-managed creation timestamp",
                "updated_at": "database-managed mutable-row audit timestamp",
            },
        ),
        _entity_mapping(
            domain.CompetitorEvidence,
            storage.CompetitorEvidence,
            routes={"content_hash": "content_hash:hex-bytes"},
            storage_only={"created_at": "database-managed creation timestamp"},
        ),
        _entity_mapping(
            domain.GapHypothesis,
            storage.GapHypothesis,
            routes={},
            storage_only={
                "contradicting_claim_ids": "gap stage supplies explicit contradictions",
                "created_at": "database-managed creation timestamp",
            },
        ),
        _entity_mapping(
            domain.Opportunity,
            storage.Opportunity,
            routes={},
            storage_only={
                "canonical_problem_id": "resolved through the Gap Hypothesis lineage",
                "canonical_key": "opportunity stage derives a deterministic global key",
                "created_at": "database-managed creation timestamp",
                "updated_at": "database-managed mutable-row audit timestamp",
            },
        ),
        _entity_mapping(
            domain.MissionOpportunityAssessment,
            storage.MissionOpportunityAssessment,
            routes={
                "lifecycle_state": "lifecycle_status",
                "score_snapshot_id": "opportunity_score_snapshots.id",
                "evidence_card_id": "evidence_cards.id",
                "assessed_at": "updated_at",
            },
            storage_only={"created_at": "database-managed creation timestamp"},
        ),
        _entity_mapping(
            domain.OpportunityScoreSnapshot,
            storage.OpportunityScoreSnapshot,
            routes={
                "opportunity_id": "mission_opportunity_assessments.opportunity_id",
                "mission_revision_id": "mission_opportunity_assessments.mission_revision_id",
                "evidence_confidence": "confidence",
            },
            storage_only={
                "assessment_id": "assessment context supplies normalized ownership",
                "run_id": "scoring stage context supplies the research run",
                "evidence_components": "typed mapper serializes all EvidenceStrength components",
                "opportunity_fit_components": (
                    "typed mapper serializes all OpportunityFit components"
                ),
                "weights": "typed mapper preserves both axis weight maps",
            },
        ),
        _entity_mapping(
            domain.CriticResult,
            storage.CriticResult,
            routes={"opportunity_id": "mission_opportunity_assessments.opportunity_id"},
            storage_only={
                "assessment_id": "critic stage context supplies the assessment",
                "run_id": "critic stage context supplies the research run",
                "agent_call_id": "provider audit context supplies the exact call",
                "id": "critic persistence context supplies the result UUID",
                "created_at": "database-managed creation timestamp",
            },
        ),
        _entity_mapping(
            domain.ProductHypothesis,
            storage.ProductHypothesis,
            routes={"explicit_request_id": "requested_by", "proposition": "content.proposition"},
            storage_only={
                "evidence_card_id": "explicit post-VALIDATE request supplies the frozen card"
            },
        ),
        _entity_mapping(
            domain.LifecycleEvent,
            storage.LifecycleEvent,
            routes={
                "from_state": "from_status",
                "to_state": "to_status",
                "evidence_ids": "details.evidence_ids",
            },
            storage_only={"run_id": "lifecycle transition context supplies optional run lineage"},
        ),
        _entity_mapping(
            domain.AgentCall,
            storage.AgentCall,
            routes={"request": "request-derived columns", "result": "result-derived columns"},
            storage_only={
                "provider": "derived from AgentResult",
                "operation": "derived from the validated SemanticOperation request",
                "output_schema_name": "derived from the versioned AgentRequest",
                "output_schema_sha256": "derived from canonical finite schema JSON",
                "effort": "derived from the bounded AgentRequest policy",
                "status": "derived from AgentResult through explicit provider status mapping",
                "duration_ms": "derived from AgentResult",
                "requested_model": "provider request context supplies requested model identity",
                "resolved_model": "provider result context supplies resolved model identity",
                "cli_version": "provider result context supplies Codex CLI identity",
                "repair_attempts": "provider execution maps the bounded repair flag to count",
                "usage": "provider result context supplies bounded usage metadata",
                "error_class": "provider result supplies sanitized stable error class",
                "output_json": "validated completed output retained for deterministic replay",
                "output_sha256": "provider execution hashes retained validated output",
            },
        ),
    )
}
