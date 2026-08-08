"""Storage-neutral, versioned contracts for the GapForge research engine."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, StringConstraints, field_validator, model_validator

SCHEMA_VERSION = "0.1"

ShortText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]
LongText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=20_000)]
Identifier = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]


class Contract(BaseModel):
    """Strict base class shared by persisted and provider-facing contracts."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)
    schema_version: Literal["0.1"] = "0.1"


class MissionStatus(StrEnum):
    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    ARCHIVED = "ARCHIVED"


class RunMode(StrEnum):
    HUNT = "HUNT"
    MONITOR = "MONITOR"


class EpistemicStatus(StrEnum):
    SUPPORTED = "SUPPORTED"
    HYPOTHESIS = "HYPOTHESIS"
    UNKNOWN = "UNKNOWN"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    RESEARCH_UNAVAILABLE = "RESEARCH_UNAVAILABLE"


class Availability(StrEnum):
    AVAILABLE = "AVAILABLE"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
    CONTENT_UNAVAILABLE = "CONTENT_UNAVAILABLE"
    RESEARCH_UNAVAILABLE = "RESEARCH_UNAVAILABLE"


class Source(StrEnum):
    HACKER_NEWS = "HACKER_NEWS"
    GITHUB = "GITHUB"
    REDDIT = "REDDIT"
    BRAVE = "BRAVE"
    STATIC_WEB = "STATIC_WEB"
    USER = "USER"


class QueryIntentKind(StrEnum):
    BROAD = "BROAD"
    TARGETED = "TARGETED"


class Verdict(StrEnum):
    REJECT = "REJECT"
    RESEARCH_MORE = "RESEARCH_MORE"
    VALIDATE = "VALIDATE"


class LifecycleState(StrEnum):
    DISCOVERED = "DISCOVERED"
    RESEARCHING = "RESEARCHING"
    RESEARCH_MORE = "RESEARCH_MORE"
    VALIDATE = "VALIDATE"
    REJECTED = "REJECTED"


class TrendLabel(StrEnum):
    RISING = "RISING"
    FLAT = "FLAT"
    FALLING = "FALLING"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class ClaimKind(StrEnum):
    USER_PAIN = "USER_PAIN"
    PRICE = "PRICE"
    FEATURE = "FEATURE"
    COMPETITOR = "COMPETITOR"
    MARKET = "MARKET"
    OTHER = "OTHER"


class AlternativeKind(StrEnum):
    SAAS = "SAAS"
    APP = "APP"
    SPREADSHEET = "SPREADSHEET"
    MANUAL_WORK = "MANUAL_WORK"
    EMPLOYEE = "EMPLOYEE"
    AGENCY = "AGENCY"
    INTERNAL_SCRIPT = "INTERNAL_SCRIPT"
    OPEN_SOURCE = "OPEN_SOURCE"
    PLATFORM_FEATURE = "PLATFORM_FEATURE"
    DO_NOTHING = "DO_NOTHING"


class GapType(StrEnum):
    WORKFLOW = "WORKFLOW"
    INTEGRATION = "INTEGRATION"
    UX = "UX"
    PRICE = "PRICE"
    SEGMENT = "SEGMENT"
    LOCALIZATION = "LOCALIZATION"
    TRUST = "TRUST"
    AUTOMATION = "AUTOMATION"
    PRIVACY = "PRIVACY"
    COLLABORATION = "COLLABORATION"
    DISTRIBUTION = "DISTRIBUTION"
    COMPLEXITY = "COMPLEXITY"
    SPEED = "SPEED"
    MOBILE = "MOBILE"
    BUSINESS_MODEL = "BUSINESS_MODEL"


class ResearchMission(Contract):
    id: UUID
    status: MissionStatus = MissionStatus.DRAFT
    created_at: datetime


class MissionRevision(Contract):
    id: UUID
    mission_id: UUID
    revision: int = Field(ge=1)
    parent_revision_id: UUID | None = None
    change_reason: ShortText
    prompt: LongText
    output_locale: Annotated[str, StringConstraints(pattern=r"^[a-z]{2}(?:-[A-Z]{2})?$", max_length=10)]
    created_at: datetime

    @model_validator(mode="after")
    def require_parent_after_first(self) -> MissionRevision:
        if (self.revision == 1) != (self.parent_revision_id is None):
            raise ValueError("only revision 1 may omit parent_revision_id")
        return self


class QueryIntent(Contract):
    id: Identifier
    kind: QueryIntentKind
    concept: ShortText
    audience: ShortText | None = None
    behavior: ShortText | None = None
    negatives: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=12)
    sources: tuple[Source, ...] = Field(min_length=1, max_length=5)
    rationale: ShortText
    prior_yield: float | None = Field(default=None, ge=0, le=1)


class QueryPlan(Contract):
    round_number: int = Field(ge=1, le=2)
    intents: tuple[QueryIntent, ...] = Field(max_length=12)

    @model_validator(mode="after")
    def enforce_intent_caps(self) -> QueryPlan:
        broad = sum(intent.kind is QueryIntentKind.BROAD for intent in self.intents)
        targeted = len(self.intents) - broad
        if broad > 8 or targeted > 4:
            raise ValueError("query plan exceeds 8 broad or 4 targeted intents")
        if len({intent.id for intent in self.intents}) != len(self.intents):
            raise ValueError("query intent IDs must be unique")
        return self


class Engagement(Contract):
    score: int = Field(default=0, ge=0)
    comments: int = Field(default=0, ge=0)
    reactions: int = Field(default=0, ge=0)


class RawSignal(Contract):
    id: Identifier
    source: Source
    external_id: Identifier
    canonical_url: HttpUrl
    parent_thread_id: Identifier | None = None
    author_identity: Identifier | None = None
    title: ShortText | None = None
    body: LongText | None = None
    original_language: Annotated[str, StringConstraints(min_length=2, max_length=16)] = "und"
    source_created_at: datetime
    collected_at: datetime
    source_edited_at: datetime | None = None
    source_deleted_at: datetime | None = None
    engagement: Engagement = Field(default_factory=Engagement)
    metadata: dict[str, Any] = Field(default_factory=dict)
    content_hash: Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
    normalization_version: Literal["1"] = "1"

    @model_validator(mode="after")
    def require_content_or_tombstone(self) -> RawSignal:
        if self.source_deleted_at is None and not (self.title or self.body):
            raise ValueError("non-deleted raw signal requires title or body")
        return self


class RawSignalRevision(Contract):
    id: Identifier
    raw_signal_id: Identifier
    revision: int = Field(ge=1)
    title: ShortText | None = None
    body: LongText | None = None
    content_hash: Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
    observed_at: datetime
    tombstone: bool = False


class PainSignal(Contract):
    id: Identifier
    raw_signal_revision_id: Identifier
    pain: ShortText
    user_context: ShortText | None = None
    job_to_be_done: ShortText | None = None
    severity: float = Field(ge=0, le=1)
    frequency: float = Field(ge=0, le=1)
    workaround: ShortText | None = None
    existing_solution: ShortText | None = None
    switching_signal: bool = False
    payment_signal: bool = False
    urgency_signal: bool = False
    emotion_signal: bool = False
    confidence: float = Field(ge=0, le=1)
    excerpt: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=2_000)]


class CanonicalProblem(Contract):
    id: Identifier
    summary: ShortText


class ProblemCluster(Contract):
    id: Identifier
    canonical_problem_id: Identifier
    state: Literal["PROVISIONAL", "ACTIVE", "DORMANT"]
    last_growth_at: datetime


class ProblemClusterMembership(Contract):
    cluster_id: Identifier
    pain_signal_id: Identifier
    accepted_at: datetime


class MergeCandidate(Contract):
    id: Identifier
    left_problem_id: Identifier
    right_problem_id: Identifier
    similarity: float = Field(ge=0, le=1)
    status: Literal["PENDING", "ACCEPTED", "REJECTED", "REVERSED"] = "PENDING"
    rationale: ShortText

    @model_validator(mode="after")
    def reject_self_merge(self) -> MergeCandidate:
        if self.left_problem_id == self.right_problem_id:
            raise ValueError("a problem cannot be merged with itself")
        return self


class Citation(Contract):
    evidence_id: Identifier
    source_url: HttpUrl
    excerpt: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=2_000)]
    observed_at: datetime


class AtomicClaim(Contract):
    id: Identifier
    text: ShortText
    kind: ClaimKind
    status: EpistemicStatus
    evidence_ids: tuple[Identifier, ...] = Field(default_factory=tuple, max_length=50)
    citations: tuple[Citation, ...] = Field(default_factory=tuple, max_length=50)
    contradicts_claim_ids: tuple[Identifier, ...] = Field(default_factory=tuple, max_length=50)

    @model_validator(mode="after")
    def supported_claim_has_evidence(self) -> AtomicClaim:
        if self.status is EpistemicStatus.SUPPORTED and not self.evidence_ids:
            raise ValueError("SUPPORTED claims require evidence IDs")
        if self.kind in {ClaimKind.PRICE, ClaimKind.FEATURE} and self.status is EpistemicStatus.SUPPORTED:
            if not self.citations:
                raise ValueError("supported price/feature claims require captured citations")
        return self


class EvidenceCard(Contract):
    id: Identifier
    opportunity_id: Identifier
    known_author_ids: tuple[Identifier, ...] = Field(default_factory=tuple, max_length=500)
    thread_ids: tuple[Identifier, ...] = Field(default_factory=tuple, max_length=500)
    user_sources: tuple[Source, ...] = Field(default_factory=tuple, max_length=5)
    observed_days: tuple[datetime, ...] = Field(default_factory=tuple, max_length=500)
    severity: float = Field(ge=0, le=1)
    behavioral_workarounds: int = Field(ge=0)
    paid_or_wtp_signals: int = Field(ge=0)
    supporting_claim_ids: tuple[Identifier, ...] = Field(default_factory=tuple, max_length=500)
    contradicting_claim_ids: tuple[Identifier, ...] = Field(default_factory=tuple, max_length=500)
    representative_evidence_ids: tuple[Identifier, ...] = Field(default_factory=tuple, max_length=50)
    confidence: float = Field(ge=0, le=1)
    missing_evidence: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=50)

    @field_validator("known_author_ids", "thread_ids", "user_sources", "supporting_claim_ids", "contradicting_claim_ids")
    @classmethod
    def must_be_unique(cls, value: tuple[Any, ...]) -> tuple[Any, ...]:
        if len(set(value)) != len(value):
            raise ValueError("independence metrics must contain unique values")
        return value


class ProblemHypothesis(Contract):
    id: Identifier
    canonical_problem_id: Identifier
    icp: ShortText
    job_to_be_done: ShortText
    trigger: ShortText
    current_behavior: ShortText
    pain: ShortText
    workflow_failure: ShortText
    falsification_test: ShortText
    supporting_claim_ids: tuple[Identifier, ...] = Field(min_length=1, max_length=50)
    contradicting_claim_ids: tuple[Identifier, ...] = Field(default_factory=tuple, max_length=50)


class Competitor(Contract):
    id: Identifier
    name: ShortText
    kind: AlternativeKind
    canonical_url: HttpUrl | None = None


class CompetitorEvidence(Contract):
    competitor_id: Identifier
    claim_ids: tuple[Identifier, ...] = Field(min_length=1, max_length=50)


class GapHypothesis(Contract):
    id: Identifier
    canonical_problem_id: Identifier
    gap_type: GapType
    statement: ShortText
    user_evidence_ids: tuple[Identifier, ...] = Field(min_length=1, max_length=50)
    competitor_evidence_ids: tuple[Identifier, ...] = Field(min_length=1, max_length=50)


class Opportunity(Contract):
    id: Identifier
    gap_hypothesis_id: Identifier
    title: ShortText


class ScoreComponents(Contract):
    values: dict[ShortText, float] = Field(max_length=20)
    weights: dict[ShortText, float] = Field(max_length=20)

    @field_validator("values")
    @classmethod
    def bounded_values(cls, value: dict[str, float]) -> dict[str, float]:
        if any(not 0 <= score <= 100 for score in value.values()):
            raise ValueError("component values must be between 0 and 100")
        return value

    @model_validator(mode="after")
    def matching_weights(self) -> ScoreComponents:
        if set(self.values) != set(self.weights):
            raise ValueError("every score component requires one weight")
        if any(weight < 0 for weight in self.weights.values()):
            raise ValueError("weights cannot be negative")
        if self.weights and abs(sum(self.weights.values()) - 1.0) > 1e-6:
            raise ValueError("weights must sum to 1")
        return self


class OpportunityScoreSnapshot(Contract):
    id: Identifier
    opportunity_id: Identifier
    mission_revision_id: UUID
    evidence_strength: ScoreComponents
    opportunity_fit: ScoreComponents
    penalties: dict[ShortText, float] = Field(default_factory=dict, max_length=20)
    pre_penalty_score: float = Field(ge=0, le=100)
    final_score: float = Field(ge=0, le=100)
    evidence_confidence: float = Field(ge=0, le=1)
    algorithm_version: Literal["gapforge-score-v1"] = "gapforge-score-v1"
    explanation: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=50)
    created_at: datetime


class CriticResult(Contract):
    opportunity_id: Identifier
    verdict: Verdict
    confidence: float = Field(ge=0, le=1)
    fatal_flags: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=20)
    weak_assumptions: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=30)
    contradictions: tuple[Identifier, ...] = Field(default_factory=tuple, max_length=50)
    missing_evidence: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=30)
    recommended_intents: tuple[QueryIntent, ...] = Field(default_factory=tuple, max_length=4)
    summary: ShortText


class SourceCheckpoint(Contract):
    source: Source
    cursor: ShortText | None = None
    watermark: datetime | None = None


class SourceWarning(Contract):
    code: Identifier
    message: ShortText
    retryable: bool = False


class CollectRequest(Contract):
    mission_revision_id: UUID
    intent: QueryIntent
    since: datetime
    until: datetime
    max_requests: int = Field(ge=1, le=60)
    max_signals: int = Field(ge=1, le=300)
    checkpoint: SourceCheckpoint | None = None

    @model_validator(mode="after")
    def chronological_window(self) -> CollectRequest:
        if self.since >= self.until:
            raise ValueError("since must precede until")
        return self


class CollectedItem(Contract):
    source: Source
    external_id: Identifier
    canonical_url: HttpUrl
    parent_thread_id: Identifier | None = None
    author_identity: Identifier | None = None
    title: ShortText | None = None
    body: LongText | None = None
    source_created_at: datetime
    source_edited_at: datetime | None = None
    source_deleted_at: datetime | None = None
    engagement: Engagement = Field(default_factory=Engagement)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CollectResult(Contract):
    source: Source
    availability: Availability
    items: tuple[CollectedItem, ...] = Field(default_factory=tuple, max_length=300)
    checkpoint: SourceCheckpoint | None = None
    request_count: int = Field(ge=0, le=60)
    warnings: tuple[SourceWarning, ...] = Field(default_factory=tuple, max_length=50)


class SearchResult(Contract):
    id: Identifier
    title: ShortText
    url: HttpUrl
    snippet: ShortText
    observed_at: datetime
    rank: int = Field(ge=1, le=20)


class SearchResponse(Contract):
    availability: Availability
    query: ShortText
    results: tuple[SearchResult, ...] = Field(default_factory=tuple, max_length=20)
    request_count: int = Field(ge=0, le=20)
    warnings: tuple[SourceWarning, ...] = Field(default_factory=tuple, max_length=20)


class FetchSnapshot(Contract):
    url: HttpUrl
    final_url: HttpUrl
    text: LongText
    content_type: Literal["text/html", "text/plain"]
    sha256: Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
    observed_at: datetime


class FetchResult(Contract):
    availability: Availability
    snapshot: FetchSnapshot | None = None
    warnings: tuple[SourceWarning, ...] = Field(default_factory=tuple, max_length=20)

    @model_validator(mode="after")
    def snapshot_matches_availability(self) -> FetchResult:
        if (self.availability is Availability.AVAILABLE) != (self.snapshot is not None):
            raise ValueError("only AVAILABLE fetch results contain a snapshot")
        return self


class RepairRequest(Contract):
    original_call_id: Identifier
    validation_errors: tuple[ShortText, ...] = Field(min_length=1, max_length=20)
    repair_attempt: Literal[1] = 1
