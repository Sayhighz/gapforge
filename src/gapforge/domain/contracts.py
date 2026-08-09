"""Storage-neutral, versioned contracts for the GapForge research engine."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    StringConstraints,
    field_validator,
    model_validator,
)

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


class RunStatus(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_WARNINGS = "COMPLETED_WITH_WARNINGS"
    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    AUTH_REQUIRED = "AUTH_REQUIRED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TaskStatus(StrEnum):
    QUEUED = "QUEUED"
    LEASED = "LEASED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


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


class CompetitorResearchStatus(StrEnum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"
    RESEARCH_UNAVAILABLE = "RESEARCH_UNAVAILABLE"


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


class RunWarning(Contract):
    """Stable, lossless warning emitted while a run still produces useful evidence."""

    code: Identifier
    details: dict[str, Any] = Field(default_factory=dict, max_length=50)

    @field_validator("details")
    @classmethod
    def details_are_bounded(cls, value: dict[str, Any]) -> dict[str, Any]:
        return _bounded_metadata(value)


class ResearchRun(Contract):
    id: UUID
    mission_revision_id: UUID
    mode: RunMode
    status: RunStatus
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    warnings: tuple[RunWarning, ...] = Field(default_factory=tuple, max_length=50)

    @model_validator(mode="after")
    def timestamps_match_status(self) -> ResearchRun:
        terminal = self.status not in {RunStatus.QUEUED, RunStatus.RUNNING}
        if terminal != (self.finished_at is not None):
            raise ValueError("terminal run statuses require finished_at")
        if self.started_at and self.finished_at and self.started_at > self.finished_at:
            raise ValueError("run timestamps are not chronological")
        return self


class ResearchTask(Contract):
    id: UUID
    run_id: UUID
    task_type: Identifier
    status: TaskStatus
    attempt: int = Field(default=0, ge=0, le=10)
    lease_owner: Identifier | None = None
    lease_expires_at: datetime | None = None
    checkpoint: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def lease_fields_match_status(self) -> ResearchTask:
        leased = self.status is TaskStatus.LEASED
        if leased != bool(self.lease_owner and self.lease_expires_at):
            raise ValueError(
                "LEASED tasks require owner and expiry; other tasks cannot retain a lease"
            )
        return self

    @field_validator("checkpoint")
    @classmethod
    def bound_checkpoint(cls, value: dict[str, Any]) -> dict[str, Any]:
        return _bounded_metadata(value)


class MissionRevision(Contract):
    id: UUID
    mission_id: UUID
    revision: int = Field(ge=1)
    parent_revision_id: UUID | None = None
    change_reason: ShortText
    prompt: LongText
    output_locale: Annotated[
        str, StringConstraints(pattern=r"^[a-z]{2}(?:-[A-Z]{2})?$", max_length=10)
    ]
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

    @field_validator("metadata")
    @classmethod
    def bound_raw_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        return _bounded_metadata(value)


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
    excerpt: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=2_000)
    ]


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


class MergeDecision(Contract):
    candidate_id: Identifier
    action: Literal["ACCEPT", "REJECT", "REVERSE"]
    actor: Identifier
    reason: ShortText
    decided_at: datetime


class Citation(Contract):
    evidence_id: Identifier
    source_url: HttpUrl
    excerpt: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=2_000)
    ]
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
        if (
            self.kind in {ClaimKind.PRICE, ClaimKind.FEATURE}
            and self.status is EpistemicStatus.SUPPORTED
        ):
            if not self.citations:
                raise ValueError("supported price/feature claims require captured citations")
        if any(citation.evidence_id not in self.evidence_ids for citation in self.citations):
            raise ValueError("citation evidence IDs must be declared by the claim")
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
    representative_evidence_ids: tuple[Identifier, ...] = Field(
        default_factory=tuple, max_length=50
    )
    confidence: float = Field(ge=0, le=1)
    missing_evidence: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=50)

    @field_validator(
        "known_author_ids",
        "thread_ids",
        "user_sources",
        "supporting_claim_ids",
        "contradicting_claim_ids",
    )
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
    id: Identifier
    competitor_id: Identifier
    source_url: HttpUrl
    captured_excerpt: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=2_000)
    ]
    observed_at: datetime
    content_hash: Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
    evidence_kind: Identifier
    metadata: dict[str, Any] = Field(default_factory=dict)
    claim_ids: tuple[Identifier, ...] = Field(min_length=1, max_length=50)

    @field_validator("metadata")
    @classmethod
    def bound_competitor_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        return _bounded_metadata(value)


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


class MissionOpportunityAssessment(Contract):
    id: Identifier
    mission_revision_id: UUID
    opportunity_id: Identifier
    lifecycle_state: LifecycleState
    relevance: float = Field(ge=0, le=1)
    verdict: Verdict | None = None
    competitor_research_status: CompetitorResearchStatus = CompetitorResearchStatus.INCOMPLETE
    score_snapshot_id: Identifier | None = None
    evidence_card_id: Identifier | None = None
    rejected_at: datetime | None = None
    assessed_at: datetime

    @model_validator(mode="after")
    def validate_requires_evidence_artifacts(self) -> MissionOpportunityAssessment:
        if self.verdict is Verdict.VALIDATE and not (
            self.score_snapshot_id and self.evidence_card_id
        ):
            raise ValueError("VALIDATE assessment requires score snapshot and Evidence Card")
        return self


class ProductHypothesis(Contract):
    id: Identifier
    assessment_id: Identifier
    explicit_request_id: Identifier
    proposition: LongText
    created_at: datetime


class LifecycleEvent(Contract):
    id: Identifier
    assessment_id: Identifier
    from_state: LifecycleState | None
    to_state: LifecycleState
    reason: ShortText
    evidence_ids: tuple[Identifier, ...] = Field(default_factory=tuple, max_length=100)
    created_at: datetime


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
    raw_metrics: dict[ShortText, float] = Field(default_factory=dict, max_length=50)
    penalties: dict[ShortText, float] = Field(default_factory=dict, max_length=20)
    pre_penalty_score: float = Field(ge=0, le=100)
    final_score: float = Field(ge=0, le=100)
    evidence_confidence: float = Field(ge=0, le=1)
    algorithm_version: Literal["gapforge-score-v1"] = "gapforge-score-v1"
    explanation: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=50)
    created_at: datetime

    @field_validator("raw_metrics")
    @classmethod
    def finite_metrics(cls, value: dict[str, float]) -> dict[str, float]:
        import math

        if any(not math.isfinite(metric) for metric in value.values()):
            raise ValueError("raw metrics must be finite")
        return value

    @field_validator("penalties")
    @classmethod
    def bounded_penalties(cls, value: dict[str, float]) -> dict[str, float]:
        if any(not 0 <= penalty <= 100 for penalty in value.values()):
            raise ValueError("penalties must be between 0 and 100")
        return value


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


class CriticInput(Contract):
    opportunity_id: Identifier
    problem_hypothesis: ProblemHypothesis
    evidence_card: EvidenceCard
    gap_hypothesis: GapHypothesis
    competitor_claims: tuple[AtomicClaim, ...] = Field(default_factory=tuple, max_length=100)
    permitted_claim_ids: tuple[Identifier, ...] = Field(default_factory=tuple, max_length=500)


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

    @field_validator("metadata")
    @classmethod
    def bound_collected_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        return _bounded_metadata(value)


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


class AgentEffort(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class SemanticOperation(StrEnum):
    QUERY_PLAN = "QUERY_PLAN"
    EXTRACT = "EXTRACT"
    RELEVANCE = "RELEVANCE"
    CLUSTER = "CLUSTER"
    HYPOTHESIS = "HYPOTHESIS"
    GAP = "GAP"
    CRITIC = "CRITIC"
    DEEP_RESEARCH = "DEEP_RESEARCH"


class AgentStatus(StrEnum):
    COMPLETED = "COMPLETED"
    INVALID_OUTPUT = "INVALID_OUTPUT"
    AUTH_REQUIRED = "AUTH_REQUIRED"
    TIMEOUT = "TIMEOUT"
    FAILED = "FAILED"


class AgentRequest(Contract):
    call_id: Identifier
    task: SemanticOperation
    effort: AgentEffort
    input_json: dict[str, Any] = Field(max_length=100)
    permitted_evidence_ids: tuple[Identifier, ...] = Field(max_length=500)
    permitted_urls: tuple[HttpUrl, ...] = Field(default_factory=tuple, max_length=100)
    output_schema_name: Identifier
    timeout_seconds: int = Field(ge=1, le=1_800)

    @field_validator("input_json")
    @classmethod
    def bound_agent_input(cls, value: dict[str, Any]) -> dict[str, Any]:
        return _bounded_metadata(value)

    @model_validator(mode="after")
    def effort_matches_task(self) -> AgentRequest:
        expected = {
            SemanticOperation.QUERY_PLAN: AgentEffort.MEDIUM,
            SemanticOperation.EXTRACT: AgentEffort.LOW,
            SemanticOperation.RELEVANCE: AgentEffort.LOW,
            SemanticOperation.CLUSTER: AgentEffort.MEDIUM,
            SemanticOperation.HYPOTHESIS: AgentEffort.MEDIUM,
            SemanticOperation.GAP: AgentEffort.MEDIUM,
            SemanticOperation.CRITIC: AgentEffort.MEDIUM,
            SemanticOperation.DEEP_RESEARCH: AgentEffort.HIGH,
        }
        if self.effort is not expected[self.task]:
            raise ValueError("agent effort does not match the bounded task policy")
        return self


class AgentResult(Contract):
    call_id: Identifier
    status: AgentStatus
    output_json: dict[str, Any] | None = Field(default=None, max_length=100)
    provider: Literal["codex_cli", "fake"]
    model_requested: ShortText | None = None
    effort: AgentEffort
    cli_version: ShortText | None = None
    duration_ms: int = Field(ge=0)
    repair_attempted: bool = False
    error_class: Identifier | None = None

    @model_validator(mode="after")
    def output_matches_status(self) -> AgentResult:
        if (self.status is AgentStatus.COMPLETED) != (self.output_json is not None):
            raise ValueError("only completed agent results contain output")
        if self.output_json is not None:
            _bounded_metadata(self.output_json)
        return self


class AgentCall(Contract):
    id: Identifier
    run_id: UUID
    task_id: UUID
    request: AgentRequest
    result: AgentResult
    created_at: datetime

    @model_validator(mode="after")
    def call_ids_match(self) -> AgentCall:
        if self.id != self.request.call_id or self.id != self.result.call_id:
            raise ValueError("agent call, request, and result IDs must match")
        return self


def _bounded_metadata(value: dict[str, Any]) -> dict[str, Any]:
    """Reject oversized source-controlled metadata before storage or agent use."""
    import json

    if len(value) > 50:
        raise ValueError("metadata may contain at most 50 keys")
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("metadata must be JSON serializable") from exc
    if len(encoded.encode("utf-8")) > 20_000:
        raise ValueError("metadata exceeds 20,000 bytes")
    return value
