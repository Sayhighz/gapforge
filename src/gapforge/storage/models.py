"""SQLAlchemy persistence model for the complete v0.1 lineage skeleton."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar
from uuid import UUID, uuid4

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    CheckConstraint,
    Computed,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

JSONValue = dict[str, Any]


class Base(DeclarativeBase):
    """Declarative base with deterministic constraint names for Alembic."""

    type_annotation_map: ClassVar[dict[object, object]] = {JSONValue: JSONB}
    metadata = MetaData(
        naming_convention={
            "ix": "ix_%(table_name)s_%(column_0_name)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


class IdMixin:
    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)


class CreatedAtMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class UpdatedAtMixin(CreatedAtMixin):
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class ResearchMission(IdMixin, UpdatedAtMixin, Base):
    __tablename__ = "research_missions"
    __table_args__ = (
        CheckConstraint("status IN ('DRAFT', 'ACTIVE', 'PAUSED', 'ARCHIVED')", name="valid_status"),
        Index("ix_research_missions_status_created", "status", "created_at"),
    )

    status: Mapped[str] = mapped_column(String(16), nullable=False, default="DRAFT")
    title: Mapped[str] = mapped_column(String(240), nullable=False)
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    paused_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class MissionRevision(IdMixin, CreatedAtMixin, Base):
    __tablename__ = "mission_revisions"
    __table_args__ = (
        UniqueConstraint("mission_id", "revision_number"),
        CheckConstraint("revision_number >= 1", name="positive_revision_number"),
        Index("ix_mission_revisions_mission_created", "mission_id", "created_at"),
    )

    mission_id: Mapped[UUID] = mapped_column(
        ForeignKey("research_missions.id", ondelete="RESTRICT"), nullable=False
    )
    revision_number: Mapped[int] = mapped_column(Integer, nullable=False)
    parent_revision_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("mission_revisions.id", ondelete="RESTRICT")
    )
    change_reason: Mapped[str] = mapped_column(Text, nullable=False)
    mission_text: Mapped[str] = mapped_column(Text, nullable=False)
    original_language: Mapped[str] = mapped_column(String(32), nullable=False)
    output_locale: Mapped[str] = mapped_column(String(32), nullable=False)
    interpretation: Mapped[JSONValue] = mapped_column(JSONB, nullable=False, default=dict)


class ResearchRun(IdMixin, UpdatedAtMixin, Base):
    __tablename__ = "research_runs"
    __table_args__ = (
        CheckConstraint("mode IN ('HUNT', 'MONITOR')", name="valid_mode"),
        CheckConstraint(
            "status IN ('QUEUED', 'RUNNING', 'COMPLETED', 'COMPLETED_WITH_WARNINGS', "
            "'BUDGET_EXHAUSTED', 'AUTH_REQUIRED', 'FAILED', 'CANCELLED')",
            name="valid_status",
        ),
        Index("ix_research_runs_status_priority", "status", "priority", "created_at"),
        Index(
            "uq_research_runs_one_global_active",
            text("((1))"),
            unique=True,
            postgresql_where=text("status = 'RUNNING'"),
        ),
        Index(
            "uq_research_runs_revision_active",
            "mission_revision_id",
            unique=True,
            postgresql_where=text("status IN ('QUEUED', 'RUNNING')"),
        ),
    )

    mission_revision_id: Mapped[UUID] = mapped_column(
        ForeignKey("mission_revisions.id", ondelete="RESTRICT"), nullable=False
    )
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="QUEUED")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    deadline_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    budget_limits: Mapped[JSONValue] = mapped_column(JSONB, nullable=False)
    budget_used: Mapped[JSONValue] = mapped_column(JSONB, nullable=False, default=dict)
    warnings: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)
    last_checkpoint: Mapped[JSONValue] = mapped_column(JSONB, nullable=False, default=dict)


class ResearchTask(IdMixin, UpdatedAtMixin, Base):
    __tablename__ = "research_tasks"
    __table_args__ = (
        UniqueConstraint("run_id", "idempotency_key"),
        CheckConstraint("attempt_count >= 0", name="nonnegative_attempt_count"),
        CheckConstraint("max_attempts >= 1", name="positive_max_attempts"),
        CheckConstraint(
            "status IN ('PENDING', 'LEASED', 'SUCCEEDED', 'FAILED', 'CANCELLED')",
            name="valid_status",
        ),
        Index(
            "ix_research_tasks_claim",
            "status",
            "available_at",
            "priority",
            "created_at",
        ),
        Index("ix_research_tasks_lease_expiry", "lease_expires_at"),
    )

    run_id: Mapped[UUID] = mapped_column(
        ForeignKey("research_runs.id", ondelete="CASCADE"), nullable=False
    )
    task_type: Mapped[str] = mapped_column(String(80), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="PENDING")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    idempotency_key: Mapped[str] = mapped_column(String(240), nullable=False)
    payload: Mapped[JSONValue] = mapped_column(JSONB, nullable=False, default=dict)
    checkpoint: Mapped[JSONValue] = mapped_column(JSONB, nullable=False, default=dict)
    result: Mapped[JSONValue | None] = mapped_column(JSONB)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    retry_class: Mapped[str | None] = mapped_column(String(80))
    last_error: Mapped[str | None] = mapped_column(Text)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    lease_owner: Mapped[str | None] = mapped_column(String(160))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ProviderCallLease(IdMixin, UpdatedAtMixin, Base):
    __tablename__ = "provider_call_leases"
    __table_args__ = (
        UniqueConstraint("run_id", "call_key"),
        CheckConstraint("octet_length(output_schema_sha256) = 32", name="schema_hash_length"),
        CheckConstraint("octet_length(request_sha256) = 32", name="request_hash_length"),
        CheckConstraint(
            "call_key ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'",
            name="canonical_call_key",
        ),
        CheckConstraint("repair_attempt IN (0, 1)", name="repair_attempt_range"),
        Index("ix_provider_call_leases_run_expiry", "run_id", "lease_expires_at"),
    )

    run_id: Mapped[UUID] = mapped_column(
        ForeignKey("research_runs.id", ondelete="CASCADE"), nullable=False
    )
    task_id: Mapped[UUID] = mapped_column(
        ForeignKey("research_tasks.id", ondelete="CASCADE"), nullable=False
    )
    call_key: Mapped[str] = mapped_column(String(240), nullable=False)
    lease_owner: Mapped[str] = mapped_column(String(160), nullable=False)
    lease_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    operation: Mapped[str] = mapped_column(String(80), nullable=False)
    output_schema_name: Mapped[str] = mapped_column(String(200), nullable=False)
    output_schema_sha256: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    request_sha256: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    requested_model: Mapped[str] = mapped_column(String(160), nullable=False, default="")
    effort: Mapped[str] = mapped_column(String(16), nullable=False)
    repair_attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class SourceCheckpoint(IdMixin, UpdatedAtMixin, Base):
    __tablename__ = "source_checkpoints"
    __table_args__ = (
        UniqueConstraint("mission_revision_id", "source"),
        Index("ix_source_checkpoints_source_watermark", "source", "watermark_at"),
    )

    mission_revision_id: Mapped[UUID] = mapped_column(
        ForeignKey("mission_revisions.id", ondelete="CASCADE"), nullable=False
    )
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    cursor: Mapped[JSONValue] = mapped_column(JSONB, nullable=False, default=dict)
    watermark_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_successful_run_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("research_runs.id", ondelete="SET NULL")
    )


class RawSignal(IdMixin, UpdatedAtMixin, Base):
    __tablename__ = "raw_signals"
    __table_args__ = (
        UniqueConstraint("source", "external_id"),
        Index("ix_raw_signals_canonical_url", "canonical_url"),
        Index(
            "ix_raw_signals_canonical_url_trgm",
            "canonical_url",
            postgresql_using="gin",
            postgresql_ops={"canonical_url": "gin_trgm_ops"},
        ),
        Index("ix_raw_signals_source_collected", "source", "collected_at"),
        Index("ix_raw_signals_author_pseudonym", "author_pseudonym"),
    )

    source: Mapped[str] = mapped_column(String(64), nullable=False)
    external_id: Mapped[str] = mapped_column(String(512), nullable=False)
    canonical_url: Mapped[str] = mapped_column(Text, nullable=False)
    parent_external_id: Mapped[str | None] = mapped_column(String(512))
    author_pseudonym: Mapped[str | None] = mapped_column(String(128))
    author_kind: Mapped[str] = mapped_column(String(24), nullable=False, default="UNKNOWN")
    source_created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_edited_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_tombstone: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class RawSignalRevision(IdMixin, CreatedAtMixin, Base):
    __tablename__ = "raw_signal_revisions"
    __table_args__ = (
        UniqueConstraint(
            "raw_signal_id", "revision_number", name="uq_raw_signal_revisions_version"
        ),
        CheckConstraint("revision_number >= 1", name="positive_revision_number"),
        UniqueConstraint("raw_signal_id", "content_hash", name="uq_raw_signal_revisions_content"),
        Index("ix_raw_signal_revisions_hash", "content_hash"),
        Index("ix_raw_signal_revisions_duplicate_group", "duplicate_group_key"),
        Index(
            "ix_raw_signal_revisions_search_document",
            "search_document",
            postgresql_using="gin",
        ),
        Index(
            "ix_raw_signal_revisions_search_text_trgm",
            "search_text",
            postgresql_using="gin",
            postgresql_ops={"search_text": "gin_trgm_ops"},
        ),
        CheckConstraint(
            "duplicate_group_key ~ '^[0-9a-f]{64}$'",
            name="duplicate_group_key_format",
        ),
    )

    raw_signal_id: Mapped[UUID] = mapped_column(
        ForeignKey("raw_signals.id", ondelete="RESTRICT"), nullable=False
    )
    revision_number: Mapped[int] = mapped_column(Integer, nullable=False)
    domain_revision_id: Mapped[str] = mapped_column(String(200), nullable=False, unique=True)
    title: Mapped[str | None] = mapped_column(Text)
    body: Mapped[str | None] = mapped_column(Text)
    original_language: Mapped[str] = mapped_column(String(32), nullable=False)
    engagement: Mapped[JSONValue] = mapped_column(JSONB, nullable=False, default=dict)
    source_metadata: Mapped[JSONValue] = mapped_column(JSONB, nullable=False, default=dict)
    content_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    normalization_version: Mapped[str] = mapped_column(String(32), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_tombstone: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    duplicate_group_key: Mapped[str] = mapped_column(String(64), nullable=False)
    search_text: Mapped[str] = mapped_column(
        Text,
        Computed("coalesce(title, '') || ' ' || coalesce(body, '')", persisted=True),
    )
    search_document: Mapped[str] = mapped_column(
        TSVECTOR,
        Computed(
            "to_tsvector('simple', coalesce(title, '') || ' ' || coalesce(body, ''))",
            persisted=True,
        ),
    )


class PainSignal(IdMixin, CreatedAtMixin, Base):
    __tablename__ = "pain_signals"
    __table_args__ = (
        UniqueConstraint("raw_signal_revision_id", "extraction_version"),
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="confidence_range"),
        CheckConstraint("severity >= 0 AND severity <= 1", name="severity_range"),
        CheckConstraint("frequency >= 0 AND frequency <= 1", name="frequency_range"),
        Index("ix_pain_signals_status_created", "cluster_status", "created_at"),
    )

    raw_signal_revision_id: Mapped[UUID] = mapped_column(
        ForeignKey("raw_signal_revisions.id", ondelete="RESTRICT"), nullable=False
    )
    extraction_version: Mapped[str] = mapped_column(String(32), nullable=False)
    cluster_status: Mapped[str] = mapped_column(String(24), nullable=False, default="UNCLUSTERED")
    pain: Mapped[str] = mapped_column(Text, nullable=False)
    user_context: Mapped[str | None] = mapped_column(Text)
    jtbd: Mapped[str | None] = mapped_column(Text)
    severity: Mapped[Decimal] = mapped_column(Numeric(18, 17), nullable=False)
    frequency: Mapped[Decimal] = mapped_column(Numeric(18, 17), nullable=False)
    workaround: Mapped[str | None] = mapped_column(Text)
    existing_solution: Mapped[str | None] = mapped_column(Text)
    signals: Mapped[JSONValue] = mapped_column(JSONB, nullable=False, default=dict)
    confidence: Mapped[Decimal] = mapped_column(Numeric(18, 17), nullable=False)
    excerpt: Mapped[str] = mapped_column(Text, nullable=False)


class CanonicalProblem(IdMixin, UpdatedAtMixin, Base):
    __tablename__ = "canonical_problems"
    __table_args__ = (Index("ix_canonical_problems_status", "status"),)

    canonical_key: Mapped[str] = mapped_column(String(240), nullable=False, unique=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="ACTIVE")


class ProblemCluster(IdMixin, UpdatedAtMixin, Base):
    __tablename__ = "problem_clusters"
    __table_args__ = (
        Index("ix_problem_clusters_problem_status", "canonical_problem_id", "status"),
    )

    canonical_problem_id: Mapped[UUID] = mapped_column(
        ForeignKey("canonical_problems.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="PROVISIONAL")
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    last_growth_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ProblemClusterMembership(IdMixin, CreatedAtMixin, Base):
    __tablename__ = "problem_cluster_memberships"
    __table_args__ = (
        UniqueConstraint("problem_cluster_id", "pain_signal_id"),
        CheckConstraint("similarity >= 0 AND similarity <= 1", name="similarity_range"),
        Index("ix_cluster_memberships_pain", "pain_signal_id"),
    )

    problem_cluster_id: Mapped[UUID] = mapped_column(
        ForeignKey("problem_clusters.id", ondelete="RESTRICT"), nullable=False
    )
    pain_signal_id: Mapped[UUID] = mapped_column(
        ForeignKey("pain_signals.id", ondelete="RESTRICT"), nullable=False
    )
    similarity: Mapped[float] = mapped_column(Float, nullable=False)
    rationale: Mapped[str] = mapped_column(Text, nullable=False)
    removed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    removed_reason: Mapped[str | None] = mapped_column(Text)


class MergeCandidate(IdMixin, UpdatedAtMixin, Base):
    __tablename__ = "merge_candidates"
    __table_args__ = (
        UniqueConstraint("left_problem_id", "right_problem_id"),
        CheckConstraint("left_problem_id <> right_problem_id", name="different_problems"),
        CheckConstraint("similarity >= 0 AND similarity <= 1", name="similarity_range"),
        Index("ix_merge_candidates_status_created", "status", "created_at"),
    )

    left_problem_id: Mapped[UUID] = mapped_column(
        ForeignKey("canonical_problems.id", ondelete="RESTRICT"), nullable=False
    )
    right_problem_id: Mapped[UUID] = mapped_column(
        ForeignKey("canonical_problems.id", ondelete="RESTRICT"), nullable=False
    )
    similarity: Mapped[float] = mapped_column(Float, nullable=False)
    rationale: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="PENDING")
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    decision_reason: Mapped[str | None] = mapped_column(Text)
    lifecycle_event_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("lifecycle_events.id", ondelete="SET NULL")
    )


class EvidenceCard(IdMixin, CreatedAtMixin, Base):
    __tablename__ = "evidence_cards"
    __table_args__ = (
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="confidence_range"),
        ForeignKeyConstraint(
            ["opportunity_id", "canonical_problem_id"],
            ["opportunities.id", "opportunities.canonical_problem_id"],
            ondelete="RESTRICT",
        ),
        Index("ix_evidence_cards_problem_created", "canonical_problem_id", "created_at"),
        Index("ix_evidence_cards_opportunity_created", "opportunity_id", "created_at"),
    )

    canonical_problem_id: Mapped[UUID] = mapped_column(
        ForeignKey("canonical_problems.id", ondelete="RESTRICT"), nullable=False
    )
    opportunity_id: Mapped[UUID] = mapped_column(nullable=False)
    run_id: Mapped[UUID] = mapped_column(
        ForeignKey("research_runs.id", ondelete="RESTRICT"), nullable=False
    )
    algorithm_version: Mapped[str] = mapped_column(String(32), nullable=False)
    independent_authors: Mapped[int] = mapped_column(Integer, nullable=False)
    independent_threads: Mapped[int] = mapped_column(Integer, nullable=False)
    source_count: Mapped[int] = mapped_column(Integer, nullable=False)
    metrics: Mapped[JSONValue] = mapped_column(JSONB, nullable=False)
    supporting_claim_ids: Mapped[list[UUID]] = mapped_column(
        ARRAY(PG_UUID(as_uuid=True)), nullable=False
    )
    contradicting_claim_ids: Mapped[list[UUID]] = mapped_column(
        ARRAY(PG_UUID(as_uuid=True)), nullable=False
    )
    representative_signal_ids: Mapped[list[UUID]] = mapped_column(
        ARRAY(PG_UUID(as_uuid=True)), nullable=False
    )
    confidence: Mapped[Decimal] = mapped_column(Numeric(18, 17), nullable=False)
    missing_evidence: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)


class AtomicClaim(IdMixin, CreatedAtMixin, Base):
    __tablename__ = "atomic_claims"
    __table_args__ = (
        CheckConstraint(
            "status IN ('SUPPORTED', 'HYPOTHESIS', 'UNKNOWN', 'INSUFFICIENT_EVIDENCE', "
            "'RESEARCH_UNAVAILABLE')",
            name="valid_status",
        ),
        CheckConstraint(
            "status <> 'SUPPORTED' OR cardinality(evidence_ids) > 0", name="supported_has_evidence"
        ),
        Index("ix_atomic_claims_subject", "subject_type", "subject_id", "created_at"),
    )

    subject_type: Mapped[str] = mapped_column(String(64), nullable=False)
    subject_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    claim_type: Mapped[str] = mapped_column(String(64), nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    evidence_ids: Mapped[list[UUID]] = mapped_column(ARRAY(PG_UUID(as_uuid=True)), nullable=False)
    citations: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)
    contradicts_claim_ids: Mapped[list[UUID]] = mapped_column(
        ARRAY(PG_UUID(as_uuid=True)), nullable=False, default=list
    )
    observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ProblemHypothesis(IdMixin, CreatedAtMixin, Base):
    __tablename__ = "problem_hypotheses"
    __table_args__ = (Index("ix_problem_hypotheses_problem", "canonical_problem_id", "created_at"),)

    canonical_problem_id: Mapped[UUID] = mapped_column(
        ForeignKey("canonical_problems.id", ondelete="RESTRICT"), nullable=False
    )
    evidence_card_id: Mapped[UUID] = mapped_column(
        ForeignKey("evidence_cards.id", ondelete="RESTRICT"), nullable=False
    )
    icp: Mapped[str] = mapped_column(Text, nullable=False)
    jtbd: Mapped[str] = mapped_column(Text, nullable=False)
    trigger: Mapped[str] = mapped_column(Text, nullable=False)
    current_behavior: Mapped[str] = mapped_column(Text, nullable=False)
    pain: Mapped[str] = mapped_column(Text, nullable=False)
    workflow_failure: Mapped[str] = mapped_column(Text, nullable=False)
    falsifier: Mapped[str] = mapped_column(Text, nullable=False)
    supporting_claim_ids: Mapped[list[UUID]] = mapped_column(
        ARRAY(PG_UUID(as_uuid=True)), nullable=False
    )
    contradicting_claim_ids: Mapped[list[UUID]] = mapped_column(
        ARRAY(PG_UUID(as_uuid=True)), nullable=False
    )


class Competitor(IdMixin, UpdatedAtMixin, Base):
    __tablename__ = "competitors"
    __table_args__ = (UniqueConstraint("normalized_name", "canonical_url"),)

    name: Mapped[str] = mapped_column(String(240), nullable=False)
    normalized_name: Mapped[str] = mapped_column(String(240), nullable=False)
    canonical_url: Mapped[str] = mapped_column(Text, nullable=False, default="")
    alternative_type: Mapped[str] = mapped_column(String(64), nullable=False)


class CompetitorEvidence(IdMixin, CreatedAtMixin, Base):
    __tablename__ = "competitor_evidence"
    __table_args__ = (
        UniqueConstraint("competitor_id", "source_url", "content_hash"),
        Index("ix_competitor_evidence_observed", "competitor_id", "observed_at"),
    )

    competitor_id: Mapped[UUID] = mapped_column(
        ForeignKey("competitors.id", ondelete="RESTRICT"), nullable=False
    )
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    captured_excerpt: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    content_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    evidence_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    claim_ids: Mapped[list[UUID]] = mapped_column(ARRAY(PG_UUID(as_uuid=True)), nullable=False)
    metadata_json: Mapped[JSONValue] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict
    )


class GapHypothesis(IdMixin, CreatedAtMixin, Base):
    __tablename__ = "gap_hypotheses"
    __table_args__ = (
        Index("ix_gap_hypotheses_problem_created", "canonical_problem_id", "created_at"),
        CheckConstraint("cardinality(user_evidence_ids) > 0", name="has_user_evidence"),
        CheckConstraint("cardinality(competitor_evidence_ids) > 0", name="has_competitor_evidence"),
    )

    canonical_problem_id: Mapped[UUID] = mapped_column(
        ForeignKey("canonical_problems.id", ondelete="RESTRICT"), nullable=False
    )
    gap_type: Mapped[str] = mapped_column(String(64), nullable=False)
    statement: Mapped[str] = mapped_column(Text, nullable=False)
    user_evidence_ids: Mapped[list[UUID]] = mapped_column(
        ARRAY(PG_UUID(as_uuid=True)), nullable=False
    )
    competitor_evidence_ids: Mapped[list[UUID]] = mapped_column(
        ARRAY(PG_UUID(as_uuid=True)), nullable=False
    )
    contradicting_claim_ids: Mapped[list[UUID]] = mapped_column(
        ARRAY(PG_UUID(as_uuid=True)), nullable=False
    )


class Opportunity(IdMixin, UpdatedAtMixin, Base):
    __tablename__ = "opportunities"
    __table_args__ = (
        UniqueConstraint("id", "canonical_problem_id"),
        Index("ix_opportunities_problem", "canonical_problem_id"),
    )

    canonical_problem_id: Mapped[UUID] = mapped_column(
        ForeignKey("canonical_problems.id", ondelete="RESTRICT"), nullable=False
    )
    gap_hypothesis_id: Mapped[UUID] = mapped_column(
        ForeignKey("gap_hypotheses.id", ondelete="RESTRICT"), nullable=False
    )
    canonical_key: Mapped[str] = mapped_column(String(240), nullable=False, unique=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)


class MissionOpportunityAssessment(IdMixin, UpdatedAtMixin, Base):
    __tablename__ = "mission_opportunity_assessments"
    __table_args__ = (
        UniqueConstraint("mission_revision_id", "opportunity_id"),
        CheckConstraint(
            "lifecycle_status IN ('DISCOVERED', 'RESEARCHING', 'RESEARCH_MORE', 'VALIDATE', "
            "'REJECTED')",
            name="valid_lifecycle_status",
        ),
        CheckConstraint(
            "verdict IS NULL OR verdict IN ('REJECT', 'RESEARCH_MORE', 'VALIDATE')",
            name="valid_verdict",
        ),
        CheckConstraint(
            "competitor_research_status IN ('COMPLETE', 'INCOMPLETE', 'RESEARCH_UNAVAILABLE')",
            name="competitor_research_status",
        ),
        Index(
            "ix_assessments_revision_status",
            "mission_revision_id",
            "lifecycle_status",
            "updated_at",
        ),
    )

    mission_revision_id: Mapped[UUID] = mapped_column(
        ForeignKey("mission_revisions.id", ondelete="RESTRICT"), nullable=False
    )
    opportunity_id: Mapped[UUID] = mapped_column(
        ForeignKey("opportunities.id", ondelete="RESTRICT"), nullable=False
    )
    lifecycle_status: Mapped[str] = mapped_column(String(24), nullable=False, default="DISCOVERED")
    relevance: Mapped[Decimal] = mapped_column(Numeric(18, 17), nullable=False)
    verdict: Mapped[str | None] = mapped_column(String(24))
    competitor_research_status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="INCOMPLETE"
    )
    rejected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OpportunityScoreSnapshot(IdMixin, CreatedAtMixin, Base):
    __tablename__ = "opportunity_score_snapshots"
    __table_args__ = (
        CheckConstraint(
            "evidence_strength >= 0 AND evidence_strength <= 100",
            name="evidence_strength_range",
        ),
        CheckConstraint(
            "opportunity_fit >= 0 AND opportunity_fit <= 100",
            name="opportunity_fit_range",
        ),
        CheckConstraint("final_score >= 0 AND final_score <= 100", name="final_score_range"),
        CheckConstraint(
            "pre_penalty_score >= 0 AND pre_penalty_score <= 100",
            name="pre_penalty_score_range",
        ),
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="confidence_range"),
        Index("ix_score_snapshots_assessment_created", "assessment_id", "created_at"),
    )

    assessment_id: Mapped[UUID] = mapped_column(
        ForeignKey("mission_opportunity_assessments.id", ondelete="RESTRICT"), nullable=False
    )
    run_id: Mapped[UUID] = mapped_column(
        ForeignKey("research_runs.id", ondelete="RESTRICT"), nullable=False
    )
    algorithm_version: Mapped[str] = mapped_column(String(32), nullable=False)
    raw_metrics: Mapped[JSONValue] = mapped_column(JSONB, nullable=False)
    evidence_strength: Mapped[Decimal] = mapped_column(Numeric(20, 17), nullable=False)
    opportunity_fit: Mapped[Decimal] = mapped_column(Numeric(20, 17), nullable=False)
    evidence_components: Mapped[JSONValue] = mapped_column(JSONB, nullable=False)
    opportunity_fit_components: Mapped[JSONValue] = mapped_column(JSONB, nullable=False)
    weights: Mapped[JSONValue] = mapped_column(JSONB, nullable=False)
    penalties: Mapped[JSONValue] = mapped_column(JSONB, nullable=False)
    pre_penalty_score: Mapped[Decimal] = mapped_column(Numeric(20, 17), nullable=False)
    final_score: Mapped[Decimal] = mapped_column(Numeric(20, 17), nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(18, 17), nullable=False)
    explanation: Mapped[JSONValue] = mapped_column(JSONB, nullable=False)


class CriticResult(IdMixin, CreatedAtMixin, Base):
    __tablename__ = "critic_results"
    __table_args__ = (
        CheckConstraint("verdict IN ('REJECT', 'RESEARCH_MORE', 'VALIDATE')", name="valid_verdict"),
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="confidence_range"),
        Index("ix_critic_results_assessment_created", "assessment_id", "created_at"),
    )

    assessment_id: Mapped[UUID] = mapped_column(
        ForeignKey("mission_opportunity_assessments.id", ondelete="RESTRICT"), nullable=False
    )
    run_id: Mapped[UUID] = mapped_column(
        ForeignKey("research_runs.id", ondelete="RESTRICT"), nullable=False
    )
    agent_call_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_calls.id", ondelete="RESTRICT"), nullable=False
    )
    verdict: Mapped[str] = mapped_column(String(24), nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(18, 17), nullable=False)
    fatal_flags: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)
    weak_assumptions: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)
    contradictions: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)
    missing_evidence: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)
    recommended_intents: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)
    summary: Mapped[str] = mapped_column(Text, nullable=False)


class ProductHypothesis(IdMixin, CreatedAtMixin, Base):
    __tablename__ = "product_hypotheses"
    __table_args__ = (Index("ix_product_hypotheses_assessment", "assessment_id", "created_at"),)

    assessment_id: Mapped[UUID] = mapped_column(
        ForeignKey("mission_opportunity_assessments.id", ondelete="RESTRICT"), nullable=False
    )
    requested_by: Mapped[str] = mapped_column(String(160), nullable=False)
    content: Mapped[JSONValue] = mapped_column(JSONB, nullable=False)
    evidence_card_id: Mapped[UUID] = mapped_column(
        ForeignKey("evidence_cards.id", ondelete="RESTRICT"), nullable=False
    )


class LifecycleEvent(IdMixin, CreatedAtMixin, Base):
    __tablename__ = "lifecycle_events"
    __table_args__ = (
        Index("ix_lifecycle_events_assessment_created", "assessment_id", "created_at"),
    )

    assessment_id: Mapped[UUID] = mapped_column(
        ForeignKey("mission_opportunity_assessments.id", ondelete="RESTRICT"), nullable=False
    )
    run_id: Mapped[UUID | None] = mapped_column(ForeignKey("research_runs.id", ondelete="RESTRICT"))
    from_status: Mapped[str | None] = mapped_column(String(24))
    to_status: Mapped[str] = mapped_column(String(24), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    details: Mapped[JSONValue] = mapped_column(JSONB, nullable=False, default=dict)


class AgentCall(IdMixin, CreatedAtMixin, Base):
    __tablename__ = "agent_calls"
    __table_args__ = (
        CheckConstraint("duration_ms >= 0", name="nonnegative_duration"),
        CheckConstraint(
            "repair_attempts >= 0 AND repair_attempts <= 1", name="repair_attempt_range"
        ),
        CheckConstraint(
            "octet_length(output_schema_sha256) = 32",
            name="output_schema_sha256_length",
        ),
        CheckConstraint("octet_length(request_sha256) = 32", name="request_sha256_length"),
        CheckConstraint(
            "operation IN ('query_plan', 'extract', 'relevance', 'cluster', "
            "'hypothesis', 'gap', 'critic', 'deep_research', 'legacy_unknown')",
            name="valid_operation",
        ),
        CheckConstraint(
            "length(btrim(output_schema_name)) > 0",
            name="nonempty_output_schema_name",
        ),
        CheckConstraint(
            "status IN ('COMPLETED', 'INVALID_OUTPUT', 'AUTH_REQUIRED', 'TIMEOUT', 'FAILED')",
            name="valid_status",
        ),
        CheckConstraint(
            "(status = 'COMPLETED') = (output_json IS NOT NULL)",
            name="completed_output_presence",
        ),
        CheckConstraint(
            "output_json IS NULL OR (jsonb_typeof(output_json) = 'object' "
            "AND octet_length(output_json::text) <= 32768)",
            name="bounded_object_output",
        ),
        CheckConstraint(
            "(output_json IS NULL) = (output_sha256 IS NULL) "
            "AND (output_sha256 IS NULL OR octet_length(output_sha256) = 32)",
            name="completed_output_sha256_length",
        ),
        Index("ix_agent_calls_run_created", "run_id", "created_at"),
    )

    run_id: Mapped[UUID] = mapped_column(
        ForeignKey("research_runs.id", ondelete="RESTRICT"), nullable=False
    )
    task_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("research_tasks.id", ondelete="SET NULL")
    )
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    operation: Mapped[str] = mapped_column(String(80), nullable=False)
    output_schema_name: Mapped[str] = mapped_column(String(200), nullable=False)
    output_schema_sha256: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    request_sha256: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    requested_model: Mapped[str] = mapped_column(String(160), nullable=False, default="")
    resolved_model: Mapped[str | None] = mapped_column(String(160))
    effort: Mapped[str] = mapped_column(String(16), nullable=False)
    cli_version: Mapped[str | None] = mapped_column(String(80))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    duration_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    repair_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    usage: Mapped[JSONValue] = mapped_column(JSONB, nullable=False, default=dict)
    error_class: Mapped[str | None] = mapped_column(String(80))
    output_json: Mapped[JSONValue | None] = mapped_column(JSONB(none_as_null=True))
    output_sha256: Mapped[bytes | None] = mapped_column(LargeBinary(32))


APPEND_ONLY_MODELS = (
    MissionRevision,
    RawSignalRevision,
    AtomicClaim,
    OpportunityScoreSnapshot,
    CriticResult,
    ProductHypothesis,
    LifecycleEvent,
    AgentCall,
)
