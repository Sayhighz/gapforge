"""reconcile research contracts

Revision ID: 4d8f8a2c7b31
Revises: 132931969d6b
Create Date: 2026-08-09 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "4d8f8a2c7b31"
down_revision: str | None = "132931969d6b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM pain_signals
            WHERE severity IS NULL
               OR frequency IS NULL
               OR frequency !~ '^(0(?:\\.[0-9]+)?|1(?:\\.0+)?)$'
          ) THEN
            RAISE EXCEPTION 'pain_signals contain values that cannot migrate losslessly';
          END IF;
        END
        $$
        """
    )
    op.alter_column(
        "pain_signals",
        "severity",
        existing_type=sa.Float(),
        type_=sa.Numeric(6, 5),
        nullable=False,
        postgresql_using="severity::numeric(6,5)",
    )
    op.alter_column(
        "pain_signals",
        "frequency",
        existing_type=sa.String(length=64),
        type_=sa.Numeric(6, 5),
        nullable=False,
        postgresql_using="frequency::numeric(6,5)",
    )
    op.alter_column(
        "pain_signals",
        "confidence",
        existing_type=sa.Float(),
        type_=sa.Numeric(6, 5),
        postgresql_using="confidence::numeric(6,5)",
    )
    op.create_check_constraint(
        op.f("ck_pain_signals_severity_range"),
        "pain_signals",
        "severity >= 0 AND severity <= 1",
    )
    op.create_check_constraint(
        op.f("ck_pain_signals_frequency_range"),
        "pain_signals",
        "frequency >= 0 AND frequency <= 1",
    )

    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM raw_signal_revisions) THEN
            RAISE EXCEPTION
              'legacy raw revision IDs have no lossless domain lineage; migration refused';
          END IF;
        END
        $$
        """
    )
    op.add_column(
        "raw_signal_revisions",
        sa.Column("domain_revision_id", sa.String(length=200), nullable=False),
    )
    op.add_column(
        "raw_signal_revisions",
        sa.Column("duplicate_group_key", sa.String(length=64), nullable=False),
    )
    op.create_unique_constraint(
        op.f("uq_raw_signal_revisions_domain_revision_id"),
        "raw_signal_revisions",
        ["domain_revision_id"],
    )
    op.create_check_constraint(
        op.f("ck_raw_signal_revisions_duplicate_group_key_format"),
        "raw_signal_revisions",
        "duplicate_group_key ~ '^[0-9a-f]{64}$'",
    )
    op.add_column(
        "raw_signal_revisions",
        sa.Column(
            "search_text",
            sa.Text(),
            sa.Computed("coalesce(title, '') || ' ' || coalesce(body, '')", persisted=True),
            nullable=False,
        ),
    )
    op.add_column(
        "raw_signal_revisions",
        sa.Column(
            "search_document",
            postgresql.TSVECTOR(),
            sa.Computed(
                "to_tsvector('simple', coalesce(title, '') || ' ' || coalesce(body, ''))",
                persisted=True,
            ),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_raw_signal_revisions_duplicate_group",
        "raw_signal_revisions",
        ["duplicate_group_key"],
    )
    op.create_index(
        "ix_raw_signal_revisions_search_document",
        "raw_signal_revisions",
        ["search_document"],
        postgresql_using="gin",
    )
    op.create_index(
        "ix_raw_signal_revisions_search_text_trgm",
        "raw_signal_revisions",
        ["search_text"],
        postgresql_using="gin",
        postgresql_ops={"search_text": "gin_trgm_ops"},
    )
    op.create_index(
        "ix_raw_signals_canonical_url_trgm",
        "raw_signals",
        ["canonical_url"],
        postgresql_using="gin",
        postgresql_ops={"canonical_url": "gin_trgm_ops"},
    )

    op.add_column(
        "evidence_cards",
        sa.Column("opportunity_id", sa.UUID(), nullable=True),
    )
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM evidence_cards card
            LEFT JOIN opportunities opportunity
              ON opportunity.canonical_problem_id = card.canonical_problem_id
            GROUP BY card.id
            HAVING count(opportunity.id) <> 1
          ) THEN
            RAISE EXCEPTION 'Evidence Cards require exactly one opportunity for migration';
          END IF;
        END
        $$
        """
    )
    op.execute(
        "UPDATE evidence_cards card SET opportunity_id = opportunity.id "
        "FROM opportunities opportunity "
        "WHERE opportunity.canonical_problem_id = card.canonical_problem_id"
    )
    op.alter_column("evidence_cards", "opportunity_id", nullable=False)
    op.create_foreign_key(
        op.f("fk_evidence_cards_opportunity_id_opportunities"),
        "evidence_cards",
        "opportunities",
        ["opportunity_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_evidence_cards_opportunity_created",
        "evidence_cards",
        ["opportunity_id", "created_at"],
    )
    op.alter_column(
        "evidence_cards",
        "confidence",
        existing_type=sa.Float(),
        type_=sa.Numeric(6, 5),
        postgresql_using="confidence::numeric(6,5)",
    )

    op.add_column(
        "mission_opportunity_assessments",
        sa.Column(
            "competitor_research_status",
            sa.String(length=24),
            nullable=False,
            server_default="INCOMPLETE",
        ),
    )
    op.alter_column(
        "mission_opportunity_assessments",
        "competitor_research_status",
        server_default=None,
    )
    op.alter_column(
        "mission_opportunity_assessments",
        "relevance",
        existing_type=sa.Float(),
        type_=sa.Numeric(6, 5),
        postgresql_using="relevance::numeric(6,5)",
    )
    op.create_check_constraint(
        op.f("ck_mission_opportunity_assessments_valid_competitor_research_status"),
        "mission_opportunity_assessments",
        "competitor_research_status IN ('COMPLETE', 'INCOMPLETE', 'RESEARCH_UNAVAILABLE')",
    )
    op.create_check_constraint(
        op.f("ck_mission_opportunity_assessments_valid_verdict"),
        "mission_opportunity_assessments",
        "verdict IS NULL OR verdict IN ('REJECT', 'RESEARCH_MORE', 'VALIDATE')",
    )

    op.add_column(
        "atomic_claims",
        sa.Column(
            "citations",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.add_column(
        "atomic_claims",
        sa.Column(
            "contradicts_claim_ids",
            postgresql.ARRAY(sa.UUID()),
            nullable=False,
            server_default=sa.text("'{}'::uuid[]"),
        ),
    )
    op.alter_column("atomic_claims", "citations", server_default=None)
    op.alter_column("atomic_claims", "contradicts_claim_ids", server_default=None)

    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM competitor_evidence) THEN
            RAISE EXCEPTION
              'legacy competitor evidence has no claim lineage; migration refused';
          END IF;
        END
        $$
        """
    )
    op.add_column(
        "competitor_evidence",
        sa.Column("claim_ids", postgresql.ARRAY(sa.UUID()), nullable=False),
    )

    op.add_column(
        "opportunity_score_snapshots",
        sa.Column(
            "evidence_components",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "opportunity_score_snapshots",
        sa.Column(
            "opportunity_fit_components",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "opportunity_score_snapshots",
        sa.Column("pre_penalty_score", sa.Numeric(10, 6), nullable=True),
    )
    op.execute(
        "ALTER TABLE opportunity_score_snapshots "
        "DISABLE TRIGGER trg_opportunity_score_snapshots_append_only"
    )
    op.execute(
        """
        UPDATE opportunity_score_snapshots
        SET evidence_components = jsonb_build_object(
              'values', jsonb_build_object('legacy_axis', evidence_strength),
              'weights', jsonb_build_object('legacy_axis', 1)
            ),
            opportunity_fit_components = jsonb_build_object(
              'values', jsonb_build_object('legacy_axis', opportunity_fit),
              'weights', jsonb_build_object('legacy_axis', 1)
            ),
            pre_penalty_score = sqrt(evidence_strength * opportunity_fit)::numeric(10,6)
        """
    )
    op.execute(
        "ALTER TABLE opportunity_score_snapshots "
        "ENABLE TRIGGER trg_opportunity_score_snapshots_append_only"
    )
    op.alter_column("opportunity_score_snapshots", "evidence_components", nullable=False)
    op.alter_column("opportunity_score_snapshots", "opportunity_fit_components", nullable=False)
    op.alter_column("opportunity_score_snapshots", "pre_penalty_score", nullable=False)
    for column in ("evidence_strength", "opportunity_fit", "final_score"):
        op.alter_column(
            "opportunity_score_snapshots",
            column,
            existing_type=sa.Float(),
            type_=sa.Numeric(10, 6),
            postgresql_using=f"{column}::numeric(10,6)",
        )
    op.alter_column(
        "opportunity_score_snapshots",
        "confidence",
        existing_type=sa.Float(),
        type_=sa.Numeric(6, 5),
        postgresql_using="confidence::numeric(6,5)",
    )
    op.create_check_constraint(
        op.f("ck_opportunity_score_snapshots_pre_penalty_score_range"),
        "opportunity_score_snapshots",
        "pre_penalty_score >= 0 AND pre_penalty_score <= 100",
    )

    op.add_column("agent_calls", sa.Column("operation", sa.String(length=80), nullable=True))
    op.add_column(
        "agent_calls",
        sa.Column("output_schema_name", sa.String(length=200), nullable=True),
    )
    op.add_column(
        "agent_calls",
        sa.Column("output_schema_sha256", sa.LargeBinary(length=32), nullable=True),
    )
    op.execute("ALTER TABLE agent_calls DISABLE TRIGGER trg_agent_calls_append_only")
    op.execute(
        """
        UPDATE agent_calls
        SET operation = 'legacy_unknown',
            output_schema_name = 'legacy-unknown-v0',
            output_schema_sha256 = decode(
              '9cc588acc54470c94be50eb688bbfeae459fe9c608b8edcc7a6b31541b20519f',
              'hex'
            )
        """
    )
    op.execute("ALTER TABLE agent_calls ENABLE TRIGGER trg_agent_calls_append_only")
    op.alter_column("agent_calls", "operation", nullable=False)
    op.alter_column("agent_calls", "output_schema_name", nullable=False)
    op.alter_column("agent_calls", "output_schema_sha256", nullable=False)
    op.create_check_constraint(
        op.f("ck_agent_calls_output_schema_sha256_length"),
        "agent_calls",
        "octet_length(output_schema_sha256) = 32",
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("ck_agent_calls_output_schema_sha256_length"), "agent_calls", type_="check"
    )
    op.drop_column("agent_calls", "output_schema_sha256")
    op.drop_column("agent_calls", "output_schema_name")
    op.drop_column("agent_calls", "operation")

    op.drop_constraint(
        op.f("ck_opportunity_score_snapshots_pre_penalty_score_range"),
        "opportunity_score_snapshots",
        type_="check",
    )
    for column in ("confidence", "final_score", "opportunity_fit", "evidence_strength"):
        op.alter_column(
            "opportunity_score_snapshots",
            column,
            existing_type=sa.Numeric(),
            type_=sa.Float(),
            postgresql_using=f"{column}::double precision",
        )
    op.drop_column("opportunity_score_snapshots", "pre_penalty_score")
    op.drop_column("opportunity_score_snapshots", "opportunity_fit_components")
    op.drop_column("opportunity_score_snapshots", "evidence_components")

    op.drop_column("atomic_claims", "contradicts_claim_ids")
    op.drop_column("atomic_claims", "citations")
    op.drop_column("competitor_evidence", "claim_ids")

    op.drop_constraint(
        op.f("ck_mission_opportunity_assessments_valid_verdict"),
        "mission_opportunity_assessments",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_mission_opportunity_assessments_valid_competitor_research_status"),
        "mission_opportunity_assessments",
        type_="check",
    )
    op.alter_column(
        "mission_opportunity_assessments",
        "relevance",
        existing_type=sa.Numeric(),
        type_=sa.Float(),
        postgresql_using="relevance::double precision",
    )
    op.drop_column("mission_opportunity_assessments", "competitor_research_status")

    op.alter_column(
        "evidence_cards",
        "confidence",
        existing_type=sa.Numeric(),
        type_=sa.Float(),
        postgresql_using="confidence::double precision",
    )
    op.drop_index("ix_evidence_cards_opportunity_created", table_name="evidence_cards")
    op.drop_constraint(
        op.f("fk_evidence_cards_opportunity_id_opportunities"),
        "evidence_cards",
        type_="foreignkey",
    )
    op.drop_column("evidence_cards", "opportunity_id")

    op.drop_index("ix_raw_signals_canonical_url_trgm", table_name="raw_signals")
    op.drop_index("ix_raw_signal_revisions_search_text_trgm", table_name="raw_signal_revisions")
    op.drop_index("ix_raw_signal_revisions_search_document", table_name="raw_signal_revisions")
    op.drop_index("ix_raw_signal_revisions_duplicate_group", table_name="raw_signal_revisions")
    op.drop_column("raw_signal_revisions", "search_document")
    op.drop_column("raw_signal_revisions", "search_text")
    op.drop_constraint(
        op.f("ck_raw_signal_revisions_duplicate_group_key_format"),
        "raw_signal_revisions",
        type_="check",
    )
    op.drop_constraint(
        op.f("uq_raw_signal_revisions_domain_revision_id"),
        "raw_signal_revisions",
        type_="unique",
    )
    op.drop_column("raw_signal_revisions", "duplicate_group_key")
    op.drop_column("raw_signal_revisions", "domain_revision_id")

    op.drop_constraint(op.f("ck_pain_signals_frequency_range"), "pain_signals", type_="check")
    op.drop_constraint(op.f("ck_pain_signals_severity_range"), "pain_signals", type_="check")
    op.alter_column(
        "pain_signals",
        "confidence",
        existing_type=sa.Numeric(),
        type_=sa.Float(),
        postgresql_using="confidence::double precision",
    )
    op.alter_column(
        "pain_signals",
        "frequency",
        existing_type=sa.Numeric(),
        type_=sa.String(length=64),
        nullable=True,
        postgresql_using="frequency::text",
    )
    op.alter_column(
        "pain_signals",
        "severity",
        existing_type=sa.Numeric(),
        type_=sa.Float(),
        nullable=True,
        postgresql_using="severity::double precision",
    )
