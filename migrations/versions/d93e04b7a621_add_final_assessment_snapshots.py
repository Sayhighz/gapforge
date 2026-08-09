"""add immutable per-run final assessment snapshots

Revision ID: d93e04b7a621
Revises: c82ad16ef503
Create Date: 2026-08-09 18:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "d93e04b7a621"
down_revision: str | None = "c82ad16ef503"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "final_assessment_snapshots",
        sa.Column("assessment_id", sa.UUID(), nullable=False),
        sa.Column("run_id", sa.UUID(), nullable=False),
        sa.Column("gap_hypothesis_id", sa.UUID(), nullable=False),
        sa.Column("evidence_card_id", sa.UUID(), nullable=False),
        sa.Column("score_snapshot_id", sa.UUID(), nullable=False),
        sa.Column("critic_result_id", sa.UUID(), nullable=False),
        sa.Column("round_number", sa.Integer(), nullable=False),
        sa.Column("verdict", sa.String(24), nullable=False),
        sa.Column("competitor_research_status", sa.String(24), nullable=False),
        sa.Column("gates", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "round_number IN (1, 2)",
            name=op.f("ck_final_assessment_snapshots_valid_round_number"),
        ),
        sa.CheckConstraint(
            "verdict IN ('REJECT', 'RESEARCH_MORE', 'VALIDATE')",
            name=op.f("ck_final_assessment_snapshots_valid_verdict"),
        ),
        sa.CheckConstraint(
            "competitor_research_status IN ('COMPLETE', 'INCOMPLETE', 'RESEARCH_UNAVAILABLE')",
            name=op.f("ck_final_assessment_snapshots_competitor_research_status"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(gates) = 'array' AND jsonb_array_length(gates) = 14",
            name=op.f("ck_final_assessment_snapshots_complete_gates"),
        ),
        sa.CheckConstraint(
            "octet_length(gates::text) <= 16384",
            name=op.f("ck_final_assessment_snapshots_bounded_gates"),
        ),
        sa.ForeignKeyConstraint(
            ["assessment_id"],
            ["mission_opportunity_assessments.id"],
            name=op.f(
                "fk_final_assessment_snapshots_assessment_id_mission_opportunity_assessments"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["research_runs.id"],
            name=op.f("fk_final_assessment_snapshots_run_id_research_runs"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["gap_hypothesis_id"],
            ["gap_hypotheses.id"],
            name=op.f("fk_final_assessment_snapshots_gap_hypothesis_id_gap_hypotheses"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["evidence_card_id"],
            ["evidence_cards.id"],
            name=op.f("fk_final_assessment_snapshots_evidence_card_id_evidence_cards"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["score_snapshot_id"],
            ["opportunity_score_snapshots.id"],
            name=op.f(
                "fk_final_assessment_snapshots_score_snapshot_id_opportunity_score_snapshots"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["critic_result_id"],
            ["critic_results.id"],
            name=op.f("fk_final_assessment_snapshots_critic_result_id_critic_results"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_final_assessment_snapshots")),
        sa.UniqueConstraint(
            "run_id",
            "assessment_id",
            "round_number",
            name="uq_final_assessment_snapshots_run_assessment_round",
        ),
    )
    op.create_index(
        "ix_final_snapshots_assessment_created",
        "final_assessment_snapshots",
        ["assessment_id", "created_at"],
    )
    op.execute(
        """
        CREATE FUNCTION gapforge_validate_final_snapshot_insert()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            assessment_opportunity uuid;
            assessment_revision uuid;
            opportunity_problem uuid;
        BEGIN
            SELECT assessment.opportunity_id, assessment.mission_revision_id,
                   opportunity.canonical_problem_id
            INTO assessment_opportunity, assessment_revision, opportunity_problem
            FROM mission_opportunity_assessments AS assessment
            JOIN opportunities AS opportunity ON opportunity.id = assessment.opportunity_id
            WHERE assessment.id = NEW.assessment_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'final snapshot references unknown assessment';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM research_runs
                WHERE id = NEW.run_id AND mission_revision_id = assessment_revision
            ) THEN
                RAISE EXCEPTION 'final snapshot run belongs to another mission revision';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM evidence_cards
                WHERE id = NEW.evidence_card_id
                  AND run_id = NEW.run_id
                  AND opportunity_id = assessment_opportunity
            ) THEN
                RAISE EXCEPTION 'final snapshot Evidence Card lineage is invalid';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM opportunity_score_snapshots
                WHERE id = NEW.score_snapshot_id
                  AND run_id = NEW.run_id
                  AND assessment_id = NEW.assessment_id
            ) THEN
                RAISE EXCEPTION 'final snapshot score lineage is invalid';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM critic_results
                WHERE id = NEW.critic_result_id
                  AND run_id = NEW.run_id
                  AND assessment_id = NEW.assessment_id
            ) THEN
                RAISE EXCEPTION 'final snapshot critic lineage is invalid';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM gap_hypotheses
                WHERE id = NEW.gap_hypothesis_id
                  AND canonical_problem_id = opportunity_problem
            ) THEN
                RAISE EXCEPTION 'final snapshot gap lineage is invalid';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_final_snapshots_validate_insert "
        "BEFORE INSERT ON final_assessment_snapshots "
        "FOR EACH ROW EXECUTE FUNCTION gapforge_validate_final_snapshot_insert()"
    )
    op.execute(
        "CREATE TRIGGER trg_final_snapshots_append_only "
        "BEFORE UPDATE OR DELETE ON final_assessment_snapshots "
        "FOR EACH ROW EXECUTE FUNCTION gapforge_reject_append_only_mutation()"
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM final_assessment_snapshots) THEN
                RAISE EXCEPTION
                    'final assessment snapshots cannot downgrade; export/reset first';
            END IF;
        END;
        $$
        """
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_final_snapshots_append_only ON final_assessment_snapshots"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_final_snapshots_validate_insert ON final_assessment_snapshots"
    )
    op.execute("DROP FUNCTION IF EXISTS gapforge_validate_final_snapshot_insert()")
    op.drop_index(
        "ix_final_snapshots_assessment_created",
        table_name="final_assessment_snapshots",
    )
    op.drop_table("final_assessment_snapshots")
