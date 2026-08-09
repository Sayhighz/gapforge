"""order lifecycle events per assessment

Revision ID: c82ad16ef503
Revises: b71df7c19a42
Create Date: 2026-08-09 16:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c82ad16ef503"
down_revision: str | None = "b71df7c19a42"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE lifecycle_events DISABLE TRIGGER trg_lifecycle_events_append_only")
    op.add_column("lifecycle_events", sa.Column("event_number", sa.Integer(), nullable=True))
    op.add_column(
        "lifecycle_events",
        sa.Column("gap_hypothesis_id", sa.UUID(), nullable=True),
    )
    op.execute(
        """
        WITH numbered AS (
            SELECT id, row_number() OVER (
                PARTITION BY assessment_id ORDER BY created_at, id
            ) AS event_number
            FROM lifecycle_events
        )
        UPDATE lifecycle_events AS target
        SET event_number = numbered.event_number
        FROM numbered
        WHERE target.id = numbered.id
        """
    )
    op.execute(
        """
        UPDATE lifecycle_events AS event
        SET gap_hypothesis_id = opportunity.gap_hypothesis_id
        FROM mission_opportunity_assessments AS assessment
        JOIN opportunities AS opportunity ON opportunity.id = assessment.opportunity_id
        WHERE event.assessment_id = assessment.id
        """
    )
    op.alter_column("lifecycle_events", "gap_hypothesis_id", nullable=False)
    op.create_foreign_key(
        op.f("fk_lifecycle_events_gap_hypothesis_id_gap_hypotheses"),
        "lifecycle_events",
        "gap_hypotheses",
        ["gap_hypothesis_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.alter_column("lifecycle_events", "event_number", nullable=False)
    op.create_check_constraint(
        op.f("ck_lifecycle_events_positive_event_number"),
        "lifecycle_events",
        "event_number >= 1",
    )
    op.create_check_constraint(
        op.f("ck_lifecycle_events_valid_transition"),
        "lifecycle_events",
        "COALESCE((from_status IS NULL AND to_status = 'DISCOVERED') OR "
        "(from_status = 'DISCOVERED' AND to_status = 'RESEARCHING') OR "
        "(from_status = 'RESEARCHING' AND "
        "to_status IN ('RESEARCH_MORE', 'VALIDATE', 'REJECTED')) OR "
        "(from_status = 'RESEARCH_MORE' AND "
        "to_status IN ('RESEARCHING', 'VALIDATE', 'REJECTED')) OR "
        "(from_status = 'REJECTED' AND to_status = 'RESEARCH_MORE'), false)",
    )
    op.create_unique_constraint(
        op.f("uq_lifecycle_events_assessment_id"),
        "lifecycle_events",
        ["assessment_id", "event_number"],
    )
    op.drop_index("ix_lifecycle_events_assessment_created", table_name="lifecycle_events")
    op.create_index(
        "ix_lifecycle_events_assessment_version",
        "lifecycle_events",
        ["assessment_id", "event_number"],
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                WITH chain AS (
                    SELECT assessment_id, event_number, from_status, to_status,
                           lag(to_status) OVER (
                               PARTITION BY assessment_id ORDER BY event_number
                           ) AS prior_status
                    FROM lifecycle_events
                )
                SELECT 1 FROM chain
                WHERE (event_number = 1 AND from_status IS NOT NULL)
                   OR (event_number > 1 AND from_status IS DISTINCT FROM prior_status)
            ) THEN
                RAISE EXCEPTION
                    'existing lifecycle history is not a contiguous append-only chain';
            END IF;
            IF EXISTS (
                WITH latest AS (
                    SELECT DISTINCT ON (assessment_id)
                           assessment_id, to_status, created_at
                    FROM lifecycle_events
                    ORDER BY assessment_id, event_number DESC
                )
                SELECT 1
                FROM mission_opportunity_assessments AS assessment
                LEFT JOIN latest ON latest.assessment_id = assessment.id
                WHERE CASE
                    WHEN latest.assessment_id IS NULL THEN
                        assessment.lifecycle_status <> 'DISCOVERED'
                        OR assessment.verdict IS NOT NULL
                        OR assessment.rejected_at IS NOT NULL
                    ELSE
                        assessment.lifecycle_status IS DISTINCT FROM latest.to_status
                        OR assessment.verdict IS DISTINCT FROM CASE latest.to_status
                            WHEN 'VALIDATE' THEN 'VALIDATE'
                            WHEN 'REJECTED' THEN 'REJECT'
                            WHEN 'RESEARCH_MORE' THEN 'RESEARCH_MORE'
                            ELSE NULL
                        END
                        OR assessment.rejected_at IS DISTINCT FROM CASE
                            WHEN latest.to_status = 'REJECTED' THEN latest.created_at
                            ELSE NULL
                        END
                END
            ) THEN
                RAISE EXCEPTION
                    'existing assessment projection does not match lifecycle history';
            END IF;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION gapforge_validate_lifecycle_event_insert()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            current_status text;
            latest_version integer;
            latest_status text;
            assessment_problem uuid;
            gap_problem uuid;
        BEGIN
            SELECT assessment.lifecycle_status, opportunity.canonical_problem_id
            INTO current_status, assessment_problem
            FROM mission_opportunity_assessments AS assessment
            JOIN opportunities AS opportunity ON opportunity.id = assessment.opportunity_id
            WHERE assessment.id = NEW.assessment_id FOR UPDATE OF assessment;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'lifecycle event references unknown assessment';
            END IF;
            SELECT canonical_problem_id INTO gap_problem
            FROM gap_hypotheses WHERE id = NEW.gap_hypothesis_id;
            IF NOT FOUND OR gap_problem IS DISTINCT FROM assessment_problem THEN
                RAISE EXCEPTION 'lifecycle gap snapshot belongs to another opportunity problem';
            END IF;
            SELECT event_number, to_status INTO latest_version, latest_status
            FROM lifecycle_events
            WHERE assessment_id = NEW.assessment_id
            ORDER BY event_number DESC LIMIT 1;
            IF NEW.event_number <> coalesce(latest_version, 0) + 1 THEN
                RAISE EXCEPTION 'lifecycle event is not the next assessment version';
            END IF;
            IF latest_version IS NULL THEN
                IF NEW.from_status IS NOT NULL OR current_status <> 'DISCOVERED' THEN
                    RAISE EXCEPTION 'first lifecycle event must start without prior status';
                END IF;
            ELSIF NEW.from_status IS DISTINCT FROM latest_status
                  OR current_status IS DISTINCT FROM latest_status THEN
                RAISE EXCEPTION 'lifecycle event does not continue current assessment state';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_lifecycle_events_validate_insert "
        "BEFORE INSERT ON lifecycle_events "
        "FOR EACH ROW EXECUTE FUNCTION gapforge_validate_lifecycle_event_insert()"
    )
    op.execute(
        """
        CREATE FUNCTION gapforge_project_lifecycle_event_insert()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            UPDATE mission_opportunity_assessments
            SET lifecycle_status = NEW.to_status,
                verdict = CASE NEW.to_status
                    WHEN 'VALIDATE' THEN 'VALIDATE'
                    WHEN 'REJECTED' THEN 'REJECT'
                    WHEN 'RESEARCH_MORE' THEN 'RESEARCH_MORE'
                    ELSE NULL
                END,
                rejected_at = CASE
                    WHEN NEW.to_status = 'REJECTED' THEN NEW.created_at
                    ELSE NULL
                END
            WHERE id = NEW.assessment_id;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_lifecycle_events_project_insert "
        "AFTER INSERT ON lifecycle_events "
        "FOR EACH ROW EXECUTE FUNCTION gapforge_project_lifecycle_event_insert()"
    )
    op.execute(
        """
        CREATE FUNCTION gapforge_validate_assessment_lifecycle_projection()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            latest_event lifecycle_events%ROWTYPE;
            expected_verdict text;
            expected_rejected_at timestamptz;
        BEGIN
            SELECT * INTO latest_event FROM lifecycle_events
            WHERE assessment_id = NEW.id
            ORDER BY event_number DESC LIMIT 1;
            IF NOT FOUND THEN
                IF NEW.lifecycle_status <> 'DISCOVERED'
                   OR NEW.verdict IS NOT NULL
                   OR NEW.rejected_at IS NOT NULL THEN
                    RAISE EXCEPTION
                        'assessment without lifecycle history must be undiscovered projection';
                END IF;
                RETURN NEW;
            END IF;
            expected_verdict := CASE latest_event.to_status
                WHEN 'VALIDATE' THEN 'VALIDATE'
                WHEN 'REJECTED' THEN 'REJECT'
                WHEN 'RESEARCH_MORE' THEN 'RESEARCH_MORE'
                ELSE NULL
            END;
            expected_rejected_at := CASE
                WHEN latest_event.to_status = 'REJECTED' THEN latest_event.created_at
                ELSE NULL
            END;
            IF NEW.lifecycle_status IS DISTINCT FROM latest_event.to_status
               OR NEW.verdict IS DISTINCT FROM expected_verdict
               OR NEW.rejected_at IS DISTINCT FROM expected_rejected_at THEN
                RAISE EXCEPTION
                    'assessment projection does not match latest lifecycle event';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_assessments_validate_lifecycle_projection "
        "BEFORE INSERT OR UPDATE OF lifecycle_status, verdict, rejected_at "
        "ON mission_opportunity_assessments "
        "FOR EACH ROW EXECUTE FUNCTION gapforge_validate_assessment_lifecycle_projection()"
    )
    op.execute("ALTER TABLE lifecycle_events ENABLE TRIGGER trg_lifecycle_events_append_only")


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS trg_assessments_validate_lifecycle_projection "
        "ON mission_opportunity_assessments"
    )
    op.execute("DROP FUNCTION IF EXISTS gapforge_validate_assessment_lifecycle_projection()")
    op.execute("DROP TRIGGER IF EXISTS trg_lifecycle_events_project_insert ON lifecycle_events")
    op.execute("DROP FUNCTION IF EXISTS gapforge_project_lifecycle_event_insert()")
    op.execute("DROP TRIGGER IF EXISTS trg_lifecycle_events_validate_insert ON lifecycle_events")
    op.execute("DROP FUNCTION IF EXISTS gapforge_validate_lifecycle_event_insert()")
    op.drop_index("ix_lifecycle_events_assessment_version", table_name="lifecycle_events")
    op.create_index(
        "ix_lifecycle_events_assessment_created",
        "lifecycle_events",
        ["assessment_id", "created_at"],
    )
    op.drop_constraint(
        op.f("uq_lifecycle_events_assessment_id"),
        "lifecycle_events",
        type_="unique",
    )
    op.drop_constraint(
        op.f("ck_lifecycle_events_positive_event_number"),
        "lifecycle_events",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_lifecycle_events_valid_transition"),
        "lifecycle_events",
        type_="check",
    )
    op.drop_constraint(
        op.f("fk_lifecycle_events_gap_hypothesis_id_gap_hypotheses"),
        "lifecycle_events",
        type_="foreignkey",
    )
    op.drop_column("lifecycle_events", "gap_hypothesis_id")
    op.drop_column("lifecycle_events", "event_number")
