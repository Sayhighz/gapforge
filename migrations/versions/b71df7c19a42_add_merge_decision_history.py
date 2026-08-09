"""add append-only merge decision history

Revision ID: b71df7c19a42
Revises: 8f6c2b4d9a10
Create Date: 2026-08-09 14:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b71df7c19a42"
down_revision: str | None = "8f6c2b4d9a10"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("merge_candidates", sa.Column("decided_by", sa.String(160), nullable=True))
    op.add_column("merge_candidates", sa.Column("decision_event_id", sa.UUID(), nullable=True))
    op.create_check_constraint(
        op.f("ck_merge_candidates_valid_status"),
        "merge_candidates",
        "status IN ('PENDING', 'ACCEPTED', 'REJECTED', 'REVERSED')",
    )
    op.create_table(
        "merge_decision_events",
        sa.Column("candidate_id", sa.UUID(), nullable=False),
        sa.Column("decision_number", sa.Integer(), nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("from_status", sa.String(16), nullable=False),
        sa.Column("to_status", sa.String(16), nullable=False),
        sa.Column("actor", sa.String(160), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "decision_number >= 1",
            name=op.f("ck_merge_decision_events_positive_decision_number"),
        ),
        sa.CheckConstraint(
            "action IN ('ACCEPT', 'REJECT', 'REVERSE')",
            name=op.f("ck_merge_decision_events_valid_action"),
        ),
        sa.CheckConstraint(
            "from_status IN ('PENDING', 'ACCEPTED', 'REJECTED', 'REVERSED')",
            name=op.f("ck_merge_decision_events_valid_from_status"),
        ),
        sa.CheckConstraint(
            "to_status IN ('ACCEPTED', 'REJECTED', 'REVERSED')",
            name=op.f("ck_merge_decision_events_valid_to_status"),
        ),
        sa.CheckConstraint(
            "(action = 'ACCEPT' AND from_status = 'PENDING' AND to_status = 'ACCEPTED') OR "
            "(action = 'REJECT' AND from_status = 'PENDING' AND to_status = 'REJECTED') OR "
            "(action = 'REVERSE' AND from_status = 'ACCEPTED' AND to_status = 'REVERSED')",
            name=op.f("ck_merge_decision_events_valid_transition"),
        ),
        sa.CheckConstraint(
            "length(btrim(actor)) BETWEEN 1 AND 160",
            name=op.f("ck_merge_decision_events_bounded_actor"),
        ),
        sa.CheckConstraint(
            "length(btrim(reason)) BETWEEN 1 AND 500",
            name=op.f("ck_merge_decision_events_bounded_reason"),
        ),
        sa.ForeignKeyConstraint(
            ["candidate_id"],
            ["merge_candidates.id"],
            name=op.f("fk_merge_decision_events_candidate_id_merge_candidates"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_merge_decision_events")),
        sa.UniqueConstraint(
            "candidate_id",
            "decision_number",
            name=op.f("uq_merge_decision_events_candidate_id"),
        ),
        sa.UniqueConstraint(
            "id",
            "candidate_id",
            name="uq_merge_decision_events_id_candidate_id",
        ),
    )
    op.create_index(
        "ix_merge_decision_events_candidate_version",
        "merge_decision_events",
        ["candidate_id", "decision_number"],
    )
    op.create_foreign_key(
        "fk_merge_candidates_decision_event_same_candidate",
        "merge_candidates",
        "merge_decision_events",
        ["decision_event_id", "id"],
        ["id", "candidate_id"],
        ondelete="RESTRICT",
    )
    op.execute(
        """
        CREATE FUNCTION gapforge_validate_merge_decision_insert()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            candidate_row merge_candidates%ROWTYPE;
            projected_event merge_decision_events%ROWTYPE;
            latest_version integer;
        BEGIN
            SELECT * INTO candidate_row FROM merge_candidates
            WHERE id = NEW.candidate_id FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'merge decision references unknown candidate';
            END IF;
            SELECT max(decision_number) INTO latest_version FROM merge_decision_events
            WHERE candidate_id = NEW.candidate_id;
            IF NEW.decision_number <> coalesce(latest_version, 0) + 1 THEN
                RAISE EXCEPTION 'merge decision version is not the next candidate version';
            END IF;
            IF NEW.from_status <> candidate_row.status THEN
                RAISE EXCEPTION 'merge decision from_status does not match candidate';
            END IF;
            IF candidate_row.decision_event_id IS NULL THEN
                IF candidate_row.status <> 'PENDING' THEN
                    RAISE EXCEPTION 'decided candidate lacks a current decision projection';
                END IF;
            ELSE
                SELECT * INTO projected_event FROM merge_decision_events
                WHERE id = candidate_row.decision_event_id
                  AND candidate_id = candidate_row.id;
                IF NOT FOUND
                   OR projected_event.decision_number <> latest_version
                   OR projected_event.to_status <> candidate_row.status
                   OR projected_event.actor IS DISTINCT FROM candidate_row.decided_by
                   OR projected_event.reason IS DISTINCT FROM candidate_row.decision_reason
                   OR projected_event.created_at IS DISTINCT FROM candidate_row.decided_at THEN
                    RAISE EXCEPTION 'candidate does not project its latest merge decision';
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_merge_decision_events_validate_insert "
        "BEFORE INSERT ON merge_decision_events "
        "FOR EACH ROW EXECUTE FUNCTION gapforge_validate_merge_decision_insert()"
    )
    op.execute(
        """
        CREATE FUNCTION gapforge_project_merge_decision_insert()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            UPDATE merge_candidates
            SET status = NEW.to_status,
                decided_at = NEW.created_at,
                decided_by = NEW.actor,
                decision_reason = NEW.reason,
                decision_event_id = NEW.id
            WHERE id = NEW.candidate_id;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_merge_decision_events_project_insert "
        "AFTER INSERT ON merge_decision_events "
        "FOR EACH ROW EXECUTE FUNCTION gapforge_project_merge_decision_insert()"
    )
    op.execute(
        """
        CREATE FUNCTION gapforge_validate_merge_candidate_projection()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            event_row merge_decision_events%ROWTYPE;
            latest_version integer;
        BEGIN
            IF NEW.decision_event_id IS NULL THEN
                IF NEW.status <> 'PENDING' THEN
                    RAISE EXCEPTION 'decided merge candidate requires decision event';
                END IF;
                IF NEW.decided_by IS NOT NULL OR NEW.decision_reason IS NOT NULL
                   OR NEW.decided_at IS NOT NULL THEN
                    RAISE EXCEPTION 'pending merge candidate cannot retain decision metadata';
                END IF;
                RETURN NEW;
            END IF;
            SELECT * INTO event_row FROM merge_decision_events
            WHERE id = NEW.decision_event_id AND candidate_id = NEW.id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'merge candidate decision event does not match candidate';
            END IF;
            SELECT max(decision_number) INTO latest_version FROM merge_decision_events
            WHERE candidate_id = NEW.id;
            IF event_row.decision_number <> latest_version
               OR NEW.status <> event_row.to_status
               OR NEW.decided_by IS DISTINCT FROM event_row.actor
               OR NEW.decision_reason IS DISTINCT FROM event_row.reason
               OR NEW.decided_at IS DISTINCT FROM event_row.created_at THEN
                RAISE EXCEPTION 'merge candidate projection does not match latest decision event';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_merge_candidates_validate_projection "
        "BEFORE INSERT OR UPDATE ON merge_candidates "
        "FOR EACH ROW EXECUTE FUNCTION gapforge_validate_merge_candidate_projection()"
    )
    op.execute(
        "CREATE TRIGGER trg_merge_decision_events_append_only "
        "BEFORE UPDATE OR DELETE ON merge_decision_events "
        "FOR EACH ROW EXECUTE FUNCTION gapforge_reject_append_only_mutation()"
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM merge_decision_events) THEN
                RAISE EXCEPTION
                    'merge history cannot downgrade; export/reset decision events';
            END IF;
        END;
        $$
        """
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_merge_decision_events_append_only ON merge_decision_events"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_merge_decision_events_project_insert ON merge_decision_events"
    )
    op.execute("DROP FUNCTION IF EXISTS gapforge_project_merge_decision_insert()")
    op.execute(
        "DROP TRIGGER IF EXISTS trg_merge_decision_events_validate_insert ON merge_decision_events"
    )
    op.execute("DROP FUNCTION IF EXISTS gapforge_validate_merge_decision_insert()")
    op.execute(
        "DROP TRIGGER IF EXISTS trg_merge_candidates_validate_projection ON merge_candidates"
    )
    op.execute("DROP FUNCTION IF EXISTS gapforge_validate_merge_candidate_projection()")
    op.drop_constraint(
        "fk_merge_candidates_decision_event_same_candidate",
        "merge_candidates",
        type_="foreignkey",
    )
    op.drop_index(
        "ix_merge_decision_events_candidate_version",
        table_name="merge_decision_events",
    )
    op.drop_table("merge_decision_events")
    op.drop_constraint(
        op.f("ck_merge_candidates_valid_status"),
        "merge_candidates",
        type_="check",
    )
    op.drop_column("merge_candidates", "decision_event_id")
    op.drop_column("merge_candidates", "decided_by")
