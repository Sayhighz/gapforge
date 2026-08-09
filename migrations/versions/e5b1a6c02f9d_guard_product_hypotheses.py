"""guard explicit post-VALIDATE Product Hypotheses

Revision ID: e5b1a6c02f9d
Revises: d93e04b7a621
Create Date: 2026-08-09 21:00:00
"""

from collections.abc import Sequence

from alembic import op

revision: str = "e5b1a6c02f9d"
down_revision: str | None = "d93e04b7a621"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM product_hypotheses) THEN
                RAISE EXCEPTION
                    'legacy Product Hypotheses cannot be upgraded without request lineage; '
                    'export/reset first';
            END IF;
        END;
        $$
        """
    )
    op.create_unique_constraint(
        "uq_product_hypotheses_assessment_request",
        "product_hypotheses",
        ["assessment_id", "requested_by"],
    )
    op.create_check_constraint(
        "bounded_requested_by",
        "product_hypotheses",
        "length(btrim(requested_by)) BETWEEN 1 AND 160 "
        "AND requested_by = btrim(requested_by) "
        "AND requested_by !~ '[[:cntrl:]]'",
    )
    op.create_check_constraint(
        "bounded_content",
        "product_hypotheses",
        "jsonb_typeof(content) = 'object' "
        "AND content ? 'schema_version' "
        "AND content ->> 'schema_version' = '0.1' "
        "AND content ? 'proposition' "
        "AND jsonb_typeof(content -> 'proposition') = 'string' "
        "AND content - 'schema_version' - 'proposition' = '{}'::jsonb "
        "AND length(btrim(content ->> 'proposition')) BETWEEN 1 AND 20000 "
        "AND content ->> 'proposition' = btrim(content ->> 'proposition') "
        "AND octet_length(content::text) <= 81000",
    )
    op.execute(
        """
        CREATE FUNCTION gapforge_validate_product_hypothesis_insert()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            latest_card_id uuid;
        BEGIN
            PERFORM 1
            FROM mission_opportunity_assessments
            WHERE id = NEW.assessment_id
              AND verdict = 'VALIDATE'
              AND lifecycle_status = 'VALIDATE'
            FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION
                    'Product Hypothesis requires a current VALIDATE assessment';
            END IF;

            SELECT snapshot.evidence_card_id
            INTO latest_card_id
            FROM final_assessment_snapshots AS snapshot
            JOIN research_runs AS run ON run.id = snapshot.run_id
            WHERE snapshot.assessment_id = NEW.assessment_id
              AND snapshot.verdict = 'VALIDATE'
            ORDER BY COALESCE(run.started_at, run.created_at) DESC,
                     snapshot.round_number DESC,
                     snapshot.created_at DESC,
                     snapshot.id DESC
            LIMIT 1;
            IF NOT FOUND THEN
                RAISE EXCEPTION
                    'Product Hypothesis requires a persisted VALIDATE final snapshot';
            END IF;
            IF NEW.evidence_card_id <> latest_card_id THEN
                RAISE EXCEPTION
                    'Product Hypothesis Evidence Card is not the latest VALIDATE snapshot';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_product_hypotheses_validate_insert "
        "BEFORE INSERT ON product_hypotheses "
        "FOR EACH ROW EXECUTE FUNCTION gapforge_validate_product_hypothesis_insert()"
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM product_hypotheses) THEN
                RAISE EXCEPTION
                    'guarded Product Hypotheses cannot downgrade; export/reset first';
            END IF;
        END;
        $$
        """
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_product_hypotheses_validate_insert ON product_hypotheses"
    )
    op.execute("DROP FUNCTION IF EXISTS gapforge_validate_product_hypothesis_insert()")
    op.drop_constraint(
        op.f("ck_product_hypotheses_bounded_content"),
        "product_hypotheses",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_product_hypotheses_bounded_requested_by"),
        "product_hypotheses",
        type_="check",
    )
    op.drop_constraint(
        "uq_product_hypotheses_assessment_request",
        "product_hypotheses",
        type_="unique",
    )
