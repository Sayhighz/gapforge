"""add provider audit replay

Revision ID: 8f6c2b4d9a10
Revises: 4d8f8a2c7b31
Create Date: 2026-08-09 12:20:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "8f6c2b4d9a10"
down_revision: str | None = "4d8f8a2c7b31"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM provider_call_leases) THEN
            RAISE EXCEPTION
              'active legacy provider leases cannot be reconciled; stop workers and retry';
          END IF;
          IF EXISTS (SELECT 1 FROM agent_calls) THEN
            RAISE EXCEPTION
              'legacy agent calls lack replay output; export/reset before migration';
          END IF;
        END
        $$
        """
    )
    op.add_column("provider_call_leases", sa.Column("task_id", sa.UUID(), nullable=False))
    op.add_column(
        "provider_call_leases", sa.Column("operation", sa.String(length=80), nullable=False)
    )
    op.add_column(
        "provider_call_leases",
        sa.Column("output_schema_name", sa.String(length=200), nullable=False),
    )
    op.add_column(
        "provider_call_leases",
        sa.Column("output_schema_sha256", sa.LargeBinary(length=32), nullable=False),
    )
    op.add_column(
        "provider_call_leases",
        sa.Column("request_sha256", sa.LargeBinary(length=32), nullable=False),
    )
    op.add_column(
        "provider_call_leases", sa.Column("provider", sa.String(length=32), nullable=False)
    )
    op.add_column(
        "provider_call_leases",
        sa.Column("requested_model", sa.String(length=160), nullable=False),
    )
    op.add_column("provider_call_leases", sa.Column("effort", sa.String(length=16), nullable=False))
    op.add_column(
        "provider_call_leases",
        sa.Column("repair_attempt", sa.Integer(), nullable=False, server_default="0"),
    )
    op.alter_column("provider_call_leases", "repair_attempt", server_default=None)
    op.create_foreign_key(
        op.f("fk_provider_call_leases_task_id_research_tasks"),
        "provider_call_leases",
        "research_tasks",
        ["task_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_check_constraint(
        op.f("ck_provider_call_leases_schema_hash_length"),
        "provider_call_leases",
        "octet_length(output_schema_sha256) = 32",
    )
    op.create_check_constraint(
        op.f("ck_provider_call_leases_request_hash_length"),
        "provider_call_leases",
        "octet_length(request_sha256) = 32",
    )
    op.create_check_constraint(
        op.f("ck_provider_call_leases_canonical_call_key"),
        "provider_call_leases",
        "call_key ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'",
    )
    op.create_check_constraint(
        op.f("ck_provider_call_leases_repair_attempt_range"),
        "provider_call_leases",
        "repair_attempt IN (0, 1)",
    )

    op.add_column(
        "agent_calls",
        sa.Column("output_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "agent_calls",
        sa.Column("request_sha256", sa.LargeBinary(length=32), nullable=False),
    )
    op.create_check_constraint(
        op.f("ck_agent_calls_request_sha256_length"),
        "agent_calls",
        "octet_length(request_sha256) = 32",
    )
    op.create_check_constraint(
        op.f("ck_agent_calls_valid_status"),
        "agent_calls",
        "status IN ('COMPLETED', 'INVALID_OUTPUT', 'AUTH_REQUIRED', 'TIMEOUT', 'FAILED')",
    )
    op.create_check_constraint(
        op.f("ck_agent_calls_completed_output_presence"),
        "agent_calls",
        "(status = 'COMPLETED') = (output_json IS NOT NULL)",
    )
    op.create_check_constraint(
        op.f("ck_agent_calls_bounded_object_output"),
        "agent_calls",
        "output_json IS NULL OR (jsonb_typeof(output_json) = 'object' "
        "AND octet_length(output_json::text) <= 20000)",
    )
    op.create_check_constraint(
        op.f("ck_agent_calls_completed_output_sha256_length"),
        "agent_calls",
        "output_json IS NULL OR (output_sha256 IS NOT NULL "
        "AND octet_length(output_sha256) = 32)",
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("ck_agent_calls_completed_output_sha256_length"),
        "agent_calls",
        type_="check",
    )
    op.drop_constraint(op.f("ck_agent_calls_bounded_object_output"), "agent_calls", type_="check")
    op.drop_constraint(
        op.f("ck_agent_calls_completed_output_presence"), "agent_calls", type_="check"
    )
    op.drop_constraint(op.f("ck_agent_calls_valid_status"), "agent_calls", type_="check")
    op.drop_constraint(
        op.f("ck_agent_calls_request_sha256_length"), "agent_calls", type_="check"
    )
    op.drop_column("agent_calls", "request_sha256")
    op.drop_column("agent_calls", "output_json")
    op.drop_constraint(
        op.f("ck_provider_call_leases_repair_attempt_range"),
        "provider_call_leases",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_provider_call_leases_canonical_call_key"),
        "provider_call_leases",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_provider_call_leases_schema_hash_length"),
        "provider_call_leases",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_provider_call_leases_request_hash_length"),
        "provider_call_leases",
        type_="check",
    )
    op.drop_constraint(
        op.f("fk_provider_call_leases_task_id_research_tasks"),
        "provider_call_leases",
        type_="foreignkey",
    )
    for column in (
        "repair_attempt",
        "effort",
        "requested_model",
        "provider",
        "request_sha256",
        "output_schema_sha256",
        "output_schema_name",
        "operation",
        "task_id",
    ):
        op.drop_column("provider_call_leases", column)
