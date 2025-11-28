"""Add data quality profiling schedules table.

Revision ID: 20251127_000042
Revises: 20251123_000041
Create Date: 2025-11-27 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20251127_000042"
down_revision = "20251123_000041"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "data_quality_profiling_schedules",
        sa.Column("profiling_schedule_id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("table_group_id", sa.String(length=200), nullable=False),
        sa.Column("table_group_name", sa.String(length=200), nullable=True),
        sa.Column("connection_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("data_object_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("schedule_expression", sa.String(length=120), nullable=False),
        sa.Column("timezone", sa.String(length=60), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.sql.expression.true()),
        sa.Column("last_profile_run_id", sa.String(length=120), nullable=True),
        sa.Column("last_run_status", sa.String(length=30), nullable=True),
        sa.Column("last_run_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_error", sa.Text(), nullable=True),
        sa.Column("total_runs", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["connection_id"],
            ["system_connections.system_connection_id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["data_object_id"],
            ["data_objects.id"],
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint(
            "table_group_id",
            "schedule_expression",
            "timezone",
            name="uq_dq_profiling_schedule_identity",
        ),
    )
    op.create_index(
        "ix_dq_profiling_schedules_active",
        "data_quality_profiling_schedules",
        ["is_active"],
    )


def downgrade() -> None:
    op.drop_index("ix_dq_profiling_schedules_active", table_name="data_quality_profiling_schedules")
    op.drop_table("data_quality_profiling_schedules")
