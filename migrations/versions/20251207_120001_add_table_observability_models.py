"""add table observability models

Revision ID: 20251207_120001
Revises: 20251205_000048
Create Date: 2025-12-07 12:00:01.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20251207_120001"
down_revision = "20251205_000048"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "table_observability_category_schedules",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("category_name", sa.String(length=120), nullable=False),
        sa.Column("cron_expression", sa.String(length=120), nullable=False),
        sa.Column(
            "timezone",
            sa.String(length=60),
            nullable=False,
            server_default=sa.text("'UTC'"),
        ),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column("last_run_status", sa.String(length=30), nullable=True),
        sa.Column("last_run_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "category_name",
            name="uq_table_observability_category_schedules_category",
        ),
    )

    op.create_table(
        "table_observability_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("schedule_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("category_name", sa.String(length=120), nullable=False),
        sa.Column(
            "status",
            sa.String(length=30),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("table_count", sa.Integer(), nullable=False),
        sa.Column("metrics_collected", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["schedule_id"],
            ["table_observability_category_schedules.id"],
            name="fk_table_observability_runs_schedule_id",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_table_observability_runs_category_status",
        "table_observability_runs",
        ["category_name", "status"],
    )

    op.create_table(
        "table_observability_metrics",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("selection_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("system_connection_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("schema_name", sa.String(length=120), nullable=True),
        sa.Column("table_name", sa.String(length=200), nullable=False),
        sa.Column("metric_category", sa.String(length=120), nullable=False),
        sa.Column("metric_name", sa.String(length=200), nullable=False),
        sa.Column("metric_unit", sa.String(length=60), nullable=True),
        sa.Column("metric_value_number", sa.Numeric(precision=20, scale=4), nullable=True),
        sa.Column("metric_value_text", sa.Text(), nullable=True),
        sa.Column("metric_payload", sa.JSON(), nullable=True),
        sa.Column("metric_status", sa.String(length=30), nullable=True),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("timezone('utc', now())"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["table_observability_runs.id"],
            name="fk_table_observability_metrics_run_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["selection_id"],
            ["connection_table_selections.id"],
            name="fk_table_observability_metrics_selection_id",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["system_connection_id"],
            ["system_connections.system_connection_id"],
            name="fk_table_observability_metrics_system_connection_id",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_table_observability_metrics_table_metric",
        "table_observability_metrics",
        ["table_name", "metric_name"],
    )
    op.create_index(
        "ix_table_observability_metrics_recorded_at",
        "table_observability_metrics",
        ["recorded_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_table_observability_metrics_recorded_at",
        table_name="table_observability_metrics",
    )
    op.drop_index(
        "ix_table_observability_metrics_table_metric",
        table_name="table_observability_metrics",
    )
    op.drop_table("table_observability_metrics")

    op.drop_index(
        "ix_table_observability_runs_category_status",
        table_name="table_observability_runs",
    )
    op.drop_table("table_observability_runs")
    op.drop_table("table_observability_category_schedules")
