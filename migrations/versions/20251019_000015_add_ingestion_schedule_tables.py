"""add ingestion scheduling tables

Revision ID: 20251019_000015
Revises: 20251019_000014
Create Date: 2025-10-19 12:30:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20251019_000015"
down_revision = "20251019_000014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_type WHERE typname = 'ingestion_load_strategy_enum'
            ) THEN
                CREATE TYPE ingestion_load_strategy_enum AS ENUM ('timestamp', 'numeric_key', 'full');
            END IF;
        END$$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_type WHERE typname = 'ingestion_run_status_enum'
            ) THEN
                CREATE TYPE ingestion_run_status_enum AS ENUM ('scheduled', 'running', 'completed', 'failed');
            END IF;
        END$$;
        """
    )

    load_strategy_enum = postgresql.ENUM(
        "timestamp",
        "numeric_key",
        "full",
        name="ingestion_load_strategy_enum",
        create_type=False,
    )
    run_status_enum = postgresql.ENUM(
        "scheduled",
        "running",
        "completed",
        "failed",
        name="ingestion_run_status_enum",
        create_type=False,
    )

    op.create_table(
        "ingestion_schedules",
        sa.Column("ingestion_schedule_id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("connection_table_selection_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("schedule_expression", sa.String(length=120), nullable=False),
        sa.Column("timezone", sa.String(length=60), nullable=True),
    sa.Column("load_strategy", load_strategy_enum, nullable=False, server_default="timestamp"),
        sa.Column("watermark_column", sa.String(length=120), nullable=True),
        sa.Column("primary_key_column", sa.String(length=120), nullable=True),
        sa.Column("target_schema", sa.String(length=120), nullable=True),
        sa.Column("target_table_name", sa.String(length=200), nullable=True),
        sa.Column("batch_size", sa.Integer(), nullable=False, server_default="5000"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.sql.expression.true()),
        sa.Column("last_watermark_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_watermark_id", sa.BigInteger(), nullable=True),
        sa.Column("last_run_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_status", sa.String(length=20), nullable=True),
        sa.Column("last_run_error", sa.Text(), nullable=True),
        sa.Column("total_runs", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_rows_loaded", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["connection_table_selection_id"],
            ["connection_table_selections.id"],
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "ix_ingestion_schedules_active",
        "ingestion_schedules",
        ["is_active", "load_strategy"],
    )

    op.create_table(
        "ingestion_runs",
        sa.Column("ingestion_run_id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("ingestion_schedule_id", postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column("status", run_status_enum, nullable=False, server_default="scheduled"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rows_loaded", sa.Integer(), nullable=True),
        sa.Column("watermark_timestamp_before", sa.DateTime(timezone=True), nullable=True),
        sa.Column("watermark_timestamp_after", sa.DateTime(timezone=True), nullable=True),
        sa.Column("watermark_id_before", sa.BigInteger(), nullable=True),
        sa.Column("watermark_id_after", sa.BigInteger(), nullable=True),
        sa.Column("query_text", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["ingestion_schedule_id"],
            ["ingestion_schedules.ingestion_schedule_id"],
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "ix_ingestion_runs_schedule_status",
        "ingestion_runs",
        ["ingestion_schedule_id", "status"],
    )


def downgrade() -> None:
    op.drop_index("ix_ingestion_runs_schedule_status", table_name="ingestion_runs")
    op.drop_table("ingestion_runs")

    op.drop_index("ix_ingestion_schedules_active", table_name="ingestion_schedules")
    op.drop_table("ingestion_schedules")

    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_type WHERE typname = 'ingestion_run_status_enum'
            ) THEN
                DROP TYPE ingestion_run_status_enum;
            END IF;
        END$$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_type WHERE typname = 'ingestion_load_strategy_enum'
            ) THEN
                DROP TYPE ingestion_load_strategy_enum;
            END IF;
        END$$;
        """
    )
