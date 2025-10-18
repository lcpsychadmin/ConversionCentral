"""add ingestion layer tables

Revision ID: 20251003_000006
Revises: 20251003_000005
Create Date: 2025-10-03 13:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20251003_000006"
down_revision = "20251003_000005"
branch_labels = None
depends_on = None

system_connection_type_enum = postgresql.ENUM(
    "jdbc",
    "odbc",
    "api",
    "file",
    "saprfc",
    "other",
    name="system_connection_type_enum",
    create_type=False,
)
system_connection_auth_method_enum = postgresql.ENUM(
    "username_password",
    "oauth",
    "key_vault_reference",
    name="system_connection_auth_method_enum",
    create_type=False,
)
ingestion_job_status_enum = postgresql.ENUM(
    "pending",
    "running",
    "completed",
    "failed",
    name="ingestion_job_status_enum",
    create_type=False,
)


def upgrade() -> None:
    system_connection_type_enum.create(op.get_bind(), checkfirst=True)
    system_connection_auth_method_enum.create(op.get_bind(), checkfirst=True)
    ingestion_job_status_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "system_connections",
        sa.Column("system_connection_id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("system_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connection_type", system_connection_type_enum, nullable=False, server_default="jdbc"),
        sa.Column("connection_string", sa.Text(), nullable=False),
        sa.Column("auth_method", system_connection_auth_method_enum, nullable=False, server_default="username_password"),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["system_id"], ["systems.id"], ondelete="CASCADE"),
    )

    op.create_table(
        "ingestion_jobs",
        sa.Column("ingestion_job_id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("execution_context_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("table_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", ingestion_job_status_enum, nullable=False, server_default="pending"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["execution_context_id"], ["execution_contexts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["table_id"], ["tables.id"], ondelete="CASCADE"),
    )


def downgrade() -> None:
    op.drop_table("ingestion_jobs")
    op.drop_table("system_connections")
    ingestion_job_status_enum.drop(op.get_bind(), checkfirst=True)
    system_connection_auth_method_enum.drop(op.get_bind(), checkfirst=True)
    system_connection_type_enum.drop(op.get_bind(), checkfirst=True)
