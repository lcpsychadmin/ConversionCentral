"""add constructed tables domain

Revision ID: 20251003_000008
Revises: 20251003_000007
Create Date: 2025-10-03 16:30:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20251003_000008"
down_revision = "20251003_000007"
branch_labels = None
depends_on = None

constructed_table_status_enum = postgresql.ENUM(
    "draft",
    "pending_approval",
    "approved",
    "rejected",
    name="constructed_table_status_enum",
    create_type=False,
)
constructed_table_approval_role_enum = postgresql.ENUM(
    "data_steward",
    "business_owner",
    "technical_lead",
    "admin",
    name="constructed_table_approval_role_enum",
    create_type=False,
)
constructed_table_approval_decision_enum = postgresql.ENUM(
    "approved",
    "rejected",
    name="constructed_table_approval_decision_enum",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    constructed_table_status_enum.create(bind, checkfirst=True)
    constructed_table_approval_role_enum.create(bind, checkfirst=True)
    constructed_table_approval_decision_enum.create(bind, checkfirst=True)

    op.create_table(
        "constructed_tables",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("execution_context_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("purpose", sa.Text(), nullable=True),
        sa.Column("status", constructed_table_status_enum, nullable=False, server_default="draft"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["execution_context_id"], ["execution_contexts.id"], ondelete="CASCADE"),
    )

    op.create_table(
        "constructed_fields",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("constructed_table_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("data_type", sa.String(length=100), nullable=False),
        sa.Column("is_nullable", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("default_value", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["constructed_table_id"], ["constructed_tables.id"], ondelete="CASCADE"),
    )

    op.create_table(
        "constructed_data",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("constructed_table_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("row_identifier", sa.String(length=200), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["constructed_table_id"], ["constructed_tables.id"], ondelete="CASCADE"),
    )

    op.create_table(
        "constructed_table_approvals",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("constructed_table_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("approver_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("role", constructed_table_approval_role_enum, nullable=False),
        sa.Column("decision", constructed_table_approval_decision_enum, nullable=False),
        sa.Column("comments", sa.Text(), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["constructed_table_id"], ["constructed_tables.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["approver_id"], ["users.id"], ondelete="CASCADE"),
    )

    op.create_index(
        op.f("ix_constructed_fields_constructed_table_id"),
        "constructed_fields",
        ["constructed_table_id"],
    )
    op.create_index(
        op.f("ix_constructed_data_constructed_table_id"),
        "constructed_data",
        ["constructed_table_id"],
    )
    op.create_index(
        op.f("ix_constructed_table_approvals_constructed_table_id"),
        "constructed_table_approvals",
        ["constructed_table_id"],
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_constructed_table_approvals_constructed_table_id"), table_name="constructed_table_approvals")
    op.drop_index(op.f("ix_constructed_data_constructed_table_id"), table_name="constructed_data")
    op.drop_index(op.f("ix_constructed_fields_constructed_table_id"), table_name="constructed_fields")
    op.drop_table("constructed_table_approvals")
    op.drop_table("constructed_data")
    op.drop_table("constructed_fields")
    op.drop_table("constructed_tables")

    bind = op.get_bind()
    constructed_table_approval_decision_enum.drop(bind, checkfirst=True)
    constructed_table_approval_role_enum.drop(bind, checkfirst=True)
    constructed_table_status_enum.drop(bind, checkfirst=True)
