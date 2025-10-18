"""add validation approvals tables

Revision ID: 20251003_000007
Revises: 20251003_000006
Create Date: 2025-10-03 14:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20251003_000007"
down_revision = "20251003_000006"
branch_labels = None
depends_on = None

validation_approval_role_enum = postgresql.ENUM(
    "sme",
    "data_owner",
    "approver",
    "admin",
    name="validation_approval_role_enum",
    create_type=False,
)
validation_approval_decision_enum = postgresql.ENUM(
    "approved",
    "rejected",
    name="validation_approval_decision_enum",
    create_type=False,
)


def upgrade() -> None:
    validation_approval_role_enum.create(op.get_bind(), checkfirst=True)
    validation_approval_decision_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "pre_load_validation_approvals",
        sa.Column("preload_validation_approval_id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("pre_load_validation_result_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("approver_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("role", validation_approval_role_enum, nullable=False),
        sa.Column("decision", validation_approval_decision_enum, nullable=False),
        sa.Column("comments", sa.Text(), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["pre_load_validation_result_id"], ["pre_load_validation_results.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["approver_id"], ["users.id"], ondelete="CASCADE"),
    )

    op.create_table(
        "post_load_validation_approvals",
        sa.Column("postload_validation_approval_id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("post_load_validation_result_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("approver_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("role", validation_approval_role_enum, nullable=False),
        sa.Column("decision", validation_approval_decision_enum, nullable=False),
        sa.Column("comments", sa.Text(), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["post_load_validation_result_id"], ["post_load_validation_results.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["approver_id"], ["users.id"], ondelete="CASCADE"),
    )


def downgrade() -> None:
    op.drop_table("post_load_validation_approvals")
    op.drop_table("pre_load_validation_approvals")
    validation_approval_decision_enum.drop(op.get_bind(), checkfirst=True)
    validation_approval_role_enum.drop(op.get_bind(), checkfirst=True)
