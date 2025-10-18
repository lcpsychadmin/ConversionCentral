"""add table load order sequencing

Revision ID: 20251003_000010
Revises: 20251003_000009
Create Date: 2025-10-03 02:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20251003_000010"
down_revision = "20251003_000009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    role_enum = postgresql.ENUM(
        "sme",
        "data_owner",
        "approver",
        "admin",
        name="table_load_order_approval_role_enum",
        create_type=False,
    )
    decision_enum = postgresql.ENUM(
        "approved",
        "rejected",
        name="table_load_order_approval_decision_enum",
        create_type=False,
    )

    bind = op.get_bind()
    role_enum.create(bind, checkfirst=True)
    decision_enum.create(bind, checkfirst=True)

    op.create_table(
        "table_load_orders",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("data_object_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("table_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["data_object_id"], ["data_objects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["table_id"], ["tables.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("data_object_id", "table_id", name="uq_table_load_order_table"),
        sa.UniqueConstraint("data_object_id", "sequence", name="uq_table_load_order_sequence"),
        sa.CheckConstraint("sequence > 0", name="ck_table_load_order_sequence_positive"),
    )

    op.create_table(
        "table_load_order_approvals",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("table_load_order_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("approver_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("role", role_enum, nullable=False),
        sa.Column("decision", decision_enum, nullable=False),
        sa.Column("comments", sa.Text(), nullable=True),
        sa.Column(
            "approved_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint([
            "table_load_order_id"
        ], ["table_load_orders.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["approver_id"], ["users.id"], ondelete="CASCADE"),
        sa.UniqueConstraint(
            "table_load_order_id",
            "approver_id",
            "role",
            name="uq_table_load_order_approval_unique",
        ),
    )


def downgrade() -> None:
    op.drop_table("table_load_order_approvals")
    op.drop_table("table_load_orders")

    role_enum = postgresql.ENUM(
        "sme",
        "data_owner",
        "approver",
        "admin",
        name="table_load_order_approval_role_enum",
        create_type=False,
    )
    decision_enum = postgresql.ENUM(
        "approved",
        "rejected",
        name="table_load_order_approval_decision_enum",
        create_type=False,
    )

    bind = op.get_bind()
    decision_enum.drop(bind, checkfirst=True)
    role_enum.drop(bind, checkfirst=True)
