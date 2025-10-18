"""add security layer with roles and process area assignments

Revision ID: 20251003_000009
Revises: 20251003_000008
Create Date: 2025-10-03 18:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20251003_000009"
down_revision = "20251003_000008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "roles",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False, unique=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "process_area_role_assignments",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("process_area_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("role_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("granted_by", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["process_area_id"], ["process_areas.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["role_id"], ["roles.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["granted_by"], ["users.id"], ondelete="SET NULL"),
        sa.UniqueConstraint(
            "process_area_id",
            "user_id",
            "role_id",
            name="uq_process_area_role_assignment",
        ),
    )

    op.create_index(
        op.f("ix_process_area_role_assignments_process_area_id"),
        "process_area_role_assignments",
        ["process_area_id"],
    )
    op.create_index(
        op.f("ix_process_area_role_assignments_user_id"),
        "process_area_role_assignments",
        ["user_id"],
    )
    op.create_index(
        op.f("ix_process_area_role_assignments_role_id"),
        "process_area_role_assignments",
        ["role_id"],
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_process_area_role_assignments_role_id"), table_name="process_area_role_assignments")
    op.drop_index(op.f("ix_process_area_role_assignments_user_id"), table_name="process_area_role_assignments")
    op.drop_index(op.f("ix_process_area_role_assignments_process_area_id"), table_name="process_area_role_assignments")
    op.drop_table("process_area_role_assignments")
    op.drop_table("roles")
