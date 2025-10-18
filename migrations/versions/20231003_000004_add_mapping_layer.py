"""add mapping layer tables

Revision ID: 20231003_000004
Revises: 20231003_000003
Create Date: 2025-10-03 03:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20231003_000004"
down_revision = "20231003_000003"
branch_labels = None
depends_on = None


mapping_status_enum = postgresql.ENUM(
    "draft",
    "approved",
    "rejected",
    name="mapping_status_enum",
    create_type=False,
)
mapping_set_status_enum = postgresql.ENUM(
    "draft",
    "active",
    "superseded",
    "archived",
    name="mapping_set_status_enum",
    create_type=False,
)


def upgrade() -> None:
    mapping_set_status_enum.create(op.get_bind(), checkfirst=True)
    mapping_status_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "mapping_sets",
        sa.Column("mapping_set_id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("release_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
    sa.Column("status", mapping_set_status_enum, nullable=False, server_default="draft"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_by", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["release_id"], ["releases.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("release_id", "version", name="uq_mapping_set_release_version"),
    )

    op.create_table(
        "mappings",
        sa.Column("mapping_id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("mapping_set_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_field_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("target_field_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("transformation_rule", sa.Text(), nullable=True),
        sa.Column("default_value", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
    sa.Column("status", mapping_status_enum, nullable=False, server_default="draft"),
        sa.Column("created_by", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["mapping_set_id"], ["mapping_sets.mapping_set_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["source_field_id"], ["fields.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["target_field_id"], ["fields.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"], ondelete="SET NULL"),
    )


def downgrade() -> None:
    op.drop_table("mappings")
    op.drop_table("mapping_sets")
    mapping_status_enum.drop(op.get_bind(), checkfirst=True)
    mapping_set_status_enum.drop(op.get_bind(), checkfirst=True)
