"""align release and process area relationships

Revision ID: 20251003_000012
Revises: 20251003_000011
Create Date: 2025-10-03 17:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20251003_000012"
down_revision = "20251003_000011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("DELETE FROM releases WHERE project_id IS NULL"))
    op.alter_column(
        "releases",
        "project_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=False,
    )

    op.drop_constraint("process_areas_project_id_fkey", "process_areas", type_="foreignkey")
    op.drop_column("process_areas", "project_id")

    op.create_table(
        "release_data_objects",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("release_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("data_object_id", postgresql.UUID(as_uuid=True), nullable=False),
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
        sa.ForeignKeyConstraint(["release_id"], ["releases.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["data_object_id"], ["data_objects.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("release_id", "data_object_id", name="uq_release_data_object"),
    )
    op.create_index(
        op.f("ix_release_data_objects_release_id"),
        "release_data_objects",
        ["release_id"],
    )
    op.create_index(
        op.f("ix_release_data_objects_data_object_id"),
        "release_data_objects",
        ["data_object_id"],
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_release_data_objects_data_object_id"), table_name="release_data_objects")
    op.drop_index(op.f("ix_release_data_objects_release_id"), table_name="release_data_objects")
    op.drop_table("release_data_objects")

    op.add_column(
        "process_areas",
        sa.Column("project_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        None,
        "process_areas",
        "projects",
        ["project_id"],
        ["id"],
        ondelete="CASCADE",
    )

    op.alter_column(
        "releases",
        "project_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=True,
    )
