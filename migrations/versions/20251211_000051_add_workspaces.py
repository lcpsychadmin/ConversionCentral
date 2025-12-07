"""add workspaces and scope entities

Revision ID: 20251211_000051
Revises: 20251210_000050
Create Date: 2025-12-06 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
import uuid


# revision identifiers, used by Alembic.
revision = "20251211_000051"
down_revision = "20251210_000050"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workspaces",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_default", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("name", name="uq_workspaces_name"),
    )
    op.create_index("ix_workspaces_is_default", "workspaces", ["is_default"], unique=False)

    op.add_column(
        "data_objects",
        sa.Column("workspace_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "data_definitions",
        sa.Column("workspace_id", postgresql.UUID(as_uuid=True), nullable=True),
    )

    op.create_foreign_key(
        "fk_data_objects_workspace",
        source_table="data_objects",
        referent_table="workspaces",
        local_cols=["workspace_id"],
        remote_cols=["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_data_definitions_workspace",
        source_table="data_definitions",
        referent_table="workspaces",
        local_cols=["workspace_id"],
        remote_cols=["id"],
        ondelete="CASCADE",
    )

    conn = op.get_bind()
    default_workspace_id = str(uuid.uuid4())
    conn.execute(
        sa.text(
            """
            INSERT INTO workspaces (id, name, description, is_default, is_active, created_at, updated_at)
            VALUES (:id, :name, :description, true, true, now(), now())
            """
        ),
        {
            "id": default_workspace_id,
            "name": "Default Workspace",
            "description": "Initial workspace for existing records",
        },
    )

    conn.execute(
        sa.text(
            "UPDATE data_objects SET workspace_id = :workspace_id WHERE workspace_id IS NULL"
        ),
        {"workspace_id": default_workspace_id},
    )
    conn.execute(
        sa.text(
            """
            UPDATE data_definitions AS dd
            SET workspace_id = dob.workspace_id
            FROM data_objects AS dob
            WHERE dd.data_object_id = dob.id
            """
        )
    )


def downgrade() -> None:
    op.drop_constraint("fk_data_definitions_workspace", "data_definitions", type_="foreignkey")
    op.drop_constraint("fk_data_objects_workspace", "data_objects", type_="foreignkey")

    op.drop_column("data_definitions", "workspace_id")
    op.drop_column("data_objects", "workspace_id")

    op.drop_index("ix_workspaces_is_default", table_name="workspaces")
    op.drop_table("workspaces")
