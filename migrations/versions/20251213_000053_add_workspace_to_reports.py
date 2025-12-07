"""link reports to workspaces

Revision ID: 20251213_000053
Revises: 20251212_000052
Create Date: 2025-12-06 15:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import text


# revision identifiers, used by Alembic.
revision = "20251213_000053"
down_revision = "20251212_000052"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "reports",
        sa.Column("workspace_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_reports_workspace",
        source_table="reports",
        referent_table="workspaces",
        local_cols=["workspace_id"],
        remote_cols=["id"],
        ondelete="SET NULL",
    )

    conn = op.get_bind()
    conn.execute(
        text(
            """
            UPDATE reports AS r
            SET workspace_id = dbo.workspace_id
            FROM data_objects AS dbo
            WHERE r.data_object_id = dbo.id AND dbo.workspace_id IS NOT NULL
            """
        )
    )

    default_workspace_id = conn.execute(
        text("SELECT id FROM workspaces WHERE is_default = true ORDER BY created_at LIMIT 1")
    ).scalar()
    if default_workspace_id:
        conn.execute(
            text("UPDATE reports SET workspace_id = :workspace_id WHERE workspace_id IS NULL"),
            {"workspace_id": default_workspace_id},
        )


def downgrade() -> None:
    op.drop_constraint("fk_reports_workspace", "reports", type_="foreignkey")
    op.drop_column("reports", "workspace_id")
