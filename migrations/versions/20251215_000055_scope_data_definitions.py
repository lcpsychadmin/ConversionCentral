"""synchronize data definition workspace ids

Revision ID: 20251215_000055
Revises: 20251214_000054
Create Date: 2025-12-07 18:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251215_000055"
down_revision = "20251214_000054"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    conn.execute(
        sa.text(
            """
            UPDATE data_definitions AS dd
            SET workspace_id = dob.workspace_id
            FROM data_objects AS dob
            WHERE dd.data_object_id = dob.id
              AND dob.workspace_id IS NOT NULL
              AND (dd.workspace_id IS DISTINCT FROM dob.workspace_id OR dd.workspace_id IS NULL)
            """
        )
    )

    default_workspace_id = conn.execute(
        sa.text(
            """
            SELECT id
            FROM workspaces
            WHERE is_active = TRUE
            ORDER BY is_default DESC, created_at ASC
            LIMIT 1
            """
        )
    ).scalar()

    if default_workspace_id:
        conn.execute(
            sa.text(
                "UPDATE data_definitions SET workspace_id = :workspace_id WHERE workspace_id IS NULL"
            ),
            {"workspace_id": default_workspace_id},
        )


def downgrade() -> None:
    # Data realignment is not reversible without historical snapshots.
    pass
