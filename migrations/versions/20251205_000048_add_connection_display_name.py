"""add display name to system connections

Revision ID: 20251205_000048
Revises: 20251204_000047
Create Date: 2025-12-05 08:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251205_000048"
down_revision = "20251204_000047"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "system_connections",
        sa.Column(
            "display_name",
            sa.String(length=120),
            nullable=False,
            server_default="Connection",
        ),
    )
    op.execute(
        "UPDATE system_connections SET display_name = 'Connection' WHERE display_name IS NULL"
    )
    op.alter_column("system_connections", "display_name", server_default=None)


def downgrade() -> None:
    op.drop_column("system_connections", "display_name")
