"""add ingestion enabled flag to system connections

Revision ID: 20251019_000016
Revises: 20251019_000015
Create Date: 2025-10-19 18:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251019_000016"
down_revision = "20251019_000015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "system_connections",
        sa.Column(
            "ingestion_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.sql.expression.true(),
        ),
    )
    op.execute(
        "ALTER TABLE system_connections ALTER COLUMN ingestion_enabled DROP DEFAULT"
    )


def downgrade() -> None:
    op.drop_column("system_connections", "ingestion_enabled")
