"""add is_construction column to data_definition_tables

Revision ID: 20251020_000017
Revises: 20251019_000016
Create Date: 2025-10-20 20:25:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251020_000017"
down_revision = "20251019_000016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "data_definition_tables",
        sa.Column(
            "is_construction",
            sa.Boolean(),
            nullable=False,
            server_default=sa.sql.expression.false(),
        ),
    )
    op.execute(
        "ALTER TABLE data_definition_tables ALTER COLUMN is_construction DROP DEFAULT"
    )


def downgrade() -> None:
    op.drop_column("data_definition_tables", "is_construction")
