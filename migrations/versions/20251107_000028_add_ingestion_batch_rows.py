"""add ingestion batch rows setting

Revision ID: 20251107_000028
Revises: 20251107_000027
Create Date: 2025-11-07 00:05:28.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251107_000028"
down_revision = "20251107_000027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "databricks_sql_settings",
        sa.Column("ingestion_batch_rows", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("databricks_sql_settings", "ingestion_batch_rows")
