"""add ingestion method to databricks settings

Revision ID: 20251107_000029
Revises: 20251107_000028
Create Date: 2025-11-07 00:45:29.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251107_000029"
down_revision = "20251107_000028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "databricks_sql_settings",
        sa.Column("ingestion_method", sa.String(length=20), nullable=False, server_default="sql"),
    )
    op.alter_column(
        "databricks_sql_settings",
        "ingestion_method",
        server_default=None,
    )


def downgrade() -> None:
    op.drop_column("databricks_sql_settings", "ingestion_method")
