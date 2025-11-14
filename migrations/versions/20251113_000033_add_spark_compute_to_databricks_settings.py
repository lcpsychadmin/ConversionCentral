"""add spark compute mode to databricks settings

Revision ID: 20251113_000033
Revises: 20251110_000032
Create Date: 2025-11-13 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251113_000033"
down_revision = "20251110_000032"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "databricks_sql_settings",
        sa.Column("spark_compute", sa.String(length=20), nullable=True, server_default="classic"),
    )
    op.alter_column(
        "databricks_sql_settings",
        "spark_compute",
        server_default=None,
    )


def downgrade() -> None:
    op.drop_column("databricks_sql_settings", "spark_compute")
