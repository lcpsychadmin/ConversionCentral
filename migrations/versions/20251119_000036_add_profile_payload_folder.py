"""add profiling payload folder to databricks settings

Revision ID: 20251119_000036
Revises: 20251116_000035
Create Date: 2025-11-19 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251119_000036"
down_revision = "20251116_000035"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "databricks_sql_settings",
        sa.Column("profile_payload_base_path", sa.String(length=400), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("databricks_sql_settings", "profile_payload_base_path")
