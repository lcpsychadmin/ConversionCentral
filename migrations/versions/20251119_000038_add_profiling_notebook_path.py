"""add profiling notebook path

Revision ID: 20251119_000038
Revises: 20251119_000037
Create Date: 2025-11-19 19:50:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20251119_000038"
down_revision = "20251119_000037"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "databricks_sql_settings",
        sa.Column("profiling_notebook_path", sa.String(length=400), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("databricks_sql_settings", "profiling_notebook_path")
