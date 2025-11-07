"""add rows_expected to ingestion runs

Revision ID: 20251107_000027
Revises: 20251106_000026
Create Date: 2025-11-07 00:00:27.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251107_000027"
down_revision = "20251106_000026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("ingestion_runs", sa.Column("rows_expected", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    op.drop_column("ingestion_runs", "rows_expected")
