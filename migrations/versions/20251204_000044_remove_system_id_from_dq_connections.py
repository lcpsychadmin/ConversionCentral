"""Remove system_id from dq_connections.

Revision ID: 20251204_000044
Revises: 20251201_000043
Create Date: 2025-12-04 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251204_000044"
down_revision = "20251201_000043"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("dq_connections", "system_id")


def downgrade() -> None:
    op.add_column(
        "dq_connections",
        sa.Column("system_id", sa.String(length=255), nullable=True),
    )
