"""Allow null system id on system connections.

Revision ID: 20251204_000046
Revises: 20251204_000045
Create Date: 2025-12-04 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20251204_000046"
down_revision = "20251204_000045"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "system_connections",
        "system_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=True,
    )


def downgrade() -> None:
    bind = op.get_bind()
    missing = bind.execute(
        sa.text("SELECT COUNT(*) FROM system_connections WHERE system_id IS NULL")
    ).scalar()
    if missing:
        raise RuntimeError(
            "Cannot revert system_connections.system_id to NOT NULL while NULL values exist."
        )
    op.alter_column(
        "system_connections",
        "system_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=False,
    )
