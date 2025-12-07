"""restore system connection system id

Revision ID: 20251209_000049
Revises: 20251207_120001
Create Date: 2025-12-09 00:00:49.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20251209_000049"
down_revision = "20251207_120001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "system_connections",
        sa.Column("system_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "system_connections_system_id_fkey",
        "system_connections",
        "systems",
        ["system_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint(
        "system_connections_system_id_fkey",
        "system_connections",
        type_="foreignkey",
    )
    op.drop_column("system_connections", "system_id")
