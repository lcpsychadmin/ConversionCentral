"""remove system-id and ingestion fields from system connections

Revision ID: 20251204_000047
Revises: 20251204_000046
Create Date: 2025-12-04 12:30:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20251204_000047"
down_revision = "20251204_000046"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("system_connections_system_id_fkey", "system_connections", type_="foreignkey")
    op.drop_column("system_connections", "system_id")
    op.drop_column("system_connections", "ingestion_enabled")


def downgrade() -> None:
    op.add_column(
        "system_connections",
        sa.Column("system_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "system_connections",
        sa.Column(
            "ingestion_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.sql.expression.true(),
        ),
    )
    op.create_foreign_key(
        "system_connections_system_id_fkey",
        "system_connections",
        "systems",
        ["system_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.execute(
        "UPDATE system_connections SET ingestion_enabled = true WHERE ingestion_enabled IS NULL"
    )
    op.alter_column("system_connections", "ingestion_enabled", server_default=None)
