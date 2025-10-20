"""add connection table selections

Revision ID: 20251019_000014
Revises: 20251012_000013
Create Date: 2025-10-19 09:30:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20251019_000014"
down_revision = "20251012_000013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "connection_table_selections",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("system_connection_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("schema_name", sa.String(length=120), nullable=False),
        sa.Column("table_name", sa.String(length=200), nullable=False),
        sa.Column("table_type", sa.String(length=50), nullable=True),
        sa.Column("column_count", sa.Integer(), nullable=True),
        sa.Column("estimated_rows", sa.BigInteger(), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["system_connection_id"],
            ["system_connections.system_connection_id"],
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "system_connection_id",
            "schema_name",
            "table_name",
            name="uq_connection_table_selection",
        ),
    )


def downgrade() -> None:
    op.drop_table("connection_table_selections")
