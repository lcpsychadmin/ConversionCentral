"""add databricks connection fields

Revision ID: 20251210_000050
Revises: 20251209_000049
Create Date: 2025-12-05 10:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251210_000050"
down_revision = "20251209_000049"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "system_connections",
        sa.Column(
            "use_databricks_managed_connection",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column(
        "system_connections",
        sa.Column("databricks_catalog", sa.String(length=120), nullable=True),
    )
    op.add_column(
        "system_connections",
        sa.Column("databricks_schema", sa.String(length=120), nullable=True),
    )
    op.alter_column(
        "system_connections",
        "use_databricks_managed_connection",
        server_default=None,
    )


def downgrade() -> None:
    op.drop_column("system_connections", "databricks_schema")
    op.drop_column("system_connections", "databricks_catalog")
    op.drop_column("system_connections", "use_databricks_managed_connection")
