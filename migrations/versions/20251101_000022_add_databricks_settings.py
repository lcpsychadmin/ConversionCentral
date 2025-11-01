"""add databricks sql settings table

Revision ID: 20251101_000022
Revises: 20251028_000021
Create Date: 2025-11-01 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20251101_000022"
down_revision = "20251028_000021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "databricks_sql_settings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("display_name", sa.String(length=120), nullable=False, server_default="Primary Warehouse"),
        sa.Column("workspace_host", sa.String(length=255), nullable=False),
        sa.Column("http_path", sa.String(length=400), nullable=False),
        sa.Column("access_token", sa.Text(), nullable=True),
        sa.Column("catalog", sa.String(length=120), nullable=True),
        sa.Column("schema_name", sa.String(length=120), nullable=True),
        sa.Column("warehouse_name", sa.String(length=180), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("is_active", name="uq_databricks_sql_settings_active"),
    )

    op.execute("ALTER TABLE databricks_sql_settings ALTER COLUMN is_active DROP DEFAULT")
    op.execute("ALTER TABLE databricks_sql_settings ALTER COLUMN display_name DROP DEFAULT")


def downgrade() -> None:
    op.drop_table("databricks_sql_settings")
