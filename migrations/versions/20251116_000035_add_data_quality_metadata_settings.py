"""add data quality settings to databricks sql configuration

Revision ID: 20251116_000035
Revises: 20251115_000034
Create Date: 2025-11-16 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251116_000035"
down_revision = "20251115_000034"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "databricks_sql_settings",
        sa.Column("data_quality_schema", sa.String(length=120), nullable=True),
    )
    op.add_column(
        "databricks_sql_settings",
        sa.Column(
            "data_quality_storage_format",
            sa.String(length=20),
            nullable=False,
            server_default="delta",
        ),
    )
    op.add_column(
        "databricks_sql_settings",
        sa.Column(
            "data_quality_auto_manage_tables",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
    )

    op.execute(
        "UPDATE databricks_sql_settings SET data_quality_schema = 'dq_metadata' WHERE data_quality_schema IS NULL"
    )

    op.execute("ALTER TABLE databricks_sql_settings ALTER COLUMN data_quality_storage_format DROP DEFAULT")
    op.execute("ALTER TABLE databricks_sql_settings ALTER COLUMN data_quality_auto_manage_tables DROP DEFAULT")


def downgrade() -> None:
    op.drop_column("databricks_sql_settings", "data_quality_auto_manage_tables")
    op.drop_column("databricks_sql_settings", "data_quality_storage_format")
    op.drop_column("databricks_sql_settings", "data_quality_schema")
