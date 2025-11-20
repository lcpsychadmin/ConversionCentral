"""add databricks cluster policies cache

Revision ID: 20251119_000037
Revises: 20251119_000036
Create Date: 2025-11-19 00:05:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20251119_000037"
down_revision = "20251119_000036"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "databricks_sql_settings",
        sa.Column("profiling_policy_id", sa.String(length=120), nullable=True),
    )

    op.create_table(
        "databricks_cluster_policies",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("setting_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("policy_id", sa.String(length=120), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("definition", sa.JSON(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["setting_id"], ["databricks_sql_settings.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("setting_id", "policy_id", name="uq_cluster_policy_setting_policy"),
    )

    op.execute("ALTER TABLE databricks_cluster_policies ALTER COLUMN is_active DROP DEFAULT")


def downgrade() -> None:
    op.drop_table("databricks_cluster_policies")
    op.drop_column("databricks_sql_settings", "profiling_policy_id")
