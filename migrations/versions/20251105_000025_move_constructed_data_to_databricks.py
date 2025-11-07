"""drop constructed_data table and add constructed schema column

Revision ID: 20251105_000025
Revises: 20251101_000024
Create Date: 2025-11-05 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20251105_000025"
down_revision = "20251101_000024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    op.add_column(
        "databricks_sql_settings",
        sa.Column("constructed_schema", sa.String(length=120), nullable=True),
    )

    inspector = sa.inspect(bind)
    if "constructed_data" in inspector.get_table_names():
        row_count = bind.execute(sa.text("SELECT COUNT(*) FROM constructed_data")).scalar() or 0
        if row_count > 0:
            raise RuntimeError(
                "Upgrade blocked: constructed_data table still contains rows. Export or migrate the data "
                "to Databricks before applying migration 20251105_000025."
            )
        op.drop_table("constructed_data")


def downgrade() -> None:
    op.create_table(
        "constructed_data",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("constructed_table_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("row_identifier", sa.String(length=200), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["constructed_table_id"],
            ["constructed_tables.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_constructed_data_constructed_table_id"),
        "constructed_data",
        ["constructed_table_id"],
    )

    op.drop_column("databricks_sql_settings", "constructed_schema")
