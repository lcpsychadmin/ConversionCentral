"""link constructed tables to data definitions

Revision ID: 20251021_000018
Revises: 20251020_000017
Create Date: 2025-10-21 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20251021_000018"
down_revision = "20251020_000017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "constructed_tables",
        sa.Column("data_definition_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "constructed_tables",
        sa.Column("data_definition_table_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.alter_column(
        "constructed_tables",
        "execution_context_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=True,
    )
    op.create_unique_constraint(
        "uq_constructed_tables_definition_table",
        "constructed_tables",
        ["data_definition_table_id"],
    )
    op.create_foreign_key(
        "fk_constructed_tables_data_definition",
        "constructed_tables",
        "data_definitions",
        ["data_definition_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_constructed_tables_data_definition_table",
        "constructed_tables",
        "data_definition_tables",
        ["data_definition_table_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_constructed_tables_data_definition_table",
        "constructed_tables",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_constructed_tables_data_definition",
        "constructed_tables",
        type_="foreignkey",
    )
    op.drop_constraint(
        "uq_constructed_tables_definition_table",
        "constructed_tables",
        type_="unique",
    )
    op.alter_column(
        "constructed_tables",
        "execution_context_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=False,
    )
    op.drop_column("constructed_tables", "data_definition_table_id")
    op.drop_column("constructed_tables", "data_definition_id")
