"""add load order to data definition tables

Revision ID: 20251007_000011
Revises: 20251007_000010
Create Date: 2025-10-07 21:45:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20251007_000011"
down_revision = "20251007_000010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "data_definition_tables",
        sa.Column("load_order", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("data_definition_tables", "load_order")
