"""add uniqueness and validation flags

Revision ID: 20251106_000026
Revises: 20251105_000025
Create Date: 2025-11-06 00:00:26.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251106_000026"
down_revision = "20251105_000025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "data_definition_fields",
        sa.Column("is_unique", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.add_column(
        "constructed_data_validation_rules",
        sa.Column(
            "is_system_generated",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )

    op.alter_column(
        "data_definition_fields",
        "is_unique",
        server_default=None,
        existing_type=sa.Boolean(),
    )
    op.alter_column(
        "constructed_data_validation_rules",
        "is_system_generated",
        server_default=None,
        existing_type=sa.Boolean(),
    )


def downgrade() -> None:
    op.drop_column("constructed_data_validation_rules", "is_system_generated")
    op.drop_column("data_definition_fields", "is_unique")
