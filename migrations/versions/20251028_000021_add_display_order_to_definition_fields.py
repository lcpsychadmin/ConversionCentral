"""add display order to data definition and constructed fields

Revision ID: 20251028_000021
Revises: 20251025_000020
Create Date: 2025-10-28 00:00:21.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251028_000021"
down_revision = "20251025_000020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "data_definition_fields",
        sa.Column("display_order", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "constructed_fields",
        sa.Column("display_order", sa.Integer(), nullable=False, server_default="0"),
    )

    op.execute(
        sa.text(
            """
            WITH ordered AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY definition_table_id
                        ORDER BY created_at NULLS LAST, id
                    ) - 1 AS rn
                FROM data_definition_fields
            )
            UPDATE data_definition_fields AS ddf
            SET display_order = ordered.rn
            FROM ordered
            WHERE ddf.id = ordered.id;
            """
        )
    )

    op.execute(
        sa.text(
            """
            WITH ordered AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY constructed_table_id
                        ORDER BY created_at NULLS LAST, id
                    ) - 1 AS rn
                FROM constructed_fields
            )
            UPDATE constructed_fields AS cf
            SET display_order = ordered.rn
            FROM ordered
            WHERE cf.id = ordered.id;
            """
        )
    )

    op.alter_column(
        "data_definition_fields",
        "display_order",
        server_default=None,
        existing_type=sa.Integer(),
    )
    op.alter_column(
        "constructed_fields",
        "display_order",
        server_default=None,
        existing_type=sa.Integer(),
    )


def downgrade() -> None:
    op.drop_column("constructed_fields", "display_order")
    op.drop_column("data_definition_fields", "display_order")
