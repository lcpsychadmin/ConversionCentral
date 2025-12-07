"""Add per-application constructed schema metadata.

Revision ID: 20251201_000043
Revises: 20251127_000042
Create Date: 2025-12-01 12:00:00.000000
"""

from __future__ import annotations

import re

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251201_000043"
down_revision = "20251127_000042"
branch_labels = None
depends_on = None

_DEFAULT_SCHEMA = "constructed_data"
_NORMALIZE_PATTERN = re.compile(r"[^A-Za-z0-9_]")
_MULTIPLE_UNDERSCORES = re.compile(r"_+")


def upgrade() -> None:
    op.add_column(
        "systems",
        sa.Column("constructed_schema_name", sa.String(length=120), nullable=True),
    )
    op.add_column(
        "systems",
        sa.Column("constructed_sql_schema_name", sa.String(length=120), nullable=True),
    )
    op.add_column(
        "constructed_tables",
        sa.Column("schema_name", sa.String(length=120), nullable=True),
    )
    op.create_index(
        "ix_constructed_tables_schema_name",
        "constructed_tables",
        ["schema_name"],
    )

    _backfill_constructed_table_schemas()


def downgrade() -> None:
    op.drop_index("ix_constructed_tables_schema_name", table_name="constructed_tables")
    op.drop_column("constructed_tables", "schema_name")
    op.drop_column("systems", "constructed_sql_schema_name")
    op.drop_column("systems", "constructed_schema_name")


def _normalize_schema(value: str | None) -> str:
    if not value:
        return _DEFAULT_SCHEMA

    normalized = value.strip()
    if not normalized:
        return _DEFAULT_SCHEMA

    normalized = _NORMALIZE_PATTERN.sub("_", normalized)
    normalized = _MULTIPLE_UNDERSCORES.sub("_", normalized)
    normalized = normalized.strip("_")

    if not normalized:
        return _DEFAULT_SCHEMA

    if normalized[0].isdigit():
        normalized = f"_{normalized}"

    if len(normalized) > 120:
        normalized = normalized[:120].rstrip("_") or normalized[:120]

    return normalized.lower()


def _backfill_constructed_table_schemas() -> None:
    bind = op.get_bind()
    results = bind.execute(
        sa.text(
            """
            SELECT ct.id AS constructed_table_id,
                   s.constructed_schema_name,
                   s.physical_name
            FROM constructed_tables ct
            LEFT JOIN data_definitions dd ON ct.data_definition_id = dd.id
            LEFT JOIN systems s ON dd.system_id = s.id
            """
        )
    ).mappings().all()

    update_stmt = sa.text(
        "UPDATE constructed_tables SET schema_name = :schema WHERE id = :table_id"
    )

    for row in results:
        schema = row.get("constructed_schema_name") or row.get("physical_name") or _DEFAULT_SCHEMA
        bind.execute(update_stmt, {"schema": _normalize_schema(schema), "table_id": row["constructed_table_id"]})
