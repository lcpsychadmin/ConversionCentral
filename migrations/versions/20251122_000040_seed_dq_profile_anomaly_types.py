"""seed dq profile anomaly types

Revision ID: 20251122_000040
Revises: 20251122_000039
Create Date: 2025-11-22 14:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from app.constants.profile_anomaly_types import PROFILE_ANOMALY_TYPES


revision = "20251122_000040"
down_revision = "20251122_000039"
branch_labels = None
depends_on = None


_ANOMALY_TYPES_TABLE = sa.table(
    "dq_profile_anomaly_types",
    sa.column("anomaly_type_id", sa.String(length=120)),
    sa.column("name", sa.String(length=255)),
    sa.column("category", sa.String(length=120)),
    sa.column("default_severity", sa.String(length=50)),
    sa.column("default_likelihood", sa.String(length=50)),
    sa.column("description", sa.Text()),
)


def _flatten_profile_anomaly_types() -> list[dict[str, str | None]]:
    """Map the canonical constant structure to the table schema."""
    mapped_rows: list[dict[str, str | None]] = []
    for anomaly in PROFILE_ANOMALY_TYPES:
        mapped_rows.append(
            {
                "anomaly_type_id": anomaly["anomaly_type_id"],
                "name": anomaly["name"],
                "category": anomaly.get("category"),
                "default_severity": anomaly.get("default_severity"),
                "default_likelihood": anomaly.get("default_likelihood"),
                "description": anomaly.get("description"),
            }
        )
    return mapped_rows


def upgrade() -> None:
    rows = _flatten_profile_anomaly_types()
    if rows:
        op.bulk_insert(_ANOMALY_TYPES_TABLE, rows)


def downgrade() -> None:
    anomaly_ids = [row["anomaly_type_id"] for row in PROFILE_ANOMALY_TYPES]
    if not anomaly_ids:
        return

    delete_stmt = _ANOMALY_TYPES_TABLE.delete().where(
        _ANOMALY_TYPES_TABLE.c.anomaly_type_id.in_(anomaly_ids)
    )
    op.execute(delete_stmt)
