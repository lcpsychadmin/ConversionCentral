"""add report associations to published outputs

Revision ID: 20251110_000032
Revises: 20251108_000031
Create Date: 2025-11-10 00:32:00.000000
"""

from typing import Final

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: Final[str] = "20251110_000032"
down_revision: Final[str] = "20251108_000031"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "reports",
        sa.Column("process_area_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "reports",
        sa.Column("data_object_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_index("ix_reports_process_area_id", "reports", ["process_area_id"], postgresql_using="btree")
    op.create_index("ix_reports_data_object_id", "reports", ["data_object_id"], postgresql_using="btree")
    op.create_foreign_key(
        "fk_reports_process_area",
        "reports",
        "process_areas",
        ["process_area_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_reports_data_object",
        "reports",
        "data_objects",
        ["data_object_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint("fk_reports_data_object", "reports", type_="foreignkey")
    op.drop_constraint("fk_reports_process_area", "reports", type_="foreignkey")
    op.drop_index("ix_reports_data_object_id", table_name="reports")
    op.drop_index("ix_reports_process_area_id", table_name="reports")
    op.drop_column("reports", "data_object_id")
    op.drop_column("reports", "process_area_id")
