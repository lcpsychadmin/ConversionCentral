"""add reporting catalog tables

Revision ID: 20251108_000031
Revises: 20251107_000030
Create Date: 2025-11-08 00:31:00.000000
"""

from collections.abc import Sequence
from typing import Final

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: Final[str] = "20251108_000031"
down_revision: Final[str] = "20251107_000031"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


REPORT_STATUS_ENUM = "report_status_enum"


def upgrade() -> None:
    status_enum = postgresql.ENUM("draft", "published", name=REPORT_STATUS_ENUM, create_type=True)
    status_enum.create(op.get_bind(), checkfirst=True)

    status_enum_no_create = postgresql.ENUM("draft", "published", name=REPORT_STATUS_ENUM, create_type=False)

    op.create_table(
        "reports",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", status_enum_no_create, nullable=False, server_default="draft"),
        sa.Column("definition", sa.JSON(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_index("ix_reports_status", "reports", ["status"], postgresql_using="btree")
    op.create_index("ix_reports_published_at", "reports", ["published_at"], postgresql_using="btree")

    op.execute("ALTER TABLE reports ALTER COLUMN status DROP DEFAULT")


def downgrade() -> None:
    op.drop_index("ix_reports_published_at", table_name="reports")
    op.drop_index("ix_reports_status", table_name="reports")
    op.drop_table("reports")

    status_enum = postgresql.ENUM("draft", "published", name=REPORT_STATUS_ENUM)
    status_enum.drop(op.get_bind(), checkfirst=True)
