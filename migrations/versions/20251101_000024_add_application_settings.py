"""add application settings table

Revision ID: 20251101_000024
Revises: 20251101_000023
Create Date: 2025-11-01 01:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20251101_000024"
down_revision = "20251101_000023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "application_settings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("key", sa.String(length=120), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("key", name="uq_application_settings_key"),
    )
    op.execute("ALTER TABLE application_settings ALTER COLUMN created_at DROP DEFAULT")
    op.execute("ALTER TABLE application_settings ALTER COLUMN updated_at DROP DEFAULT")


def downgrade() -> None:
    op.drop_table("application_settings")
