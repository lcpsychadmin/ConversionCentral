"""add application database settings table

Revision ID: 20251101_000023
Revises: 20251101_000022
Create Date: 2025-11-01 00:30:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20251101_000023"
down_revision = "20251101_000022"
branch_labels = None
depends_on = None

application_database_engine_enum = sa.Enum(
    "default_postgres",
    "custom_postgres",
    "sqlserver",
    name="application_database_engine_enum",
)


def upgrade() -> None:
    application_database_engine_enum.create(op.get_bind(), checkfirst=True)
    op.create_table(
        "application_database_settings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("display_name", sa.String(length=200), nullable=True),
        sa.Column("engine", application_database_engine_enum, nullable=False),
        sa.Column("connection_url", sa.Text(), nullable=True),
        sa.Column("connection_display", sa.String(length=512), nullable=True),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("is_active", name="uq_application_database_settings_active"),
    )
    op.execute("ALTER TABLE application_database_settings ALTER COLUMN is_active DROP DEFAULT")
    op.execute("ALTER TABLE application_database_settings ALTER COLUMN applied_at DROP DEFAULT")
    op.execute("ALTER TABLE application_database_settings ALTER COLUMN created_at DROP DEFAULT")
    op.execute("ALTER TABLE application_database_settings ALTER COLUMN updated_at DROP DEFAULT")


def downgrade() -> None:
    op.drop_table("application_database_settings")
    application_database_engine_enum.drop(op.get_bind(), checkfirst=True)
