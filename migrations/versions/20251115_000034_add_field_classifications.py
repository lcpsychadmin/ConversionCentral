"""add reference models for field classifications

Revision ID: 20251115_000034
Revises: 20251113_000033
Create Date: 2025-11-15 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20251115_000034"
down_revision = "20251113_000033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "legal_requirements",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False, unique=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=50), nullable=False, server_default="active"),
        sa.Column("display_order", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.execute("ALTER TABLE legal_requirements ALTER COLUMN created_at DROP DEFAULT")
    op.execute("ALTER TABLE legal_requirements ALTER COLUMN updated_at DROP DEFAULT")
    op.execute("ALTER TABLE legal_requirements ALTER COLUMN status DROP DEFAULT")

    op.create_table(
        "security_classifications",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False, unique=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=50), nullable=False, server_default="active"),
        sa.Column("display_order", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.execute("ALTER TABLE security_classifications ALTER COLUMN created_at DROP DEFAULT")
    op.execute("ALTER TABLE security_classifications ALTER COLUMN updated_at DROP DEFAULT")
    op.execute("ALTER TABLE security_classifications ALTER COLUMN status DROP DEFAULT")

    op.add_column(
        "fields",
        sa.Column("legal_requirement_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "fields",
        sa.Column("security_classification_id", postgresql.UUID(as_uuid=True), nullable=True),
    )

    op.create_foreign_key(
        "fk_fields_legal_requirement",
        source_table="fields",
        referent_table="legal_requirements",
        local_cols=["legal_requirement_id"],
        remote_cols=["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_fields_security_classification",
        source_table="fields",
        referent_table="security_classifications",
        local_cols=["security_classification_id"],
        remote_cols=["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint("fk_fields_security_classification", "fields", type_="foreignkey")
    op.drop_constraint("fk_fields_legal_requirement", "fields", type_="foreignkey")

    op.drop_column("fields", "security_classification_id")
    op.drop_column("fields", "legal_requirement_id")

    op.drop_table("security_classifications")
    op.drop_table("legal_requirements")
