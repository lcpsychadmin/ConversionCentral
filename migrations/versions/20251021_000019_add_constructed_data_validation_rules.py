"""add constructed data validation rules table

Revision ID: 20251021_000019
Revises: 20251021_000018
Create Date: 2025-10-21 13:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20251021_000019"
down_revision = "20251021_000018"
branch_labels = None
depends_on = None

validation_rule_type_enum = postgresql.ENUM(
    "required",
    "unique",
    "range",
    "pattern",
    "custom",
    "cross_field",
    name="validation_rule_type_enum",
)


def upgrade() -> None:
    op.create_table(
        "constructed_data_validation_rules",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("constructed_table_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("rule_type", validation_rule_type_enum, nullable=False),
        sa.Column("field_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("configuration", sa.JSON(), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=False, server_default=sa.text("'Validation failed'")),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("applies_to_new_only", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["constructed_table_id"], ["constructed_tables.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["field_id"], ["constructed_fields.id"], ondelete="CASCADE"),
    )
    op.create_index(
        op.f("ix_constructed_data_validation_rules_constructed_table_id"),
        "constructed_data_validation_rules",
        ["constructed_table_id"],
    )



def downgrade() -> None:
    op.drop_index(
        op.f("ix_constructed_data_validation_rules_constructed_table_id"),
        table_name="constructed_data_validation_rules",
    )
    op.drop_table("constructed_data_validation_rules")

    bind = op.get_bind()
    validation_rule_type_enum.drop(bind, checkfirst=True)
