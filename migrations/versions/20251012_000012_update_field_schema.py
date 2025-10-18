"""update field schema to new data definition columns

Revision ID: 20251012_000012
Revises: 20251007_000011
Create Date: 2025-10-12 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20251012_000012"
down_revision = "20251007_000011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("fields", "data_type", new_column_name="field_type", existing_type=sa.String(length=100))
    op.alter_column("fields", "length", new_column_name="field_length", existing_type=sa.Integer())
    op.alter_column("fields", "scale", new_column_name="decimal_places", existing_type=sa.Integer())
    op.alter_column("fields", "is_required", new_column_name="system_required", existing_type=sa.Boolean(), existing_nullable=False)
    op.alter_column("fields", "validation_rules", new_column_name="data_validation", existing_type=sa.Text())

    op.drop_column("fields", "physical_name")
    op.drop_column("fields", "nullable")
    op.drop_column("fields", "precision")

    op.add_column("fields", sa.Column("description", sa.Text(), nullable=True))
    op.add_column("fields", sa.Column("application_usage", sa.Text(), nullable=True))
    op.add_column("fields", sa.Column("business_definition", sa.Text(), nullable=True))
    op.add_column("fields", sa.Column("enterprise_attribute", sa.String(length=200), nullable=True))
    op.add_column("fields", sa.Column("business_process_required", sa.Boolean(), nullable=False, server_default=sa.false()))
    op.add_column("fields", sa.Column("suppressed_field", sa.Boolean(), nullable=False, server_default=sa.false()))
    op.add_column("fields", sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()))
    op.add_column("fields", sa.Column("legal_regulatory_implications", sa.Text(), nullable=True))
    op.add_column("fields", sa.Column("grouping_tab", sa.String(length=200), nullable=True))

    op.alter_column("fields", "business_process_required", server_default=None)
    op.alter_column("fields", "suppressed_field", server_default=None)
    op.alter_column("fields", "active", server_default=None)


def downgrade() -> None:
    op.alter_column("fields", "active", server_default=sa.true())
    op.alter_column("fields", "suppressed_field", server_default=sa.false())
    op.alter_column("fields", "business_process_required", server_default=sa.false())

    op.drop_column("fields", "grouping_tab")
    op.drop_column("fields", "legal_regulatory_implications")
    op.drop_column("fields", "active")
    op.drop_column("fields", "suppressed_field")
    op.drop_column("fields", "business_process_required")
    op.drop_column("fields", "enterprise_attribute")
    op.drop_column("fields", "business_definition")
    op.drop_column("fields", "application_usage")
    op.drop_column("fields", "description")

    op.alter_column("fields", "data_validation", new_column_name="validation_rules", existing_type=sa.Text())
    op.alter_column("fields", "system_required", new_column_name="is_required", existing_type=sa.Boolean(), existing_nullable=False)
    op.alter_column("fields", "decimal_places", new_column_name="scale", existing_type=sa.Integer())
    op.alter_column("fields", "field_length", new_column_name="length", existing_type=sa.Integer())
    op.alter_column("fields", "field_type", new_column_name="data_type", existing_type=sa.String(length=100))

    op.add_column("fields", sa.Column("precision", sa.Integer(), nullable=True))
    op.add_column("fields", sa.Column("nullable", sa.Boolean(), nullable=False, server_default=sa.true()))
    op.add_column("fields", sa.Column("physical_name", sa.String(length=200), nullable=False, server_default=""))

    op.alter_column("fields", "nullable", server_default=None)
    op.alter_column("fields", "physical_name", server_default=None)