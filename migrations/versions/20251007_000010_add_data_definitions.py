"""add data definition domain

Revision ID: 20251007_000010
Revises: 20251003_000009
Create Date: 2025-10-07 09:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20251007_000010"
down_revision = "20251003_000012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "data_definitions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("data_object_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("system_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["data_object_id"], ["data_objects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["system_id"], ["systems.id"], ondelete="CASCADE"),
        sa.UniqueConstraint(
            "data_object_id",
            "system_id",
            name="uq_data_definition_object_system",
        ),
    )

    op.create_table(
        "data_definition_tables",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("data_definition_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("table_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("alias", sa.String(length=200), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["data_definition_id"], ["data_definitions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["table_id"], ["tables.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("data_definition_id", "table_id", name="uq_data_definition_table"),
    )

    op.create_table(
        "data_definition_fields",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("definition_table_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("field_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["definition_table_id"], ["data_definition_tables.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["field_id"], ["fields.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("definition_table_id", "field_id", name="uq_data_definition_field"),
    )

    op.create_index(
        op.f("ix_data_definitions_data_object_id"),
        "data_definitions",
        ["data_object_id"],
    )
    op.create_index(
        op.f("ix_data_definitions_system_id"),
        "data_definitions",
        ["system_id"],
    )
    op.create_index(
        op.f("ix_data_definition_tables_definition_id"),
        "data_definition_tables",
        ["data_definition_id"],
    )
    op.create_index(
        op.f("ix_data_definition_tables_table_id"),
        "data_definition_tables",
        ["table_id"],
    )
    op.create_index(
        op.f("ix_data_definition_fields_definition_table_id"),
        "data_definition_fields",
        ["definition_table_id"],
    )
    op.create_index(
        op.f("ix_data_definition_fields_field_id"),
        "data_definition_fields",
        ["field_id"],
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_data_definition_fields_field_id"), table_name="data_definition_fields")
    op.drop_index(op.f("ix_data_definition_fields_definition_table_id"), table_name="data_definition_fields")
    op.drop_index(op.f("ix_data_definition_tables_table_id"), table_name="data_definition_tables")
    op.drop_index(op.f("ix_data_definition_tables_definition_id"), table_name="data_definition_tables")
    op.drop_index(op.f("ix_data_definitions_system_id"), table_name="data_definitions")
    op.drop_index(op.f("ix_data_definitions_data_object_id"), table_name="data_definitions")

    op.drop_table("data_definition_fields")
    op.drop_table("data_definition_tables")
    op.drop_table("data_definitions")
