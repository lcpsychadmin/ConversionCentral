"""add data definition relationships

Revision ID: 20251012_000013
Revises: 20251012_000012
Create Date: 2025-10-12 19:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20251012_000013"
down_revision = "20251012_000012"
branch_labels = None
depends_on = None


relationship_type_enum = postgresql.ENUM(
    "one_to_one",
    "one_to_many",
    "many_to_one",
    "many_to_many",
    name="data_definition_relationship_type_enum",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    relationship_type_enum.create(bind, checkfirst=True)

    op.create_table(
        "data_definition_relationships",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("data_definition_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("primary_table_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("primary_field_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("foreign_table_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("foreign_field_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("relationship_type", relationship_type_enum, nullable=False, server_default="one_to_one"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint([
            "data_definition_id"
        ], ["data_definitions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint([
            "primary_table_id"
        ], ["data_definition_tables.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint([
            "primary_field_id"
        ], ["data_definition_fields.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint([
            "foreign_table_id"
        ], ["data_definition_tables.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint([
            "foreign_field_id"
        ], ["data_definition_fields.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "data_definition_id",
            "primary_field_id",
            "foreign_field_id",
            name="uq_data_definition_relationship_fields",
        ),
        sa.CheckConstraint(
            "primary_field_id <> foreign_field_id",
            name="ck_data_definition_relationship_distinct_fields",
        ),
        sa.CheckConstraint(
            "primary_table_id <> foreign_table_id",
            name="ck_data_definition_relationship_distinct_tables",
        ),
    )

    op.create_index(
        op.f("ix_data_definition_relationships_definition_id"),
        "data_definition_relationships",
        ["data_definition_id"],
    )
    op.create_index(
        op.f("ix_data_definition_relationships_primary_table_id"),
        "data_definition_relationships",
        ["primary_table_id"],
    )
    op.create_index(
        op.f("ix_data_definition_relationships_foreign_table_id"),
        "data_definition_relationships",
        ["foreign_table_id"],
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_data_definition_relationships_foreign_table_id"), table_name="data_definition_relationships")
    op.drop_index(op.f("ix_data_definition_relationships_primary_table_id"), table_name="data_definition_relationships")
    op.drop_index(op.f("ix_data_definition_relationships_definition_id"), table_name="data_definition_relationships")
    op.drop_table("data_definition_relationships")
    bind = op.get_bind()
    relationship_type_enum.drop(bind, checkfirst=True)
