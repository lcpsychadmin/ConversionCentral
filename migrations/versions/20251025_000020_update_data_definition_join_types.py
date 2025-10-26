"""update data definition join types

Revision ID: 20251025_000020
Revises: 20251021_000019
Create Date: 2025-10-25 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20251025_000020"
down_revision = "20251021_000019"
branch_labels = None
depends_on = None


old_enum = postgresql.ENUM(
    "one_to_one",
    "one_to_many",
    "many_to_one",
    "many_to_many",
    name="data_definition_relationship_type_enum",
    create_type=False,
)

new_enum = postgresql.ENUM(
    "inner",
    "left",
    "right",
    name="data_definition_join_type_enum",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    new_enum.create(bind, checkfirst=True)

    op.add_column(
        "data_definition_relationships",
        sa.Column("join_type", new_enum, nullable=False, server_default="inner"),
    )
    op.execute("UPDATE data_definition_relationships SET join_type = 'inner'")
    op.alter_column("data_definition_relationships", "join_type", server_default=None)

    op.drop_column("data_definition_relationships", "relationship_type")
    old_enum.drop(bind, checkfirst=True)


def downgrade() -> None:
    bind = op.get_bind()
    old_enum.create(bind, checkfirst=True)

    op.add_column(
        "data_definition_relationships",
        sa.Column("relationship_type", old_enum, nullable=False, server_default="one_to_one"),
    )
    op.execute("UPDATE data_definition_relationships SET relationship_type = 'one_to_one'")
    op.alter_column("data_definition_relationships", "relationship_type", server_default=None)

    op.drop_column("data_definition_relationships", "join_type")
    new_enum.drop(bind, checkfirst=True)
