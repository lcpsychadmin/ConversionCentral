"""link definition tables to connections

Revision ID: 20251204_000045
Revises: 20251204_000044
Create Date: 2025-12-04 12:00:00.000000
"""

from collections.abc import Sequence
from typing import Final

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: Final[str] = "20251204_000045"
down_revision: Final[str] = "20251204_000044"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "data_definition_tables",
        sa.Column("system_connection_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "data_definition_tables",
        sa.Column("connection_table_selection_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_index(
        "ix_data_definition_tables_system_connection_id",
        "data_definition_tables",
        ["system_connection_id"],
        unique=False,
    )
    op.create_index(
        "ix_data_definition_tables_connection_table_selection_id",
        "data_definition_tables",
        ["connection_table_selection_id"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_definition_table_connection",
        "data_definition_tables",
        "system_connections",
        ["system_connection_id"],
        ["system_connection_id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_definition_table_catalog_selection",
        "data_definition_tables",
        "connection_table_selections",
        ["connection_table_selection_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.execute(
        """
WITH matched AS (
    SELECT
        ddt.id AS definition_table_id,
        cts.id AS selection_id,
        sc.system_connection_id AS connection_id,
        ROW_NUMBER() OVER (
            PARTITION BY ddt.id
            ORDER BY cts.last_seen_at DESC NULLS LAST, cts.updated_at DESC NULLS LAST
        ) AS row_rank
    FROM data_definition_tables AS ddt
    JOIN data_definitions AS dd ON dd.id = ddt.data_definition_id
    JOIN tables AS t ON t.id = ddt.table_id
    JOIN system_connections AS sc ON sc.system_id = dd.system_id
    JOIN connection_table_selections AS cts
        ON cts.system_connection_id = sc.system_connection_id
       AND lower(cts.table_name) = lower(COALESCE(t.physical_name, t.name))
       AND COALESCE(lower(cts.schema_name), '') = COALESCE(lower(COALESCE(t.schema_name, '')), '')
)
UPDATE data_definition_tables AS ddt
SET
    system_connection_id = matched.connection_id,
    connection_table_selection_id = matched.selection_id
FROM matched
WHERE matched.definition_table_id = ddt.id
  AND matched.row_rank = 1;
        """
    )


def downgrade() -> None:
    op.drop_constraint("fk_definition_table_catalog_selection", "data_definition_tables", type_="foreignkey")
    op.drop_constraint("fk_definition_table_connection", "data_definition_tables", type_="foreignkey")
    op.drop_index("ix_data_definition_tables_connection_table_selection_id", table_name="data_definition_tables")
    op.drop_index("ix_data_definition_tables_system_connection_id", table_name="data_definition_tables")
    op.drop_column("data_definition_tables", "connection_table_selection_id")
    op.drop_column("data_definition_tables", "system_connection_id")
