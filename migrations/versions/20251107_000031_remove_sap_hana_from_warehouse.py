"""remove sap_hana from data warehouse enum

Revision ID: 20251107_000031
Revises: 20251107_000030
Create Date: 2025-11-07 00:31:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251107_000031"
down_revision = "20251107_000030"
branch_labels = None
depends_on = None


NEW_TYPE_NAME = "data_warehouse_type_enum_new"
OLD_TYPE_NAME = "data_warehouse_type_enum"


def upgrade() -> None:
    # 1) create new enum type without 'sap_hana'
    op.execute(f"CREATE TYPE {NEW_TYPE_NAME} AS ENUM ('databricks_sql')")

    # 2) alter column to use the new type (cast via text)
    op.execute(
        f"ALTER TABLE ingestion_schedules ALTER COLUMN target_warehouse TYPE {NEW_TYPE_NAME} USING target_warehouse::text::{NEW_TYPE_NAME}"
    )

    # 3) drop old type and rename new to original name
    op.execute(f"DROP TYPE {OLD_TYPE_NAME}")
    op.execute(f"ALTER TYPE {NEW_TYPE_NAME} RENAME TO {OLD_TYPE_NAME}")


def downgrade() -> None:
    # recreate the old enum type including 'sap_hana'
    op.execute("CREATE TYPE data_warehouse_type_enum_old AS ENUM ('databricks_sql', 'sap_hana')")

    # alter column back to the old type
    op.execute(
        "ALTER TABLE ingestion_schedules ALTER COLUMN target_warehouse TYPE data_warehouse_type_enum_old USING target_warehouse::text::data_warehouse_type_enum_old"
    )

    # drop the current enum and rename the recreated one back to the original
    op.execute("DROP TYPE data_warehouse_type_enum")
    op.execute("ALTER TYPE data_warehouse_type_enum_old RENAME TO data_warehouse_type_enum")