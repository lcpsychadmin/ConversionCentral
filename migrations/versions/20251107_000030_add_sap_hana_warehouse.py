"""add sap hana data warehouse support

Revision ID: 20251107_000030
Revises: 20251107_000029
Create Date: 2025-11-07 00:30:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20251107_000030"
down_revision = "20251107_000029"
branch_labels = None
depends_on = None


DATA_WAREHOUSE_ENUM_NAME = "data_warehouse_type_enum"


def upgrade() -> None:
    data_warehouse_enum = postgresql.ENUM(
        "databricks_sql",
        "sap_hana",
        name=DATA_WAREHOUSE_ENUM_NAME,
    )
    data_warehouse_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "sap_hana_settings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "display_name",
            sa.String(length=120),
            nullable=False,
            server_default="SAP HANA Warehouse",
        ),
        sa.Column("host", sa.String(length=255), nullable=False),
        sa.Column("port", sa.Integer(), nullable=False, server_default=sa.text("30015")),
        sa.Column("database_name", sa.String(length=120), nullable=False),
        sa.Column("username", sa.String(length=120), nullable=False),
        sa.Column("password", sa.Text(), nullable=True),
        sa.Column("schema_name", sa.String(length=120), nullable=True),
        sa.Column("tenant", sa.String(length=120), nullable=True),
        sa.Column("use_ssl", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("ingestion_batch_rows", sa.Integer(), nullable=True),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("is_active", name="uq_sap_hana_settings_active"),
    )

    op.execute("ALTER TABLE sap_hana_settings ALTER COLUMN display_name DROP DEFAULT")
    op.execute("ALTER TABLE sap_hana_settings ALTER COLUMN port DROP DEFAULT")
    op.execute("ALTER TABLE sap_hana_settings ALTER COLUMN use_ssl DROP DEFAULT")
    op.execute("ALTER TABLE sap_hana_settings ALTER COLUMN is_active DROP DEFAULT")

    op.add_column(
        "ingestion_schedules",
        sa.Column(
            "target_warehouse",
            data_warehouse_enum,
            nullable=False,
            server_default="databricks_sql",
        ),
    )
    op.add_column(
        "ingestion_schedules",
        sa.Column(
            "sap_hana_setting_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
    )
    op.create_foreign_key(
        "fk_ingestion_schedule_sap_hana_setting",
        "ingestion_schedules",
        "sap_hana_settings",
        ["sap_hana_setting_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.execute("UPDATE ingestion_schedules SET target_warehouse = 'databricks_sql'")
    op.alter_column("ingestion_schedules", "target_warehouse", server_default=None)


def downgrade() -> None:
    op.drop_constraint(
        "fk_ingestion_schedule_sap_hana_setting",
        "ingestion_schedules",
        type_="foreignkey",
    )
    op.drop_column("ingestion_schedules", "sap_hana_setting_id")
    op.drop_column("ingestion_schedules", "target_warehouse")

    op.drop_table("sap_hana_settings")

    data_warehouse_enum = postgresql.ENUM(
        "databricks_sql",
        "sap_hana",
        name=DATA_WAREHOUSE_ENUM_NAME,
    )
    data_warehouse_enum.drop(op.get_bind(), checkfirst=False)
