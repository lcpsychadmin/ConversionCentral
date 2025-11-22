"""Remove profile payload folder setting.

Revision ID: 20251123_000041_remove_profile_payload_folder
Revises: 20251122_000040_seed_dq_profile_anomaly_types
Create Date: 2025-11-23 00:00:41.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251123_000041_remove_profile_payload_folder"
down_revision = "20251122_000040_seed_dq_profile_anomaly_types"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("databricks_sql_settings", schema=None) as batch_op:
        batch_op.drop_column("profile_payload_base_path")


def downgrade():
    with op.batch_alter_table("databricks_sql_settings", schema=None) as batch_op:
        batch_op.add_column(sa.Column("profile_payload_base_path", sa.String(length=400), nullable=True))
