"""make process area project optional

Revision ID: 20251003_000011
Revises: 20251003_000010
Create Date: 2025-10-03 03:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20251003_000011"
down_revision = "20251003_000010"
branch_labels = None
depends_on = None


def upgrade() -> None:
	op.alter_column(
		"process_areas",
		"project_id",
		existing_type=postgresql.UUID(as_uuid=True),
		nullable=True,
	)


def downgrade() -> None:
	op.execute(sa.text("DELETE FROM process_areas WHERE project_id IS NULL"))
	op.alter_column(
		"process_areas",
		"project_id",
		existing_type=postgresql.UUID(as_uuid=True),
		nullable=False,
	)
