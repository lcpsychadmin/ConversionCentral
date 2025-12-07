"""add slug to workspaces

Revision ID: 20251214_000054
Revises: 20251213_000053
Create Date: 2025-12-07 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251214_000054"
down_revision = "20251213_000053"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("workspaces", sa.Column("slug", sa.String(length=200), nullable=True))
    op.create_unique_constraint("uq_workspaces_slug", "workspaces", ["slug"])


def downgrade() -> None:
    op.drop_constraint("uq_workspaces_slug", "workspaces", type_="unique")
    op.drop_column("workspaces", "slug")
