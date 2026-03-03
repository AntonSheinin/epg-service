"""add last_updated_sources_count to import_status

Revision ID: 0005_import_status_sources_count
Revises: 0004_drop_chan_update_at
Create Date: 2026-03-03 10:30:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0005_import_status_sources_count"
down_revision = "0004_drop_chan_update_at"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "import_status",
        sa.Column("last_updated_sources_count", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("import_status", "last_updated_sources_count")
