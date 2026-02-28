"""remove last_channels_update_at from import status

Revision ID: 0004_drop_chan_update_at
Revises: 0003_not_null_timestamps
Create Date: 2026-02-28 16:25:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0004_drop_chan_update_at"
down_revision = "0003_not_null_timestamps"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("import_status", "last_channels_update_at")


def downgrade() -> None:
    op.add_column(
        "import_status",
        sa.Column("last_channels_update_at", sa.DateTime(timezone=True), nullable=True),
    )
