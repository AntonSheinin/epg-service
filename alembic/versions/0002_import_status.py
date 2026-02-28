"""add import status table

Revision ID: 0002_import_status
Revises: 0001_initial
Create Date: 2026-02-28 12:00:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0002_import_status"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "import_status",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("last_epg_update_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_channels_update_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_updated_channels_count", sa.Integer(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("import_status")
