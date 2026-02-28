"""make timestamp columns non-nullable

Revision ID: 0003_not_null_timestamps
Revises: 0002_import_status
Create Date: 2026-02-28 16:10:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0003_not_null_timestamps"
down_revision = "0002_import_status"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Backfill existing NULL timestamps before applying NOT NULL constraints.
    op.execute(sa.text("UPDATE channels SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL"))
    op.execute(sa.text("UPDATE programs SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL"))
    op.execute(sa.text("UPDATE import_status SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL"))

    op.alter_column(
        "channels",
        "created_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=False,
    )
    op.alter_column(
        "programs",
        "created_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=False,
    )
    op.alter_column(
        "import_status",
        "updated_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "import_status",
        "updated_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=True,
    )
    op.alter_column(
        "programs",
        "created_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=True,
    )
    op.alter_column(
        "channels",
        "created_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=True,
    )
