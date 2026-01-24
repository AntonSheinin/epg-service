"""initial schema

Revision ID: 0001_initial
Revises:
Create Date: 2026-01-02 12:20:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "channels",
        sa.Column("xmltv_id", sa.String(), primary_key=True),
        sa.Column("display_name", sa.String(), nullable=False),
        sa.Column("icon_url", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "programs",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("xmltv_channel_id", sa.String(), nullable=False),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("stop_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("title", sa.String(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["xmltv_channel_id"],
            ["channels.xmltv_id"],
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "xmltv_channel_id",
            "start_time",
            "title",
            name="uq_program_channel_time_title",
        ),
    )

    op.create_index(
        "idx_programs_channel_time",
        "programs",
        ["xmltv_channel_id", "start_time"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_programs_channel_time", table_name="programs")
    op.drop_table("programs")
    op.drop_table("channels")
