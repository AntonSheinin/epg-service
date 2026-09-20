"""store ordered multilingual programme descriptions

Revision ID: 0006_multilingual_descriptions
Revises: 0005_import_status_sources_count
Create Date: 2026-09-20 00:00:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0006_multilingual_descriptions"
down_revision = "0005_import_status_sources_count"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "programs",
        sa.Column("descriptions", postgresql.JSONB(), nullable=True),
    )
    op.execute(
        """
        UPDATE programs
        SET descriptions = CASE
            WHEN description IS NOT NULL
                AND description !~ '^[[:space:]]*$'
            THEN jsonb_build_array(
                jsonb_build_object('language', NULL, 'text', description)
            )
            ELSE '[]'::jsonb
        END
        """
    )
    op.alter_column(
        "programs",
        "descriptions",
        existing_type=postgresql.JSONB(),
        nullable=False,
        server_default=sa.text("'[]'::jsonb"),
    )
    op.drop_column("programs", "description")


def downgrade() -> None:
    op.add_column(
        "programs",
        sa.Column("description", sa.Text(), nullable=True),
    )
    op.execute(
        """
        UPDATE programs
        SET description = CASE
            WHEN jsonb_typeof(descriptions) = 'array' THEN
                CASE
                    WHEN jsonb_array_length(descriptions) > 0
                        AND jsonb_typeof(descriptions -> 0) = 'object'
                        AND jsonb_typeof(descriptions -> 0 -> 'text') = 'string'
                        AND (descriptions -> 0 ->> 'text') !~ '^[[:space:]]*$'
                    THEN regexp_replace(
                        regexp_replace(
                            descriptions -> 0 ->> 'text',
                            '^[[:space:]]+',
                            ''
                        ),
                        '[[:space:]]+$',
                        ''
                    )
                    ELSE NULL
                END
            ELSE NULL
        END
        """
    )
    op.alter_column(
        "programs",
        "descriptions",
        existing_type=postgresql.JSONB(),
        server_default=None,
    )
    op.drop_column("programs", "descriptions")
