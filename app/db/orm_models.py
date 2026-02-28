"""
SQLAlchemy ORM models for persistence.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all ORM models."""


class ChannelRecord(Base):
    """Channel persistence model."""

    __tablename__ = "channels"

    xmltv_id: Mapped[str] = mapped_column(String, primary_key=True)
    display_name: Mapped[str] = mapped_column(String, nullable=False)
    icon_url: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    def __repr__(self) -> str:
        return f"<ChannelRecord(xmltv_id={self.xmltv_id}, display_name={self.display_name})>"


class ProgramRecord(Base):
    """Program persistence model."""

    __tablename__ = "programs"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    xmltv_channel_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("channels.xmltv_id", ondelete="CASCADE"),
        nullable=False,
    )
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    stop_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint(
            "xmltv_channel_id",
            "start_time",
            "title",
            name="uq_program_channel_time_title",
        ),
        Index("idx_programs_channel_time", "xmltv_channel_id", "start_time"),
    )

    def __repr__(self) -> str:
        return f"<ProgramRecord(id={self.id}, title={self.title}, channel={self.xmltv_channel_id})>"


class ImportStatusRecord(Base):
    """Singleton import status persistence model."""

    __tablename__ = "import_status"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    last_epg_update_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    last_updated_channels_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    def __repr__(self) -> str:
        return (
            "<ImportStatusRecord("
            f"id={self.id}, "
            f"last_epg_update_at={self.last_epg_update_at}, "
            f"last_updated_channels_count={self.last_updated_channels_count}"
            ")>"
        )


__all__ = ["Base", "ChannelRecord", "ProgramRecord", "ImportStatusRecord"]
