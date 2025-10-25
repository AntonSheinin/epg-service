"""
SQLAlchemy ORM Models for EPG Service

This module defines the database models for channels and programs.
"""
from datetime import datetime, timezone
from sqlalchemy import String, Text, DateTime, Index, UniqueConstraint, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all ORM models"""
    pass


class Channel(Base):
    """Channel model for storing XMLTV channel information"""
    __tablename__ = "channels"

    xmltv_id: Mapped[str] = mapped_column(String, primary_key=True)
    display_name: Mapped[str] = mapped_column(String, nullable=False)
    icon_url: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    def __repr__(self) -> str:
        return f"<Channel(xmltv_id={self.xmltv_id}, display_name={self.display_name})>"


class Program(Base):
    """Program model for storing EPG program information"""
    __tablename__ = "programs"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    xmltv_channel_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("channels.xmltv_id", ondelete="CASCADE"),
        nullable=False
    )
    start_time: Mapped[str] = mapped_column(String, nullable=False)
    stop_time: Mapped[str] = mapped_column(String, nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    # Constraints
    __table_args__ = (
        UniqueConstraint("xmltv_channel_id", "start_time", "title", name="uq_program_channel_time_title"),
        Index("idx_programs_channel_time", "xmltv_channel_id", "start_time"),
    )

    def __repr__(self) -> str:
        return f"<Program(id={self.id}, title={self.title}, channel={self.xmltv_channel_id})>"
