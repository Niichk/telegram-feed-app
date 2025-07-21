from sqlalchemy import BigInteger, String, ForeignKey, Text, DateTime, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.ext.asyncio import AsyncAttrs
from datetime import datetime
from typing import List
from sqlalchemy.sql import func

class Base(AsyncAttrs, DeclarativeBase):
    pass

class User(Base):
    __tablename__ = 'users'

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    # Mapped[str] = mapped_column(String(150), nullable=True) # УДАЛЕНО
    # Mapped[str] = mapped_column(String(150), nullable=True) # УДАЛЕНО
    subscription_count: Mapped[int] = mapped_column(default=0, server_default="0", nullable=False)

    # Аудит
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Channel(Base):
    __tablename__ = 'channels'

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    title: Mapped[str] = mapped_column(String(200))
    username: Mapped[str] = mapped_column(String(150), nullable=True, unique=True)
    avatar_url: Mapped[str] = mapped_column(String(500), nullable=True)

    # Аудит
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    posts: Mapped[List["Post"]] = relationship(back_populates="channel", cascade="all, delete-orphan")

class Subscription(Base):
    __tablename__ = 'subscriptions'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('users.id'))
    channel_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('channels.id'))

    # Аудит
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # ИСПРАВЛЕНИЕ: правильные ограничения для Subscription
    __table_args__ = (
        UniqueConstraint('user_id', 'channel_id', name='_user_channel_subscription_uc'),
    )

class Post(Base):
    __tablename__ = 'posts'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    channel_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('channels.id'), index=True)
    message_id: Mapped[int] = mapped_column(BigInteger)
    text: Mapped[str] = mapped_column(Text, nullable=True)
    date: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    grouped_id: Mapped[int] = mapped_column(BigInteger, nullable=True, index=True)

    # Медиа и метаданные
    media: Mapped[list[dict]] = mapped_column(JSONB, nullable=True)
    views: Mapped[int] = mapped_column(BigInteger, nullable=True, default=0)
    reactions: Mapped[list[dict]] = mapped_column(JSONB, nullable=True)
    forwarded_from: Mapped[dict] = mapped_column(JSONB, nullable=True)

    # Аудит
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )

    # Relationships
    channel: Mapped["Channel"] = relationship(back_populates="posts")

    # ТОЛЬКО ЭТО - никакого class Config:
    __table_args__ = (
        UniqueConstraint('channel_id', 'message_id', name='_channel_message_uc'),
        Index('ix_posts_channel_date', 'channel_id', 'date'),
        Index('ix_posts_grouped', 'grouped_id'),
        Index('ix_posts_views', 'views'),
    )

class BackfillRequest(Base):
    __tablename__ = 'backfill_requests'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())