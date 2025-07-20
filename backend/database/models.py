from sqlalchemy import BigInteger, String, ForeignKey, Text, DateTime, JSON, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.ext.asyncio import AsyncAttrs
from datetime import datetime
from sqlalchemy import Text, DateTime
from sqlalchemy.orm import relationship
from typing import List
from sqlalchemy.sql import func

# Базовый класс для всех наших моделей
class Base(AsyncAttrs, DeclarativeBase):
    pass

# Модель для хранения пользователей
class User(Base):
    __tablename__ = 'users'

    # Уникальный ID пользователя из Telegram
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    # Имя пользователя (может быть None)
    first_name: Mapped[str] = mapped_column(String(150), nullable=True)
    # Юзернейм пользователя (может быть None)
    username: Mapped[str] = mapped_column(String(150), nullable=True)

class Channel(Base):
    __tablename__ = 'channels'

    # Уникальный ID канала из Telegram
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    # Название канала
    title: Mapped[str] = mapped_column(String(200))
    # Юзернейм канала (для публичных каналов)
    username: Mapped[str] = mapped_column(String(150), nullable=True)
    posts: Mapped[List["Post"]] = relationship(back_populates="channel")
    avatar_url: Mapped[str] = mapped_column(String(500), nullable=True)

class Subscription(Base):
    __tablename__ = 'subscriptions'

    # Первичный ключ для самой таблицы
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    # Внешний ключ, ссылающийся на ID пользователя
    user_id: Mapped[int] = mapped_column(ForeignKey('users.id'))
    # Внешний ключ, ссылающийся на ID канала
    channel_id: Mapped[int] = mapped_column(ForeignKey('channels.id'))

class Post(Base):
    __tablename__ = 'posts'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    channel_id: Mapped[int] = mapped_column(ForeignKey('channels.id'))
    message_id: Mapped[int] = mapped_column() 
    text: Mapped[str] = mapped_column(Text, nullable=True)
    date: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    grouped_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=True, index=True)
    media: Mapped[list[dict]] = mapped_column(JSON, nullable=True)
    
    channel: Mapped["Channel"] = relationship(back_populates="posts")

    # 2. ДОБАВЬ ЭТОТ БЛОК В КОНЕЦ КЛАССА
    # Он создает правило уникальности для пары "канал-сообщение"
    __table_args__ = (
        UniqueConstraint('channel_id', 'message_id', name='_channel_message_uc'),
    )

class BackfillRequest(Base):
    __tablename__ = 'backfill_requests'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    # Пользователь, для которого нужно догрузить посты
    user_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    # Время создания заявки, устанавливается автоматически
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())