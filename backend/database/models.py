from sqlalchemy import BigInteger, String, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncAttrs
from datetime import datetime
from sqlalchemy import Text, DateTime
from sqlalchemy.orm import relationship
from typing import List

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
    # ID канала, к которому относится пост
    channel_id: Mapped[int] = mapped_column(ForeignKey('channels.id'))
    # ID самого сообщения в Telegram
    message_id: Mapped[int] = mapped_column(unique=True)
    # Текст поста
    text: Mapped[str] = mapped_column(Text, nullable=True)
    # Дата публикации
    date: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    media_type: Mapped[str] = mapped_column(String(20), nullable=True) 
    media_url: Mapped[str] = mapped_column(String, nullable=True)     
    channel: Mapped["Channel"] = relationship(back_populates="posts")