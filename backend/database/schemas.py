from pydantic import BaseModel, ConfigDict
from datetime import datetime

# Схема для отображения информации о канале в посте
class ChannelInPost(BaseModel):
    id: int
    title: str
    username: str | None

# Схема для отображения поста в ленте
class PostInFeed(BaseModel):
    model_config = ConfigDict(from_attributes=True) # Позволяет Pydantic работать с объектами SQLAlchemy

    message_id: int
    text: str | None # Текст может отсутствовать
    date: datetime
    channel: ChannelInPost
    media_type: str | None
    media_url: str | None