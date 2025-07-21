from pydantic import BaseModel, ConfigDict, validator, HttpUrl
from datetime import datetime
from typing import List, Optional


class ReactionItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    emoticon: Optional[str] = None
    count: int
    document_id: Optional[int] = None  # Для кастомных эмодзи


class MediaItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    type: str
    url: str
    # ДОБАВЛЕНО: новое опциональное поле для превью
    thumbnail_url: Optional[str] = None

    @validator('type')
    def validate_media_type(cls, v):
        allowed_types = ['photo', 'video', 'audio']
        if v not in allowed_types:
            raise ValueError(f'Media type must be one of {allowed_types}')
        return v


class ChannelInPost(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    title: str
    username: Optional[str] = None
    avatar_url: Optional[str] = None


class PostInFeed(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    message_id: int
    text: Optional[str] = None
    date: datetime
    channel: ChannelInPost
    media: Optional[List[MediaItem]] = None
    views: Optional[int] = None
    reactions: Optional[List[ReactionItem]] = None
    forwarded_from: Optional[dict] = None


class FeedResponse(BaseModel):
    posts: List[PostInFeed]
    status: str
    
    @validator('status')
    def validate_status(cls, v):
        allowed_statuses = ['ok', 'backfilling', 'empty']
        if v not in allowed_statuses:
            raise ValueError(f'Status must be one of {allowed_statuses}')
        return v


# Дополнительные схемы для других endpoints
class ChannelInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    title: str
    username: Optional[str] = None
    avatar_url: Optional[str] = None


class SubscriptionResponse(BaseModel):
    channels: List[ChannelInfo]