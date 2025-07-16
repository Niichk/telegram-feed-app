from pydantic import BaseModel, ConfigDict
from datetime import datetime

# --- НОВАЯ СХЕМА для одного медиа-элемента ---
class MediaItem(BaseModel):
    type: str
    url: str

class ChannelInPost(BaseModel):
    id: int
    title: str
    username: str | None

class PostInFeed(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    message_id: int
    text: str | None
    date: datetime
    channel: ChannelInPost
    media: list[MediaItem] | None