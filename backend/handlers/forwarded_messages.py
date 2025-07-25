import asyncio
import logging
import json
import os
from collections import defaultdict
from aiogram import F, Router, types
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

from database.requests import add_subscription

router = Router()
user_locks = defaultdict(asyncio.Lock)
REDIS_URL = os.getenv("REDIS_URL")
redis_client = aioredis.from_url(REDIS_URL) if REDIS_URL else None

@router.message(F.forward_from_chat)
async def handle_forwarded_message(message: types.Message, session: AsyncSession):
    if not message.from_user or not message.forward_from_chat or message.forward_from_chat.type != 'channel':
        return await message.reply("Пожалуйста, перешлите сообщение из публичного канала.")

    user_lock = user_locks[message.from_user.id]
    async with user_lock:
        response_msg, new_channel = await add_subscription(
            session=session, user_id=message.from_user.id,
            channel_id=message.forward_from_chat.id,
            channel_title=message.forward_from_chat.title or "",
            channel_un=message.forward_from_chat.username or ""
        )
    await message.answer(response_msg)

    if new_channel and redis_client:
        try:
            task = {
                "user_chat_id": str(message.chat.id),
                "channel_id": str(new_channel.id),
                "channel_title": new_channel.title,
            }
            await redis_client.lpush("new_channel_tasks", json.dumps(task))
            logging.info(f"Создана задача на обработку канала {new_channel.id}")
        except Exception as e:
            logging.error(f"Не удалось создать задачу в Redis: {e}")