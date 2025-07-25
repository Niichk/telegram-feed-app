# backend/handlers/forwarded_messages.py

import asyncio
import logging
import json
from collections import defaultdict
from aiogram import F, Router, types
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis
import os

from database.requests import add_subscription
from .user_commands import get_main_keyboard

router = Router()
user_locks = defaultdict(asyncio.Lock)
PROCESSED_MEDIA_GROUPS = set()

# Инициализируем Redis клиент для отправки задач воркеру
REDIS_URL = os.getenv("REDIS_URL")
redis_client = aioredis.from_url(REDIS_URL) if REDIS_URL else None

async def remove_media_group_id_after_delay(media_group_id: str, delay: int):
    await asyncio.sleep(delay)
    PROCESSED_MEDIA_GROUPS.discard(media_group_id)

@router.message(F.forward_from_chat)
async def handle_forwarded_message(message: types.Message, session: AsyncSession):
    if message.media_group_id:
        if message.media_group_id in PROCESSED_MEDIA_GROUPS:
            return
        PROCESSED_MEDIA_GROUPS.add(message.media_group_id)
        asyncio.create_task(remove_media_group_id_after_delay(str(message.media_group_id), 5))

    if not message.from_user:
        await message.reply("Не могу определить, кто отправил сообщение.")
        return

    user_lock = user_locks[message.from_user.id]
    async with user_lock:
        if not message.forward_from_chat or message.forward_from_chat.type != 'channel':
            await message.reply("Это не похоже на канал. Пожалуйста, перешлите сообщение из публичного канала.")
            return

        user = message.from_user
        channel_forward = message.forward_from_chat

        response_message, new_channel_obj = await add_subscription(
            session=session,
            user_id=user.id,
            channel_id=channel_forward.id,
            channel_title=channel_forward.title or "",
            channel_un=channel_forward.username or ""
        )
    
    await message.answer(response_message)

    if new_channel_obj and redis_client:
        try:
            # Создаем "тикет" для воркера
            task_payload = {
                "user_chat_id": message.chat.id,
                "channel_id": new_channel_obj.id,
                "channel_title": new_channel_obj.title,
            }
            # Отправляем тикет в очередь 'new_channel_tasks'
            await redis_client.lpush("new_channel_tasks", json.dumps(task_payload))
            logging.info(f"Создана задача на обработку канала {new_channel_obj.id} для пользователя {user.id}")
        except Exception as e:
            logging.error(f"Не удалось создать задачу в Redis для нового канала: {e}")