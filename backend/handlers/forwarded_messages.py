import asyncio
import logging
import json
import os
from typing import Any, Dict  # ДОБАВИТЬ
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
        logging.info(f"🔄 Пользователь {message.from_user.id} добавляет канал: {message.forward_from_chat.title}")
        
        response_msg, new_channel = await add_subscription(
            session=session, user_id=message.from_user.id,
            channel_id=message.forward_from_chat.id,
            channel_title=message.forward_from_chat.title or "",
            channel_un=message.forward_from_chat.username or ""
        )
    
    await message.answer(response_msg)

    if new_channel and redis_client:
        try:
            task: Dict[str, Any] = {  # ДОБАВИТЬ type hint
                "user_chat_id": str(message.chat.id),
                "channel_id": str(new_channel.id),
                "channel_title": new_channel.title,
            }
            
            logging.info(f"📤 Отправляю задачу в Redis: {task}")
            
            # ИСПРАВЛЕНИЕ: Явно указываем тип для json.dumps
            task_json: str = json.dumps(task)
            await redis_client.lpush("new_channel_tasks", json.dumps(task))  # type: ignore[misc]
            
            logging.info(f"✅ Задача успешно отправлена в Redis для канала {new_channel.title}")
            
        except Exception as e:
            logging.error(f"❌ Ошибка отправки задачи в Redis: {e}", exc_info=True)
    else:
        if not new_channel:
            logging.info(f"ℹ️ Канал уже существует, задача в Redis не отправляется")
        if not redis_client:
            logging.error(f"❌ Redis client не инициализирован! REDIS_URL = {os.getenv('REDIS_URL')}")