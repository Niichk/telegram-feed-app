import os
import json
import logging
import aioredis
from typing import Any, Dict
from collections import defaultdict
from aiogram import Router, types, F
from sqlalchemy.ext.asyncio import AsyncSession

from database.requests import add_subscription

router = Router()
user_locks = defaultdict(lambda: None)

# ИСПРАВЛЕНИЕ: Правильная инициализация Redis
REDIS_URL = os.getenv("REDIS_URL") or os.getenv("REDIS_PUBLIC_URL")

# Инициализация Redis клиента
redis_client = None

async def init_redis():
    """Инициализация Redis клиента"""
    global redis_client
    if REDIS_URL and not redis_client:
        try:
            redis_client = aioredis.from_url(REDIS_URL)
            logging.info(f"✅ Redis client инициализирован: {REDIS_URL[:20]}...")
        except Exception as e:
            logging.error(f"❌ Ошибка инициализации Redis: {e}")
            redis_client = None
    elif not REDIS_URL:
        logging.warning("⚠️ REDIS_URL не установлен в environment variables")

# Вызываем инициализацию при импорте модуля
import asyncio
try:
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(init_redis())
    else:
        asyncio.run(init_redis())
except Exception:
    # Если нет активного event loop, инициализируем позже
    pass

@router.message(F.forward_from_chat)
async def handle_forwarded_message(message: types.Message, session: AsyncSession):
    if not message.from_user or not message.forward_from_chat or message.forward_from_chat.type != 'channel':
        return await message.reply("Пожалуйста, перешлите сообщение из публичного канала.")

    # Инициализируем Redis если еще не инициализирован
    if not redis_client:
        await init_redis()

    user_lock = user_locks[message.from_user.id]
    if user_lock is None:
        import asyncio
        user_lock = asyncio.Lock()
        user_locks[message.from_user.id] = user_lock # type: ignore

    async with user_lock:
        logging.info(f"🔄 Пользователь {message.from_user.id} добавляет канал: {message.forward_from_chat.title}")
        
        response_msg, new_channel = await add_subscription(
            session=session, 
            user_id=message.from_user.id,
            channel_id=message.forward_from_chat.id,
            channel_title=message.forward_from_chat.title or "",
            channel_un=message.forward_from_chat.username or ""
        )
    
    await message.answer(response_msg)

    if new_channel and redis_client:
        try:
            task: Dict[str, Any] = {
                "user_chat_id": str(message.from_user.id),  # ✅ ИСПРАВЛЕНО: from_user.id
                "channel_id": str(new_channel.id),
                "channel_title": new_channel.title,
            }
            
            logging.info(f"📤 Отправляю задачу в Redis: {task}")
            
            task_json: str = json.dumps(task)
            await redis_client.lpush("new_channel_tasks", task_json)
            
            logging.info(f"✅ Задача успешно отправлена в Redis для канала {new_channel.title}")
            
        except Exception as e:
            logging.error(f"❌ Ошибка отправки задачи в Redis: {e}", exc_info=True)
    else:
        if not new_channel:
            logging.info(f"ℹ️ Канал уже существует, задача в Redis не отправляется")
        if not redis_client:
            logging.error(f"❌ Redis client не инициализирован! REDIS_URL = {REDIS_URL}")