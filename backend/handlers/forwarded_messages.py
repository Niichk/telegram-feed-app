import os
import json
import logging
import redis.asyncio as aioredis
from typing import Any, Dict
from collections import defaultdict
from aiogram import Router, types, F
from sqlalchemy.ext.asyncio import AsyncSession

from database.requests import add_subscription

router = Router()
user_locks = defaultdict(lambda: None)

@router.message(F.forward_from_chat)
async def handle_forwarded_message(message: types.Message, session: AsyncSession, redis_client: aioredis.Redis):
    if not message.from_user or not message.forward_from_chat or message.forward_from_chat.type != 'channel':
        return await message.reply("Пожалуйста, перешлите сообщение из публичного канала.")
    
    if message.media_group_id:
        cache_key = f"media_group_processed:{message.media_group_id}"
        
        # Атомарно устанавливаем ключ, ТОЛЬКО если он не существует (nx=True).
        # Команда вернет True, если ключ был успешно установлен (т.е. мы первые).
        # Команда вернет False, если ключ уже существовал (т.е. мы дубликат).
        is_first = await redis_client.set(cache_key, "1", ex=10, nx=True)
        
        # Если это не первое сообщение в группе, выходим.
        if not is_first:
            logging.info(f"Игнорируем дубликат из медиа-группы {message.media_group_id}")
            return

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
            logging.error(f"❌ Redis client не инициализирован!")