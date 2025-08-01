import os
import json
import logging
import redis.asyncio as aioredis
from typing import Any, Dict
from collections import defaultdict
from aiogram import Router, types, F
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio

# --- ДОБАВЛЕНО: Импортируем клиент Telethon для проверки ---
from worker import client
from database.requests import add_subscription

router = Router()
user_locks = defaultdict(lambda: asyncio.Lock())

@router.message(F.forward_from_chat)
async def handle_forwarded_message(message: types.Message, session: AsyncSession, redis_client: aioredis.Redis):
    if not message.from_user or not message.forward_from_chat or message.forward_from_chat.type != 'channel':
        return await message.reply("Пожалуйста, перешлите сообщение из публичного канала.")
    
    # --- НАЧАЛО НОВОЙ ЛОГИКИ: ПРОВЕРКА НА ПРИВАТНЫЙ КАНАЛ ---
    channel_forward = message.forward_from_chat
    channel_title_for_reply = channel_forward.title or "Без названия"

    try:
        if not client.is_connected():
            await client.connect()
        # Пытаемся получить доступ к каналу. Если он приватный, здесь будет ошибка.
        entity_identifier = channel_forward.username or channel_forward.id
        await client.get_entity(entity_identifier)

    except (ValueError, TypeError):
        # Если не смогли получить доступ - это приватный канал.
        logging.warning(f"Пользователь {message.from_user.id} попытался добавить приватный канал: {channel_title_for_reply}")
        await message.answer(
            f"❌ **Канал «{channel_title_for_reply}» — приватный.**\n\n"
            f"К сожалению, на данный момент бот поддерживает подписку только на публичные каналы.",
            parse_mode="Markdown"
        )
        return
    except Exception as e:
        logging.error(f"Неизвестная ошибка при проверке канала {channel_title_for_reply}: {e}")
        await message.answer("Произошла ошибка при проверке канала. Попробуйте позже.")
        return
    # --- КОНЕЦ НОВОЙ ЛОГИКИ ---

    # Обработка медиа-групп (остается без изменений)
    if message.media_group_id:
        cache_key = f"media_group_processed:{message.media_group_id}"
        is_first = await redis_client.set(cache_key, "1", ex=10, nx=True)
        if not is_first:
            logging.info(f"Игнорируем дубликат из медиа-группы {message.media_group_id}")
            return

    # Логика блокировки на пользователя (остается без изменений)
    user_lock = user_locks[message.from_user.id]

    async with user_lock:
        logging.info(f"🔄 Пользователь {message.from_user.id} добавляет канал: {channel_title_for_reply}")
        
        response_msg, new_channel = await add_subscription(
            session=session, 
            user_id=message.from_user.id,
            channel_id=channel_forward.id,
            channel_title=channel_title_for_reply,
            # Важно: передаем None для приватных каналов, даже если они прошли проверку
            channel_un=channel_forward.username or None
        )
    
    await message.answer(response_msg)

    # Отправка задачи в Redis (остается без изменений)
    if new_channel and redis_client:
        try:
            task: Dict[str, Any] = {
                "user_chat_id": str(message.from_user.id),
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