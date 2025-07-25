from aiogram import Router, types, F
from sqlalchemy.ext.asyncio import AsyncSession
from database.requests import add_subscription
from worker import fetch_posts_for_channel, upload_avatar_to_s3, client
from database.engine import session_maker
from database.models import Channel
import logging
import asyncio
from collections import defaultdict
from .user_commands import get_main_keyboard

router = Router()

user_locks = defaultdict(asyncio.Lock)
PROCESSED_MEDIA_GROUPS = set()

async def remove_media_group_id_after_delay(media_group_id: str, delay: int):
    await asyncio.sleep(delay)
    PROCESSED_MEDIA_GROUPS.discard(media_group_id)

# --- ИЗМЕНЕННАЯ ФУНКЦИЯ ДЛЯ ФОНОВОЙ ЗАДАЧИ ---
async def process_new_channel_background(message: types.Message, channel_id: int):
    """
    Эта функция выполняется в фоне, принимая ID канала, а не объект.
    """
    # Создаем новую, независимую сессию БД для этой задачи
    async with session_maker() as session:
        try:
            # Получаем "свежий" объект канала из БД по ID
            channel_obj = await session.get(Channel, channel_id)
            if not channel_obj:
                logging.error(f"Фоновая задача не смогла найти канал с ID {channel_id}")
                return

            if not client.is_connected():
                await client.connect()

            entity_identifier = channel_obj.username or channel_obj.id
            channel_entity = await client.get_entity(entity_identifier)

            avatar_url = await upload_avatar_to_s3(channel_entity)
            if avatar_url:
                # Обновляем аватар в рамках нашей сессии
                channel_obj.avatar_url = avatar_url
                session.add(channel_obj)
                await session.commit()

            # Запускаем загрузку постов
            await fetch_posts_for_channel(channel=channel_obj, db_session=session, post_limit=20)

            # После успешной загрузки отправляем финальное сообщение
            await message.answer(
                f"👍 Готово! Последние посты из «{channel_obj.title}» добавлены в вашу ленту.",
                reply_markup=get_main_keyboard()
            )
            
        except Exception as e:
            # Если любая из операций выше провалится, мы сообщим об этом
            logging.error(f"Критическая ошибка при фоновой обработке канала {channel_id}: {e}", exc_info=True)
            await message.answer(
                f"❌ Произошла ошибка при загрузке постов. Пожалуйста, попробуйте добавить канал еще раз.",
                reply_markup=get_main_keyboard()
            )

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

    if new_channel_obj:
        # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
        # Передаем в фоновую задачу только ID, а не весь объект
        asyncio.create_task(process_new_channel_background(message, new_channel_obj.id))