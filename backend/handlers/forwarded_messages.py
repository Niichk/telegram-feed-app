from aiogram import Router, types, F
from sqlalchemy.ext.asyncio import AsyncSession
from database.requests import add_subscription
from worker import fetch_posts_for_channel, upload_avatar_to_s3, client
from database.models import Channel
import logging
import asyncio
from collections import defaultdict

router = Router()

# Словарь для хранения блокировок по ID пользователя
user_locks = defaultdict(asyncio.Lock)

# --- ДОБАВЛЕНО: Кэш для отслеживания уже обработанных медиа-групп ---
PROCESSED_MEDIA_GROUPS = set()

async def remove_media_group_id_after_delay(media_group_id: str, delay: int):
    """Асинхронная задача для очистки кэша медиа-групп."""
    await asyncio.sleep(delay)
    PROCESSED_MEDIA_GROUPS.discard(media_group_id)


@router.message(F.forward_from_chat)
async def handle_forwarded_message(message: types.Message, session: AsyncSession):
    # --- ДОБАВЛЕНО: Логика для обработки альбомов (медиа-групп) ---
    if message.media_group_id:
        # Если ID группы уже в нашем кэше, значит мы уже обрабатываем этот альбом. Игнорируем.
        if message.media_group_id in PROCESSED_MEDIA_GROUPS:
            return
        # Если ID новый, добавляем его в кэш и запускаем обработку.
        PROCESSED_MEDIA_GROUPS.add(message.media_group_id)
        # Запускаем задачу, которая удалит ID из кэша через 5 секунд, чтобы не засорять память.
        asyncio.create_task(remove_media_group_id_after_delay(str(message.media_group_id), 5))
    # --- КОНЕЦ НОВОЙ ЛОГИКИ ---

    if not message.from_user:
        await message.reply("Не могу определить, кто отправил сообщение. Попробуйте перезапустить бота.")
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

    if not new_channel_obj:
        return

    try:
        if not client.is_connected():
            await client.connect()

        entity_identifier = new_channel_obj.username or new_channel_obj.id
        channel_entity = await client.get_entity(entity_identifier)

        avatar_url = await upload_avatar_to_s3(channel_entity)
        if avatar_url:
            # ИСПРАВЛЕНИЕ: Используем fresh query вместо merge
            async with session.begin():
                channel_to_update = await session.get(Channel, new_channel_obj.id)
                if channel_to_update:
                    channel_to_update.avatar_url = avatar_url

        await fetch_posts_for_channel(channel=new_channel_obj, db_session=session, post_limit=20)

        # ИСПРАВЛЕНИЕ: Получаем title из базы данных в активной сессии
        fresh_channel = await session.get(Channel, new_channel_obj.id)
        channel_title = fresh_channel.title if fresh_channel else "канал"
        
        await message.answer(f"👍 Готово! Последние посты из «{channel_title}» добавлены в вашу ленту.")
        
    except ValueError as e:
        logging.error(f"Не удалось получить доступ к каналу {new_channel_obj.id}: {e}")
        await message.answer(
            f"❌ **Не удалось получить доступ к каналу «{new_channel_obj.title}».**\n\n"
            f"Скорее всего, это частный канал.",
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.error(f"Критическая ошибка при обработке нового канала {new_channel_obj.id}: {e}")
        await message.answer("Произошла внутренняя ошибка. Попробуйте позже.")