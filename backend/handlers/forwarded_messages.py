from aiogram import Router, types, F
from sqlalchemy.ext.asyncio import AsyncSession
from database.requests import add_subscription
from worker import fetch_posts_for_channel, upload_avatar_to_s3, client 
import logging

router = Router()

@router.message(F.forward_from_chat)
async def handle_forwarded_message(message: types.Message, session: AsyncSession):
    """
    Обрабатывает пересланное сообщение, добавляет подписку и СРАЗУ ЖЕ
    запускает сбор постов для этого канала.
    """
    if not message.forward_from_chat or message.forward_from_chat.type != 'channel':
        await message.reply("Это не похоже на канал. Пожалуйста, перешлите сообщение из публичного канала.")
        return

    if not message.from_user:
        await message.reply("Не могу определить, кто отправил сообщение. Попробуйте перезапустить бота.")
        return

    user = message.from_user
    channel_forward = message.forward_from_chat

    # Вызываем функцию добавления подписки
    response_message, new_channel_obj = await add_subscription(
        session=session,
        user_id=user.id,
        user_fn=user.first_name,
        user_un=user.username or "",
        channel_id=channel_forward.id,
        channel_title=channel_forward.title or "",
        channel_un=channel_forward.username or ""
    )
    
    await message.answer(response_message)

    # Если был создан НОВЫЙ канал, запускаем сбор
    if new_channel_obj:
        try:
            if not client.is_connected():
                await client.connect()
            
            # ИСПРАВЛЕНИЕ: Получаем Telethon entity через username или ID
            telethon_entity = None
            if channel_forward.username:
                # Если есть username, используем его
                telethon_entity = await client.get_entity(channel_forward.username)
            else:
                # Если нет username, пробуем получить через ID
                try:
                    telethon_entity = await client.get_entity(channel_forward.id)
                except ValueError:
                    logging.warning(f"Не удалось получить entity для канала {channel_forward.id}, пропускаем аватар")
            
            # Загружаем аватар если получили entity
            if telethon_entity:
                avatar_url = await upload_avatar_to_s3(telethon_entity)
                if avatar_url:
                    new_channel_obj.avatar_url = avatar_url
                    session.add(new_channel_obj)
                    await session.commit()

            # Вызываем функцию сбора постов
            await fetch_posts_for_channel(channel=new_channel_obj, db_session=session, post_limit=20)
            
            await message.answer(f"👍 Готово! Последние посты из «{new_channel_obj.title}» добавлены в вашу ленту.")
            
        except Exception as e:
            logging.error(f"Ошибка при обработке нового канала {new_channel_obj.title}: {e}")
            await message.answer(f"Канал добавлен, но возникла ошибка при загрузке постов. Попробуйте позже.")