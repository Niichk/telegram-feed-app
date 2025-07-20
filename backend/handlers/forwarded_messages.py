from aiogram import Router, types, F
from sqlalchemy.ext.asyncio import AsyncSession
from database.requests import add_subscription
from worker import fetch_posts_for_channel, upload_avatar_to_s3, client 
from telethon.tl.types import InputPeerChannel 

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

    # --- ИЗМЕНЕНО: Логика вызова ---
    # 1. Вызываем функцию добавления подписки
    response_message, new_channel_obj = await add_subscription(
        session=session,
        user_id=user.id,
        user_fn=user.first_name,
        user_un=user.username or "",
        channel_id=channel_forward.id,
        channel_title=channel_forward.title or "",
        channel_un=channel_forward.username or ""
    )
    
    # 2. Отправляем пользователю предварительный ответ
    await message.answer(response_message)

    # 3. Если был создан НОВЫЙ канал (а не найдена старая подписка),
    #    то запускаем для него сбор постов.
    if new_channel_obj:
        # Убедимся, что клиент Telethon подключен, прежде чем его использовать
        if not client.is_connected():
            await client.connect()
        
        # 1. Получаем entity канала через Telethon
        channel_entity = await client.get_entity(new_channel_obj.id)
        
        # 2. Скачиваем и загружаем аватар, получаем URL
        avatar_url = await upload_avatar_to_s3(channel_entity)
        
        # 3. Сохраняем URL в наш объект и в базу данных
        if avatar_url:
            new_channel_obj.avatar_url = avatar_url
            session.add(new_channel_obj)
            await session.commit()

        # Вызываем функцию сбора постов для свежедобавленного канала
        await fetch_posts_for_channel(channel=new_channel_obj, db_session=session, post_limit=20) # Можно поставить лимит побольше для первого раза
        
        await message.answer(f"👍 Готово! Последние посты из «{new_channel_obj.title}» добавлены в вашу ленту.")