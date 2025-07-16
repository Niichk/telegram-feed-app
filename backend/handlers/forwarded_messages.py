from aiogram import Router, types, F
from sqlalchemy.ext.asyncio import AsyncSession
from database.requests import add_subscription
# --- ИЗМЕНЕНО: Импортируем нашу новую функцию из воркера ---
from worker import fetch_posts_for_channel, client

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
        
        # Вызываем функцию сбора постов для свежедобавленного канала
        await fetch_posts_for_channel(channel=new_channel_obj, db_session=session, post_limit=20) # Можно поставить лимит побольше для первого раза
        
        await message.answer(f"👍 Готово! Последние посты из «{new_channel_obj.title}» добавлены в вашу ленту.")