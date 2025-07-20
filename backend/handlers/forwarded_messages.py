from aiogram import Router, types, F
from sqlalchemy.ext.asyncio import AsyncSession
from database.requests import add_subscription
from database.engine import session_maker  # ДОБАВИТЬ: импорт session_maker
from worker import fetch_posts_for_channel, upload_avatar_to_s3, client
import logging
from asyncio import Lock
from collections import defaultdict

router = Router()

# Создаем словарь для хранения блокировок по ID пользователя
user_locks = defaultdict(Lock)

@router.message(F.forward_from_chat)
async def handle_forwarded_message(message: types.Message, session: AsyncSession):
    if not message.from_user:
        await message.reply("Не могу определить, кто отправил сообщение. Попробуйте перезапустить бота.")
        return

    # Получаем блокировку конкретно для этого пользователя
    user_lock = user_locks[message.from_user.id]

    # ИСПРАВЛЕНИЕ: Все DB операции внутри одной блокировки
    async with user_lock:
        if not message.forward_from_chat or message.forward_from_chat.type != 'channel':
            await message.reply("Это не похоже на канал. Пожалуйста, перешлите сообщение из публичного канала.")
            return

        user = message.from_user
        channel_forward = message.forward_from_chat

        # Шаг 1: Пытаемся добавить подписку
        response_message, new_channel_obj = await add_subscription(
            session=session,
            user_id=user.id,
            channel_id=channel_forward.id,
            channel_title=channel_forward.title or "",
            channel_un=channel_forward.username or ""
        )

        # Шаг 2: Отправляем пользователю ответ сразу
        await message.answer(response_message)

        # Шаг 3: Если канал не новый, выходим
        if not new_channel_obj:
            return

        # ИСПРАВЛЕНИЕ: Сохраняем данные канала для дальнейшей обработки
        channel_data = {
            'id': new_channel_obj.id,
            'title': new_channel_obj.title,
            'username': new_channel_obj.username
        }

    # --- БЛОКИРОВКА ОСВОБОЖДЕНА ---
    # Теперь выполняем долгие операции с новой сессией

    # Шаг 4: Выполняем все долгие сетевые операции с новой сессией
    try:
        if not client.is_connected():
            await client.connect()

        entity_identifier = channel_data['username'] or channel_data['id']
        channel_entity = await client.get_entity(entity_identifier)

        # ИСПРАВЛЕНИЕ: Создаем новую сессию для долгих операций
        async with session_maker() as new_session:
            # Получаем объект канала в новой сессии
            from database.models import Channel
            from sqlalchemy import select
            
            stmt = select(Channel).where(Channel.id == channel_data['id'])
            result = await new_session.execute(stmt)
            channel_obj = result.scalars().first()
            
            if not channel_obj:
                logging.error(f"Канал {channel_data['id']} не найден в БД")
                return

            # Загружаем и сохраняем аватар
            avatar_url = await upload_avatar_to_s3(channel_entity)
            if avatar_url:
                channel_obj.avatar_url = avatar_url
                await new_session.commit()

            # Загружаем посты
            await fetch_posts_for_channel(
                channel=channel_obj, 
                db_session=new_session, 
                post_limit=20
            )

        await message.answer(f"👍 Готово! Последние посты из «{channel_data['title']}» добавлены в вашу ленту.")

    except ValueError as e:
        logging.error(f"Не удалось получить доступ к каналу {channel_data['id']}: {e}")
        await message.answer(
            f"❌ **Не удалось получить доступ к каналу «{channel_data['title']}».**\n\n"
            f"Скорее всего, это частный канал. Чтобы я мог читать из него посты, "
            f"мой рабочий аккаунт должен быть добавлен в этот канал как участник.",
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.error(f"Критическая ошибка при обработке нового канала {channel_data['id']}: {e}")
        await message.answer("Произошла внутренняя ошибка. Попробуйте позже.")