from aiogram import Router, types, F
from sqlalchemy.ext.asyncio import AsyncSession
from database.requests import add_subscription
from worker import fetch_posts_for_channel, upload_avatar_to_s3, client
import logging
from asyncio import Lock


router = Router()

channel_lock = Lock()

@router.message(F.forward_from_chat)
async def handle_forwarded_message(message: types.Message, session: AsyncSession):
    async with channel_lock:
        # Проверяем, что сообщение действительно из канала
        if not message.forward_from_chat or message.forward_from_chat.type != 'channel':
            await message.reply("Это не похоже на канал. Пожалуйста, перешлите сообщение из публичного канала.")
            return

        if not message.from_user:
            await message.reply("Не могу определить, кто отправил сообщение. Попробуйте перезапустить бота.")
            return

        user = message.from_user
        channel_forward = message.forward_from_chat

        # Шаг 1: Пытаемся добавить подписку
        # --- ИЗМЕНЕНО: Больше не передаем личные данные ---
        response_message, new_channel_obj = await add_subscription(
            session=session,
            user_id=user.id,
            # user_fn=user.first_name, # УДАЛЕНО
            # user_un=user.username or "", # УДАЛЕНО
            channel_id=channel_forward.id,
            channel_title=channel_forward.title or "",
            channel_un=channel_forward.username or ""
        )

        # Шаг 2: Отправляем пользователю ответ (либо "успешно", либо "уже подписаны")
        await message.answer(response_message)

        # --- ГЛАВНОЕ ИЗМЕНЕНИЕ ЗДЕСЬ ---
        # Шаг 3: Если new_channel_obj равен None, значит, подписка уже была. Просто выходим.
        if not new_channel_obj:
            return

        # Шаг 4: Если же канал ДЕЙСТВИТЕЛЬНО новый, продолжаем работу...
        try:
            if not client.is_connected():
                await client.connect()

            entity_identifier = new_channel_obj.username or new_channel_obj.id
            channel_entity = await client.get_entity(entity_identifier)

            avatar_url = await upload_avatar_to_s3(channel_entity)
            if avatar_url:
                await session.merge(new_channel_obj)
                new_channel_obj.avatar_url = avatar_url
                await session.commit()

            await fetch_posts_for_channel(channel=new_channel_obj, db_session=session, post_limit=20)

            await message.answer(f"👍 Готово! Последние посты из «{new_channel_obj.title}» добавлены в вашу ленту.")

        except ValueError as e:
            logging.error(f"Не удалось получить доступ к каналу {new_channel_obj.id}: {e}")
            await message.answer(
                f"❌ **Не удалось получить доступ к каналу «{new_channel_obj.title}».**\n\n"
                f"Скорее всего, это частный канал.",
                parse_mode="Markdown"
            )