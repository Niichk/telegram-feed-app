from aiogram import Router, types, F
from sqlalchemy.ext.asyncio import AsyncSession
from database.requests import add_subscription

router = Router()

@router.message(F.forward_from_chat)
async def handle_forwarded_message(message: types.Message, session: AsyncSession):
            """
            Обрабатывает пересланное сообщение, валидирует его и добавляет подписку.
            """
            # 1. Защита от некорректных данных
            if not message.forward_from_chat or message.forward_from_chat.type != 'channel':
                await message.reply("Это не похоже на канал. Пожалуйста, перешлите сообщение из публичного канала.")
                return

            if not message.from_user:
                await message.reply("Не могу определить, кто отправил сообщение. Попробуйте перезапустить бота.")
                return

            # 2. Если все проверки пройдены, безопасно извлекаем данные
            user = message.from_user
            channel = message.forward_from_chat

            # 3. Вызываем функцию для добавления в БД, передавая ей сессию
            response_message = await add_subscription(
                session=session,
                user_id=user.id,
                user_fn=user.first_name,
                user_un=user.username or "",
                channel_id=channel.id,
                channel_title=channel.title or "",
                channel_un=channel.username or ""
            )

            # 4. Отправляем ответ пользователю
            await message.answer(response_message)