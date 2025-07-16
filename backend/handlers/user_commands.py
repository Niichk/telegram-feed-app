from aiogram import Router, types
from aiogram.filters import Command
from sqlalchemy.ext.asyncio import AsyncSession
from database.requests import get_user_subscriptions

router = Router()

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("Привет! Чтобы добавить канал в ленту, просто перешли мне любой пост из него. 🚀")

# Новый обработчик для команды /subscriptions
@router.message(Command("subscriptions"))
async def cmd_subscriptions(message: types.Message, session: AsyncSession):
    """
    Отправляет пользователю список его активных подписок.
    """
    if not message.from_user:
        await message.answer("Не могу определить ваш профиль.")
        return

    # Получаем подписки из базы данных
    subscriptions = await get_user_subscriptions(session, message.from_user.id)

    if not subscriptions:
        await message.answer("У вас пока нет активных подписок. \n\nЧтобы добавить канал, просто перешлите мне пост из него.")
        return

    # Формируем красивый ответ
    response_text = "✨ **Ваши подписки:**\n\n"
    for i, channel in enumerate(subscriptions, 1):
        # Если у канала есть юзернейм, делаем его кликабельной ссылкой
        if channel.username:
            response_text += f"{i}. <a href='https://t.me/{channel.username}'>{channel.title}</a>\n"
        else:
            response_text += f"{i}. {channel.title}\n"
    
    await message.answer(response_text, parse_mode="HTML", disable_web_page_preview=True)