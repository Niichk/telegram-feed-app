from aiogram import Router, types
from aiogram.filters import Command
from sqlalchemy.ext.asyncio import AsyncSession
from database.requests import get_user_subscriptions
from aiogram.utils.keyboard import InlineKeyboardBuilder

router = Router()

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("Привет! Чтобы добавить канал в ленту, просто перешли мне любой пост из него. 🚀")

# Новый обработчик для команды /subscriptions
@router.message(Command("subscriptions"))
async def cmd_subscriptions(message: types.Message, session: AsyncSession):
    """
    Отправляет пользователю список его подписок
    С КЛИКАБЕЛЬНЫМИ ССЫЛКАМИ и кнопками для отписки.
    """
    if not message.from_user:
        await message.answer("Не могу определить ваш профиль.")
        return

    subscriptions = await get_user_subscriptions(session, message.from_user.id)

    if not subscriptions:
        await message.answer("У вас пока нет активных подписок. \n\nЧтобы добавить канал, просто перешлите мне пост из него.")
        return

    # --- ОБНОВЛЕННАЯ ЛОГИКА ---
    # 1. Готовим и текст, и клавиатуру одновременно
    response_text = "✨ **Ваши подписки:**\n\n"
    builder = InlineKeyboardBuilder()

    for i, channel in enumerate(subscriptions, 1):
        # Добавляем строчку с названием канала в основной текст.
        # Если есть юзернейм - делаем его ссылкой.
        if channel.username:
            response_text += f"{i}. <a href='https://t.me/{channel.username}'>{channel.title}</a>\n"
        else:
            response_text += f"{i}. {channel.title}\n"
        
        # Для каждой строчки добавляем соответствующую кнопку
        builder.button(
            text=f"❌ Отписаться от «{channel.title}»", 
            callback_data=f"unsub:{channel.id}"
        )

    # Выстраиваем кнопки в один столбец
    builder.adjust(1) 

    # 2. Отправляем сообщение с текстом, клавиатурой и нужными параметрами
    await message.answer(
        response_text,
        reply_markup=builder.as_markup(),
        parse_mode="HTML", # <-- Важно вернуть, чтобы ссылки работали
        disable_web_page_preview=True # <-- Полезно, чтобы не было превью сайтов
    )