from aiogram import Router, types, F
from aiogram.filters import Command
from sqlalchemy.ext.asyncio import AsyncSession
from database.requests import get_user_subscriptions
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

router = Router()

def get_main_keyboard():
    """Создает основную клавиатуру каждый раз заново"""
    builder = ReplyKeyboardBuilder()
    builder.button(text="📜 Мои подписки")
    builder.button(text="ℹ️ Помощь")
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True, input_field_placeholder="Выберите действие...")

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "Привет! Чтобы добавить канал в ленту, просто перешли мне любой пост из него. 🚀\n\n"
        "Используй меню ниже для навигации.",
        reply_markup=get_main_keyboard()
    )

@router.message(F.text == "ℹ️ Помощь")
async def cmd_help(message: types.Message):
    web_app_url = "https://frontend-app-production-c1ed.up.railway.app"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📱 Открыть ленту", web_app=types.WebAppInfo(url=web_app_url))
    
    await message.answer(
        "Этот бот создает персональную ленту из Telegram-каналов.\n\n"
        "**Как пользоваться:**\n"
        "1. **Добавить канал:** Перешлите в бот любой пост из публичного канала.\n"
        "2. **Просмотр подписок:** Нажмите кнопку '📜 Мои подписки' или введите команду /subscriptions.\n"
        "3. **Читать ленту:** Нажмите на кнопку 'Открыть ленту' слева",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )

@router.message((Command("subscriptions")) | (F.text == "📜 Мои подписки"))
async def cmd_subscriptions(message: types.Message, session: AsyncSession):
    if not message.from_user:
        await message.answer("Не могу определить ваш профиль.")
        return

    subscriptions = await get_user_subscriptions(session, message.from_user.id)

    if not subscriptions:
        await message.answer("У вас пока нет активных подписок. \n\nЧтобы добавить канал, просто перешлите мне пост из него.")
        return

    # Используем HTML-форматирование для всего текста
    response_text = "✨ <b>Ваши подписки:</b>\n\n"
    builder = InlineKeyboardBuilder()

    for i, channel in enumerate(subscriptions, 1):
        if channel.username:
            response_text += f"{i}. <a href='https://t.me/{channel.username}'>{channel.title}</a>\n"
        else:
            response_text += f"{i}. {channel.title}\n"
        
        builder.button(
            text=f"❌ Отписаться от «{channel.title}»", 
            callback_data=f"unsub:{channel.id}"
        )

    builder.adjust(1) 

    await message.answer(
        response_text,
        reply_markup=builder.as_markup(),
        parse_mode="HTML",
        disable_web_page_preview=True
    )