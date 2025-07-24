import os
from aiogram import Router, F, types, Bot
from aiogram.filters import Command, or_f
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# Импортируем нашу клавиатуру, чтобы возвращать пользователя в главное меню
from .user_commands import get_main_keyboard

router = Router()

# Создаем "состояния", в которых может находиться пользователь
class FeedbackState(StatesGroup):
    awaiting_message = State() # Состояние ожидания сообщения с отзывом

# Обработчик, который запускает процесс сбора отзыва
@router.message(or_f(Command("feedback"), F.text == "✍️ Оставить отзыв"))
async def start_feedback(message: types.Message, state: FSMContext):
    await state.set_state(FeedbackState.awaiting_message)
    await message.answer(
        "Я внимательно вас слушаю. Пожалуйста, отправьте свой отзыв, идею или сообщение об ошибке одним сообщением. \n\nДля отмены введите /cancel",
        # Временно убираем основную клавиатуру, чтобы не мешала
        reply_markup=types.ReplyKeyboardRemove()
    )

# Обработчик для отмены
@router.message(FeedbackState.awaiting_message, Command("cancel"))
async def cancel_feedback(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Действие отменено.",
        reply_markup=get_main_keyboard()
    )

# Обработчик, который получает сообщение с отзывом
@router.message(FeedbackState.awaiting_message, F.text)
async def process_feedback(message: types.Message, state: FSMContext, bot: Bot):
    # Завершаем состояние FSM
    await state.clear()

    # Отправляем пользователю благодарность и возвращаем основную клавиатуру
    await message.answer(
        "Спасибо! Ваш отзыв очень важен и поможет сделать сервис лучше. ✨",
        reply_markup=get_main_keyboard()
    )

    # Пересылаем сообщение админу
    admin_id = os.getenv("ADMIN_ID")
    if admin_id and message.from_user:
        # Формируем красивое сообщение для админа
        user_info = f"Отзыв от пользователя:\n" \
                    f"ID: {message.from_user.id}\n" \
                    f"Username: @{message.from_user.username or 'не указан'}"

        await bot.send_message(admin_id, user_info)
        # Пересылаем само сообщение с отзывом
        await bot.forward_message(
            chat_id=admin_id,
            from_chat_id=message.chat.id,
            message_id=message.message_id
        )