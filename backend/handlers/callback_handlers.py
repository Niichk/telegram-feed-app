from aiogram import Router, F, types
from sqlalchemy.ext.asyncio import AsyncSession
from database.requests import delete_subscription # Импортируем функцию удаления

router = Router()

# Этот хендлер будет срабатывать на callback-и, которые начинаются с "unsub:"
@router.callback_query(F.data.startswith("unsub:"))
async def process_unsubscription(callback: types.CallbackQuery, session: AsyncSession):
    # Убеждаемся, что callback пришел от реального пользователя
    if not callback.from_user:
        await callback.answer("Не могу определить пользователя.", show_alert=True)
        return

    # Извлекаем ID канала из callback_data
    try:
        if not callback.data:
            raise ValueError("callback.data is None")
        channel_id = int(callback.data.split(":")[1])
    except (IndexError, ValueError, AttributeError):
        await callback.answer("Ошибка в данных. Попробуйте снова.", show_alert=True)
        return

    # Вызываем функцию удаления из БД
    success = await delete_subscription(session, callback.from_user.id, channel_id)

    if success:
        # Если удачно, редактируем исходное сообщение, чтобы убрать кнопки
        if isinstance(callback.message, types.Message):
            await callback.message.edit_text("✅ Вы успешно отписались.")
        await callback.answer("Отписка оформлена!") # Всплывающее уведомление
    else:
        # Если подписка уже была удалена кем-то еще
        if isinstance(callback.message, types.Message):
            await callback.message.edit_text("🤔 Похоже, вы уже отписаны.")
        await callback.answer("Подписка не найдена.", show_alert=True)