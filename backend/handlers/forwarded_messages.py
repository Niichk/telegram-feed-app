from aiogram import Router, types, F
from database.requests import add_subscription

router = Router()

@router.message(F.forward_from_chat)
async def handle_forwarded_message(message: types.Message):
    # Проверяем, что сообщение переслано именно из канала
    if message.forward_from_chat is not None and message.forward_from_chat.type == 'channel':
        # Проверяем, что отправитель существует
        if message.from_user is not None:
            # Получаем данные о пользователе и канале
            user_id = message.from_user.id
            user_first_name = message.from_user.first_name
            user_username = message.from_user.username or ""

            channel_id = message.forward_from_chat.id
            channel_title = message.forward_from_chat.title or ""
            channel_username = message.forward_from_chat.username or ""
            
            # Вызываем нашу функцию для добавления подписки в БД
            response_message = await add_subscription(
                user_id, user_first_name, user_username,
                channel_id, channel_title, channel_username
            )
            
            # Отправляем ответ пользователю
            await message.answer(response_message)
        else:
            await message.answer("Не удалось определить пользователя. Попробуйте еще раз.")
    else:
        await message.answer("Пожалуйста, перешлите сообщение из публичного канала.")