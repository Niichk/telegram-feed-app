import asyncio
import logging
import os
import sys
from dotenv import load_dotenv
import json
import redis.asyncio as aioredis

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand

from database.engine import session_maker, create_db
from handlers import user_commands, forwarded_messages, callback_handlers, feedback_handler 
from middlewares.db import DbSessionMiddleware


# --- Настройка в самом начале ---
load_dotenv()
DB_URL_FOR_LOG = os.getenv("DATABASE_URL")

# --- ДОБАВЛЕНО: Добавляем версию кода в лог ---
logging.basicConfig(level=logging.INFO)
logging.info("ЗАПУСК КОДА ВЕРСИИ 2.0 (без first_name/username)")
# ---------------------------------------------

logging.info(f"!!! BOT STARTING WITH DATABASE_URL: {DB_URL_FOR_LOG} !!!")


API_TOKEN = os.getenv("API_TOKEN")


async def listen_for_task_results(bot: Bot):
    """Слушает уведомления от воркера о завершенных задачах."""
    REDIS_URL = os.getenv("REDIS_URL")
    if not REDIS_URL:
        return
    
    redis_client = aioredis.from_url(REDIS_URL)
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("task_completion_notifications")
    logging.info("Бот начал прослушивание уведомлений о завершении задач.")

    while True:
        try:
            message = await pubsub.get_message(ignore_subscribe_messages=True)
            if message and message.get("type") == "message":
                task_result = json.loads(message["data"])
                chat_id = task_result.get("user_chat_id")
                channel_title = task_result.get("channel_title")
                
                if chat_id and channel_title:
                    from handlers.user_commands import get_main_keyboard # Локальный импорт
                    await bot.send_message(
                        chat_id=chat_id,
                        text=f"👍 Готово! Последние посты из «{channel_title}» добавлены в вашу ленту.",
                        reply_markup=get_main_keyboard()
                    )
        except Exception as e:
            logging.error(f"Ошибка в обработчике уведомлений Redis: {e}")
            await asyncio.sleep(5)


async def main():
    if not API_TOKEN:
        logging.critical("Критическая ошибка: переменная API_TOKEN не установлена!")
        sys.exit(1)

    bot = Bot(token=API_TOKEN)
    dp = Dispatcher()

    main_menu_commands = [
        BotCommand(command="/start", description="▶️ Запустить бота"),
        BotCommand(command="/subscriptions", description="📜 Мои подписки"),
        BotCommand(command="/help", description="ℹ️ Помощь"),
        BotCommand(command="/feedback", description="✍️ Оставить отзыв")
    ]
    await bot.set_my_commands(main_menu_commands)

    await create_db()
    logging.info("Bot: New database tables created.")

    dp.update.middleware(DbSessionMiddleware(session_pool=session_maker))
    dp.include_router(user_commands.router)
    dp.include_router(forwarded_messages.router)
    dp.include_router(callback_handlers.router)
    dp.include_router(feedback_handler.router)

    await bot.delete_webhook(drop_pending_updates=True)
    
    # Запускаем и бота, и слушателя уведомлений от воркера
    await asyncio.gather(
        dp.start_polling(bot),
        listen_for_task_results(bot)
    )