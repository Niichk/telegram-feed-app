import asyncio
import logging
import os
import sys
import json
import redis.asyncio as aioredis
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand

from database.engine import session_maker, create_db
from handlers import user_commands, forwarded_messages, callback_handlers, feedback_handler
from middlewares.db import DbSessionMiddleware

load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")
REDIS_URL = os.getenv("REDIS_URL") or os.getenv("REDIS_PUBLIC_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def listen_for_task_results(bot: Bot):
    """Слушает уведомления от воркера о завершенных задачах."""
    REDIS_URL = os.getenv("REDIS_URL") or os.getenv("REDIS_PUBLIC_URL")
    if not REDIS_URL:
        return

    redis_client = aioredis.from_url(REDIS_URL)
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("task_completion_notifications")
    logging.info("Бот слушает уведомления о завершении задач.")

    while True:
        try:
            # ИСПРАВЛЕНИЕ: Используем timeout для неблокирующего ожидания
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message.get("type") == "message" and message["data"]:
                task_result = json.loads(message["data"])
                chat_id = task_result.get("user_chat_id")
                channel_title = task_result.get("channel_title")

                if chat_id and channel_title:
                    from handlers.user_commands import get_main_keyboard
                    await bot.send_message(
                        chat_id=int(chat_id),
                        text=f"👍 Готово! Последние посты из «{channel_title}» добавлены в вашу ленту.",
                        reply_markup=get_main_keyboard()
                    )
        except json.JSONDecodeError as e:
            logging.error(f"Ошибка декодирования JSON: {e}")
        except Exception as e:
            logging.error(f"Ошибка в Redis-слушателе бота: {e}")
            await asyncio.sleep(1) # Небольшая пауза при ошибке

async def main():
    # Диагностика переменных окружения
    logging.info("🚀 Запуск бота...")
    logging.info(f"API_TOKEN: {'✅' if API_TOKEN else '❌'}")
    logging.info(f"REDIS_URL: {'✅' if REDIS_URL else '❌'}")
    logging.info(f"DATABASE_URL: {'✅' if DATABASE_URL else '❌'}")
    
    if not API_TOKEN:
        logging.error("❌ API_TOKEN не установлен!")
        return
    
    if not REDIS_URL:
        logging.warning("⚠️ REDIS_URL не установлен - воркер не будет получать задачи!")
    
    if not DATABASE_URL:
        logging.error("❌ DATABASE_URL не установлен!")
        return

    bot = Bot(token=API_TOKEN)
    dp = Dispatcher()
    
    # Инициализируем Redis в handlers
    if REDIS_URL:
        # Устанавливаем REDIS_URL в os.environ для доступа из handlers
        os.environ["REDIS_URL"] = REDIS_URL
        logging.info("✅ REDIS_URL установлен для handlers")

    commands = [
        BotCommand(command="/start", description="▶️ Запустить бота"),
        BotCommand(command="/subscriptions", description="📜 Мои подписки"),
        BotCommand(command="/help", description="ℹ️ Помощь"),
        BotCommand(command="/feedback", description="✍️ Оставить отзыв")
    ]
    await bot.set_my_commands(commands)

    await create_db()

    dp.update.middleware(DbSessionMiddleware(session_pool=session_maker))
    dp.include_router(user_commands.router)
    dp.include_router(forwarded_messages.router)
    dp.include_router(callback_handlers.router)
    dp.include_router(feedback_handler.router)

    await bot.delete_webhook(drop_pending_updates=True)

    await asyncio.gather(
        dp.start_polling(bot),
        listen_for_task_results(bot)
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот остановлен.")