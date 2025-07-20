import asyncio
import logging
import os
import sys
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand

from database.engine import session_maker, create_db
from handlers import user_commands, forwarded_messages, callback_handlers
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


async def main():
    if not API_TOKEN:
        logging.critical("Критическая ошибка: переменная API_TOKEN не установлена!")
        sys.exit(1)

    bot = Bot(token=API_TOKEN)
    dp = Dispatcher()

    main_menu_commands = [
        BotCommand(command="/start", description="▶️ Запустить бота"),
        BotCommand(command="/subscriptions", description="📜 Мои подписки")
    ]
    await bot.set_my_commands(main_menu_commands)

    await create_db()
    logging.info("Bot: New database tables created.")

    dp.update.middleware(DbSessionMiddleware(session_pool=session_maker))
    dp.include_router(user_commands.router)
    dp.include_router(forwarded_messages.router)
    dp.include_router(callback_handlers.router)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот остановлен вручную")