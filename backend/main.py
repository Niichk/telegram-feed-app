import asyncio
import logging
import os
import sys
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher
# 1. ДОБАВЬ ЭТОТ ИМПОРТ
from aiogram.types import BotCommand

from database.engine import session_maker, create_db
from handlers import user_commands, forwarded_messages
from middlewares.db import DbSessionMiddleware 


# --- Настройка в самом начале ---
load_dotenv()
logging.basicConfig(level=logging.INFO)

API_TOKEN = os.getenv("API_TOKEN")


async def main():
    if not API_TOKEN:
        logging.critical("Критическая ошибка: переменная API_TOKEN не установлена!")
        sys.exit(1)

    bot = Bot(token=API_TOKEN)
    dp = Dispatcher()

    # 2. ДОБАВЬ ЭТОТ БЛОК ПЕРЕД ЗАПУСКОМ БОТА
    # --- Настройка меню команд ---
    main_menu_commands = [
        BotCommand(command="/start", description="▶️ Запустить бота"),
        BotCommand(command="/subscriptions", description="📜 Мои подписки")
    ]
    await bot.set_my_commands(main_menu_commands)
    # ---------------------------

    dp.update.middleware(DbSessionMiddleware(session_pool=session_maker))
    dp.include_router(user_commands.router)
    dp.include_router(forwarded_messages.router)

    await create_db()
    
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот остановлен вручную")