import asyncio
import logging
import os
import sys
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher

from database.engine import session_maker, create_db
from handlers import user_commands, forwarded_messages
from middlewares.db import DbSessionMiddleware 


# --- Настройка в самом начале ---
load_dotenv()
logging.basicConfig(level=logging.INFO)

# Получаем токен из переменных окружения
API_TOKEN = os.getenv("API_TOKEN")


async def main():
    # --- Проверку и создание бота переносим сюда, внутрь main ---
    if not API_TOKEN:
        logging.critical("Критическая ошибка: переменная API_TOKEN не установлена!")
        sys.exit(1)

    bot = Bot(token=API_TOKEN) # Теперь Pylance "видит" проверку выше
    dp = Dispatcher()
    # -------------------------------------------------------------

    dp.update.middleware(DbSessionMiddleware(session_pool=session_maker))
    # Подключаем роутеры к главному диспетчеру
    dp.include_router(user_commands.router)
    dp.include_router(forwarded_messages.router)

    # Создаем таблицы в БД
    await create_db()
    
    # Запускаем бота
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот остановлен вручную")