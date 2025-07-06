import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from .models import Base

load_dotenv()
# --- Строка подключения к базе данных ---
# Формат: postgresql+asyncpg://USER:PASSWORD@HOST:PORT/DB_NAME
# Данные берем из нашего docker-compose.yml
DB_URL = os.getenv("DB_URL")

if DB_URL is None:
    raise ValueError("Environment variable DB_URL is not set")

engine = create_async_engine(DB_URL, echo=False)

# Создаем фабрику сессий для взаимодействия с базой
session_maker = async_sessionmaker(engine, expire_on_commit=False)


# Функция для создания таблиц в базе данных
async def create_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# Функция для удаления таблиц (для тестов)
async def drop_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)