import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from .models import Base

load_dotenv()
# --- Строка подключения к базе данных ---
# Формат: postgresql+asyncpg://USER:PASSWORD@HOST:PORT/DB_NAME
# Данные берем из нашего docker-compose.yml
DB_URL = os.getenv("DATABASE_URL")

if DB_URL is None:
    raise ValueError("Environment variable DB_URL is not set")

if DB_URL and DB_URL.startswith("postgresql://"):
    DB_URL = DB_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

if not DB_URL:
    PGHOST = os.getenv("PGHOST")
    PGUSER = os.getenv("PGUSER")
    PGPASSWORD = os.getenv("PGPASSWORD")
    PGDATABASE = os.getenv("PGDATABASE")
    PGPORT = os.getenv("PGPORT")
    DB_URL = f"postgresql+asyncpg://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
# ---------------------------------

engine = create_async_engine(DB_URL, echo=False)
session_maker = async_sessionmaker(engine, expire_on_commit=False)


# Функция для создания таблиц в базе данных
async def create_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# Функция для удаления таблиц (для тестов)
async def drop_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)