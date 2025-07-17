import hmac
import hashlib
import os 
import json
import logging
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import List, AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession
from urllib.parse import parse_qsl

from database import requests as db
from database import schemas
from database.engine import create_db, session_maker
from worker import backfill_user_channels

from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from redis import asyncio as aioredis

load_dotenv()
# Загружаем токен из переменных окружения
BOT_TOKEN = os.getenv("API_TOKEN")
REDIS_URL = os.getenv("REDIS_URL") 

app = FastAPI(title="Feed Reader API")

@app.on_event("startup")
async def on_startup():
    await create_db()
    # --- 3. ИНИЦИАЛИЗАЦИЯ КЭША ПРИ СТАРТЕ ---
    if REDIS_URL:
        # Убедимся, что URL корректен для aioredis
        redis_client = aioredis.from_url(REDIS_URL, encoding="utf8", decode_responses=True)
        FastAPICache.init(RedisBackend(redis_client), prefix="fastapi-cache")
        print("FastAPI-Cache with Redis backend is initialized.")

# Настройка CORS
origins = [
    "https://frontend-app-production-c1ed.up.railway.app",
    "http://localhost",
    "http://127.0.0.1",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET"], # Оставляем только GET, так как других методов нет
    allow_headers=["Authorization"],
)

async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    async with session_maker() as session:
        yield session

def is_valid_tma_data(init_data: str) -> dict | None:
    if not BOT_TOKEN:
        print("CRITICAL: BOT_TOKEN is not configured!")
        return None
    try:
        parsed_data = dict(parse_qsl(init_data))
        tma_hash = parsed_data.pop('hash')
        data_check_string = "\n".join(f"{key}={value}" for key, value in sorted(parsed_data.items()))
        secret_key = hmac.new("WebAppData".encode(), BOT_TOKEN.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if calculated_hash == tma_hash:
            return parsed_data
        return None
    except Exception:
        return None

# Единственный, правильный эндпоинт
@app.get("/api/feed/", response_model=schemas.FeedResponse)
@cache(expire=120)
async def get_feed_for_user(
    # Убираем background_tasks из параметров
    session: AsyncSession = Depends(get_db_session),
    page: int = 1,
    authorization: str | None = Header(None)
):
    # ВЕСЬ КОД ВНУТРИ ФУНКЦИИ ОСТАЕТСЯ БЕЗ ИЗМЕНЕНИЙ!
    if authorization is None or not authorization.startswith("tma "):
        raise HTTPException(status_code=401, detail="Not authorized")
    
    init_data = authorization.split(" ", 1)[1]
    validated_data = is_valid_tma_data(init_data)
    
    if validated_data is None:
        raise HTTPException(status_code=403, detail="Invalid hash")

    try:
        user_info = json.loads(validated_data['user'])
        user_id = user_info['id']
    except (KeyError, json.JSONDecodeError):
        raise HTTPException(status_code=403, detail="Invalid user data in initData")

    limit = 20
    offset = (page - 1) * limit
    feed = await db.get_user_feed(session=session, user_id=user_id, limit=limit, offset=offset)

    status = "ok"
    if len(feed) < limit and page > 1:
        logging.info(f"Посты для пользователя {user_id} заканчиваются. Создаю заявку на дозагрузку.")
        await db.create_backfill_request(session, user_id)
        status = "backfilling"

    return {"posts": feed, "status": status}