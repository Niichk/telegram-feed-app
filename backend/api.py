import hmac
import hashlib
import os
import json
import logging
import asyncio
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, Depends, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import List, AsyncGenerator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from urllib.parse import parse_qsl

from database import requests as db
from database import schemas
from database.engine import create_db, session_maker
from database.models import Post
from database.schemas import PostInFeed
from worker import backfill_user_channels

from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi.responses import StreamingResponse
from fastapi_cache.decorator import cache
from redis import asyncio as aioredis

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

load_dotenv()

limiter = Limiter(key_func=get_remote_address)

BOT_TOKEN = os.getenv("API_TOKEN")
REDIS_URL = os.getenv("REDIS_URL")
FRONTEND_URL = os.getenv("FRONTEND_URL")

app = FastAPI(title="Feed Reader API")
app.state.limiter = limiter

# Fix: Wrap the handler to match FastAPI's expected signature
def rate_limit_exceeded_handler(request, exc):
    return _rate_limit_exceeded_handler(request, exc)

app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)

@app.on_event("startup")
async def on_startup():
    await create_db()
    if REDIS_URL:
        redis_client = aioredis.from_url(REDIS_URL, encoding="utf8", decode_responses=True)
        FastAPICache.init(RedisBackend(redis_client), prefix="fastapi-cache")
        print("FastAPI-Cache with Redis backend is initialized.")

origins = []
if FRONTEND_URL:
    origins.append(FRONTEND_URL)
else:
    origins = [
    "https://telegram-feed-app-production.up.railway.app",
    "https://*.railway.app",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost",
    "http://127.0.0.1",
]

redis_subscriber = aioredis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ВРЕМЕННО: разрешаем все источники для отладки
    allow_credentials=False,  # ИСПРАВЛЕНИЕ: убираем credentials для "*" origins
    allow_methods=["GET", "POST", "OPTIONS"],  # ИСПРАВЛЕНИЕ: явно разрешаем OPTIONS
    allow_headers=["*"],  # ИСПРАВЛЕНИЕ: разрешаем все заголовки
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
    
def feed_key_builder(
    func,
    namespace: str = "",
    *,
    request: Request | None = None,
    response=None,
    args=(),
    kwargs=None,
):
    """
    Создает уникальный ключ кэша для ленты пользователя.
    Ключ включает ID пользователя, чтобы ленты не перемешивались.
    """
    if request is None:
        return f"{namespace}:no-request"
    # Получаем user_id точно так же, как в самом эндпоинте
    auth_header = request.headers.get("authorization")
    user_id = "anonymous" # по умолчанию
    if auth_header and auth_header.startswith("tma "):
        init_data = auth_header.split(" ", 1)[1]
        validated_data = is_valid_tma_data(init_data)
        if validated_data:
            try:
                user_info = json.loads(validated_data['user'])
                user_id = user_info['id']
            except (KeyError, json.JSONDecodeError):
                user_id = "invalid_user"

    # Создаем ключ из префикса, пути, ID пользователя и параметров запроса (например, номера страницы)
    return f"{namespace}:{request.url.path}:{user_id}:{request.query_params}"

@app.get("/api/feed/stream/") # Убрали user_id из URL, получим его из авторизации
async def stream_user_posts(
    session: AsyncSession = Depends(get_db_session),
    authorization: str | None = Header(None)
):
    # 1. Валидация пользователя (такая же, как в /api/feed/)
    if not authorization or not authorization.startswith("tma ") or not redis_subscriber:
        raise HTTPException(status_code=401, detail="Not authorized or Redis not configured")

    validated_data = is_valid_tma_data(authorization.split(" ", 1)[1])
    if not validated_data:
        raise HTTPException(status_code=403, detail="Invalid hash")
    
    try:
        user_id = json.loads(validated_data['user'])['id']
    except (KeyError, json.JSONDecodeError):
        raise HTTPException(status_code=403, detail="Invalid user data")

    # 2. Функция-генератор для SSE
    async def event_generator():
        # Подписываемся на личный канал уведомлений пользователя
        channel_name = f"user_feed:{user_id}"
        if not redis_subscriber:
            logging.error("Redis subscriber is not configured.")
            return
        pubsub = redis_subscriber.pubsub()
        await pubsub.subscribe(channel_name)
        
        try:
            # Бесконечный цикл для ожидания сообщений
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=15)
                if message:
                    post_id = int(message['data'])
                    
                    # Получаем свежий пост из БД
                    fresh_post_query = select(Post).options(selectinload(Post.channel)).where(Post.id == post_id)
                    post = (await session.execute(fresh_post_query)).scalars().first()
                    
                    if post:
                        # Формируем JSON в формате, который ожидает фронтенд
                        post_data = PostInFeed.model_validate(post).model_dump_json()
                        # Отправляем событие клиенту
                        yield f"data: {post_data}\n\n"
                
                # Проверка, что клиент еще подключен
                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            logging.info(f"Client {user_id} disconnected.")
            await pubsub.unsubscribe(channel_name)
            raise

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/api/subscriptions/", response_model=schemas.SubscriptionResponse)
@limiter.limit("30/minute")
async def get_user_subscriptions_api(
    request: Request,
    session: AsyncSession = Depends(get_db_session),
    authorization: str | None = Header(None)
):
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

    subscriptions = await db.get_user_subscriptions(session, user_id)
    return {"channels": subscriptions}

@app.get("/api/feed/", response_model=schemas.FeedResponse)
@cache(expire=120, key_builder=feed_key_builder)
@limiter.limit("30/minute")
async def get_feed_for_user(
    request: Request,  # ДОБАВИТЬ: нужен для limiter
    session: AsyncSession = Depends(get_db_session),
    page: int = Query(1, ge=1),
    authorization: str | None = Header(None)
):
    # ДОБАВЛЕНИЕ: логирование для отладки
    logging.info(f"Получен запрос на feed, page={page}, auth={'есть' if authorization else 'нет'}")
    
    if authorization is None or not authorization.startswith("tma "):
        logging.warning(f"Неверная авторизация: {authorization}")
        raise HTTPException(status_code=401, detail="Not authorized")

    init_data = authorization.split(" ", 1)[1]
    validated_data = is_valid_tma_data(init_data)

    if validated_data is None:
        logging.warning("Невалидные TMA данные")
        raise HTTPException(status_code=403, detail="Invalid hash")

    try:
        user_info = json.loads(validated_data['user'])
        user_id = user_info['id']
        logging.info(f"Пользователь определен: {user_id}")
    except (KeyError, json.JSONDecodeError):
        logging.error("Ошибка парсинга данных пользователя")
        raise HTTPException(status_code=403, detail="Invalid user data in initData")

    limit = 20
    offset = (page - 1) * limit
    feed = await db.get_user_feed(session=session, user_id=user_id, limit=limit, offset=offset)

    status = "ok"
    if len(feed) < limit:
        if page > 1:
            request_exists = await db.check_backfill_request_exists(session, user_id)
            if not request_exists:
                logging.info(f"Посты для пользователя {user_id} заканчиваются. Создаю заявку на дозагрузку.")
                await db.create_backfill_request(session, user_id)
            else:
                logging.info(f"Заявка на дозагрузку для пользователя {user_id} уже существует.")
            status = "backfilling"
        elif page == 1 and not feed:
             status = "empty"

    logging.info(f"Возвращаю {len(feed)} постов, статус: {status}")
    return {"posts": feed, "status": status}

# ДОБАВЛЕНИЕ: отдельный endpoint для проверки health
@app.get("/health")
async def health_check():
    return {"status": "ok", "message": "API работает"}

@app.post("/api/internal/channel-added")
async def notify_channel_added(
    user_id: int,
    channel_id: int,
    authorization: str | None = Header(None)
):
    """Внутренний endpoint для уведомления о добавлении канала"""
    if authorization != f"Bearer {BOT_TOKEN}":
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    # Можно добавить логику для немедленной обработки нового канала
    logging.info(f"Получено уведомление о добавлении канала {channel_id} для пользователя {user_id}")
    return {"status": "ok"}