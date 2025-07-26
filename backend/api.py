import hmac
import hashlib
import os
import json
import logging
import asyncio
import time
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, Depends, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import AsyncGenerator, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from urllib.parse import parse_qsl, unquote

from database import requests as db
from database import schemas
from database.engine import create_db, session_maker
from database.models import Post, Subscription # Убедимся, что Subscription импортирована
from database.schemas import PostInFeed
# from worker import backfill_user_channels

from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi.responses import StreamingResponse, Response
from fastapi_cache.decorator import cache
from redis import asyncio as aioredis

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded


load_dotenv()

# --- КОНФИГУРАЦИЯ ---
limiter = Limiter(key_func=lambda request: "global")
BOT_TOKEN = os.getenv("API_TOKEN")
REDIS_URL = os.getenv("REDIS_URL") or os.getenv("REDIS_PUBLIC_URL")
FRONTEND_URL = os.getenv("FRONTEND_URL")
IS_DEVELOPMENT = os.getenv("ENVIRONMENT") == "development"
PAGE_SIZE = 20

# --- ИНИЦИАЛИЗАЦИЯ APP ---
app = FastAPI(title="Feed Reader API")

# --- ОБРАБОТЧИКИ ОШИБОК ---
def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    return _rate_limit_exceeded_handler(request, exc) # type: ignore

app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler) # type: ignore

# --- MIDDLEWARE (CORS) ---
allowed_origins = []
if FRONTEND_URL:
    allowed_origins.append(FRONTEND_URL)
if IS_DEVELOPMENT:
    allowed_origins.extend(["http://localhost:5173", "http://127.0.0.1:5173"])

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins or ["*"], # Если список пуст, разрешаем все для локальной отладки
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ АВТОРИЗАЦИИ ---
def is_valid_tma_data(init_data: str) -> Optional[dict]:
    if not BOT_TOKEN:
        logging.critical("CRITICAL: BOT_TOKEN is not configured!")
        return None
    try:
        # Декодируем URL-кодированную строку
        decoded_init_data = unquote(init_data)
        parsed_data = dict(parse_qsl(decoded_init_data))
        tma_hash = parsed_data.pop('hash')
        data_check_string = "\n".join(f"{key}={value}" for key, value in sorted(parsed_data.items()))
        
        secret_key = hmac.new("WebAppData".encode(), BOT_TOKEN.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        
        if calculated_hash == tma_hash:
            return parsed_data
        return None
    except Exception as e:
        logging.error(f"TMA validation error: {e}")
        return None

async def DYNAMIC_CACHE_CONTROL(response: Response):
    """
    Эта зависимость добавляет заголовки, которые запрещают
    браузеру/клиенту кэшировать ответ. Это заставляет его
    всегда обращаться к нашему серверу за свежими данными.
    """
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'

def get_user_id_from_request(request: Request) -> str:
    # ИСПРАВЛЕНИЕ: Игнорируем OPTIONS запросы в rate limiter
    if request.method == "OPTIONS":
        return get_remote_address(request) # Для OPTIONS запросов используем IP

    # Пытаемся получить initData из заголовка
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.startswith("tma "):
        init_data = auth_header.split(" ", 1)[1]
        validated_data = is_valid_tma_data(init_data)
        if validated_data and 'user' in validated_data:
            try:
                user_info = json.loads(validated_data['user'])
                return str(user_info['id'])
            except (KeyError, json.JSONDecodeError):
                pass
                
    return get_remote_address(request)

# Переопределяем key_func у limiter после объявления функции
app.state.limiter = Limiter(key_func=get_user_id_from_request)

# --- ЗАВИСИМОСТИ (Dependencies) ---
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    async with session_maker() as session:
        yield session

async def get_current_user_id(
    authorization: Optional[str] = Header(None),
    # ИСПРАВЛЕНИЕ: Добавляем возможность получать авторизацию из query-параметра
    auth_query: Optional[str] = Query(None, alias="authorization")
) -> int:
    # Выбираем источник данных: сначала заголовок, потом query-параметр
    auth_string = authorization or auth_query
    
    if not auth_string or not auth_string.startswith("tma "):
        raise HTTPException(status_code=401, detail="Not authorized")

    init_data = auth_string.split(" ", 1)[1]
    validated_data = is_valid_tma_data(init_data)

    if not validated_data or 'user' not in validated_data:
        raise HTTPException(status_code=403, detail="Invalid hash or user data")

    try:
        user_info = json.loads(validated_data['user'])
        return user_info['id']
    except (KeyError, json.JSONDecodeError):
        raise HTTPException(status_code=403, detail="Invalid user data format")


# --- STARTUP EVENT ---
@app.on_event("startup")
async def on_startup():
    await create_db()
    if REDIS_URL:
        # ГЛАВНЫЙ ФИКС: Убираем `decode_responses=True`.
        # Библиотека fastapi-cache ожидает байты, а не строки, от Redis.
        redis_client = aioredis.from_url(REDIS_URL, encoding="utf8")
        FastAPICache.init(RedisBackend(redis_client), prefix="fastapi-cache")
        logging.info("FastAPI-Cache with Redis backend is initialized correctly.")


# --- КЛЮЧ КЭШИРОВАНИЯ ---
def feed_key_builder(
    func,
    namespace: str = "",
    *,
    request: Optional[Request] = None,
    response: Optional[Response] = None,
    args=(),
    kwargs={},
):
    """
    Создает ключ кеша, который зависит от ПОЛНОЙ строки авторизации (initData).
    Это гарантирует, что для разных пользователей (и даже разных сессий
    одного пользователя) будут созданы уникальные ключи, предотвращая
    пересечение кеша между аккаунтами.
    """
    if request is None:
        # Fallback для случаев, когда request недоступен
        return f"{namespace}:{time.time()}"

    # Сначала пытаемся получить из заголовка, потом из query-параметра
    auth_header = request.headers.get("authorization")
    auth_query = request.query_params.get("authorization")
    auth_string = auth_header or auth_query

    # Если данных для авторизации нет, мы не можем создать пользовательский ключ
    if not auth_string:
        return f"{namespace}:{request.url.path}?{request.query_params}:{time.time()}"

    # Используем криптографический хеш от всей строки авторизации.
    # Это создает уникальный и безопасный идентификатор для ключа кеша.
    unique_auth_hash = hashlib.sha256(auth_string.encode('utf-8')).hexdigest()

    page = kwargs.get("page", 1) # Получаем номер страницы из аргументов функции

    # Новый, безопасный ключ кеша
    cache_key = f"{namespace}:{request.url.path}:{unique_auth_hash}:page={page}"
    return cache_key


# --- ЭНДПОИНТЫ ---

@app.get("/api/feed/stream/")
async def stream_user_posts(
    user_id: int = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_db_session)
):
    if not REDIS_URL:
         raise HTTPException(status_code=503, detail="Stream service is not configured")
    
    redis_subscriber = aioredis.from_url(REDIS_URL, decode_responses=True)

    async def event_generator():
        channel_name = f"user_feed:{user_id}"
        pubsub = redis_subscriber.pubsub()
        await pubsub.subscribe(channel_name)
        
        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=15)
                if message and message.get("type") == "message":
                    post_id_str = message.get("data")
                    if post_id_str and post_id_str.isdigit():
                        # Загружаем пост в новой сессии, чтобы избежать проблем с состоянием
                        async with session_maker() as post_session:
                           post = await post_session.get(Post, int(post_id_str), options=[selectinload(Post.channel)])
                        if post:
                            post_data = PostInFeed.model_validate(post).model_dump_json()
                            yield f"data: {post_data}\n\n"
                
                yield f"event: heartbeat\ndata: \n\n"
                await asyncio.sleep(15)

        except asyncio.CancelledError:
            logging.info(f"Client {user_id} disconnected from stream.")
            await pubsub.unsubscribe(channel_name)
            await redis_subscriber.close()
            raise

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/feed/", response_model=schemas.FeedResponse, dependencies=[Depends(DYNAMIC_CACHE_CONTROL)])
@cache(expire=120, key_builder=feed_key_builder)
@limiter.limit("30/minute")
async def get_feed_for_user(
    request: Request,
    user_id: int = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_db_session),
    page: int = Query(1, ge=1)
):
    # Тело функции остается таким же, как в последней версии
    offset = (page - 1) * PAGE_SIZE
    feed = await db.get_user_feed(session=session, user_id=user_id, limit=PAGE_SIZE, offset=offset)
    has_posts = bool(feed)

    if page == 1 and not has_posts:
        subscriptions = await db.get_user_subscriptions(session=session, user_id=user_id)
        if subscriptions:
            if not await db.check_backfill_request_exists(session, user_id):
                 await db.create_backfill_request(session, user_id)
            return {"posts": [], "status": "backfilling"}
        else:
            return {"posts": [], "status": "empty"}

    status = "backfilling" if len(feed) < PAGE_SIZE else "ok"
    return {"posts": feed, "status": status}


@app.get("/health")
async def health_check():
    return {"status": "ok"}