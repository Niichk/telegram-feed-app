import asyncio
import logging
import os
import boto3
import io
import time
import bleach
import signal
import json
import redis.asyncio as aioredis
from typing import Dict, Any
from dotenv import load_dotenv
from telethon import TelegramClient, types
from telethon.errors import ChannelPrivateError, FloodWaitError
from sqlalchemy import select, distinct
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from database.engine import session_maker, create_db
from database.models import Channel, Post, BackfillRequest, Subscription
from telethon.sessions import StringSession
from PIL import Image
from html import escape
from markdown_it import MarkdownIt

# --- НАСТРОЙКА (без изменений) ---
md_parser = MarkdownIt('commonmark', {'breaks': True, 'html': False, 'linkify': True})
shutdown_event = asyncio.Event()
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
S3_BUCKET_NAME, S3_REGION = os.getenv("S3_BUCKET_NAME"), os.getenv("S3_REGION")
API_ID_STR, API_HASH = os.getenv("API_ID"), os.getenv("API_HASH")
SESSION_STRING = os.getenv("TELETHON_SESSION")
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY")
REDIS_URL = os.getenv("REDIS_URL")
API_ID = int(API_ID_STR) if API_ID_STR else 0
POST_LIMIT, SLEEP_TIME = 20, 300
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH) if SESSION_STRING else None
s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=S3_REGION)

# --- THREAD-SAFE КЛАССЫ ---
class ThreadSafeEntityCache:
    def __init__(self, max_size: int = 500):
        self._cache: Dict[str, Any] = {}
        self._access_times: Dict[str, float] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._max_size = max_size
        self._main_lock = asyncio.Lock()
    async def get_entity(self, cache_key: str, fetch_func):
        if cache_key in self._cache:
            async with self._main_lock: self._access_times[cache_key] = time.time()
            return self._cache[cache_key]
        async with self._main_lock:
            if cache_key not in self._locks: self._locks[cache_key] = asyncio.Lock()
            lock = self._locks[cache_key]
        async with lock:
            if cache_key in self._cache:
                async with self._main_lock: self._access_times[cache_key] = time.time()
                return self._cache[cache_key]
            await self._cleanup_if_needed()
            try:
                entity = await fetch_func()
                async with self._main_lock:
                    self._cache[cache_key], self._access_times[cache_key] = entity, time.time()
                return entity
            except Exception as e:
                logging.error(f"Ошибка получения entity для {cache_key}: {e}")
                return None
    async def _cleanup_if_needed(self):
        async with self._main_lock:
            if len(self._cache) >= self._max_size:
                sorted_items = sorted(self._access_times.items(), key=lambda x: x[1])
                to_remove = sorted_items[:self._max_size // 5]
                for key, _ in to_remove:
                    self._cache.pop(key, None); self._access_times.pop(key, None); self._locks.pop(key, None)
                logging.info(f"Очищено {len(to_remove)} записей из entity cache")
class WorkerStats:
    def __init__(self):
        self.start_time, self.processed_channels, self.processed_posts, self.errors = time.time(), 0, 0, 0
        self._lock = asyncio.Lock()
    async def increment_posts(self, count: int):
        async with self._lock: self.processed_posts += count
    async def increment_errors(self):
        async with self._lock: self.errors += 1
    async def set_channels(self, count: int):
        async with self._lock: self.processed_channels = count
    async def get_stats(self) -> dict:
        async with self._lock: return self.__dict__
class RedisPublisher:
    def __init__(self, redis_url: str):
        self.redis_url, self._pool, self._lock = redis_url, None, asyncio.Lock()
    async def get_connection(self):
        if self._pool is None:
            async with self._lock:
                if self._pool is None:
                    self._pool = aioredis.ConnectionPool.from_url(self.redis_url, max_connections=20, retry_on_timeout=True)
        return aioredis.Redis(connection_pool=self._pool)
    async def publish(self, channel: str, message: str):
        try:
            redis_client = await self.get_connection()
            await redis_client.publish(channel, message)
        except Exception as e: logging.error(f"Redis publish error: {e}")
    async def close(self):
        if self._pool: await self._pool.disconnect()

# --- ГЛОБАЛЬНЫЕ INSTANCES ---
entity_cache = ThreadSafeEntityCache()
worker_stats = WorkerStats()
redis_publisher = RedisPublisher(REDIS_URL) if REDIS_URL else None
s3_semaphore = asyncio.Semaphore(10)

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def signal_handler(signum, frame):
    shutdown_event.set()
def process_text(text: str | None) -> str | None:
    if not text: return None
    try:
        html = md_parser.renderInline(text)
        return bleach.clean(html.replace('<a href', '<a target="_blank" rel="noopener noreferrer" href'), tags=['a', 'b', 'strong', 'i', 'em', 'pre', 'code', 'br', 's', 'u', 'blockquote'], attributes={'a': ['href', 'title', 'target', 'rel']})
    except Exception as e:
        logging.error(f"Ошибка обработки текста: {e}")
        return escape(text or "").replace('\n', '<br>')
async def get_cached_entity(channel: Channel):
    async def fetcher():
        entity = await client.get_entity(channel.username or channel.id)
        return entity[0] if isinstance(entity, list) else entity
    return await entity_cache.get_entity(str(channel.id), fetcher)
async def upload_avatar_to_s3(telethon_client: TelegramClient, channel_entity) -> str | None:
    try:
        file_key, file_in_memory = f"avatars/{channel_entity.id}.jpg", io.BytesIO()
        await telethon_client.download_profile_photo(channel_entity, file=file_in_memory)
        if file_in_memory.getbuffer().nbytes == 0: return None
        file_in_memory.seek(0)
        s3_client.upload_fileobj(file_in_memory, S3_BUCKET_NAME, file_key, ExtraArgs={'ContentType': 'image/jpeg'})
        return f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{file_key}"
    except Exception as e:
        logging.error(f"Не удалось загрузить аватар для «{channel_entity.title}»: {e}")
        await worker_stats.increment_errors(); return None

# --- ОСНОВНЫЕ ФУНКЦИИ ВОРКЕРА ---
async def fetch_posts_for_channel(channel: Channel, db_session: AsyncSession, post_limit: int):
    try:
        entity = await get_cached_entity(channel)
        if not entity: return
        posts_data = [await create_post_dict(msg, channel.id) async for msg in client.iter_messages(entity, limit=post_limit) if msg and (msg.text or msg.media)]
        if not posts_data:
            logging.info(f"Нет новых постов для «{channel.title}»")
            return
        stmt = insert(Post).values(posts_data).on_conflict_do_nothing(index_elements=['channel_id', 'message_id'])
        result = await db_session.execute(stmt)
        await db_session.commit()
        logging.info(f"Для «{channel.title}» проверено {len(posts_data)} постов. Вставлено: {result.rowcount}")
        await worker_stats.increment_posts(result.rowcount)
    except Exception as e:
        logging.error(f"Критическая ошибка при обработке «{channel.title}»: {e}", exc_info=True)
        await worker_stats.increment_errors(); await db_session.rollback()
async def create_post_dict(message: types.Message, channel_id: int) -> dict:
    # Эта функция больше не скачивает медиа, а создает "скелет" поста
    reactions = [{'count': r.count, 'emoticon': getattr(r.reaction, 'emoticon', None), 'document_id': getattr(r.reaction, 'document_id', None)} for r in (message.reactions.results if message.reactions else []) if r.count > 0]
    return {"channel_id": channel_id, "message_id": message.id, "date": message.date, "text": process_text(message.text), "media": [], "views": message.views or 0, "reactions": reactions}
async def process_channel_safely(channel: Channel, semaphore: asyncio.Semaphore):
    async with semaphore:
        async with session_maker() as session:
            await fetch_posts_for_channel(channel, session, POST_LIMIT)
async def periodic_tasks_runner():
    while not shutdown_event.is_set():
        logging.info("Начинаю периодический сбор постов...")
        async with session_maker() as session:
            query = select(Channel).join(Subscription).distinct()
            channels = (await session.execute(query)).scalars().all()
        await worker_stats.set_channels(len(channels))
        if channels:
            semaphore = asyncio.Semaphore(15)
            await asyncio.gather(*[process_channel_safely(ch, semaphore) for ch in channels])
        logging.info("Периодический сбор завершен.")
        await asyncio.sleep(SLEEP_TIME)
async def listen_for_new_channel_tasks():
    if not redis_publisher: return
    logging.info("Воркер слушает задачи на добавление каналов...")
    redis_client = await redis_publisher.get_connection()
    while not shutdown_event.is_set():
        try:
            _, task_raw = await redis_client.brpop("new_channel_tasks", timeout=1)
            if not task_raw: continue
            task = json.loads(task_raw)
            channel_id, chat_id, title = task.get("channel_id"), task.get("user_chat_id"), task.get("channel_title")
            logging.info(f"Получена задача на обработку канала ID {channel_id}")
            async with session_maker() as session:
                channel = await session.get(Channel, channel_id)
                if channel:
                    entity = await get_cached_entity(channel)
                    if entity: await upload_avatar_to_s3(client, entity)
                    await fetch_posts_for_channel(channel, session, POST_LIMIT)
                    completion = {"user_chat_id": chat_id, "channel_title": title}
                    await redis_publisher.publish("task_completion_notifications", json.dumps(completion))
        except (asyncio.CancelledError, asyncio.TimeoutError): break
        except Exception as e: logging.error(f"Ошибка в Redis-слушателе: {e}", exc_info=True)

# --- ГЛАВНАЯ ФУНКЦИЯ ВОРКЕРА ---
async def main():
    signal.signal(signal.SIGTERM, signal_handler); signal.signal(signal.SIGINT, signal_handler)
    await create_db()
    logging.info("Воркер запущен.")
    if not client:
        logging.critical("Telethon клиент не настроен! Воркер не может работать."); return
    async with client:
        logging.info("Клиент Telethon успешно запущен.")
        await asyncio.gather(
            periodic_tasks_runner(),
            listen_for_new_channel_tasks()
        )
    if redis_publisher: await redis_publisher.close()
    logging.info("Воркер корректно завершил работу.")

if __name__ == "__main__":
    if any(v is None for v in [API_ID_STR, API_HASH, SESSION_STRING]):
        logging.error("Не заданы переменные окружения для Telethon. Воркер не может запуститься.")
    else:
        try: asyncio.run(main())
        except (KeyboardInterrupt, SystemExit): logging.info("Приложение остановлено.")