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
from dotenv import load_dotenv
from telethon import TelegramClient, types
from telethon.errors import ChannelPrivateError, FloodWaitError
from telethon.tl.types import InputPeerChannel, DocumentEmpty
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from typing import Dict, Any

from database.engine import session_maker, create_db
from database.models import Channel, Post, BackfillRequest, Subscription
from telethon.sessions import StringSession
from datetime import datetime
from markdown_it import MarkdownIt
from linkify_it import LinkifyIt
from PIL import Image
from html import escape

# --- НАСТРОЙКА ---
md_parser = MarkdownIt('commonmark', {'breaks': True, 'html': False, 'linkify': True})
shutdown_event = asyncio.Event()
load_dotenv()
DB_URL_FOR_LOG = os.getenv("DATABASE_URL")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info(f"!!! WORKER STARTING WITH DATABASE_URL: {DB_URL_FOR_LOG} !!!")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_REGION = os.getenv("S3_REGION")
API_ID_STR = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("TELETHON_SESSION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REDIS_URL = os.getenv("REDIS_URL")

if not all([API_ID_STR, API_HASH, SESSION_STRING, S3_BUCKET_NAME, S3_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]):
    raise ValueError("Одна или несколько переменных окружения не установлены!")
if API_ID_STR is None: raise ValueError("API_ID environment variable is not set!")
API_ID = int(API_ID_STR)
POST_LIMIT = 10
SLEEP_TIME = 60
if SESSION_STRING is None: raise ValueError("TELETHON_SESSION environment variable is not set!")
if API_HASH is None: raise ValueError("API_HASH environment variable is not set!")

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=S3_REGION)

linkify = LinkifyIt()
md = MarkdownIt()
md.linkify = linkify

# --- THREAD-SAFE КЛАССЫ ИЗ РЕВЬЮ ---

class ThreadSafeEntityCache:
    """Thread-safe кеш для Telegram entities с автоочисткой"""
    
    def __init__(self, max_size: int = 1000):
        self._cache: Dict[str, Any] = {}
        self._access_times: Dict[str, float] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._max_size = max_size
        self._main_lock = asyncio.Lock()

    async def get_entity(self, cache_key: str, fetch_func):
        # Fast path - проверяем кеш без блокировки
        if cache_key in self._cache:
            async with self._main_lock:
                self._access_times[cache_key] = time.time()
            return self._cache[cache_key]
        
        # Получаем блокировку для этого ключа
        async with self._main_lock:
            if cache_key not in self._locks:
                self._locks[cache_key] = asyncio.Lock()
            lock = self._locks[cache_key]
        
        # Блокируемся только для этого ключа
        async with lock:
            # Double-check после получения блокировки
            if cache_key in self._cache:
                async with self._main_lock:
                    self._access_times[cache_key] = time.time()
                return self._cache[cache_key]
            
            await self._cleanup_if_needed()
            
            try:
                entity = await fetch_func()
                async with self._main_lock:
                    self._cache[cache_key] = entity
                    self._access_times[cache_key] = time.time()
                return entity
            except Exception as e:
                logging.error(f"Ошибка получения entity для {cache_key}: {e}")
                await worker_stats.increment_errors()
                return None

    async def _cleanup_if_needed(self):
        """Удаляет старые записи если кеш переполнен"""
        async with self._main_lock:
            if len(self._cache) >= self._max_size:
                # Удаляем 20% самых старых записей
                sorted_items = sorted(self._access_times.items(), key=lambda x: x[1])
                to_remove = sorted_items[:self._max_size // 5]
                
                for key, _ in to_remove:
                    self._cache.pop(key, None)
                    self._access_times.pop(key, None)
                    self._locks.pop(key, None)
                
                logging.info(f"Очищено {len(to_remove)} записей из entity cache")

class WorkerStats:
    def __init__(self):
        self.start_time = time.time()
        self.processed_channels = 0
        self.processed_posts = 0
        self.errors = 0
        self._lock = asyncio.Lock()

    async def increment_posts(self, count: int):
        async with self._lock:
            self.processed_posts += count

    async def increment_errors(self):
        async with self._lock:
            self.errors += 1
    
    async def set_channels(self, count: int):
        async with self._lock:
            self.processed_channels = count

    async def get_stats(self) -> dict:
        async with self._lock:
            return {
                'start_time': self.start_time,
                'processed_channels': self.processed_channels,
                'processed_posts': self.processed_posts,
                'errors': self.errors,
            }

class RedisPublisher:
    """Redis publisher с connection pooling"""
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self._pool = None
        self._lock = asyncio.Lock()
    
    async def get_connection(self):
        if self._pool is None:
            async with self._lock:
                if self._pool is None:
                    self._pool = aioredis.ConnectionPool.from_url(
                        self.redis_url,
                        max_connections=20,
                        retry_on_timeout=True
                    )
        return aioredis.Redis(connection_pool=self._pool)
    
    async def publish(self, channel: str, message: str):
        try:
            redis_client = await self.get_connection()
            await redis_client.publish(channel, message)
        except Exception as e:
            logging.error(f"Redis publish error: {e}")
            await worker_stats.increment_errors()

    async def close(self):
        if self._pool:
            await self._pool.disconnect()

# --- ГЛОБАЛЬНЫЕ INSTANCES ---
entity_cache = ThreadSafeEntityCache()
worker_stats = WorkerStats()
redis_publisher = RedisPublisher(REDIS_URL) if REDIS_URL else None
s3_semaphore = asyncio.Semaphore(10)

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def signal_handler(signum, frame):
    logging.info(f"Получен сигнал {signal.Signals(signum).name}. Начинаю корректное завершение...")
    shutdown_event.set()

def process_text(message_text: str | None) -> str | None:
    if not message_text: return None
    try:
        html_from_markdown = md_parser.renderInline(message_text)
        ALLOWED_TAGS = ['a', 'b', 'strong', 'i', 'em', 'pre', 'code', 'br', 's', 'u', 'blockquote']
        ALLOWED_ATTRIBUTES = {'a': ['href', 'title', 'target', 'rel']}
        safe_html = bleach.clean(html_from_markdown, tags=ALLOWED_TAGS, attributes=ALLOWED_ATTRIBUTES, strip=False)
        return safe_html.replace('<a href', '<a target="_blank" rel="noopener noreferrer" href')
    except Exception as e:
        logging.error(f"Критическая ошибка конвертации Markdown: {e}", exc_info=True)
        return escape(message_text).replace('\n', '<br>')

async def get_cached_entity(channel: Channel):
    cache_key = channel.username or str(channel.id)
    async def fetch_entity():
        entity = await client.get_entity(channel.username) if channel.username else await client.get_entity(channel.id)
        if isinstance(entity, list):
            entity = entity[0]
        return entity
    return await entity_cache.get_entity(cache_key, fetch_entity)

async def log_worker_stats():
    stats = await worker_stats.get_stats()
    uptime = time.time() - stats['start_time']
    logging.info(
        f"Статистика воркера: Uptime: {uptime/3600:.1f}ч, "
        f"Обработано в этом цикле: {stats['processed_channels']} каналов, "
        f"Ошибок (всего): {stats['errors']}"
    )

async def upload_avatar_to_s3(channel_entity) -> str | None:
    try:
        file_key = f"avatars/{channel_entity.id}.jpg"
        file_in_memory = io.BytesIO()
        await client.download_profile_photo(channel_entity, file=file_in_memory)
        if file_in_memory.getbuffer().nbytes == 0: return None
        file_in_memory.seek(0)
        s3_client.upload_fileobj(file_in_memory, S3_BUCKET_NAME, file_key, ExtraArgs={'ContentType': 'image/jpeg'})
        return f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{file_key}"
    except Exception as e:
        logging.error(f"Не удалось загрузить аватар для канала «{channel_entity.title}»: {e}")
        await worker_stats.increment_errors()
        return None

async def upload_media_to_s3(message: types.Message, channel_id: int) -> tuple[int, dict | None]:
    media_data, media_type = {}, None
    if isinstance(message.media, types.MessageMediaDocument) and getattr(message.media.document, 'size', 0) > 60 * 1024 * 1024:
        return message.id, None
    if isinstance(message.media, types.MessageMediaPhoto): media_type = 'photo'
    elif isinstance(message.media, types.MessageMediaDocument):
        mime_type = getattr(message.media.document, 'mime_type', '')
        if mime_type.startswith('video/'): media_type = 'video'
        elif mime_type.startswith('audio/'): media_type = 'audio'
        elif mime_type in ['image/gif', 'video/mp4']: media_type = 'gif'
    if not media_type: return message.id, None
    try:
        async with s3_semaphore:
            file_extension = '.webp' if media_type == 'photo' else '.gif' if media_type == 'gif' else os.path.splitext(next((attr.file_name for attr in getattr(message.media.document, 'attributes', []) if hasattr(attr, 'file_name')), ''))[1] or '.dat'
            content_type = 'image/webp' if media_type == 'photo' else 'image/gif' if media_type == 'gif' else getattr(message.media.document, 'mime_type', 'application/octet-stream')
            file_key = f"media/{channel_id}/{message.id}{file_extension}"
            file_in_memory = io.BytesIO()
            await client.download_media(message, file=file_in_memory)
            file_in_memory.seek(0)
            if media_type == 'photo':
                with Image.open(file_in_memory) as im:
                    im = im.convert("RGB")
                    output_buffer = io.BytesIO()
                    im.save(output_buffer, format="WEBP", quality=80)
                    output_buffer.seek(0)
                    file_in_memory = output_buffer
            s3_client.upload_fileobj(file_in_memory, S3_BUCKET_NAME, file_key, ExtraArgs={'ContentType': content_type})
            media_data["type"] = media_type
            media_data["url"] = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{file_key}"
            document = getattr(message.media, 'document', None)
            if media_type == 'video' and document and not isinstance(document, DocumentEmpty) and hasattr(document, 'thumbs') and document.thumbs:
                thumb_key = f"media/{channel_id}/{message.id}_thumb.webp"
                thumb_in_memory = io.BytesIO()
                await client.download_media(message, thumb=-1, file=thumb_in_memory)
                thumb_in_memory.seek(0)
                if thumb_in_memory.getbuffer().nbytes > 0:
                    with Image.open(thumb_in_memory) as im:
                        im = im.convert("RGB")
                        output_buffer = io.BytesIO()
                        im.save(output_buffer, format="WEBP", quality=75)
                        output_buffer.seek(0)
                        s3_client.upload_fileobj(output_buffer, S3_BUCKET_NAME, thumb_key, ExtraArgs={'ContentType': 'image/webp'})
                    media_data["thumbnail_url"] = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{thumb_key}"
        return message.id, media_data
    except Exception as e:
        logging.error(f"Ошибка загрузки медиа для поста {message.id}: {e}", exc_info=True)
        return message.id, None

async def create_post_dict(message: types.Message, channel_id: int) -> dict:
    reactions_data = []
    if message.reactions and message.reactions.results:
        for r in message.reactions.results:
            if r.count > 0:
                reaction_item = {"count": r.count}
                if hasattr(r.reaction, 'emoticon'): reaction_item["emoticon"] = r.reaction.emoticon
                elif hasattr(r.reaction, 'document_id'): reaction_item["document_id"] = r.reaction.document_id
                reactions_data.append(reaction_item)
    forward_data = None
    if message.fwd_from:
        try:
            if hasattr(message.fwd_from, 'from_id') and message.fwd_from.from_id:
                source_entity = await client.get_entity(message.fwd_from.from_id)
                from_name = getattr(source_entity, 'title', getattr(source_entity, 'first_name', 'Неизвестный источник'))
                username = getattr(source_entity, 'username', None)
                raw_channel_id = getattr(source_entity, 'id', None)
                channel_id_str = str(raw_channel_id)[4:] if raw_channel_id and str(raw_channel_id).startswith('-100') else str(raw_channel_id) if raw_channel_id else None
                forward_data = {"from_name": from_name, "username": username, "channel_id": channel_id_str}
            elif hasattr(message.fwd_from, 'from_name'):
                forward_data = {"from_name": message.fwd_from.from_name, "username": None, "channel_id": None}
        except Exception as e:
            logging.warning(f"Не удалось получить entity для репоста: {e}")
            forward_data = {"from_name": "Недоступный источник", "username": None, "channel_id": None}
    return {
        "channel_id": channel_id, "message_id": message.id, "date": message.date,
        "text": process_text(message.text), "grouped_id": message.grouped_id,
        "views": getattr(message, 'views', 0) or 0, "reactions": reactions_data,
        "forwarded_from": forward_data, "media": []
    }

async def fetch_posts_for_channel(channel: Channel, db_session: AsyncSession, post_limit: int = 20, offset_id: int = 0):
    channel_id, channel_title = channel.id, channel.title
    try:
        entity = await get_cached_entity(channel)
        if not entity: return

        posts_to_create_map, media_upload_params = {}, []
        async for message in client.iter_messages(entity, limit=post_limit, offset_id=offset_id):
            if not message or (not message.text and not message.media): continue
            posts_to_create_map[message.id] = await create_post_dict(message, channel_id)
            if message.media:
                media_upload_params.append((message, channel_id))

        if not posts_to_create_map:
            logging.info(f"Нет новых постов для «{channel_title}»")
            return

        if media_upload_params:
            media_tasks = [upload_media_to_s3(msg, cid) for msg, cid in media_upload_params]
            media_results = await asyncio.gather(*media_tasks, return_exceptions=True)
            for result in media_results:
                if isinstance(result, tuple):
                    msg_id, media_data = result
                    if media_data and msg_id in posts_to_create_map:
                        posts_to_create_map[msg_id]["media"] = [media_data]
        
        stmt = insert(Post).values(list(posts_to_create_map.values()))
        stmt = stmt.on_conflict_do_nothing(index_elements=['channel_id', 'message_id'])
        result = await db_session.execute(stmt)
        await db_session.commit()

        logging.info(f"Сохранено/проигнорировано {len(posts_to_create_map)} постов для «{channel_title}». Вставлено: {result.rowcount}")
        await worker_stats.increment_posts(result.rowcount)

    except Exception as e:
        logging.error(f"Критическая ошибка при обработке «{channel_title}»: {e}", exc_info=True)
        await worker_stats.increment_errors()
        await db_session.rollback()

async def process_channel_safely(channel: Channel, semaphore: asyncio.Semaphore, post_limit: int):
    async with semaphore:
        async with session_maker() as session:
            try:
                await fetch_posts_for_channel(channel, session, post_limit=post_limit)
            except Exception as e:
                logging.error(f"Не удалось обработать канал {channel.title} из-за критической ошибки: {e}", exc_info=True)
                await worker_stats.increment_errors()


async def listen_for_new_channel_tasks():
    """Слушает очередь в Redis и запускает обработку новых каналов."""
    if not redis_publisher:
        return

    logging.info("Воркер начал прослушивание задач на добавление каналов...")
    redis_client = await redis_publisher.get_connection()

    while not shutdown_event.is_set():
        try:
            # Блокирующая операция: ждем задачу из списка new_channel_tasks
            _, task_data_raw = await redis_client.brpop("new_channel_tasks")
            task_data = json.loads(task_data_raw)
            
            channel_id = task_data.get("channel_id")
            user_chat_id = task_data.get("user_chat_id")
            channel_title = task_data.get("channel_title")

            logging.info(f"Получена задача на обработку канала ID {channel_id}")

            async with session_maker() as session:
                channel = await session.get(Channel, channel_id)
                if channel:
                    # Выполняем всю тяжелую работу
                    entity = await get_cached_entity(channel)
                    if entity:
                        await upload_avatar_to_s3(entity) # Здесь используется основной клиент воркера
                    await fetch_posts_for_channel(channel, session, post_limit=20)

                    # Отправляем уведомление о завершении обратно боту
                    completion_message = {
                        "user_chat_id": user_chat_id,
                        "channel_title": channel_title
                    }
                    await redis_publisher.publish("task_completion_notifications", json.dumps(completion_message))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error(f"Ошибка в обработчике задач Redis: {e}", exc_info=True)
            await asyncio.sleep(5)

async def periodic_task_parallel():
    """Периодическая обработка всех каналов, у которых есть подписчики"""
    logging.info("Начинаю периодический сбор постов...")
    
    list_of_channels = []
    async with session_maker() as session:
        # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
        # Выбираем только те каналы, которые есть в таблице подписок.
        # distinct() гарантирует, что каждый канал будет в списке только один раз.
        query = (
            select(Channel)
            .join(Subscription, Channel.id == Subscription.channel_id)
            .distinct()
        )
        result = await session.execute(query)
        list_of_channels = result.scalars().all()
        # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    await worker_stats.set_channels(len(list_of_channels))

    if not list_of_channels:
        logging.info("В базе нет каналов для отслеживания.")
        return
        
    semaphore = asyncio.Semaphore(15)
    tasks = [process_channel_safely(channel, semaphore, POST_LIMIT) for channel in list_of_channels]
    
    if tasks:
        logging.info(f"Запускаю обработку {len(tasks)} каналов параллельно...")
        await asyncio.gather(*tasks)
    
    logging.info("Все каналы обработаны.")
    await log_worker_stats()

async def backfill_user_channels(user_id: int):
    logging.info(f"Начинаю дозагрузку старых постов для пользователя {user_id}...")
    async with session_maker() as db_session:
        from database.requests import get_user_subscriptions
        subscriptions = await get_user_subscriptions(db_session, user_id)
        if not subscriptions: return
        for channel in subscriptions:
            oldest_post_id = await db_session.scalar(select(Post.message_id).where(Post.channel_id == channel.id).order_by(Post.date.asc()).limit(1))
            offset_id = oldest_post_id or 0
            logging.info(f"Для канала «{channel.title}» ищем посты старше message_id {offset_id}.")
            await fetch_posts_for_channel(channel=channel, db_session=db_session, post_limit=20, offset_id=offset_id)
    logging.info(f"Дозагрузка для пользователя {user_id} завершена.")

async def process_backfill_requests():
    logging.info("Проверяю наличие заявок на дозагрузку...")
    async with session_maker() as db_session:
        requests = (await db_session.execute(select(BackfillRequest))).scalars().all()
        if not requests: return
        for req in requests:
            logging.info(f"Найдена заявка для пользователя {req.user_id}. Начинаю обработку.")
            try:
                await backfill_user_channels(req.user_id)
                await db_session.delete(req)
                await db_session.commit()
                logging.info(f"Заявка для пользователя {req.user_id} обработана и удалена.")
            except Exception as e:
                logging.error(f"Ошибка обработки заявки для пользователя {req.user_id}: {e}")
                await worker_stats.increment_errors()
                await db_session.rollback()

async def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    await create_db()
    logging.info("Worker: Database tables checked/created.")
    max_retries = 3
    retry_count = 0
    while retry_count < max_retries and not shutdown_event.is_set():
        try:
            async with client:
                logging.info("Клиент Telethon успешно запущен.")
                retry_count = 0 
                while not shutdown_event.is_set():
                    try:
                        # Запускаем обе задачи параллельно
                        await asyncio.gather(
                            periodic_task_parallel(),
                            process_backfill_requests(),
                            listen_for_new_channel_tasks() # <--- НАША НОВАЯ ЗАДАЧА
                        )
                        # Этот блок теперь не будет достигаться, так как gather будет работать вечно
                        # Но на случай если все задачи вдруг завершатся, оставляем логику сна
                        logging.info(f"Все задачи выполнены. Засыпаю на {SLEEP_TIME / 60:.1f} минут...")
                        await asyncio.wait_for(shutdown_event.wait(), timeout=SLEEP_TIME)
                    except asyncio.TimeoutError: continue
                    except Exception as e:
                        logging.error(f"Ошибка в основном рабочем цикле: {e}", exc_info=True)
                        await worker_stats.increment_errors()
                        await asyncio.sleep(10)
        except Exception as e:
            retry_count += 1
            logging.error(f"Критическая ошибка клиента Telethon (попытка {retry_count}/{max_retries}): {e}")
            await worker_stats.increment_errors()
            if retry_count < max_retries:
                await asyncio.sleep(30)
            else:
                shutdown_event.set()

    if redis_publisher:
        await redis_publisher.close()
    logging.info("Воркер корректно завершил работу.")