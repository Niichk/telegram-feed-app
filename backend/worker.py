import asyncio
import logging
import os
import sys
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
from sqlalchemy import select, distinct, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from collections import defaultdict

from database.engine import session_maker, create_db
from database.models import Channel, Post, BackfillRequest, Subscription
from telethon.sessions import StringSession
from PIL import Image
from html import escape
from markdown_it import MarkdownIt

print("🔄 WORKER.PY ЗАГРУЖАЕТСЯ...")
print(f"Python version: {sys.version}")

try:
    # Загрузка переменных окружения
    print("📋 Загружаю переменные окружения...")
    load_dotenv()
    
    # Проверка критических переменных
    print("🔍 Проверяю переменные окружения:")
    
    API_ID_STR = os.getenv("API_ID")
    API_HASH = os.getenv("API_HASH") 
    SESSION_STRING = os.getenv("TELETHON_SESSION")
    REDIS_URL = os.getenv("REDIS_URL") or os.getenv("REDIS_PUBLIC_URL")
    
    print(f"  API_ID: {'✅' if API_ID_STR else '❌'}")
    print(f"  API_HASH: {'✅' if API_HASH else '❌'}")
    print(f"  SESSION_STRING: {'✅' if SESSION_STRING else '❌'}")
    print(f"  REDIS_URL: {'✅' if REDIS_URL else '❌'}")
    
    # Парсинг API_ID
    try:
        API_ID = int(API_ID_STR) if API_ID_STR else None
        print(f"  API_ID parsed: {'✅' if API_ID else '❌'}")
    except Exception as e:
        print(f"  ❌ API_ID parse error: {e}")
        API_ID = None
    
    print("🔧 Инициализирую компоненты...")
    
    # Инициализация компонентов
    try:
        client = TelegramClient(StringSession(SESSION_STRING or ""), API_ID, API_HASH) if (SESSION_STRING and API_ID is not None and API_HASH) else None
        print(f"  Telethon client: {'✅' if client else '❌'}")
    except Exception as e:
        print(f"  ❌ Telethon client error: {e}")
        client = None

    class RedisPublisher: # type: ignore
        def __init__(self, redis_url: str):
            self.redis_url, self._pool, self._lock = redis_url, None, asyncio.Lock()
        async def get_connection(self):
            if self._pool is None:
                async with self._lock:
                    if self._pool is None: self._pool = aioredis.ConnectionPool.from_url(self.redis_url, max_connections=20, retry_on_timeout=True)
            return aioredis.Redis(connection_pool=self._pool)
        async def publish(self, channel: str, message: str):
            try: await (await self.get_connection()).publish(channel, message)
            except Exception as e: logging.error(f"Redis publish error: {e}")
        async def close(self):
            if self._pool: await self._pool.disconnect()

    try:
        redis_publisher = RedisPublisher(REDIS_URL) if REDIS_URL else None
        print(f"  Redis publisher: {'✅' if redis_publisher else '❌'}")
    except Exception as e:
        print(f"  ❌ Redis publisher error: {e}")
        redis_publisher = None
    
    print("✅ worker.py успешно загружен!")
    
except Exception as e:
    print(f"💥 КРИТИЧЕСКАЯ ОШИБКА ПРИ ЗАГРУЗКЕ worker.py: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# --- НАСТРОЙКА ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

# --- КОНФИГУРАЦИЯ ---
API_ID_STR, API_HASH, SESSION_STRING = os.getenv("API_ID"), os.getenv("API_HASH"), os.getenv("TELETHON_SESSION")
S3_BUCKET_NAME, S3_REGION = os.getenv("S3_BUCKET_NAME"), os.getenv("S3_REGION")
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY")
REDIS_URL = os.getenv("REDIS_URL") or os.getenv("REDIS_PUBLIC_URL")
API_ID = int(API_ID_STR) if API_ID_STR else None
POST_LIMIT, SLEEP_TIME = 20, 300

md_parser = MarkdownIt('commonmark', {'breaks': True, 'html': False, 'linkify': True})
shutdown_event = asyncio.Event()

client = TelegramClient(StringSession(SESSION_STRING or ""), API_ID, API_HASH) if (SESSION_STRING and API_ID is not None and API_HASH) else None
s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=S3_REGION)

# --- THREAD-SAFE КЛАССЫ (ИСПРАВЛЕНО) ---
class ThreadSafeEntityCache:
    def __init__(self, max_size: int = 500):
        self._cache: Dict[str, Any] = {}
        self._access_times: Dict[str, float] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._max_size = max_size
        self._main_lock = asyncio.Lock()
    async def get_entity(self, cache_key: str, fetch_func):
        if cache_key in self._cache:
            async with self._main_lock:
                self._access_times[cache_key] = time.time()
                if len(self._cache) >= self._max_size:
                    await self._cleanup_if_needed()
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
                async with self._main_lock: self._cache[cache_key], self._access_times[cache_key] = entity, time.time()
                return entity
            except Exception as e:
                logging.error(f"Ошибка получения entity для {cache_key}: {e}")
                return None
    async def _cleanup_if_needed(self):
        # Эта функция вызывается под main_lock, поэтому дополнительная блокировка не нужна
        if len(self._cache) >= self._max_size:
            to_remove = sorted(self._access_times.items(), key=lambda x: x[1])[:self._max_size // 5]
            for key, _ in to_remove:
                self._cache.pop(key, None); self._access_times.pop(key, None); self._locks.pop(key, None)
class WorkerStats:
    def __init__(self):
        self.start_time = time.time()
        self.processed_channels, self.processed_posts, self.errors = 0, 0, 0
        self._lock = asyncio.Lock()
    async def increment_posts(self, count: int):
        async with self._lock: self.processed_posts += count
    async def increment_errors(self, count: int = 1):
        async with self._lock: self.errors += count
    async def set_channels(self, count: int):
        async with self._lock: self.processed_channels = count
    async def get_stats(self) -> dict:
        async with self._lock:
            return {'start_time': self.start_time, 'processed_channels': self.processed_channels, 'processed_posts': self.processed_posts, 'errors': self.errors}
class RedisPublisher:
    def __init__(self, redis_url: str):
        self.redis_url, self._pool, self._lock = redis_url, None, asyncio.Lock()
    async def get_connection(self):
        if self._pool is None:
            async with self._lock:
                if self._pool is None: self._pool = aioredis.ConnectionPool.from_url(self.redis_url, max_connections=20, retry_on_timeout=True)
        return aioredis.Redis(connection_pool=self._pool)
    async def publish(self, channel: str, message: str):
        try: await (await self.get_connection()).publish(channel, message)
        except Exception as e: logging.error(f"Redis publish error: {e}")
    async def close(self):
        if self._pool: await self._pool.disconnect()

# --- ГЛОБАЛЬНЫЕ INSTANCES ---
entity_cache = ThreadSafeEntityCache()
worker_stats = WorkerStats()
redis_publisher = RedisPublisher(REDIS_URL) if REDIS_URL else None
s3_semaphore = asyncio.Semaphore(10)

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def signal_handler(signum, frame): shutdown_event.set()
def process_text(text: str | None) -> str | None:
    if not text: return None
    try:
        html = md_parser.renderInline(text)
        return bleach.clean(html.replace('<a href', '<a target="_blank" rel="noopener noreferrer" href'), tags=['a', 'b', 'strong', 'i', 'em', 'pre', 'code', 'br', 's', 'u', 'blockquote'], attributes={'a': ['href', 'title', 'target', 'rel']})
    except Exception: return escape(text or "").replace('\n', '<br>')
async def get_cached_entity(channel: Channel):
    async def fetcher():
        if client is None:
            logging.error("Telethon client is not initialized.")
            return None
        entity = await client.get_entity(channel.username or int(channel.id))
        return entity[0] if isinstance(entity, list) else entity
    return await entity_cache.get_entity(str(channel.id), fetcher)
async def upload_avatar_to_s3(telethon_client: TelegramClient, channel_entity) -> str | None:
    try:
        file_key, file_in_memory = f"avatars/{channel_entity.id}.jpg", io.BytesIO()
        await telethon_client.download_profile_photo(channel_entity, file=file_in_memory)
        if file_in_memory.getbuffer().nbytes == 0: return None
        file_in_memory.seek(0)
        s3_client.upload_fileobj(file_in_memory, S3_BUCKET_NAME, file_key, ExtraArgs={'ContentType': 'image/jpeg', 'ACL': 'public-read'})
        return f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{file_key}"
    except Exception as e:
        await worker_stats.increment_errors()
        return None

# --- ОСНОВНЫЕ ФУНКЦИИ ВОРКЕРА ---
async def upload_media_to_s3(message: types.Message, channel_id: int) -> tuple[int, dict | None]:
    # ДОБАВИТЬ: Проверка client
    if client is None:
        logging.error("Telethon client не инициализирован!")
        return message.id, None
        
    media_data, media_type = {}, None
    
    # ИСПРАВЛЕНИЕ: Безопасная проверка document
    if isinstance(message.media, types.MessageMediaPhoto):
        media_type = 'photo'
    elif isinstance(message.media, types.MessageMediaDocument):
        doc = message.media.document
        if not doc:
            return message.id, None
        if getattr(doc, 'size', 0) > 60 * 1024 * 1024:
            return message.id, None # Пропускаем файлы больше 60MB

        mime_type = getattr(doc, 'mime_type', '').lower()
        is_sticker = any(isinstance(attr, types.DocumentAttributeSticker) for attr in getattr(doc, 'attributes', []))

        if is_sticker:
            media_type = 'sticker'
        elif mime_type.startswith('audio/'):
            media_type = 'audio'
        elif mime_type.startswith('video/'):
            media_type = 'video'
        elif mime_type == 'image/gif':
            media_type = 'gif'

    if not media_type: 
        return message.id, None
        
    try:
        async with s3_semaphore:
            ext = '.dat'
            content_type = 'application/octet-stream'

            # Определяем расширение и тип контента
            if media_type == 'photo':
                ext, content_type = '.webp', 'image/webp'
            elif media_type == 'gif':
                ext, content_type = '.gif', 'image/gif'
            elif media_type == 'sticker':
                ext, content_type = '.webp', 'image/webp'
            elif media_type in ('video', 'audio'):
                if message.media.document:
                    doc = message.media.document
                    content_type = getattr(doc, 'mime_type', content_type)
                    file_name_attr = next((attr for attr in getattr(doc, 'attributes', []) if hasattr(attr, 'file_name')), None)
                    if file_name_attr:
                        file_ext = os.path.splitext(file_name_attr.file_name)[1]
                        if file_ext:
                            ext = file_ext

            key = f"media/{channel_id}/{message.id}{ext}"
            mem_file = io.BytesIO()
            
            await client.download_media(message, file=mem_file)
            mem_file.seek(0)
            
            if media_type == 'photo':
                with Image.open(mem_file) as im: 
                    im = im.convert("RGB")
                    buf = io.BytesIO()
                    im.save(buf, format="WEBP", quality=80)
                    buf.seek(0)
                    mem_file = buf
                    
            s3_client.upload_fileobj(mem_file, S3_BUCKET_NAME, key, ExtraArgs={'ContentType': content_type, 'ACL': 'public-read'} )
            media_data["type"] = media_type
            media_data["url"] = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{key}"
            
            # ✅ ИСПРАВЛЕНИЕ: Безопасная обработка thumbnail для видео
            if media_type == 'video' and isinstance(message.media, types.MessageMediaDocument):
                document = message.media.document
                if document and hasattr(document, 'thumbs') and document.thumbs: # type: ignore
                    try:
                        thumb_key = f"media/{channel_id}/{message.id}_thumb.webp"
                        thumb_in_memory = io.BytesIO()
                        
                        # Скачиваем thumbnail (последний = лучшее качество)
                        await client.download_media(message, thumb=-1, file=thumb_in_memory)
                        thumb_in_memory.seek(0)
                        
                        if thumb_in_memory.getbuffer().nbytes > 0:
                            # Конвертируем в WebP
                            with Image.open(thumb_in_memory) as im:
                                im = im.convert("RGB")
                                output_buffer = io.BytesIO()
                                im.save(output_buffer, format="WEBP", quality=75)
                                output_buffer.seek(0)
                                
                            # Загружаем thumbnail в S3
                            s3_client.upload_fileobj(
                                output_buffer, 
                                S3_BUCKET_NAME, 
                                thumb_key, 
                                ExtraArgs={'ContentType': content_type, 'ACL': 'public-read'} 
                            )
                            
                            media_data["thumbnail_url"] = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{thumb_key}"
                            logging.debug(f"✅ Thumbnail загружен для видео {message.id}")
                        
                    except Exception as thumb_error:
                        logging.warning(f"⚠️ Не удалось загрузить thumbnail для видео {message.id}: {thumb_error}")
                        # Продолжаем без thumbnail
            
        return message.id, media_data
        
    except Exception as e:
        logging.error(f"Ошибка загрузки медиа для поста {message.id}: {e}", exc_info=True)
        return message.id, None
    
async def create_post_dict(message: types.Message, channel_id: int) -> dict:
    # Обработка реакций
    reactions = [
        {
            'count': r.count, 
            'emoticon': getattr(r.reaction, 'emoticon', None), 
            'document_id': getattr(r.reaction, 'document_id', None)
        } 
        for r in (message.reactions.results if message.reactions else []) 
        if r.count > 0
    ]
    
    # Обработка forwarded_from
    forward_data = None
    if message.fwd_from:
        try:
            if hasattr(message.fwd_from, 'from_id') and message.fwd_from.from_id:
                if client is not None:
                    source_entity = await client.get_entity(message.fwd_from.from_id)
                    from_name = getattr(source_entity, 'title', getattr(source_entity, 'first_name', 'Неизвестный источник'))
                    username = getattr(source_entity, 'username', None)
                    raw_channel_id = getattr(source_entity, 'id', None)
                    channel_id_str = (
                        str(raw_channel_id)[4:] if raw_channel_id and str(raw_channel_id).startswith('-100') 
                        else str(raw_channel_id) if raw_channel_id else None
                    )
                    forward_data = {"from_name": from_name, "username": username, "channel_id": channel_id_str}
                else:
                    forward_data = {"from_name": "Недоступный источник", "username": None, "channel_id": None}
            elif hasattr(message.fwd_from, 'from_name'):
                forward_data = {"from_name": message.fwd_from.from_name, "username": None, "channel_id": None}
        except Exception as e:
            logging.warning(f"Не удалось получить entity для репоста: {e}")
            forward_data = {"from_name": "Недоступный источник", "username": None, "channel_id": None}
    
    return {
        "channel_id": channel_id,
        "message_id": message.id,
        "date": message.date,
        "text": process_text(getattr(message, 'text', None)),  # ✅ ИСПРАВЛЕНИЕ: Безопасное получение text
        "grouped_id": getattr(message, 'grouped_id', None),  # ✅ ИСПРАВЛЕНИЕ: Безопасное получение grouped_id
        "views": getattr(message, 'views', 0) or 0,  # ✅ ИСПРАВЛЕНИЕ: Безопасное получение views
        "reactions": reactions,
        "forwarded_from": forward_data,
        "media": []
    }

async def fetch_posts_for_channel(channel: Channel, db_session: AsyncSession, post_limit: int):
    try:
        if client is None:
            logging.error("Telethon client не инициализирован!")
            return

        entity = await get_cached_entity(channel)
        if not entity: 
            return

        # Шаг 1: Получаем последние сообщения
        messages = [
            msg async for msg in client.iter_messages(entity, limit=post_limit) 
            if msg and (getattr(msg, 'text', None) or getattr(msg, 'media', None))
        ]

        if not messages:
            logging.info(f"Нет новых постов для «{channel.title}»")
            return

        # Шаг 2: Группируем сообщения по ID альбома (grouped_id)
        grouped_messages = defaultdict(list)
        for msg in messages:
            # Если у сообщения есть grouped_id, используем его как ключ. Иначе — ID самого сообщения.
            key = msg.grouped_id or msg.id 
            grouped_messages[key].append(msg)

        # Шаг 3: Обрабатываем каждую группу (пост или альбом)
        posts_to_insert = []
        for group_id, message_group in grouped_messages.items():
            # В группе сообщений с медиа, текст обычно прикреплен к первому
            message_group.sort(key=lambda m: m.id)
            main_message = message_group[0]

            # Создаем "скелет" поста из главного сообщения
            post_data = await create_post_dict(main_message, channel.id)

            # Собираем медиа со ВСЕХ сообщений в группе
            media_list = []
            media_upload_tasks = []

            for msg_in_group in message_group:
                if getattr(msg_in_group, 'media', None):
                    media_upload_tasks.append(upload_media_to_s3(msg_in_group, channel.id))

            # Выполняем все задачи по загрузке медиа параллельно для скорости
            media_results = await asyncio.gather(*media_upload_tasks)

            # Собираем успешные результаты загрузки
            for _, media_item in media_results: # _ это message_id, он нам тут не нужен
                if media_item:
                    media_list.append(media_item)

            # Если у поста нет ни текста, ни медиа, пропускаем его
            if not media_list and not post_data.get('text'):
                continue

            # Добавляем весь список медиа в наш "скелет" поста
            post_data['media'] = media_list
            posts_to_insert.append(post_data)

        if not posts_to_insert:
            logging.info(f"Для «{channel.title}» не найдено постов для вставки после группировки.")
            return

        # Шаг 4: Вставляем в БД уже сгруппированные посты
        stmt = insert(Post).values(posts_to_insert).on_conflict_do_nothing(
            index_elements=['channel_id', 'message_id']
        ).returning(Post.id) # Используем .id для подсчета

        result = await db_session.execute(stmt)
        new_items_count = len(result.fetchall())
        await db_session.commit()

        logging.info(f"Для «{channel.title}» обработано {len(grouped_messages)} постов/групп. Найдено новых: {new_items_count}")
        if new_items_count > 0:
            await worker_stats.increment_posts(new_items_count)

    except Exception as e:
        logging.error(f"Критическая ошибка при обработке «{channel.title}»: {e}", exc_info=True)
        await worker_stats.increment_errors()
        await db_session.rollback()

async def process_channel_safely(channel: Channel, semaphore: asyncio.Semaphore):
    async with semaphore, session_maker() as session:
        await fetch_posts_for_channel(channel, session, POST_LIMIT)

async def periodic_tasks_runner():
    while not shutdown_event.is_set():
        logging.info("Начинаю периодический сбор постов...")
        async with session_maker() as session:
            channels = (await session.execute(select(Channel).join(Subscription).distinct())).scalars().all()
        await worker_stats.set_channels(len(channels))
        if channels:
            semaphore = asyncio.Semaphore(15)
            await asyncio.gather(*[process_channel_safely(ch, semaphore) for ch in channels])
        logging.info("Периодический сбор завершен.")
        try: await asyncio.wait_for(shutdown_event.wait(), timeout=SLEEP_TIME)
        except asyncio.TimeoutError: pass
        
async def listen_for_new_channel_tasks():
    if not redis_publisher: 
        logging.warning("❌ Redis publisher не настроен - новые каналы не будут обрабатываться автоматически!")
        logging.warning(f"❌ REDIS_URL = {REDIS_URL}")  # ДОБАВИТЬ для диагностики
        return
    
    logging.info("🔄 Воркер слушает задачи на добавление каналов...")
    
    try:
        redis_client = await redis_publisher.get_connection()
        logging.info("✅ Redis подключение установлено для слушания задач")
    except Exception as e:
        logging.error(f"❌ Ошибка подключения к Redis: {e}")
        return
    
    while not shutdown_event.is_set():
        try:
            logging.debug("⏳ Ожидание задач из Redis...")  # Для отладки
            
            task_raw = await redis_client.brpop("new_channel_tasks", timeout=1) # type: ignore
            
            if not task_raw: 
                continue
                
            logging.info(f"📨 Получены raw данные из Redis: {task_raw}")  # ДОБАВИТЬ
            
            task = json.loads(task_raw[1])
            channel_id = int(task.get("channel_id"))
            chat_id = int(task.get("user_chat_id"))
            title = task.get("channel_title")
            
            logging.info(f"🆕 НОВЫЙ КАНАЛ: Обрабатываю канал «{title}» (ID: {channel_id}) для пользователя {chat_id}")
            
            async with session_maker() as session:
                channel = await session.get(Channel, channel_id)
                if channel:
                    logging.info(f"📥 Начинаю загрузку постов из «{title}»...")
                    
                    entity = await get_cached_entity(channel)
                    if entity and client is not None:
                        logging.info(f"🖼️ Загружаю аватар для «{title}»...")
                        avatar_url = await upload_avatar_to_s3(client, entity)
                        if avatar_url: 
                            channel.avatar_url = avatar_url
                            session.add(channel)
                            await session.commit()
                            logging.info(f"✅ Аватар загружен для «{title}»")
                    
                    await fetch_posts_for_channel(channel, session, POST_LIMIT)
                    
                    # ОТПРАВКА УВЕДОМЛЕНИЯ
                    completion = {"user_chat_id": chat_id, "channel_title": title}
                    await redis_publisher.publish("task_completion_notifications", json.dumps(completion))
                    logging.info(f"🎉 Канал «{title}» обработан, уведомление отправлено пользователю {chat_id}")
                else:
                    logging.error(f"❌ Канал с ID {channel_id} не найден в базе данных!")
                    
        except asyncio.CancelledError: 
            logging.info("🛑 Redis listener получил сигнал отмены")
            raise
        except asyncio.TimeoutError: 
            continue
        except json.JSONDecodeError as e:
            logging.error(f"❌ Ошибка декодирования JSON: {e}")
        except Exception as e:
            logging.error(f"❌ Ошибка в Redis-слушателе: {e}", exc_info=True)
            await worker_stats.increment_errors()
            await asyncio.sleep(1)
    
    logging.info("🛑 Redis listener завершен")

async def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    await create_db()
    logging.info("Воркер запущен.")
    
    if not client: 
        logging.critical("❌ Telethon клиент не настроен!")
        return
    
    # ДОБАВИТЬ: Проверка Redis
    if not redis_publisher:
        logging.warning("⚠️ Redis не настроен - новые каналы не будут обрабатываться автоматически")
        logging.warning(f"⚠️ REDIS_URL = {os.getenv('REDIS_URL')}")
    else:
        logging.info("✅ Redis настроен корректно")
    
    async with client:
        logging.info("✅ Клиент Telethon успешно запущен.")
        
        # Запускаем задачи
        tasks = [
            asyncio.create_task(periodic_tasks_runner(), name="periodic_tasks"),
        ]
        
        if redis_publisher:
            tasks.append(asyncio.create_task(listen_for_new_channel_tasks(), name="redis_listener"))
            logging.info("🔄 Запускаю Redis listener...")
        else:
            logging.warning("⚠️ Redis listener НЕ запущен - новые каналы обрабатываться не будут")
        
        await asyncio.gather(*tasks)
    
    if redis_publisher: 
        await redis_publisher.close()
    
    logging.info("✅ Воркер корректно завершил работу.")

# ✅ ДОБАВИТЬ ТОЧКУ ВХОДА:
if __name__ == "__main__":
    print("🚀 Запускаю main()...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Воркер остановлен пользователем")
    except Exception as e:
        print(f"💥 ФАТАЛЬНАЯ ОШИБКА в main(): {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("🏁 Воркер завершил работу")