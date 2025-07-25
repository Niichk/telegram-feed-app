import asyncio
import logging
import os
import boto3
import io
import time
import bleach
import signal
import redis.asyncio as aioredis
from dotenv import load_dotenv
from telethon import TelegramClient, types
from telethon.errors import ChannelPrivateError, FloodWaitError
from telethon.tl.types import InputPeerChannel, DocumentEmpty
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert

from database.engine import session_maker, create_db
from database.models import Channel, Post, BackfillRequest, Subscription
from telethon.sessions import StringSession
from datetime import datetime
from markdown_it import MarkdownIt
from linkify_it import LinkifyIt
from PIL import Image
from html import escape

# --- НАСТРОЙКА КОНВЕРТЕРА MARKDOWN -> HTML ---
md_parser = MarkdownIt('commonmark', {'breaks': True, 'html': False, 'linkify': True})

shutdown_event = asyncio.Event()

# --- Настройка логирования и переменных окружения ---
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

if not all([API_ID_STR, API_HASH, SESSION_STRING, S3_BUCKET_NAME, S3_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]):
    raise ValueError("Одна или несколько переменных окружения не установлены!")

if API_ID_STR is None:
    raise ValueError("API_ID environment variable is not set!")
API_ID = int(API_ID_STR)
POST_LIMIT = 10
SLEEP_TIME = 60

# --- Инициализация клиентов ---
if SESSION_STRING is None:
    raise ValueError("TELETHON_SESSION environment variable is not set!")
if API_HASH is None:
    raise ValueError("API_HASH environment variable is not set!")
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=S3_REGION
)

REDIS_URL = os.getenv("REDIS_URL")
if not REDIS_URL:
    logging.warning("REDIS_URL не установлен. Push-уведомления для API будут отключены.")
    redis_publisher = None
else:
    redis_publisher = aioredis.from_url(REDIS_URL)

linkify = LinkifyIt()
md = MarkdownIt()
md.linkify = linkify
entity_cache = {}
worker_stats = {
    'start_time': time.time(),
    'processed_channels': 0,
    'processed_posts': 0,
    'errors': 0
}

def signal_handler(signum, frame):
    """Обработчик системных сигналов."""
    logging.info(f"Получен сигнал {signal.Signals(signum).name}. Начинаю корректное завершение...")
    shutdown_event.set()

def process_text(message_text: str | None) -> str | None:
    """Конвертирует Markdown в безопасный HTML."""
    if not message_text:
        return None
    try:
        html_from_markdown = md_parser.renderInline(message_text)
        ALLOWED_TAGS = ['a', 'b', 'strong', 'i', 'em', 'pre', 'code', 'br', 's', 'u', 'blockquote']
        ALLOWED_ATTRIBUTES = {'a': ['href', 'title', 'target', 'rel']}
        safe_html = bleach.clean(
            html_from_markdown,
            tags=ALLOWED_TAGS,
            attributes=ALLOWED_ATTRIBUTES,
            strip=False
        )
        return safe_html.replace('<a href', '<a target="_blank" rel="noopener noreferrer" href')
    except Exception as e:
        logging.error(f"Критическая ошибка конвертации Markdown: {e}", exc_info=True)
        return escape(message_text).replace('\n', '<br>')

async def get_cached_entity(channel: Channel):
    """Получает entity с кэшированием."""
    cache_key = channel.username or str(channel.id)
    if cache_key in entity_cache:
        return entity_cache[cache_key]
    try:
        entity = await client.get_entity(channel.username) if channel.username else await client.get_entity(channel.id)
        if isinstance(entity, list):
            entity = entity[0]
        entity_cache[cache_key] = entity
        return entity
    except ValueError as e:
        logging.warning(f"Не удалось получить entity для канала {channel.title}: {e}")
        return None
    except Exception as e:
        logging.error(f"Неожиданная ошибка при получении entity для канала {channel.title}: {e}")
        return None

async def log_worker_stats():
    """Логирует статистику работы воркера."""
    uptime = time.time() - worker_stats['start_time']
    logging.info(
        f"Статистика воркера: "
        f"Uptime: {uptime/3600:.1f}ч, "
        f"Каналов: {worker_stats['processed_channels']}, "
        f"Постов: {worker_stats['processed_posts']}, "
        f"Ошибок: {worker_stats['errors']}"
    )

async def upload_avatar_to_s3(channel_entity) -> str | None:
    """Скачивает аватар канала, загружает в S3 и возвращает URL."""
    try:
        file_key = f"avatars/{channel_entity.id}.jpg"
        file_in_memory = io.BytesIO()
        await client.download_profile_photo(channel_entity, file=file_in_memory)
        if file_in_memory.getbuffer().nbytes == 0:
            logging.info(f"У канала «{channel_entity.title}» нет аватара.")
            return None
        file_in_memory.seek(0)
        s3_client.upload_fileobj(
            file_in_memory,
            S3_BUCKET_NAME,
            file_key,
            ExtraArgs={'ContentType': 'image/jpeg'}
        )
        avatar_url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{file_key}"
        logging.info(f"Аватар для канала «{channel_entity.title}» загружен в S3: {avatar_url}")
        return avatar_url
    except Exception as e:
        logging.error(f"Не удалось загрузить аватар для канала «{channel_entity.title}»: {e}")
        worker_stats['errors'] += 1
        return None

async def upload_media_to_s3(message: types.Message, channel_id: int) -> tuple[int, dict | None]:
    """
    Загружает медиа в S3 и возвращает кортеж (message_id, media_data).
    """
    media_data = {}
    media_type = None

    if isinstance(message.media, types.MessageMediaDocument):
        file_size = getattr(message.media.document, 'size', 0)
        if file_size > 60 * 1024 * 1024:  # 60MB лимит
            logging.warning(f"Файл для поста {message.id} ({file_size} bytes) слишком большой, пропускаем")
            return message.id, None

    if isinstance(message.media, types.MessageMediaPhoto):
        media_type = 'photo'
    elif isinstance(message.media, types.MessageMediaDocument):
        mime_type = getattr(message.media.document, 'mime_type', '')
        if mime_type.startswith('video/'):
            media_type = 'video'
        elif mime_type.startswith('audio/'):
            media_type = 'audio'
        elif mime_type in ['image/gif', 'video/mp4']:
             media_type = 'gif'

    if not media_type:
        return message.id, None

    try:
        if media_type == 'photo':
            file_extension = '.webp'
            content_type = 'image/webp'
        elif media_type == 'gif':
            file_extension = '.gif'
            content_type = 'image/gif'
        else:
            # Only access document if media is MessageMediaDocument
            if isinstance(message.media, types.MessageMediaDocument):
                doc = message.media.document
                attributes = getattr(doc, 'attributes', [])
                file_name = next((attr.file_name for attr in attributes if hasattr(attr, 'file_name')), None)
                file_extension = os.path.splitext(file_name)[1] if file_name else '.dat'
                content_type = getattr(doc, 'mime_type', 'application/octet-stream')
            else:
                file_extension = '.dat'
                content_type = 'application/octet-stream'

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
        if (
            media_type == 'video'
            and document and not isinstance(document, DocumentEmpty)
            and hasattr(document, 'thumbs') and document.thumbs
        ):
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


async def create_post_object(message: types.Message, channel_id: int) -> Post:
    """Вспомогательная функция для создания объекта Post из сообщения Telethon."""
    processed_text = process_text(message.message)
    
    reactions_data = []
    if message.reactions and message.reactions.results:
        from telethon.tl.types import ReactionEmoji, ReactionCustomEmoji
        for r in message.reactions.results:
            if r.count > 0:
                reaction_item = {"count": r.count}
                # Check for ReactionEmoji (has 'emoticon') and ReactionCustomEmoji (has 'document_id')
                if isinstance(r.reaction, ReactionEmoji):
                    reaction_item["emoticon"] = r.reaction.emoticon # type: ignore
                elif isinstance(r.reaction, ReactionCustomEmoji):
                    reaction_item["document_id"] = r.reaction.document_id
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
            elif hasattr(message.fwd_from, 'from_name') and message.fwd_from.from_name:
                forward_data = {"from_name": message.fwd_from.from_name, "username": None, "channel_id": None}
        except Exception as e:
            logging.warning(f"Не удалось получить entity для репоста: {e}")
            forward_data = {"from_name": "Недоступный источник", "username": None, "channel_id": None}
            
    return Post(
        channel_id=channel_id,
        message_id=message.id,
        date=message.date,
        text=processed_text,
        grouped_id=message.grouped_id,
        views=getattr(message, 'views', 0) or 0,
        reactions=reactions_data,
        forwarded_from=forward_data,
        media=[]  # Медиа будет добавлено позже
    )

async def fetch_posts_for_channel(channel: Channel, db_session: AsyncSession, post_limit: int = 20, offset_id: int = 0):
    """
    Получает посты, параллельно загружает медиа и сохраняет в БД.
    """
    channel_id = channel.id
    channel_title = channel.title
    try:
        entity = await get_cached_entity(channel)
        if not entity:
            logging.warning(f"Не удалось получить entity для канала {channel_title}")
            return

        posts_to_create_map = {}
        media_upload_tasks = []
        
        # Шаг 1: Итерация по сообщениям
        async for message in client.iter_messages(entity, limit=post_limit, offset_id=offset_id):
            if not message or (not message.text and not message.media):
                continue

            # Пропускаем посты, которые уже есть (быстрая проверка)
            existing_post_id = await db_session.scalar(
                select(Post.id).where(Post.channel_id == channel_id, Post.message_id == message.id)
            )
            if existing_post_id:
                continue

            # Создаем "скелет" поста и добавляем в словарь
            new_post = await create_post_object(message, channel_id)
            posts_to_create_map[message.id] = new_post

            # Создаем задачу на загрузку медиа, если оно есть
            if message.media:
                media_upload_tasks.append(upload_media_to_s3(message, channel_id))
        
        # Шаг 2: Параллельное выполнение всех задач по загрузке медиа
        if media_upload_tasks:
            logging.info(f"Для «{channel_title}» запускаю параллельную загрузку {len(media_upload_tasks)} медиафайлов...")
            start_time = time.time()
            media_results_list = await asyncio.gather(*media_upload_tasks, return_exceptions=True)
            end_time = time.time()
            logging.info(f"Загрузка {len(media_upload_tasks)} медиа для «{channel_title}» заняла {end_time - start_time:.2f} сек.")
            
            # Превращаем список результатов в словарь для быстрого доступа
            media_results_map = {
                msg_id: media_data
                for result in media_results_list
                if not isinstance(result, BaseException)
                for msg_id, media_data in [result]
                if media_data
            }
        else:
            media_results_map = {}

        # Шаг 3: Прикрепляем результаты загрузки медиа к "скелетам" постов
        posts_to_save = []
        for msg_id, post_obj in posts_to_create_map.items():
            if msg_id in media_results_map:
                post_obj.media = [media_results_map[msg_id]]
            posts_to_save.append(post_obj)

        # Шаг 4: Сохранение всех постов в базу данных
        if posts_to_save:
            db_session.add_all(posts_to_save)
            await db_session.commit()
            logging.info(f"Сохранено {len(posts_to_save)} новых постов для «{channel_title}»")
            worker_stats['processed_posts'] += len(posts_to_save)
        else:
             logging.info(f"Нет новых постов для «{channel_title}»")

    except Exception as e:
        logging.error(f"Критическая ошибка при обработке «{channel_title}»: {e}", exc_info=True)
        worker_stats['errors'] += 1
        await db_session.rollback()

async def process_channel_safely(channel: Channel, semaphore: asyncio.Semaphore, post_limit: int):
    """Безопасная обертка для обработки одного канала."""
    async with semaphore:
        async with session_maker() as session:
            try:
                await fetch_posts_for_channel(channel, session, post_limit=post_limit)
            except Exception as e:
                logging.error(f"Не удалось обработать канал {channel.title} из-за критической ошибки: {e}", exc_info=True)

async def periodic_task_parallel():
    """Периодическая задача с параллельной обработкой каналов."""
    logging.info("Начинаю периодический сбор постов...")
    list_of_channels = []
    async with session_maker() as session:
        result = await session.execute(select(Channel))
        list_of_channels = result.scalars().all()

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
    """Дозагрузка старых постов для пользователя."""
    logging.info(f"Начинаю дозагрузку старых постов для пользователя {user_id}...")
    async with session_maker() as db_session:
        from database.requests import get_user_subscriptions
        subscriptions = await get_user_subscriptions(db_session, user_id)
        if not subscriptions:
            logging.info(f"У пользователя {user_id} нет подписок для дозагрузки.")
            return
        for channel in subscriptions:
            oldest_post_query = (
                select(Post.message_id)
                .where(Post.channel_id == channel.id)
                .order_by(Post.date.asc())
                .limit(1)
            )
            oldest_post_res = await db_session.execute(oldest_post_query)
            oldest_post_id = oldest_post_res.scalar_one_or_none()
            offset_id = oldest_post_id if oldest_post_id else 0
            logging.info(f"Для канала «{channel.title}» ищем посты старше message_id {offset_id}.")
            await fetch_posts_for_channel(
                channel=channel,
                db_session=db_session,
                post_limit=20,
                offset_id=offset_id
            )
    logging.info(f"Дозагрузка для пользователя {user_id} завершена.")

async def process_backfill_requests():
    """Обработка заявок на дозагрузку."""
    logging.info("Проверяю наличие заявок на дозагрузку...")
    async with session_maker() as db_session:
        result = await db_session.execute(select(BackfillRequest))
        requests = result.scalars().all()
        if not requests:
            logging.info("Новых заявок нет.")
            return
        for req in requests:
            logging.info(f"Найдена заявка для пользователя {req.user_id}. Начинаю обработку.")
            try:
                await backfill_user_channels(req.user_id)
                await db_session.delete(req)
                await db_session.commit()
                logging.info(f"Заявка для пользователя {req.user_id} обработана и удалена.")
            except Exception as e:
                logging.error(f"Ошибка обработки заявки для пользователя {req.user_id}: {e}")
                await db_session.rollback()
                worker_stats['errors'] += 1

async def main():
    """Основная функция запуска воркера."""
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
                        await asyncio.gather(
                            periodic_task_parallel(),
                            process_backfill_requests()
                        )
                        logging.info(f"Все задачи выполнены. Засыпаю на {SLEEP_TIME / 60:.1f} минут...")
                        await asyncio.wait_for(shutdown_event.wait(), timeout=SLEEP_TIME)
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        logging.error(f"Ошибка в основном рабочем цикле: {e}", exc_info=True)
                        worker_stats['errors'] += 1
                        await asyncio.sleep(10)
        except Exception as e:
            retry_count += 1
            logging.error(f"Критическая ошибка клиента Telethon (попытка {retry_count}/{max_retries}): {e}")
            worker_stats['errors'] += 1
            if retry_count < max_retries:
                logging.info("Повторная попытка подключения через 30 секунд...")
                await asyncio.sleep(30)
            else:
                logging.error("Превышено максимальное количество попыток переподключения. Воркер останавливается.")
                shutdown_event.set()
    logging.info("Воркер корректно завершил работу.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Приложение остановлено.")