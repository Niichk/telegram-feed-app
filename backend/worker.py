import asyncio
import logging
import os
import boto3
import io
import time
import bleach
from dotenv import load_dotenv
from telethon import TelegramClient, types
from telethon.errors import ChannelPrivateError, FloodWaitError
from telethon.tl.types import InputPeerChannel, DocumentEmpty
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from database.engine import session_maker
from database.models import Channel, Post, BackfillRequest
from telethon.sessions import StringSession
from datetime import datetime
from markdown_it import MarkdownIt
from linkify_it import LinkifyIt
from PIL import Image
from html import escape

# --- НАСТРОЙКА КОНВЕРТЕРА MARKDOWN -> HTML (ВСТАВИТЬ ПОСЛЕ ИМПОРТОВ) ---
# Создаем экземпляр конвертера с настройками, которые нам нужны.
# commonmark - стандартный markdown
# {'breaks': True, 'html': False, 'linkify': True} - настройки:
#   - breaks: True -> Превращать переносы строк в <br>
#   - html: False -> Не пропускать сырой HTML из текста (для безопасности)
#   - linkify: True -> Автоматически превращать текстовые ссылки в кликабельные
md_parser = MarkdownIt('commonmark', {'breaks': True, 'html': False, 'linkify': True})

# Настройка
load_dotenv()
DB_URL_FOR_LOG = os.getenv("DATABASE_URL")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info(f"!!! WORKER STARTING WITH DATABASE_URL: {DB_URL_FOR_LOG} !!!")


# --- Переменные окружения ---
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

# Настраиваем markdown-it с плагином для надежного поиска ссылок
linkify = LinkifyIt()
md = MarkdownIt()
md.linkify = linkify

# Настройки для Bleach, чтобы он не удалял полезные теги (включая ссылки)
ALLOWED_TAGS = ['a', 'b', 'strong', 'i', 'em', 'pre', 'code', 'br']
ALLOWED_ATTRIBUTES = {'a': ['href', 'title']}


# Кэш для entity и статистика
entity_cache = {}
worker_stats = {
    'start_time': time.time(),
    'processed_channels': 0,
    'processed_posts': 0,
    'errors': 0
}

def process_text(message_text: str | None) -> str | None:
    """
    Принимает текст сообщения из Telethon (который уже в формате Markdown),
    конвертирует его в HTML с помощью markdown-it-py и затем очищает
    с помощью bleach для максимальной безопасности.
    """
    if not message_text:
        return None

    try:
        # 1. Конвертируем Markdown в HTML. Библиотека сама найдет ссылки.
        html_from_markdown = md_parser.render(message_text)

        # 2. Очищаем полученный HTML, разрешая только безопасные теги.
        # Это защитит от любых вредоносных вставок.
        ALLOWED_TAGS = ['a', 'b', 'strong', 'i', 'em', 'pre', 'code', 'br', 's', 'u', 'blockquote']
        ALLOWED_ATTRIBUTES = {'a': ['href', 'title', 'target', 'rel']}
        
        safe_html = bleach.clean(
            html_from_markdown,
            tags=ALLOWED_TAGS,
            attributes=ALLOWED_ATTRIBUTES,
            strip=False
        )

        # 3. Добавляем к ссылкам target="_blank", чтобы они открывались в новой вкладке.
        # Bleach не умеет это делать сам, поэтому используем простое добавление.
        return safe_html.replace('<a href', '<a target="_blank" rel="noopener noreferrer" href')

    except Exception as e:
        logging.error(f"Критическая ошибка конвертации Markdown: {e}", exc_info=True)
        # В случае сбоя, возвращаем безопасный, экранированный текст.
        return escape(message_text).replace('\n', '<br>')
    
async def save_single_post(db_session: AsyncSession, post: Post, channel_title: str):
    """Сохраняет один пост немедленно"""
    try:
        from sqlalchemy.dialects.postgresql import insert
        
        stmt = insert(Post).values(
            channel_id=post.channel_id,
            message_id=post.message_id,
            text=post.text,
            date=post.date,
            grouped_id=post.grouped_id,
            media=post.media,
            views=post.views,
            reactions=post.reactions,
            forwarded_from=post.forwarded_from
        )
        
        stmt = stmt.on_conflict_do_update(
            constraint='_channel_message_uc',
            set_=dict(
                views=stmt.excluded.views,
                reactions=stmt.excluded.reactions,
                updated_at=func.now()
            )
        )
        
        await db_session.execute(stmt)
        await db_session.commit()
        logging.info(f"Сохранен пост {post.message_id} из канала «{channel_title}»")
        return True
    except Exception as e:
        logging.error(f"Ошибка сохранения поста {post.message_id}: {e}")
        await db_session.rollback()
        return False

async def get_cached_entity(channel: Channel):
    """Получает entity с кэшированием"""
    cache_key = channel.username or str(channel.id)

    if cache_key in entity_cache:
        return entity_cache[cache_key]

    try:
        if channel.username:
            entity = await client.get_entity(channel.username)
        else:
            entity = await client.get_entity(channel.id)

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
    """Логирует статистику работы воркера"""
    uptime = time.time() - worker_stats['start_time']
    logging.info(
        f"Статистика воркера: "
        f"Uptime: {uptime/3600:.1f}ч, "
        f"Каналов: {worker_stats['processed_channels']}, "
        f"Постов: {worker_stats['processed_posts']}, "
        f"Ошибок: {worker_stats['errors']}"
    )

async def upload_media_to_s3(message: types.Message, channel_id: int) -> dict | None:
    """Загружает медиафайл и его превью (для видео) в S3."""
    media_data = {}
    media_type = None

    # ИСПРАВЛЕНИЕ: Проверка размера файла ПЕРЕД обработкой
    if isinstance(message.media, types.MessageMediaDocument):
        file_size = getattr(message.media.document, 'size', 0)
        if file_size > 60 * 1024 * 1024:  # 60MB лимит
            logging.warning(f"Файл слишком большой ({file_size} bytes), пропускаем")
            return None

    if isinstance(message.media, types.MessageMediaPhoto):
        media_type = 'photo'
    elif isinstance(message.media, types.MessageMediaDocument):
        mime_type = getattr(message.media.document, 'mime_type', '')
        if mime_type.startswith('video/'):
            media_type = 'video'
        elif mime_type.startswith('audio/'):
            media_type = 'audio'
        elif mime_type in ['image/gif', 'video/mp4'] and 'gif' in getattr(message.media.document, 'file_name', '').lower():
            media_type = 'gif'

    if not media_type:
        return None

    try:
        # Проверяем, существует ли уже файл в S3
        if media_type == 'photo':
            file_extension = '.webp'
            content_type = 'image/webp'
        elif media_type == 'gif':
            file_extension = '.gif'
            content_type = 'image/gif'
        else:
            doc = message.media.document # type: ignore
            attributes = getattr(doc, 'attributes', [])
            file_name = next((attr.file_name for attr in attributes if hasattr(attr, 'file_name')), None)
            file_extension = os.path.splitext(file_name)[1] if file_name else '.dat'
            content_type = getattr(doc, 'mime_type', 'application/octet-stream')

        file_key = f"media/{channel_id}/{message.id}{file_extension}"
        
        # ИСПРАВЛЕНИЕ: Проверяем существование файла в S3
        try:
            s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=file_key)
            file_exists = True
        except s3_client.exceptions.NoSuchKey:
            file_exists = False
        except Exception:
            file_exists = False

        if not file_exists:
            # Загружаем основной медиафайл только если его нет
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
            # GIF остаются как есть
            
            s3_client.upload_fileobj(file_in_memory, S3_BUCKET_NAME, file_key, ExtraArgs={'ContentType': content_type})
            logging.info(f"Загружен медиафайл: {file_key}")
        
        media_data["type"] = media_type
        media_data["url"] = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{file_key}"

        # ИСПРАВЛЕНИЕ: Проверяем существование превью для видео
        document = getattr(message.media, 'document', None)
        if (
            media_type == 'video'
            and document and not isinstance(document, DocumentEmpty)
            and hasattr(document, 'thumbs') and document.thumbs
        ):
            thumb_key = f"media/{channel_id}/{message.id}_thumb.webp"
            
            # Проверяем существование превью
            try:
                s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=thumb_key)
                thumb_exists = True
            except s3_client.exceptions.NoSuchKey:
                thumb_exists = False
            except Exception:
                thumb_exists = False

            if not thumb_exists:
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
                        logging.info(f"Загружено превью для видео: {thumb_key}")
            
            media_data["thumbnail_url"] = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{thumb_key}"

        return media_data
    
    except Exception as e:
        logging.error(f"Критическая ошибка в upload_media_to_s3: {e}", exc_info=True)
        return None

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

async def process_grouped_messages(grouped_messages, channel_id_for_log, db_session):
    """Обрабатывает группу сообщений (альбом) как один пост"""
    if not grouped_messages:
        return None

    main_message = grouped_messages[0]
    media_items = []
    for msg in grouped_messages:
        media_item = await upload_media_to_s3(msg, channel_id_for_log)
        if media_item:
            media_items.append(media_item)

    reactions_data = []
    if main_message.reactions and main_message.reactions.results:
        for r in main_message.reactions.results:
            if r.count > 0:
                if hasattr(r.reaction, 'emoticon') and r.reaction.emoticon:
                    reactions_data.append({"emoticon": r.reaction.emoticon, "count": r.count})
                elif hasattr(r.reaction, 'document_id'):
                    reactions_data.append({"document_id": r.reaction.document_id, "count": r.count})

    forward_data = None
    if main_message.fwd_from:
        try:
            if hasattr(main_message.fwd_from, 'from_id') and main_message.fwd_from.from_id:
                source_entity = await client.get_entity(main_message.fwd_from.from_id)
                from_name = getattr(source_entity, 'title', getattr(source_entity, 'first_name', 'Неизвестный источник'))
                username = getattr(source_entity, 'username', None)

                raw_channel_id = getattr(source_entity, 'id', None)
                if raw_channel_id:
                    channel_id = str(raw_channel_id)[4:] if str(raw_channel_id).startswith('-100') else str(raw_channel_id)
                else:
                    channel_id = None

                forward_data = {"from_name": from_name, "username": username, "channel_id": channel_id}
            elif hasattr(main_message.fwd_from, 'from_name') and main_message.fwd_from.from_name:
                forward_data = {"from_name": main_message.fwd_from.from_name, "username": None, "channel_id": None}
        except Exception as e:
            logging.warning(f"Не удалось получить entity для репоста в альбоме: {e}")
            forward_data = {"from_name": "Недоступный источник", "username": None, "channel_id": None}

    processed_text = process_text(main_message.text)
    
    new_post = Post(
        channel_id=channel_id_for_log,
        message_id=main_message.id,
        date=main_message.date,
        text=processed_text,
        grouped_id=main_message.grouped_id,
        media=media_items,
        views=getattr(main_message, 'views', 0) or 0,
        reactions=reactions_data,
        forwarded_from=forward_data
    )
    return new_post

async def fetch_posts_for_channel(channel: Channel, db_session: AsyncSession, post_limit: int = 10, offset_id: int = 0, real_time_save: bool = False):
    """
    Получает посты из канала и сохраняет их в базу данных.
    """
    channel_id_for_log = channel.id
    channel_title_for_log = channel.title

    try:
        entity = await get_cached_entity(channel)
        if not entity: 
            logging.warning(f"Не удалось получить entity для канала {channel_title_for_log}")
            return

        new_posts = []
        updated_posts = []
        grouped_messages = {}

        # Итерируемся по сообщениям канала
        async for message in client.iter_messages(entity, limit=post_limit, offset_id=offset_id):
            if not message or (not message.text and not message.media and not message.poll):
                continue
            
            # Обрабатываем групповые сообщения (альбомы) отдельно
            if message.grouped_id:
                grouped_messages.setdefault(message.grouped_id, []).append(message)
                continue

            # Проверяем, существует ли пост в базе данных
            existing_post = await db_session.scalar(
                select(Post).where(Post.channel_id == channel_id_for_log, Post.message_id == message.id)
            )
            
            if existing_post:
                # Обновляем существующий пост
                needs_update = False
                
                # Проверка просмотров
                new_views = getattr(message, 'views', 0) or 0
                if existing_post.views != new_views:
                    existing_post.views = new_views
                    needs_update = True
                
                # Проверка реакций
                new_reactions = []
                if message.reactions and message.reactions.results:
                    for r in message.reactions.results:
                        if r.count > 0:
                            reaction_data = {"count": r.count}
                            if hasattr(r.reaction, 'emoticon') and r.reaction.emoticon:
                                reaction_data["emoticon"] = r.reaction.emoticon
                            elif hasattr(r.reaction, 'document_id'):
                                reaction_data["document_id"] = r.reaction.document_id
                            new_reactions.append(reaction_data)
                
                # Сравнение реакций без учета порядка
                if sorted(existing_post.reactions or [], key=lambda x: str(x)) != sorted(new_reactions, key=lambda x: str(x)):
                    existing_post.reactions = new_reactions
                    needs_update = True
                
                # ИСПРАВЛЕНИЕ: Проверка текста с entities
                new_text = process_text(message.text)
                if existing_post.text != (new_text if new_text is not None else ""):
                    existing_post.text = new_text if new_text is not None else ""
                    needs_update = True

                if needs_update:
                    updated_posts.append(existing_post)

            else:
                # ИСПРАВЛЕНИЕ: Создаем новый пост с entities
                processed_text = process_text(message.text)
                media_item = await upload_media_to_s3(message, channel_id_for_log)
                
                # Обработка реакций
                reactions_data = []
                if message.reactions and message.reactions.results:
                    for r in message.reactions.results:
                        if r.count > 0:
                            reaction_data = {"count": r.count}
                            if hasattr(r.reaction, 'emoticon') and r.reaction.emoticon:
                                reaction_data["emoticon"] = r.reaction.emoticon
                            elif hasattr(r.reaction, 'document_id'):
                                reaction_data["document_id"] = r.reaction.document_id
                            reactions_data.append(reaction_data)
                
                # Обработка пересланных сообщений
                forward_data = None
                if message.fwd_from:
                    try:
                        if hasattr(message.fwd_from, 'from_id') and message.fwd_from.from_id:
                            source_entity = await client.get_entity(message.fwd_from.from_id)
                            from_name = getattr(source_entity, 'title', getattr(source_entity, 'first_name', 'Неизвестный источник'))
                            username = getattr(source_entity, 'username', None)
                            raw_channel_id = getattr(source_entity, 'id', None)
                            channel_id = str(raw_channel_id)[4:] if raw_channel_id and str(raw_channel_id).startswith('-100') else str(raw_channel_id) if raw_channel_id else None
                            forward_data = {"from_name": from_name, "username": username, "channel_id": channel_id}
                        elif hasattr(message.fwd_from, 'from_name') and message.fwd_from.from_name:
                            forward_data = {"from_name": message.fwd_from.from_name, "username": None, "channel_id": None}
                    except Exception as e:
                        logging.warning(f"Не удалось получить entity для репоста: {e}")
                        forward_data = {"from_name": "Недоступный источник", "username": None, "channel_id": None}
                
                # Создаем объект поста
                new_post = Post(
                    channel_id=channel_id_for_log,
                    message_id=message.id,
                    date=message.date,
                    text=processed_text,
                    media=[media_item] if media_item else [],
                    views=getattr(message, 'views', 0) or 0,
                    reactions=reactions_data,
                    forwarded_from=forward_data
                )
                new_posts.append(new_post)

                # Сохранение в реальном времени (если включено)
                if real_time_save:
                    await save_single_post(db_session, new_post, channel_title_for_log)

        # Обработка групповых сообщений (альбомов)
        for group_id, messages in grouped_messages.items():
            if not messages:
                continue

            # Проверяем, существует ли групповой пост
            existing_group_post = await db_session.scalar(
                select(Post).where(Post.grouped_id == group_id)
            )

            if not existing_group_post:
                # Создаем новый групповой пост
                group_post = await process_grouped_messages(messages, channel_id_for_log, db_session)
                if group_post:
                    new_posts.append(group_post)
                    
                    # Сохранение в реальном времени для групповых постов
                    if real_time_save:
                        await save_single_post(db_session, group_post, channel_title_for_log)
            else:
                # Обновляем существующий групповой пост
                main_message = messages[0]
                needs_update = False
                
                new_views = getattr(main_message, 'views', 0) or 0
                if existing_group_post.views != new_views:
                    existing_group_post.views = new_views
                    needs_update = True
                
                if needs_update:
                    updated_posts.append(existing_group_post)

        # Сохранение в базу данных (только если не используется real_time_save)
        if not real_time_save and (new_posts or updated_posts):
            try:
                if new_posts:
                    from sqlalchemy.dialects.postgresql import insert
                    
                    for post in new_posts:
                        stmt = insert(Post).values(
                            channel_id=post.channel_id,
                            message_id=post.message_id,
                            text=post.text,
                            date=post.date,
                            grouped_id=post.grouped_id,
                            media=post.media,
                            views=post.views,
                            reactions=post.reactions,
                            forwarded_from=post.forwarded_from
                        )
                        
                        # При конфликте - обновляем views и reactions
                        stmt = stmt.on_conflict_do_update(
                            constraint='_channel_message_uc',
                            set_=dict(
                                views=stmt.excluded.views,
                                reactions=stmt.excluded.reactions,
                                updated_at=func.now()
                            )
                        )
                        
                        await db_session.execute(stmt)
                    
                    await db_session.commit()
                    logging.info(f"Создано/обновлено {len(new_posts)} постов для «{channel_title_for_log}»")
                
                # Сохранение обновлений существующих постов
                if updated_posts:
                    await db_session.commit()
                    logging.info(f"Обновлено {len(updated_posts)} постов в «{channel_title_for_log}»")
                    
            except Exception as e:
                logging.error(f"Ошибка сохранения постов для «{channel_title_for_log}»: {e}")
                await db_session.rollback()
                worker_stats['errors'] += 1
                raise  # Поднимаем ошибку выше для корректной обработки
        else:
            if not real_time_save:
                logging.info(f"Нет изменений для канала «{channel_title_for_log}»")

        # Обновляем статистику
        worker_stats['processed_posts'] += len(new_posts)

    except ChannelPrivateError:
        logging.warning(f"Канал «{channel_title_for_log}» стал приватным или недоступным")
        worker_stats['errors'] += 1
    except FloodWaitError as e:
        logging.warning(f"Flood wait для канала «{channel_title_for_log}»: {e.seconds} секунд")
        await asyncio.sleep(e.seconds)
        worker_stats['errors'] += 1
    except Exception as e:
        logging.error(f"Критическая ошибка при обработке «{channel_title_for_log}»: {e}", exc_info=True)
        worker_stats['errors'] += 1
        raise  # Поднимаем ошибку для обработки в вызывающем коде


async def process_channel_safely(channel: Channel, semaphore: asyncio.Semaphore, post_limit: int):
    """
    Безопасная обертка для обработки одного канала.
    Создает собственную сессию БД и отлавливает все ошибки.
    """
    async with semaphore:
        # Для каждой параллельной задачи создаем свою короткоживущую сессию
        async with session_maker() as session:
            try:
                await fetch_posts_for_channel(channel, session, post_limit=post_limit)
            except Exception as e:
                # Отлавливаем любые непредвиденные ошибки, чтобы не уронить весь gather
                logging.error(f"Не удалось обработать канал {channel.title} из-за критической ошибки: {e}", exc_info=True)

async def periodic_task_parallel():
    """
    Периодическая задача, которая обходит все каналы в базе данных
    С ПАРАЛЛЕЛЬНОЙ ОБРАБОТКОЙ и корректной статистикой.
    """
    logging.info("Начинаю периодический сбор постов...")
    
    # --- ИСПРАВЛЕНИЕ: Сбрасываем счетчики для этого цикла ---
    # Uptime и ошибки остаются общими, а счетчики каналов/постов - для цикла
    cycle_stats = {
        'channels': 0,
        'posts': 0
    }
    # ----------------------------------------------------

    list_of_channels = []
    async with session_maker() as session:
        result = await session.execute(select(Channel))
        list_of_channels = result.scalars().all()

    if not list_of_channels:
        logging.info("В базе нет каналов для отслеживания.")
        return
        
    # Вместо инкремента глобального счетчика, будем передавать локальный
    # В этой реализации это не нужно, так как мы просто считаем количество каналов
    cycle_stats['channels'] = len(list_of_channels)

    semaphore = asyncio.Semaphore(15)
    tasks = [process_channel_safely(channel, semaphore, POST_LIMIT) for channel in list_of_channels]

    if not tasks:
        return
        
    logging.info(f"Запускаю обработку {len(tasks)} каналов параллельно...")
    
    await asyncio.gather(*tasks, return_exceptions=True)
    
    logging.info("Все каналы обработаны.")
    
    # Логируем статистику
    uptime = time.time() - worker_stats['start_time']
    logging.info(
        f"Статистика воркера: Uptime: {uptime/3600:.1f}ч, "
        f"Обработано в этом цикле: {cycle_stats['channels']} каналов, "
        f"Ошибок (всего): {worker_stats['errors']}"
    )


async def backfill_user_channels(user_id: int):
    """
    Находит самые старые посты для каждого канала пользователя и запускает дозагрузку.
    """
    logging.info(f"Начинаю дозагрузку старых постов для пользователя {user_id}...")
    async with session_maker() as db_session:
        from database.requests import get_user_subscriptions
        subscriptions = await get_user_subscriptions(db_session, user_id)

        if not subscriptions:
            logging.info(f"У пользователя {user_id} нет подписок для дозагрузки.")
            return

        for channel in subscriptions:
            oldest_post_query = (
                select(Post)
                .where(Post.channel_id == channel.id)
                .order_by(Post.date.asc())
                .limit(1)
            )
            oldest_post_res = await db_session.execute(oldest_post_query)
            oldest_post = oldest_post_res.scalars().first()

            offset_id = oldest_post.message_id if oldest_post else 0
            logging.info(f"Для канала «{channel.title}» ищем посты старше message_id {offset_id}.")
            await fetch_posts_for_channel(
                channel=channel,
                db_session=db_session,
                post_limit=20, # Можно увеличить лимит для дозагрузки
                offset_id=offset_id
            )

    logging.info(f"Дозагрузка для пользователя {user_id} завершена.")

async def periodic_task():
    """
    Периодическая задача, которая обходит все каналы в базе данных.
    """
    logging.info("Начинаю периодический сбор постов...")
    async with session_maker() as db_session:
        result = await db_session.execute(select(Channel))
        channels = result.scalars().all()

        if not channels:
            logging.info("В базе нет каналов для отслеживания.")
            return

        for channel in channels:
            await fetch_posts_for_channel(channel, db_session, post_limit=POST_LIMIT)

    await log_worker_stats()

async def process_backfill_requests():
    """
    Проверяет и обрабатывает заявки на дозагрузку.
    """
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

from database.engine import create_db

async def main():
    await create_db()
    logging.info("Worker: Database tables checked/created.")

    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            async with client:
                logging.info("Клиент Telethon запущен.")
                while True:
                    try:
                        await asyncio.gather(
                            periodic_task_parallel(),
                            process_backfill_requests()
                        )
                        logging.info(f"Все задачи выполнены. Засыпаю на {SLEEP_TIME / 60:.1f} минут...")
                        await asyncio.sleep(SLEEP_TIME)
                    except Exception as e:
                        logging.error(f"Ошибка в основном цикле: {e}")
                        worker_stats['errors'] += 1
                        await asyncio.sleep(10)
        except Exception as e:
            retry_count += 1
            logging.error(f"Критическая ошибка клиента Telethon (попытка {retry_count}/{max_retries}): {e}")
            worker_stats['errors'] += 1
            if retry_count < max_retries:
                await asyncio.sleep(30)
            else:
                logging.error("Превышено максимальное количество попыток переподключения")
                raise

if __name__ == "__main__":
    asyncio.run(main())