import asyncio
import logging
import os
import boto3
import io
from dotenv import load_dotenv
from telethon import TelegramClient, types
from telethon.tl.types import InputPeerChannel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

# Убираем drop_db, оставляем только create_db
from database.engine import session_maker, create_db
from database.models import Channel, Post, BackfillRequest
from telethon.sessions import StringSession
from io import BytesIO
from datetime import datetime
from markdown_it import MarkdownIt
from PIL import Image

# Настройка
load_dotenv()
DB_URL_FOR_LOG = os.getenv("DATABASE_URL")
# Исправляем настройку логгера
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
md = MarkdownIt().enable('linkify')

async def upload_media_to_s3(message: types.Message, channel_id: int) -> dict | None:
    """Загружает медиафайл в S3 и возвращает словарь с его данными."""
    media_type = None
    if isinstance(message.media, types.MessageMediaPhoto):
        media_type = 'photo'
    elif isinstance(message.media, types.MessageMediaDocument):
        mime_type = getattr(message.media.document, 'mime_type', '') if hasattr(message.media, 'document') else ''
        if mime_type.startswith('video/'):
            media_type = 'video'
        elif mime_type.startswith('audio/'):
            media_type = 'audio'

    if not media_type:
        return None

    try:
        file_in_memory = io.BytesIO()
        await client.download_media(message, file=file_in_memory)
        file_in_memory.seek(0)
        
        # --- ИСПРАВЛЕННАЯ ЛОГИКА ---
        if media_type == 'photo':
            with Image.open(file_in_memory) as im:
                im = im.convert("RGB") # Для WebP это не всегда нужно, но и не повредит
                output_buffer = io.BytesIO()
                # Сохраняем в WebP вместо JPEG
                im.save(output_buffer, format="WEBP", quality=80) # quality для WebP подбирается отдельно
                output_buffer.seek(0)
                file_in_memory = output_buffer

            file_extension = '.webp' # Меняем расширение
            content_type = 'image/webp' # и MIME-тип
            logging.info("Изображение успешно сжато в WebP.")
        else: # Используем else, чтобы этот блок не выполнялся для фото
            if (isinstance(message.media, types.MessageMediaDocument) and hasattr(message.media, 'document') and message.media.document and not isinstance(message.media.document, types.DocumentEmpty)):
                attributes = getattr(message.media.document, 'attributes', [])
                file_name = next((attr.file_name for attr in attributes if hasattr(attr, 'file_name')), None)
                file_extension = os.path.splitext(file_name)[1] if file_name else '.dat'
                content_type = getattr(message.media.document, 'mime_type', 'application/octet-stream')
            else:
                file_extension = '.dat'
                content_type = 'application/octet-stream'

        file_key = f"media/{channel_id}/{message.id}{file_extension}"
        s3_client.upload_fileobj(file_in_memory, S3_BUCKET_NAME, file_key, ExtraArgs={'ContentType': content_type})
        
        media_url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{file_key}"
        logging.info(f"Загружен файл в S3: {file_key}")
        
        return {"type": media_type, "url": media_url}

    except Exception as e:
        logging.error(f"Ошибка загрузки медиа из поста {message.id}: {e}")
        return None
    
async def upload_avatar_to_s3(channel_entity) -> str | None:
    """Скачивает аватар канала, загружает в S3 и возвращает URL."""
    try:
        # Создаем уникальное имя файла для аватара
        file_key = f"avatars/{channel_entity.id}.jpg"
        
        # Скачиваем фото профиля во временный файл в памяти
        file_in_memory = io.BytesIO()
        await client.download_profile_photo(channel_entity, file=file_in_memory)
        
        # Если у канала нет аватара, фото будет пустым
        if file_in_memory.getbuffer().nbytes == 0:
            logging.info(f"У канала «{channel_entity.title}» нет аватара.")
            return None
            
        file_in_memory.seek(0)
        
        # Загружаем в S3
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
        return None


async def fetch_posts_for_channel(channel: Channel, db_session: AsyncSession, post_limit: int = 10, offset_id: int = 0):
    channel_id_for_log = channel.id
    channel_title_for_log = channel.title

    try:
        logging.info(f"Начинаю сбор постов для канала «{channel_title_for_log}»...")
        entity = await client.get_entity(channel.username or channel_id_for_log)
        # Ensure entity is not a list
        if isinstance(entity, list):
            entity = entity[0]
        
        async for message in client.iter_messages(entity, limit=post_limit, offset_id=offset_id):
            if not message or (not message.text and not message.media and not message.poll):
                continue
            
            # --- ИСПРАВЛЕНИЯ ---
            # 1. Ищем существующий пост в БД
            existing_post_query = await db_session.execute(
                select(Post).where(Post.channel_id == channel_id_for_log, Post.message_id == message.id)
            )
            existing_post = existing_post_query.scalars().first()

            # 2. ИСПРАВЛЕНО: Улучшенная обработка реакций
            reactions_data = []
            if message.reactions and message.reactions.results:
                for r in message.reactions.results:
                    if r.count > 0:
                        # Проверяем разные типы реакций
                        if hasattr(r.reaction, 'emoticon') and r.reaction.emoticon:
                            reactions_data.append({"emoticon": r.reaction.emoticon, "count": r.count})
                        elif hasattr(r.reaction, 'document_id'):
                            # Для кастомных эмодзи
                            reactions_data.append({"document_id": r.reaction.document_id, "count": r.count})
            
            # 3. ИСПРАВЛЕНО: Безопасное получение просмотров
            views_count = getattr(message, 'views', 0) or 0
            
            # 4. Если пост уже есть, обновляем его и идем дальше
            if existing_post:
                existing_post.views = views_count
                existing_post.reactions = reactions_data
                # Если у поста обновился текст (например, было редактирование), тоже обновим
                if message.text:
                    existing_post.text = md.render(message.text)
                logging.info(f"Обновлен пост {message.id} из «{channel_title_for_log}» (Просмотры: {views_count}, Реакции: {len(reactions_data)})")
                await db_session.commit()  # ДОБАВЛЕНО: коммит для обновлений
                continue # Переходим к следующему сообщению в цикле

            # 5. Если поста нет, создаем новый (старая логика)
            media_item = await upload_media_to_s3(message, channel_id_for_log)
            html_text = md.render(message.text) if message.text else None

            # Логика для альбомов остается почти без изменений
            if message.grouped_id:
                existing_album_post_query = await db_session.execute(
                    select(Post).where(Post.grouped_id == message.grouped_id)
                )
                existing_album_post = existing_album_post_query.scalars().first()
                
                if existing_album_post:
                    # Обновляем просмотры и реакции для главного поста альбома
                    existing_album_post.views = views_count
                    existing_album_post.reactions = reactions_data
                    
                    # Добавляем новое медиа, если его еще нет
                    if media_item and media_item not in existing_album_post.media:
                        existing_album_post.media = existing_album_post.media + [media_item]
                        logging.info(f"Добавлен медиа к альбому {message.grouped_id}.")
                    
                    logging.info(f"Обновлены данные для альбома {message.grouped_id} (Просмотры: {views_count}, Реакции: {len(reactions_data)}).")
                    await db_session.commit()  # ДОБАВЛЕНО: коммит для альбомов
                else:
                    new_post = Post(
                        channel_id=channel_id_for_log, message_id=message.id, date=message.date,
                        text=html_text, grouped_id=message.grouped_id,
                        media=[media_item] if media_item else [],
                        views=views_count, reactions=reactions_data  # ИСПРАВЛЕНО: используем переменную
                    )
                    db_session.add(new_post)
                    await db_session.commit()
                    logging.info(f"Создан новый пост-альбом {message.id} из «{channel_title_for_log}» (Просмотры: {views_count}, Реакции: {len(reactions_data)})")
            else:
                # Создание одиночного поста
                new_post = Post(
                    channel_id=channel_id_for_log, message_id=message.id, date=message.date,
                    text=html_text, media=[media_item] if media_item else [],
                    views=views_count, reactions=reactions_data  # ИСПРАВЛЕНО: используем переменную
                )
                db_session.add(new_post)
                await db_session.commit()
                logging.info(f"Создан новый пост {message.id} из «{channel_title_for_log}» (Просмотры: {views_count}, Реакции: {len(reactions_data)})")

    except Exception as e:
        logging.error(f"Критическая ошибка при обработке канала «{channel_title_for_log}» (ID: {channel_id_for_log}): {e}")
        await db_session.rollback()


async def backfill_user_channels(user_id: int):
    """
    Находит самые старые посты для каждого канала пользователя
    и запускает для них дозагрузку еще более старых постов.
    """
    logging.info(f"Начинаю дозагрузку старых постов для пользователя {user_id}...")

    # --- ИЗМЕНЕНИЕ ЗДЕСЬ: УБИРАЕМ ЛИШНИЙ `async with client:` ---
    # Клиент уже подключен в функции `main`, просто используем его.
    
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

            if oldest_post:
                logging.info(f"Самый старый пост для канала «{channel.title}» имеет ID {oldest_post.message_id}. Ищем посты старше...")
                # Эта функция использует уже подключенный глобальный клиент
                await fetch_posts_for_channel(
                    channel=channel, 
                    db_session=db_session, 
                    post_limit=20,
                    offset_id=oldest_post.message_id
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
            # Вызываем новую функцию для каждого канала
            await fetch_posts_for_channel(channel, db_session, post_limit=POST_LIMIT)

async def process_backfill_requests():
    """
    Проверяет и обрабатывает заявки на дозагрузку.
    """
    logging.info("Проверяю наличие заявок на дозагрузку...")
    async with session_maker() as db_session:
        # Находим все существующие заявки
        result = await db_session.execute(select(BackfillRequest))
        requests = result.scalars().all()

        if not requests:
            logging.info("Новых заявок нет.")
            return

        for req in requests:
            logging.info(f"Найдена заявка для пользователя {req.user_id}. Начинаю обработку.")
            # Для каждой заявки вызываем нашу старую добрую функцию дозагрузки
            await backfill_user_channels(req.user_id)
            
            # После успешной обработки удаляем заявку
            await db_session.delete(req)
        
        await db_session.commit()
        logging.info("Все заявки обработаны.")


async def main():
    await create_db()
    logging.info("Worker: Database tables checked/created.")

    async with client:
        logging.info("Клиент Telethon запущен.")
        # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
        # Теперь воркер будет выполнять ДВЕ задачи одновременно:
        # 1. Регулярно собирать новые посты (periodic_task)
        # 2. Регулярно проверять и обрабатывать заявки на дозагрузку (process_backfill_requests)
        while True:
            await asyncio.gather(
                periodic_task(),
                process_backfill_requests()
            )
            logging.info(f"Все задачи выполнены. Засыпаю на {SLEEP_TIME / 60:.1f} минут...")
            await asyncio.sleep(SLEEP_TIME)


if __name__ == "__main__":
    asyncio.run(main())