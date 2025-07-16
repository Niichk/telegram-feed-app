import asyncio
import logging
import os
import boto3
import io
from dotenv import load_dotenv
from telethon import TelegramClient, types
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from database.engine import session_maker
from database.models import Channel, Post
from telethon.sessions import StringSession
from io import BytesIO
from datetime import datetime
from markdown_it import MarkdownIt

# Настройка
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
POST_LIMIT = 10  # Лимит для первоначальной загрузки и периодических проверок
SLEEP_TIME = 300

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
md = MarkdownIt()

# --- ИЗМЕНЕНО: Новая, переиспользуемая функция для сбора постов ---
async def upload_media_to_s3(message: types.Message, channel_id: int) -> dict | None:
    """Загружает медиафайл в S3 и возвращает словарь с его данными."""
    media_type = None
    if isinstance(message.media, types.MessageMediaPhoto):
        media_type = 'photo'
    elif isinstance(message.media, types.MessageMediaDocument):
        # Try to distinguish between video and audio by mime_type
        mime_type = getattr(message.media.document, 'mime_type', '') if hasattr(message.media, 'document') else ''
        if mime_type.startswith('video/'):
            media_type = 'video'
        elif mime_type.startswith('audio/'):
            media_type = 'audio'
        else:
            media_type = None

    if not media_type:
        return None

    try:
        file_in_memory = io.BytesIO()
        await client.download_media(message, file=file_in_memory)
        file_in_memory.seek(0)

        if (
            isinstance(message.media, types.MessageMediaDocument)
            and hasattr(message.media, 'document')
            and message.media.document
            and not isinstance(message.media.document, types.DocumentEmpty)
        ):
            attributes = getattr(message.media.document, 'attributes', [])
            file_name = None
            if attributes and hasattr(attributes[0], 'file_name'):
                file_name = attributes[0].file_name
            file_extension = os.path.splitext(file_name)[1] if file_name else '.dat'
            content_type = getattr(message.media.document, 'mime_type', 'application/octet-stream')
        elif isinstance(message.media, types.MessageMediaPhoto) and hasattr(message.media, 'photo') and message.media.photo:
            file_extension = '.jpg'
            content_type = 'image/jpeg'
        else:
            file_extension = '.dat'
            content_type = 'application/octet-stream'

        file_key = f"media/{channel_id}/{message.id}{file_extension}"

        s3_client.upload_fileobj(
            file_in_memory,
            S3_BUCKET_NAME,
            file_key,
            ExtraArgs={'ContentType': content_type}
        )
        
        media_url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{file_key}"
        logging.info(f"Загружен файл в S3: {file_key}")
        
        return {"type": media_type, "url": media_url}

    except Exception as e:
        logging.error(f"Ошибка загрузки медиа из поста {message.id}: {e}")
        return None


async def fetch_posts_for_channel(channel: Channel, db_session: AsyncSession, post_limit: int = POST_LIMIT):
    logging.info(f"Начинаю сбор постов для канала «{channel.title}»...")
    try:
        entity = await client.get_entity(channel.username or channel.id)
        if isinstance(entity, list):
            if len(entity) == 0:
                logging.warning(f"Не удалось получить entity для канала {channel.username or channel.id}")
                return
            entity = entity[0]

        existing_post_ids = set((await db_session.execute(
            select(Post.message_id).where(Post.channel_id == channel.id).order_by(Post.date.desc()).limit(50)
        )).scalars().all())

        async for message in client.iter_messages(entity, limit=post_limit):
            if not message or (not message.text and not message.media):
                continue
            
            if not message.grouped_id and message.id in existing_post_ids:
                logging.info(f"Достигнут уже сохраненный пост {message.id} в «{channel.title}». Завершаю сбор.")
                break
            
            media_item = await upload_media_to_s3(message, channel.id)
            html_text = md.render(message.text) if message.text else None

            # --- Обработка альбомов (без изменений) ---
            if message.grouped_id:
                existing_post_query = await db_session.execute(
                    select(Post).where(Post.grouped_id == message.grouped_id)
                )
                existing_album_post = existing_post_query.scalars().first()
                
                if existing_album_post:
                    if media_item and media_item not in existing_album_post.media:
                        existing_album_post.media.append(media_item)
                        logging.info(f"Добавлен медиа к альбому {message.grouped_id}.")
                else:
                    try:
                        new_post = Post(
                            channel_id=channel.id, message_id=message.id, date=message.date,
                            text=html_text, grouped_id=message.grouped_id,
                            media=[media_item] if media_item else []
                        )
                        db_session.add(new_post)
                        await db_session.commit()
                        logging.info(f"Создан новый пост для альбома {message.grouped_id}.")
                    except IntegrityError:
                        await db_session.rollback()
                        logging.warning(f"Конфликт при создании поста для альбома {message.grouped_id}.")
            
            # --- ИСПРАВЛЕННАЯ ОБРАБОТКА ОДИНОЧНЫХ ПОСТОВ ---
            else:
                try:
                    new_post = Post(
                        channel_id=channel.id, message_id=message.id, date=message.date,
                        text=html_text, media=[media_item] if media_item else []
                    )
                    db_session.add(new_post)
                    # Сохраняем пост в базу СРАЗУ ЖЕ
                    await db_session.commit()
                    logging.info(f"Сохранен одиночный пост {message.id} из «{channel.title}»")
                except IntegrityError:
                    # Если такой пост уже есть, откатываем транзакцию и идем дальше
                    await db_session.rollback()
                    logging.warning(f"Пост {message.id} уже существует, пропуск.")
                except Exception as e:
                    # Если любая другая ошибка, откатываем и логируем
                    await db_session.rollback()
                    logging.error(f"Не удалось сохранить пост {message.id}: {e}")

        # Финальный коммит нужен для сохранения изменений в альбомах (когда медиа только добавляется)
        await db_session.commit()

    except Exception as e:
        logging.error(f"Критическая ошибка при обработке канала «{channel.title}»: {e}")
        await db_session.rollback()


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


async def main():
    async with client:
        logging.info("Клиент Telethon запущен.")
        while True:
            await periodic_task()
            logging.info(f"Засыпаю на {SLEEP_TIME / 60:.1f} минут...")
            await asyncio.sleep(SLEEP_TIME)


if __name__ == "__main__":
    asyncio.run(main())