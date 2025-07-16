import asyncio
import logging
import os
import boto3
import io
from dotenv import load_dotenv
from telethon import TelegramClient, types
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

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

API_ID = int(API_ID_STR)
POST_LIMIT = 10  # Лимит для первоначальной загрузки и периодических проверок
SLEEP_TIME = 300

# --- Инициализация клиентов ---
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=S3_REGION
)
md = MarkdownIt()

# --- ИЗМЕНЕНО: Новая, переиспользуемая функция для сбора постов ---
async def fetch_posts_for_channel(channel: Channel, db_session: AsyncSession, post_limit: int = POST_LIMIT):
    """
    Собирает и сохраняет последние посты для ОДНОГО конкретного канала.
    """
    logging.info(f"Начинаю сбор постов для канала «{channel.title}»...")
    try:
        # 1. Получаем entity канала
        target = channel.username if channel.username else channel.id
        entity = await client.get_entity(target)

        # 2. ОПТИМИЗАЦИЯ: Получаем ID последних постов из БД одним запросом
        existing_posts_query = (
            select(Post.message_id)
            .where(Post.channel_id == channel.id)
            .order_by(Post.date.desc())
            .limit(post_limit * 2) # Берем с запасом
        )
        existing_post_ids = set((await db_session.execute(existing_posts_query)).scalars().all())
        logging.info(f"Найдено {len(existing_post_ids)} уже сохраненных постов для «{channel.title}».")


        # 3. Итерируемся по сообщениям
        async for message in client.iter_messages(entity, limit=post_limit):
            if not message or (not message.text and not message.media):
                continue

            # ОПТИМИЗАЦИЯ: Проверяем наличие поста в памяти, а не в БД
            if message.id in existing_post_ids:
                logging.info(f"Достигнут уже сохраненный пост {message.id} в «{channel.title}». Завершаю сбор для этого канала.")
                break
            
            # (Логика загрузки медиа и сохранения поста остается без изменений)
            media_type = None
            media_url = None
            if message.media:
                if message.photo: media_type = 'photo'
                elif message.video: media_type = 'video'
                elif message.audio: media_type = 'audio'
                
                if media_type:
                    try:
                        file_in_memory = io.BytesIO()
                        await client.download_media(message, file=file_in_memory)
                        file_in_memory.seek(0)
                        file_extension = getattr(message.file, 'ext', '.dat') or '.dat'
                        file_key = f"media/{channel.id}/{message.id}{file_extension}"
                        s3_client.upload_fileobj(
                            file_in_memory,
                            S3_BUCKET_NAME,
                            file_key,
                            ExtraArgs={'ContentType': getattr(message.file, 'mime_type', 'application/octet-stream')}
                        )
                        media_url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{file_key}"
                        logging.info(f"Загружен файл в S3: {file_key}")
                    except Exception as e:
                        logging.error(f"Ошибка загрузки медиа из поста {message.id} в «{channel.title}»: {e}")
                        media_type, media_url = None, None
            
            html_text = md.render(message.text) if message.text else None

            new_post = Post(
                channel_id=channel.id, message_id=message.id,
                text=html_text, 
                date=message.date,
                media_type=media_type, media_url=media_url
            )
            db_session.add(new_post)
            logging.info(f"Сохранен новый пост {message.id} из «{channel.title}»")

        await db_session.commit()

    except (ValueError, TypeError):
        logging.warning(f"Канал «{channel.title}» приватный или недоступен. Пропускаю.")
    except Exception as e:
        logging.error(f"Неизвестная ошибка при обработке канала «{channel.title}»: {e}")
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