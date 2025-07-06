import asyncio
import logging
import os
import boto3
from dotenv import load_dotenv
from telethon import TelegramClient
from sqlalchemy import select
from database.engine import session_maker
from database.models import Channel, Post
from telethon.sessions import StringSession
from io import BytesIO

# Настройка
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Получаем переменные окружения с надежными проверками ---
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_REGION = os.getenv("S3_REGION")
API_ID_STR = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("TELETHON_SESSION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Проверяем, что все необходимые переменные заданы
if not all([API_ID_STR, API_HASH, SESSION_STRING, S3_BUCKET_NAME, S3_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]):
    raise ValueError("Одна или несколько переменных окружения не установлены!")

API_ID = int(API_ID_STR) if API_ID_STR is not None else 0
POST_LIMIT = 10
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

async def fetch_and_save_posts():
    logging.info("Начинаю сбор постов...")
    async with session_maker() as db_session:
        result = await db_session.execute(select(Channel))
        channels = result.scalars().all()

        if not channels:
            logging.info("В базе нет каналов для отслеживания.")
            return

        for channel in channels:
            try:
                entity = None
                try:
                    target = channel.username if channel.username else channel.id
                    entity = await client.get_entity(target)
                    # Ensure entity is not a list
                    if isinstance(entity, list):
                        if entity:
                            entity = entity[0]
                        else:
                            logging.warning(f"Не удалось получить entity для канала «{channel.title}». Пропускаю.")
                            continue
                except (ValueError, TypeError):
                    logging.warning(f"Канал «{channel.title}» приватный или недоступен. Пропускаю.")
                    continue

                async for message in client.iter_messages(entity, limit=POST_LIMIT):
                    # Пропускаем служебные сообщения без контента
                    if not message or (not message.text and not message.media):
                        continue
                    
                    # Проверяем, нет ли уже такого поста в базе
                    post_exists = await db_session.execute(
                        select(Post).where(Post.message_id == message.id)
                    )
                    if post_exists.scalars().first():
                        logging.info(f"Достигнут уже сохраненный пост в канале «{channel.title}». Перехожу к следующему.")
                        break

                    media_type = None
                    media_url = None

                    if message.media:
                        if message.video: media_type = 'video'
                        elif message.photo: media_type = 'photo'
                        elif message.audio: media_type = 'audio'

                        if media_type: # Если это поддерживаемый тип медиа
                            try:
                                file_in_memory = BytesIO()
                                await client.download_media(message, file=file_in_memory)
                                file_in_memory.seek(0)

                                file_extension = getattr(message.file, 'ext', '.dat') or '.dat'
                                file_name = f"{message.id}{file_extension}"

                                s3_client.upload_fileobj(
                                    file_in_memory, S3_BUCKET_NAME, file_name,
                                    ExtraArgs={'ACL': 'public-read', 'ContentType': message.file.mime_type}
                                )

                                media_url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{file_name}"
                                logging.info(f"Загружен файл в S3: {file_name}")

                            except Exception as e:
                                logging.error(f"Ошибка загрузки медиа в S3: {e}")
                                media_type = None
                                media_url = None
                    
                    # --- ИСПРАВЛЕНИЕ КРИТИЧЕСКОЙ ОШИБКИ ---
                    # Создаем пост, даже если в нем только текст
                    new_post = Post(
                        channel_id=channel.id, message_id=message.id,
                        text=message.text, date=message.date,
                        media_type=media_type, media_url=media_url
                    )
                    db_session.add(new_post)
                    logging.info(f"Сохранен новый пост из канала «{channel.title}»")

                await db_session.commit()

            except Exception as e:
                logging.error(f"Неизвестная ошибка при обработке канала «{channel.title}»: {e}")
                await db_session.rollback()

async def main():
    async with client:
        logging.info("Клиент Telethon запущен.")
        while True:
            await fetch_and_save_posts()
            logging.info(f"Засыпаю на {SLEEP_TIME / 60:.1f} минут...")
            await asyncio.sleep(SLEEP_TIME)

if __name__ == "__main__":
    asyncio.run(main())