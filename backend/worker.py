import asyncio
import logging
import os
from dotenv import load_dotenv

from telethon import TelegramClient
from sqlalchemy import select

from database.engine import session_maker
from database.models import Channel, Post

# Настройка
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

api_id_str = os.getenv("API_ID")
if api_id_str is None:
    raise ValueError("API_ID environment variable is not set")
API_ID = int(api_id_str)
API_HASH = os.getenv("API_HASH")
if API_HASH is None:
    raise ValueError("API_HASH environment variable is not set")
SESSION_NAME = "worker_session"
POST_LIMIT = 10
SLEEP_TIME = 300

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

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
                # --- РЕШЕНИЕ ПРОБЛЕМЫ ---
                entity = None
                try:
                    # Пытаемся "найти" канал. Для публичных каналов это сработает,
                    # даже если ваш аккаунт на них не подписан.
                    # Для приватных - вызовет ошибку, которую мы поймаем.
                    target = channel.username if channel.username else channel.id
                    entity = await client.get_entity(target)
                    # Ensure entity is not a list
                    if isinstance(entity, list):
                        entity = entity[0]
                except (ValueError, TypeError):
                    logging.warning(f"Канал «{channel.title}» приватный или недоступен. Пропускаю.")
                    continue # Переходим к следующему каналу в цикле
                # -------------------------

                async for message in client.iter_messages(entity, limit=POST_LIMIT):
                    if not message or (not message.text and not message.photo and not message.video and not message.audio):
                        continue
                    
                    post_exists = await db_session.execute(
                        select(Post).where(Post.message_id == message.id)
                    )
                    if post_exists.scalars().first():
                        logging.info(f"Достигнут уже сохраненный пост в канале «{channel.title}». Перехожу к следующему.")
                        break

                    media_type = None
                    media_url = None

                    if message.photo or message.video or message.audio:
                        if message.video: media_type = 'video'
                        elif message.photo: media_type = 'photo'
                        elif message.audio: media_type = 'audio'
                        
                        file_path = await client.download_media(message, file="static/media")
                        if file_path is not None:
                            file_name = os.path.basename(file_path)
                            media_url = f"http://127.0.0.1:8000/static/media/{file_name}"
                            logging.info(f"Скачан файл: {file_name}")
                        else:
                            logging.warning("Не удалось скачать медиафайл.")
                            media_url = None

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