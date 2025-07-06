from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from typing import List

from database import requests as db
from database import schemas

app = FastAPI(title="Feed Reader API")

# "Монтируем" папку static, чтобы файлы из нее были доступны по URL
# Например, файл /static/media/photo.jpg будет доступен по http://.../static/media/photo.jpg
app.mount("/static", StaticFiles(directory="static"), name="static")

# --- ВАЖНО: Настройка CORS ---

origins = [
    "https://frontend-app-production-c1ed.up.railway.app", # <-- ВАШ АДРЕС ФРОНТЕНДА
    "http://localhost", # Для локальных тестов в будущем
    "http://127.0.0.1",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins, # <-- ИСПОЛЬЗУЕМ СПИСОК
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

@app.get("/api/feed/{user_id}", response_model=List[schemas.PostInFeed])
async def get_feed_for_user(user_id: int, page: int = 1):
    """
    Получает ленту постов для указанного пользователя.
    Поддерживает пагинацию через параметр ?page=...
    """
    limit = 20 # Количество постов на странице
    offset = (page - 1) * limit # Вычисляем смещение

    feed = await db.get_user_feed(user_id, limit=limit, offset=offset)

    # Теперь мы не выбрасываем ошибку, а просто возвращаем пустой список,
    # если посты на этой странице закончились.
    return feed