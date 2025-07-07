from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List

from database import requests as db
from database import schemas
from database.engine import create_db

app = FastAPI(title="Feed Reader API")

@app.on_event("startup")
async def on_startup():
    await create_db()

# Настройка CORS
origins = [
    "https://frontend-app-production-c1ed.up.railway.app",
    "http://localhost",
    "http://127.0.0.1",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# --- УДАЛЕН БЛОК app.mount("/static", ...) ---

@app.get("/api/feed/{user_id}", response_model=List[schemas.PostInFeed])
async def get_feed_for_user(user_id: int, page: int = 1):
    limit = 20
    offset = (page - 1) * limit
    feed = await db.get_user_feed(user_id, limit=limit, offset=offset)
    return feed
