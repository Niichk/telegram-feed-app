from aiogram import Router, types
from aiogram.filters import Command

router = Router()

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("Привет! Чтобы добавить канал в ленту, просто перешли мне любой пост из него. 🚀")