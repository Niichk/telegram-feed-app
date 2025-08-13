import os
from datetime import datetime, timedelta
from aiogram import Bot, Router, F, types
from aiogram.filters import Command
from sqlalchemy.ext.asyncio import AsyncSession
from aiogram.utils.keyboard import InlineKeyboardBuilder

from database.models import User

router = Router()

# --- КОНФИГУРАЦИЯ ---
# Берем токен из .env файла
PAYMENT_PROVIDER_TOKEN = os.getenv("PAYMENT_PROVIDER_TOKEN", "") # Добавляем "" по умолчанию
# Уникальный идентификатор нашего продукта
PREMIUM_PAYLOAD = "monthly_premium_subscription_v1" # Рекомендую добавить версию
# Цена в Telegram Stars
STARS_AMOUNT = 50 

def get_premium_keyboard():
    """Создает клавиатуру с кнопкой оплаты."""
    builder = InlineKeyboardBuilder()
    # pay=True - это самый правильный способ создать кнопку для счета
    builder.button(text=f"⭐️ Получить за {STARS_AMOUNT} звезд", pay=True) 
    return builder.as_markup()

# --- ОБРАБОТЧИКИ ---

# 1. Отправка счета по команде /premium
@router.message(Command("premium"))
async def cmd_premium(message: types.Message, bot: Bot):
    if not PAYMENT_PROVIDER_TOKEN:
        await message.answer("К сожалению, функция оплаты временно недоступна.")
        return

    # Используем метод .answer_invoice, как в статье
    await message.answer_invoice(
        title="Премиум-подписка (1 месяц)",
        description=f"Убирает лимит в 10 каналов и открывает доступ к будущим функциям. Стоимость: {STARS_AMOUNT} ⭐️.",
        payload=PREMIUM_PAYLOAD,
        provider_token=PAYMENT_PROVIDER_TOKEN,
        currency="XTR",
        prices=[
            types.LabeledPrice(label=f"Премиум на 1 месяц", amount=STARS_AMOUNT)
        ],
        reply_markup=get_premium_keyboard()
    )

# 2. Предварительная проверка (НОВЫЙ, ОБЯЗАТЕЛЬНЫЙ ШАГ ИЗ СТАТЬИ)
@router.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: types.PreCheckoutQuery):
    # Отвечаем Telegram, что мы готовы принять платеж
    await pre_checkout_query.answer(ok=True)

# 3. Обработка успешной оплаты (МОЯ ЛОГИКА С ОБНОВЛЕНИЕМ БД)
@router.message(F.successful_payment)
async def successful_payment_handler(message: types.Message, session: AsyncSession):
    if not message.from_user:
        return

    if message.successful_payment.invoice_payload == PREMIUM_PAYLOAD:
        user_id = message.from_user.id
        user = await session.get(User, user_id)

        if not user:
            user = User(id=user_id, is_premium=False)
            session.add(user)

        # Рассчитываем дату истечения подписки
        now = datetime.now()
        start_date = user.premium_expires_at or now
        if start_date < now:
            start_date = now
            
        user.premium_expires_at = start_date + timedelta(days=30)
        user.is_premium = True

        await session.commit()

        await message.answer(
            f"✅ Спасибо за поддержку! Премиум активирован до "
            f"{user.premium_expires_at.strftime('%d.%m.%Y')}. "
            f"Лимит на количество каналов снят."
        )

# 4. Команда поддержки (НОВЫЙ ШАГ ИЗ СТАТЬИ)
@router.message(Command("paysupport"))
async def pay_support_handler(message: types.Message):
    await message.answer(
        "По вопросам, связанным с оплатой и подпиской, "
        "пожалуйста, используйте команду /feedback, чтобы связаться с администратором."
    )