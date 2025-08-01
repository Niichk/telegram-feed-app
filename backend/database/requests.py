from .models import User, Channel, Subscription, Post, BackfillRequest
from sqlalchemy.dialects.postgresql import insert
from .engine import session_maker
from sqlalchemy import select, and_
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
import logging
from typing import Optional


async def add_subscription(
    session: AsyncSession,
    user_id: int,
    channel_id: int,
    channel_title: str,
    channel_un: Optional[str]
) -> tuple[str, Channel | None]:

    logging.info(f"🔍 add_subscription вызвана: user_id={user_id}, channel_id={channel_id}, title={channel_title}")

    # Шаг 1: Ищем подписку
    sub_query = select(Subscription).where(
        Subscription.user_id == user_id,
        Subscription.channel_id == channel_id
    )
    existing_subscription = (await session.execute(sub_query)).scalars().first()

    if existing_subscription:
        channel = await session.get(Channel, channel_id)
        logging.info(f"ℹ️ Подписка уже существует для user_id={user_id}, channel_id={channel_id}")
        return f"Вы уже подписаны на канал «{channel.title if channel else ''}».", None

    # Шаг 3: Если подписки НЕТ, начинаем работу.
    user = await session.get(User, user_id)
    if not user:
        logging.info(f"👤 Создаю нового пользователя: user_id={user_id}")
        user = User(id=user_id, subscription_count=0)  # ✅ ИСПРАВЛЕНИЕ: Явно устанавливаем 0
        session.add(user)
    else:
        logging.info(f"👤 Пользователь найден: user_id={user_id}, subscription_count={user.subscription_count}")
        
        # ✅ ИСПРАВЛЕНИЕ: Защита от NULL значения
        if user.subscription_count is None:
            logging.warning(f"⚠️ subscription_count был NULL для user_id={user_id}, устанавливаю 0")
            user.subscription_count = 0

    # ✅ ИСПРАВЛЕНИЕ: Безопасная проверка лимита
    current_count = user.subscription_count or 0  # Защита от None
    if current_count >= 10:
        logging.warning(f"🚫 Превышен лимит подписок для user_id={user_id}: {current_count}")
        return "🚫 Превышен лимит в 10 подписок. Чтобы добавить новый канал, сначала отпишитесь от старого.", None
    
    channel = await session.get(Channel, channel_id)
    if not channel:
        logging.info(f"📺 Создаю новый канал: channel_id={channel_id}, title={channel_title}")
        channel = Channel(id=channel_id, title=channel_title, username=channel_un)
        session.add(channel)

    # Шаг 4: Создаем новую подписку.
    new_subscription = Subscription(user_id=user_id, channel_id=channel_id)
    session.add(new_subscription)

    user.subscription_count = current_count + 1
    logging.info(f"✅ Увеличиваю счетчик подписок для user_id={user_id}: {user.subscription_count}")

    # Шаг 5: Сохраняем изменения.
    await session.commit()
    logging.info(f"💾 Коммит выполнен для user_id={user_id}")

    return f"✅ Канал «{channel_title}» успешно добавлен! Начинаю загрузку последних постов...", channel

# Функция get_user_feed остается без изменений
async def get_user_feed(session: AsyncSession, user_id: int, limit: int = 20, offset: int = 0) -> list[Post]:
    feed_query = (
        select(Post)
        .join(Subscription, Post.channel_id == Subscription.channel_id)
        .where(Subscription.user_id == user_id)
        .options(selectinload(Post.channel))
        .order_by(Post.date.desc())
        .offset(offset)
        .limit(limit)
    )
    feed_result = await session.execute(feed_query)
    return list(feed_result.scalars().all())

async def get_user_subscriptions(session: AsyncSession, user_id: int) -> list[Channel]:
    """
    Возвращает список объектов Channel, на которые подписан пользователь.
    """
    subs_query = (
        select(Channel)
        .join(Subscription, Channel.id == Subscription.channel_id)
        .where(Subscription.user_id == user_id)
        .order_by(Channel.title) # Сортируем по алфавиту для удобства
    )

    result = await session.execute(subs_query)
    return list(result.scalars().all())

async def delete_subscription(session: AsyncSession, user_id: int, channel_id: int) -> bool:
    """
    Удаляет подписку пользователя на канал.
    Возвращает True, если удаление прошло успешно, и False, если подписка не была найдена.
    """
    sub_query = select(Subscription).where(
        Subscription.user_id == user_id,
        Subscription.channel_id == channel_id
    )
    existing_subscription = (await session.execute(sub_query)).scalars().first()

    if existing_subscription:
        user = await session.get(User, user_id)
        if user:
            # ✅ ИСПРАВЛЕНИЕ: Защита от NULL значения
            current_count = user.subscription_count or 0
            if current_count > 0:
                user.subscription_count = current_count - 1
            else:
                logging.warning(f"⚠️ subscription_count уже 0 для user_id={user_id}")
                user.subscription_count = 0

        await session.delete(existing_subscription)
        await session.commit()
        return True

    return False

async def create_backfill_request(session: AsyncSession, user_id: int):
    """
    Создает заявку на дозагрузку постов для пользователя.
    Если заявка уже существует, ничего не делает (благодаря on_conflict_do_nothing).
    """
    stmt = insert(BackfillRequest).values(user_id=user_id)
    stmt = stmt.on_conflict_do_nothing(index_elements=['user_id'])
    await session.execute(stmt)
    await session.commit()

async def check_backfill_request_exists(session: AsyncSession, user_id: int) -> bool:
    """
    Проверяет, существует ли уже заявка на дозагрузку для пользователя.
    Возвращает True, если заявка есть, иначе False.
    """
    stmt = select(BackfillRequest).where(BackfillRequest.user_id == user_id)
    result = await session.execute(stmt)
    return result.scalars().first() is not None