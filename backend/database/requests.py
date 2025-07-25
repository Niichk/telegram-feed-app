from .models import User, Channel, Subscription, Post, BackfillRequest
from sqlalchemy.dialects.postgresql import insert
from .engine import session_maker
from sqlalchemy import select, and_
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession


async def add_subscription(
    session: AsyncSession,
    user_id: int,
    channel_id: int,
    channel_title: str,
    channel_un: str
) -> tuple[str, Channel | None]:

    # Шаг 1: Ищем подписку
    sub_query = select(Subscription).where(
        Subscription.user_id == user_id,
        Subscription.channel_id == channel_id
    )
    existing_subscription = (await session.execute(sub_query)).scalars().first()

    # Шаг 2: Если подписка найдена, получаем объект канала и выходим.
    if existing_subscription:
        channel = await session.get(Channel, channel_id)
        return f"Вы уже подписаны на канал «{channel.title if channel else ''}».", None

    # Шаг 3: Если подписки НЕТ, начинаем работу.
    user = await session.get(User, user_id)
    if not user:
        # --- ИЗМЕНЕНО: Создаем пользователя только с ID ---
        user = User(id=user_id)
        session.add(user)

    # --- ДОБАВЛЕНО: Проверка лимита подписок ---
    if user.subscription_count >= 10:
        return "🚫 Превышен лимит в 10 подписок. Чтобы добавить новый канал, сначала отпишитесь от старого.", None
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---
    
    channel = await session.get(Channel, channel_id)
    if not channel:
        channel = Channel(id=channel_id, title=channel_title, username=channel_un)
        session.add(channel)

    # Шаг 4: Создаем новую подписку.
    new_subscription = Subscription(user_id=user_id, channel_id=channel_id)
    session.add(new_subscription)

    user.subscription_count += 1

    # Шаг 5: Сохраняем изменения.
    await session.commit()

    # Возвращаем сообщение и объект свежесозданного канала
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
        # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
        user = await session.get(User, user_id)
        if user and user.subscription_count > 0:
            user.subscription_count -= 1
        # --- КОНЕЦ ИЗМЕНЕНИЯ ---

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