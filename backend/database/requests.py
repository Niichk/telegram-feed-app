from .models import User, Channel, Subscription, Base, Post
from .engine import session_maker
from sqlalchemy import select, and_
from sqlalchemy.orm import selectinload

# Функция для добавления пользователя и канала, а также оформления подписки
from sqlalchemy import select, and_
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from .models import User, Channel, Subscription, Post

async def add_subscription(
    session: AsyncSession, 
    user_id: int, 
    user_fn: str, 
    user_un: str, 
    channel_id: int, 
    channel_title: str, 
    channel_un: str
) -> str:
    
    # Шаг 1: Сначала ищем именно ПОДПИСКУ.
    sub_query = select(Subscription).where(
        Subscription.user_id == user_id,
        Subscription.channel_id == channel_id
    )
    existing_subscription = (await session.execute(sub_query)).scalars().first()
    
    # Шаг 2: Если подписка найдена, сразу выходим.
    if existing_subscription:
        return f"Вы уже подписаны на канал «{channel_title}»."

    # Шаг 3: Если подписки НЕТ, начинаем работу.
    # Ищем пользователя или готовим к созданию.
    user = await session.get(User, user_id)
    if not user:
        user = User(id=user_id, first_name=user_fn, username=user_un)
        session.add(user)

    # Ищем канал или готовим к созданию.
    channel = await session.get(Channel, channel_id)
    if not channel:
        channel = Channel(id=channel_id, title=channel_title, username=channel_un)
        session.add(channel)

    # Шаг 4: Создаем новую подписку.
    new_subscription = Subscription(user_id=user_id, channel_id=channel_id)
    session.add(new_subscription)

    # Шаг 5: Сохраняем ВСЕ изменения одной атомарной операцией.
    await session.commit()

    return f"✅ Канал «{channel_title}» успешно добавлен в вашу ленту!"
        
async def get_user_feed(session: AsyncSession, user_id: int, limit: int = 20, offset: int = 0) -> list[Post]:
    # Один запрос вместо двух
    feed_query = (
        select(Post)
        .join(Subscription, Post.channel_id == Subscription.channel_id) # <-- Соединяем Post и Subscription
        .where(Subscription.user_id == user_id) # <-- Фильтруем по user_id
        .options(selectinload(Post.channel)) # selectinload все еще полезен!
        .order_by(Post.date.desc())
        .offset(offset)
        .limit(limit)
    )

    feed_result = await session.execute(feed_query)
    return list(feed_result.scalars().all())