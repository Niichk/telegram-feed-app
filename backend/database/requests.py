from .models import User, Channel, Subscription, Base, Post
from .engine import session_maker
from sqlalchemy import select, and_
from sqlalchemy.orm import selectinload

# Функция для добавления пользователя и канала, а также оформления подписки
async def add_subscription(user_id: int, user_fn: str, user_un: str, 
                         channel_id: int, channel_title: str, channel_un: str):
    async with session_maker() as session:
        # --- Работаем с пользователем ---
        # Проверяем, существует ли пользователь в базе
        user = await session.get(User, user_id)
        if not user:
            # Если пользователя нет, создаем нового
            new_user = User(id=user_id, first_name=user_fn, username=user_un)
            session.add(new_user)
            await session.commit() # Сохраняем, чтобы получить user.id для подписки

        # --- Работаем с каналом ---
        # Проверяем, существует ли канал в базе
        channel = await session.get(Channel, channel_id)
        if not channel:
            # Если канала нет, создаем новый
            new_channel = Channel(id=channel_id, title=channel_title, username=channel_un)
            session.add(new_channel)
            await session.commit() # Сохраняем, чтобы получить channel.id для подписки

        # --- Работаем с подпиской ---
        # Проверяем, существует ли уже такая подписка
        subscription_query = select(Subscription).where(
            and_(
                Subscription.user_id == user_id,
                Subscription.channel_id == channel_id
            )
        )
        existing_subscription = await session.execute(subscription_query)
        if not existing_subscription.scalars().first():
            # Если подписки нет, создаем ее
            new_subscription = Subscription(user_id=user_id, channel_id=channel_id)
            session.add(new_subscription)
            await session.commit()
            return f"Вы успешно подписались на канал «{channel_title}»!"
        else:
            return f"Вы уже подписаны на канал «{channel_title}»."
        
async def get_user_feed(user_id: int, limit: int = 20, offset: int = 0):
    async with session_maker() as session:
        # Находим все ID каналов, на которые подписан пользователь
        subscriptions_query = select(Subscription.channel_id).where(Subscription.user_id == user_id)
        subscriptions_result = await session.execute(subscriptions_query)
        subscribed_channel_ids = subscriptions_result.scalars().all()

        if not subscribed_channel_ids:
            return []

        # Выбираем посты из этих каналов
        feed_query = (
            select(Post)
            .where(Post.channel_id.in_(subscribed_channel_ids))
            .options(selectinload(Post.channel))
            .order_by(Post.date.desc())
            .offset(offset) # <-- ДОБАВЛЯЕМ СМЕЩЕНИЕ
            .limit(limit)
        )

        feed_result = await session.execute(feed_query)
        return feed_result.scalars().all()