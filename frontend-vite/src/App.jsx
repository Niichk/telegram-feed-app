import React, { useState, useEffect, useCallback, useRef, Component } from 'react';

// --- Вспомогательные компоненты --- (без изменений)
class ErrorBoundary extends Component {
    constructor(props) {
        super(props);
        this.state = { hasError: false, error: null };
    }

    static getDerivedStateFromError(error) {
        return { hasError: true, error: error };
    }

    componentDidCatch(error, errorInfo) {
        console.error("ErrorBoundary caught an error", error, errorInfo);
    }

    render() {
        if (this.state.hasError) {
            return (
                <div className="post-card">
                    <div className="post-content" style={{ textAlign: 'center' }}>
                        <p>🤕</p>
                        <p>Произошла ошибка при отображении этого поста.</p>
                        <button onClick={() => this.setState({ hasError: false, error: null })} className="comment-button">
                            Попробовать снова
                        </button>
                    </div>
                </div>
            );
        }
        return this.props.children;
    }
}

const SkeletonCard = () => (
    <div className="skeleton-card">
        <div className="skeleton-header">
            <div className="skeleton skeleton-avatar"></div>
            <div className="skeleton-info">
                <div className="skeleton skeleton-line skeleton-line-short"></div>
                <div className="skeleton skeleton-line" style={{ width: '40%' }}></div>
            </div>
        </div>
        <div className="skeleton skeleton-line skeleton-line-long"></div>
        <div className="skeleton skeleton-line skeleton-line-long"></div>
        <div className="skeleton skeleton-line skeleton-line-short"></div>
    </div>
);

const PostCard = React.memo(({ post }) => {
    const formatDate = (dateString) => new Date(dateString).toLocaleString('ru-RU', { day: 'numeric', month: 'long', hour: '2-digit', minute: '2-digit' });
    const getPostUrl = (p) => p.channel.username ? `https://t.me/${p.channel.username}/${p.message_id}` : `https://t.me/c/${String(p.channel.id).substring(4)}/${p.message_id}`;
    const postUrl = getPostUrl(post);
    const hasVisualMedia = post.media && post.media.some(item => item.type === 'photo' || item.type === 'video');
    const channelUrl = post.channel.username ? `https://t.me/${post.channel.username}` : '#';

    return (
        <div className={`post-card ${hasVisualMedia ? 'post-card-with-media' : ''}`}>
            <div className="post-header">
                <a href={channelUrl} target="_blank" rel="noopener noreferrer" className="channel-link">
                    <div className="channel-avatar">
                        {post.channel.avatar_url ? (
                            <img src={post.channel.avatar_url} alt={post.channel.title} loading="lazy" />
                        ) : (
                            <div className="avatar-placeholder">{post.channel.title ? post.channel.title.charAt(0) : ''}</div>
                        )}
                    </div>
                    <div className="header-info">
                        <div className="channel-title">{post.channel.title}</div>
                        <div className="post-date">{formatDate(post.date)}</div>
                    </div>
                </a>
            </div>

            {post.forwarded_from && (
                <a 
                    href={
                        post.forwarded_from.username 
                        ? `https://t.me/${post.forwarded_from.username}` 
                        : (post.forwarded_from.channel_id ? `https://t.me/c/${post.forwarded_from.channel_id}` : '#')
                    }
                    target="_blank" 
                    rel="noopener noreferrer" 
                    className="forwarded-from-banner"
                >
                    Переслано из <b>{post.forwarded_from.from_name || 'Неизвестный источник'}</b>
                </a>
            )}
            
            <PostMedia media={post.media} />
            
            {(post.text || postUrl) && (
                 <div className="post-content">
                    {post.text && <div className="post-text" dangerouslySetInnerHTML={{ __html: post.text }} />}
                    <a href={postUrl} target="_blank" rel="noopener noreferrer" className="comment-button">Комментировать</a>
                </div>
            )}

            {(post.reactions?.length > 0 || post.views) && (
                <div className="post-footer">
                    <div className="reactions">
                        {post.reactions?.map(reaction => (
                            <span key={reaction.emoticon} className="reaction-item">
                                {reaction.emoticon}
                                <span className="reaction-count">{reaction.count}</span>
                            </span>
                        ))}
                    </div>
                    <div className="views">
                        {post.views && `👁️ ${post.views}`}
                    </div>
                </div>
            )}
        </div>
    );
});

const PostMedia = React.memo(({ media }) => {
    const [currentIndex, setCurrentIndex] = useState(0);
    const [imageErrors, setImageErrors] = useState(new Set());

    if (!media || media.length === 0) return null;
    
    const visualMedia = media.filter(item => item.type === 'photo' || item.type === 'video' || item.type === 'gif');
    const audioMedia = media.filter(item => item.type === 'audio');

    if (visualMedia.length === 0 && audioMedia.length === 0) return null;

    const goToPrevious = () => setCurrentIndex(prev => (prev === 0 ? visualMedia.length - 1 : prev - 1));
    const goToNext = () => setCurrentIndex(prev => (prev === visualMedia.length - 1 ? 0 : prev + 1));

    const handleImageError = useCallback((url) => {
        setImageErrors(prev => new Set([...prev, url]));
    }, []);

    return (
        <>
            {visualMedia.length > 0 && (
                <div className="post-media-gallery">
                    {visualMedia.map((item, index) => (
                        <div key={item.url || index} className={`slider-item ${index === currentIndex ? 'active' : ''}`}>
                            {item.type === 'photo' && (
                                imageErrors.has(item.url) ? (
                                    <div className="image-placeholder">Не удалось загрузить изображение</div>
                                ) : (
                                    <img 
                                        src={item.url} 
                                        className="post-media-visual" 
                                        alt={`Изображение ${index + 1}`} 
                                        loading="lazy"
                                        onError={() => handleImageError(item.url)}
                                    />
                                )
                            )}

                            {item.type === 'gif' && (
                                imageErrors.has(item.url) ? (
                                    <div className="image-placeholder">Не удалось загрузить GIF</div>
                                ) : (
                                    <img 
                                        src={item.url} 
                                        className="post-media-visual gif-media" 
                                        alt={`GIF ${index + 1}`} 
                                        loading="lazy"
                                        onError={() => handleImageError(item.url)}
                                    />
                                )
                            )}
                            
                            {item.type === 'video' && (
                                <div className="video-container">
                                    <video 
                                        muted 
                                        playsInline 
                                        className="post-media-visual"
                                        preload="metadata"
                                        poster={item.thumbnail_url || undefined}
                                        controls={false}
                                        onLoadedMetadata={(e) => {
                                            if (!item.thumbnail_url) {
                                                e.target.currentTime = 0.1;
                                            }
                                        }}
                                        onError={(e) => {
                                            console.error('Video failed to load:', item.url);
                                        }}
                                    >
                                        <source src={item.url} type="video/mp4" />
                                        <source src={item.url} />
                                        Ваш браузер не поддерживает воспроизведение видео.
                                    </video>
                                    
                                    <div 
                                        className="video-play-overlay"
                                        onClick={(e) => {
                                            const container = e.target.closest('.video-container');
                                            const video = container.querySelector('video');
                                            
                                            e.target.style.display = 'none';
                                            if (video) {
                                                video.controls = true;
                                                video.currentTime = 0;
                                                video.play();
                                            }
                                        }}
                                    >
                                        <div className="video-play-button">▶️</div>
                                    </div>
                                </div>
                            )}
                        </div>
                    ))}
                    {visualMedia.length > 1 && (
                        <>
                            <button onClick={goToPrevious} className="slider-button prev">&lt;</button>
                            <button onClick={goToNext} className="slider-button next">&gt;</button>
                            <div className="slider-counter">{`${currentIndex + 1} / ${visualMedia.length}`}</div>
                        </>
                    )}
                </div>
            )}
            {audioMedia.map((item, index) => (
                <audio key={index} controls className="post-media-audio">
                    <source src={item.url} />
                </audio>
            ))}
        </>
    );
});

function Header({ onRefresh, onScrollUp }) {
    const handleButtonPress = (e, action) => {
        e.preventDefault();
        e.stopPropagation();
        
        const button = e.currentTarget;
        button.classList.add('button--active');
        
        if (window.Telegram?.WebApp?.HapticFeedback) {
            window.Telegram.WebApp.HapticFeedback.impactOccurred('light');
        }
        
        action();
        setTimeout(() => button.classList.remove('button--active'), 150);
    };
    
    return (
        <header className="app-header">
            <button 
                onTouchEnd={(e) => handleButtonPress(e, onScrollUp)}
                onClick={(e) => !e.touches && handleButtonPress(e, onScrollUp)}
                className="header-button"
            >
                Вверх ⬆️
            </button>
            <button 
                onTouchEnd={(e) => handleButtonPress(e, onRefresh)}
                onClick={(e) => !e.touches && handleButtonPress(e, onRefresh)}
                className="header-button"
            >
                Обновить 🔄
            </button>
        </header>
    );
}

function RadialLoader() {
  return (
    <div className="radial-loader">
      {[...Array(12)].map((_, i) => <div key={i}></div>)}
    </div>
  );
}

// --- ИСПРАВЛЕННЫЙ ОСНОВНОЙ КОМПОНЕНТ ---
function App() {
    const [posts, setPosts] = useState([]);
    const [error, setError] = useState(null);
    const [hasMore, setHasMore] = useState(true);
    const [isFetching, setIsFetching] = useState(false);
    const [initialLoading, setInitialLoading] = useState(true);
    const [isBackfilling, setIsBackfilling] = useState(false);
    
    const page = useRef(1);
    const loader = useRef(null);
    const isFetchingRef = useRef(false);

    // --- ЗАГРУЗКА ИСТОРИИ (старые посты) ---
    const fetchPosts = useCallback(async (isRefresh = false) => {
        if (isFetchingRef.current || (!hasMore && !isRefresh)) return;
        
        isFetchingRef.current = true;
        setIsFetching(true);
        
        if (isRefresh) {
            page.current = 1;
            setPosts([]); // Очищаем посты при ручном обновлении
            setError(null);
            setHasMore(true);
            setIsBackfilling(false);
        }

        try {
            const response = await fetch(
                `https://telegram-feed-app-production.up.railway.app/api/feed/?page=${page.current}`,
                { headers: { 'Authorization': `tma ${window.Telegram.WebApp.initData}` } }
            );

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({ detail: `HTTP ошибка: ${response.status}` }));
                throw new Error(errorData.detail);
            }

            const { posts: newPosts, status } = await response.json();
            
            setPosts(prev => {
                const existingIds = new Set(prev.map(p => `${p.channel.id}-${p.message_id}`));
                const uniqueNewPosts = newPosts.filter(p => !existingIds.has(`${p.channel.id}-${p.message_id}`));
                return isRefresh ? uniqueNewPosts : [...prev, ...uniqueNewPosts];
            });

            page.current += 1;
            
            if (status === "backfilling") {
                setIsBackfilling(true);
                setHasMore(false); // Мы достигли конца истории, теперь ждем дозагрузки
            } else {
                setHasMore(newPosts.length > 0);
            }
        } catch (err) {
            console.error('Fetch history error:', err);
            setError(err.message);
            setHasMore(false);
        } finally {
            isFetchingRef.current = false;
            setIsFetching(false);
            if (isRefresh || initialLoading) setInitialLoading(false);
        }
    }, [hasMore, initialLoading]);

    // --- ЭФФЕКТЫ ---

    // 1. Инициализация и загрузка первой страницы истории
    useEffect(() => {
        const tg = window.Telegram.WebApp;
        const applyTheme = () => document.body.className = tg.colorScheme;

        if (tg) {
            tg.ready();
            applyTheme();
            tg.onEvent('themeChanged', applyTheme);
            
            if (tg.initData) {
                fetchPosts(); // Загружаем первую порцию старых постов
            } else {
                setError("Не удалось определить пользователя Telegram. Откройте приложение через бота.");
                setInitialLoading(false);
            }

            return () => tg.offEvent('themeChanged', applyTheme);
        } else {
             setError("Не удалось загрузить Telegram Web App API.");
             setInitialLoading(false);
        }
    }, [fetchPosts]); // fetchPosts добавлен в зависимости

    // 2. ПОДКЛЮЧЕНИЕ К SSE ДЛЯ РЕАЛЬНОГО ВРЕМЕНИ
    useEffect(() => {
        const initData = window.Telegram?.WebApp?.initData;
        if (!initData || initialLoading) { // Не запускаем SSE, пока не загрузится первая страница
            return;
        }

        console.log("Connecting to SSE...");
        const eventSource = new EventSource(`https://telegram-feed-app-production.up.railway.app/api/feed/stream/?authorization=tma ${initData}`);
        
        eventSource.onmessage = (event) => {
            try {
                const newPost = JSON.parse(event.data);
                console.log("New post via SSE:", newPost);
                
                setPosts(prevPosts => {
                    // Проверка на дубликат
                    const isDuplicate = prevPosts.some(p => p.id === newPost.id);
                    if (!isDuplicate) {
                        window.Telegram?.WebApp?.HapticFeedback?.notificationOccurred('success');
                        return [newPost, ...prevPosts];
                    }
                    return prevPosts;
                });
            } catch (e) {
                console.error("Failed to parse SSE data:", e);
            }
        };

        eventSource.onerror = (err) => {
            console.error("EventSource failed:", err);
            eventSource.close();
        };

        // Закрываем соединение при размонтировании компонента
        return () => {
            console.log("Closing SSE connection.");
            eventSource.close();
        };
    }, [initialLoading]); // Переподключаемся, когда initialLoading становится false


    // 3. Infinite scroll для подгрузки истории
    useEffect(() => {
        const observer = new IntersectionObserver(
            (entries) => {
                if (entries[0].isIntersecting && hasMore && !isFetching) {
                    fetchPosts();
                }
            }, { rootMargin: '200px' }
        );

        const currentLoader = loader.current;
        if (currentLoader) observer.observe(currentLoader);

        return () => { if (currentLoader) observer.unobserve(currentLoader); };
    }, [hasMore, isFetching, fetchPosts]);

    const handleRefresh = useCallback(() => {
        if (window.Telegram?.WebApp?.HapticFeedback) {
            window.Telegram.WebApp.HapticFeedback.impactOccurred('light');
        }
        fetchPosts(true);
    }, [fetchPosts]);

    const scrollToTop = () => window.scrollTo({ top: 0, behavior: 'smooth' });

    if (initialLoading) {
        return (
            <>
                <Header onRefresh={() => {}} onScrollUp={() => {}} />
                <div className="feed-container">
                    {[...Array(5)].map((_, i) => <SkeletonCard key={i} />)}
                </div>
            </>
        );
    }

    if (error) {
        return <div className="status-message">Ошибка: {error}</div>;
    }
    
    if (posts.length === 0 && !isFetching && !initialLoading) {
        if (hasSubscriptions || isLoadingNewChannel) {
            return (
                <>
                    <Header onRefresh={handleRefresh} onScrollUp={scrollToTop} />
                    <div className="feed-container">
                        {isLoadingNewChannel && (
                            <div className="status-message">
                                Загружаем посты из нового канала... 📥
                            </div>
                        )}
                        {[...Array(3)].map((_, i) => <SkeletonCard key={i} />)}
                    </div>
                </>
            );
        } else {
            return <div className="status-message">Ваша лента пока пуста. Добавьте каналы через бота!</div>;
        }
    }

    return (
        <>
            <Header onRefresh={handleRefresh} onScrollUp={scrollToTop} />
            <div id="refresh-indicator" className="pull-to-refresh-indicator">
                <RadialLoader />
            </div>
            <div className="feed-container">
                {posts.map(post => (
                    <ErrorBoundary key={`${post.channel.id}-${post.message_id}`}>
                        <PostCard post={post} />
                    </ErrorBoundary>
                ))}

                {isFetching && !initialLoading && (
                    <div className="loader-container">
                        <RadialLoader />
                    </div>
                )}

                {isBackfilling && (
                    <div className="status-message">
                        Догружаем старые посты... ⏳<br/><small>Потяните, чтобы обновить.</small>
                    </div>
                )}

                <div ref={loader} />
            </div>
        </>
    );
}

export default App;