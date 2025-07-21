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
    const [hasSubscriptions, setHasSubscriptions] = useState(false);
    const [isLoadingNewChannel, setIsLoadingNewChannel] = useState(false);
    
    const page = useRef(1);
    const loader = useRef(null);
    const isFetchingRef = useRef(false);
    const pullStartY = useRef(0);
    const pullDeltaY = useRef(0);
    const isPulling = useRef(false);
    
    // Оптимизированная функция удаления дубликатов
    const removeDuplicates = useCallback((existingPosts, newPosts) => {
        const existingIds = new Set(existingPosts.map(p => `${p.channel.id}-${p.message_id}`));
        const uniqueNewPosts = newPosts.filter(p => !existingIds.has(`${p.channel.id}-${p.message_id}`));
        return [...existingPosts, ...uniqueNewPosts];
    }, []);

    // ИСПРАВЛЕНИЕ: fetchPosts определяется ПЕРВЫМ
    const fetchPosts = useCallback(async (isRefresh = false) => {
        if (isFetchingRef.current) return;
        if (!hasMore && !isRefresh) return;
        
        const controller = new AbortController();
        
        try {
            isFetchingRef.current = true;
            setIsFetching(true);
            
            if (isRefresh) {
                page.current = 1;
                setPosts([]);
                setError(null);
                setHasMore(true);
                setIsBackfilling(false);
            }

            const response = await fetch(
                `https://telegram-feed-app-production.up.railway.app/api/feed/?page=${page.current}`, 
                {
                    headers: { 'Authorization': `tma ${window.Telegram.WebApp.initData}` },
                    signal: controller.signal
                }
            );

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({ 
                    detail: `HTTP ошибка: ${response.status}` 
                }));
                throw new Error(errorData.detail);
            }

            const { posts: newPosts, status } = await response.json();

            setPosts(prev => isRefresh ? newPosts : removeDuplicates(prev, newPosts));
            page.current += 1;
            
            if (status === "backfilling") {
                setIsBackfilling(true);
                setHasMore(false);
            } else {
                setHasMore(newPosts.length > 0);
            }
            
        } catch (err) {
            if (err.name !== 'AbortError') {
                console.error('Fetch error:', err);
                setError(err.message);
                setHasMore(false);
            }
        } finally {
            isFetchingRef.current = false;
            setIsFetching(false);
            setInitialLoading(false);
        }
        
        return () => controller.abort();
    }, [hasMore, removeDuplicates]);

    const checkSubscriptions = useCallback(async () => {
        try {
            const response = await fetch(
                `https://telegram-feed-app-production.up.railway.app/api/subscriptions/`, 
                {
                    headers: { 'Authorization': `tma ${window.Telegram.WebApp.initData}` }
                }
            );
            if (response.ok) {
                const data = await response.json();
                if (data.channels && data.channels.length > 0) {
                    setHasSubscriptions(true);
                }
            }
        } catch (err) {
            console.error("Failed to check subscriptions:", err);
        }
    }, []);

    const updateSubscriptionStatus = useCallback(async () => {
        try {
            const response = await fetch(
                `https://telegram-feed-app-production.up.railway.app/api/subscriptions/`, 
                {
                    headers: { 'Authorization': `tma ${window.Telegram.WebApp.initData}` }
                }
            );
            if (response.ok) {
                const data = await response.json();
                const hasChannels = data.channels && data.channels.length > 0;
                const hadSubscriptionsBefore = hasSubscriptions;
                setHasSubscriptions(hasChannels);
                
                if (hasChannels && posts.length === 0 && !isFetching) {
                    fetchPosts(true);
                    
                    if (!hadSubscriptionsBefore) {
                        setIsLoadingNewChannel(true);
                    }
                }
            }
        } catch (err) {
            console.error("Failed to update subscription status:", err);
        }
    }, [posts.length, isFetching, hasSubscriptions, fetchPosts]);

    const handleRefresh = useCallback(() => {
        if (window.Telegram?.WebApp?.HapticFeedback) {
            window.Telegram.WebApp.HapticFeedback.impactOccurred('light');
        }
        fetchPosts(true);
    }, [fetchPosts]);

    const scrollToTop = () => {
        window.scrollTo({ top: 0, behavior: 'smooth' });
    };

    // ИСПРАВЛЕНИЕ: ЕДИНСТВЕННЫЙ useEffect для инициализации
    useEffect(() => {
        const tg = window.Telegram.WebApp;

        const applyThemeClass = () => {
            document.body.className = tg.colorScheme;
        };

        const init = async () => {
            if (tg && tg.initData) {
                try {
                    await checkSubscriptions();
                    await fetchPosts();
                } catch (error) {
                    console.error('Initialization error:', error);
                    setError("Ошибка инициализации приложения");
                    setInitialLoading(false);
                }
            } else {
                setError("Не удалось определить пользователя Telegram. Откройте приложение через бота.");
                setInitialLoading(false);
            }
        };
        
        if (tg) {
            tg.ready();
            applyThemeClass();
            tg.onEvent('themeChanged', applyThemeClass);
            init();
        } else {
            setError("Не удалось загрузить Telegram Web App API.");
            setInitialLoading(false);
        }
        
        return () => {
            isFetchingRef.current = false;
            if (tg) {
                tg.offEvent('themeChanged', applyThemeClass);
            }
        };
    }, []); // ← ПУСТЫЕ зависимости!

    useEffect(() => {
        const handleClick = (event) => {
            // Ищем тег <tg-spoiler> при клике
            const spoiler = event.target.closest('tg-spoiler');
            if (spoiler) {
                spoiler.classList.toggle('revealed');
            }
        };

        // Добавляем один обработчик на весь документ
        document.addEventListener('click', handleClick);

        // Убираем обработчик при размонтировании компонента
        return () => {
            document.removeEventListener('click', handleClick);
        };
    }, []);

    // useEffect для реалтайм обновлений
    useEffect(() => {
        if (!isLoadingNewChannel) return;
        
        const checkInterval = setInterval(async () => {
            try {
                const response = await fetch(
                    `https://telegram-feed-app-production.up.railway.app/api/feed/?page=1`, 
                    {
                        headers: { 'Authorization': `tma ${window.Telegram.WebApp.initData}` }
                    }
                );
                
                if (response.ok) {
                    const { posts: freshPosts } = await response.json();
                    
                    setPosts(current => {
                        const newUniquePosts = freshPosts.filter(newPost => 
                            !current.some(existingPost => 
                                existingPost.channel.id === newPost.channel.id && 
                                existingPost.message_id === newPost.message_id
                            )
                        );
                        
                        if (newUniquePosts.length > 0) {
                            setIsLoadingNewChannel(false);
                            return [...newUniquePosts, ...current];
                        }
                        return current;
                    });
                }
            } catch (err) {
                console.error('Realtime update error:', err);
            }
        }, 3000);
        
        const timeout = setTimeout(() => {
            clearInterval(checkInterval);
            setIsLoadingNewChannel(false);
        }, 120000);
        
        return () => {
            clearInterval(checkInterval);
            clearTimeout(timeout);
        };
    }, [isLoadingNewChannel]);

    // Периодическая проверка подписок
    useEffect(() => {
        const interval = setInterval(() => {
            if (posts.length === 0 && !isFetching && !initialLoading) {
                updateSubscriptionStatus();
            }
        }, 30000);

        return () => clearInterval(interval);
    }, [posts.length, isFetching, initialLoading, updateSubscriptionStatus]);

    // Pull to refresh
    useEffect(() => {
        const indicator = document.getElementById('refresh-indicator');
        if (!indicator) return;
        
        let animationFrame = null;
        
        const PULL_THRESHOLD = 70;
        const PULL_RESISTANCE = 2;
        
        const updateIndicatorPosition = (delta) => {
            if (animationFrame) cancelAnimationFrame(animationFrame);
            
            animationFrame = requestAnimationFrame(() => {
                const position = Math.min(delta / PULL_RESISTANCE - 50, 70);
                indicator.style.transform = `translateY(${position}px)`;
            });
        };
        
        const handleTouchStart = (e) => { 
            if (window.scrollY === 0) { 
                pullStartY.current = e.touches[0].clientY; 
                isPulling.current = true; 
            }
        };
        
        const handleTouchMove = (e) => { 
            if (!isPulling.current) return; 
            const delta = e.touches[0].clientY - pullStartY.current; 
            if (delta > 0) { 
                pullDeltaY.current = delta;
                updateIndicatorPosition(delta);
            }
        };
        
        const handleTouchEnd = () => { 
            if (!isPulling.current) return; 
            
            if (pullDeltaY.current > PULL_THRESHOLD) { 
                indicator.style.transform = 'translateY(20px)'; 
                handleRefresh(); 
                setTimeout(() => { 
                    indicator.style.transform = 'translateY(-50px)'; 
                }, 1000); 
            } else { 
                indicator.style.transform = 'translateY(-50px)'; 
            } 
            
            isPulling.current = false; 
            pullDeltaY.current = 0; 
        };
        
        window.addEventListener('touchstart', handleTouchStart);
        window.addEventListener('touchmove', handleTouchMove);
        window.addEventListener('touchend', handleTouchEnd);
        
        return () => { 
            if (animationFrame) cancelAnimationFrame(animationFrame);
            window.removeEventListener('touchstart', handleTouchStart);
            window.removeEventListener('touchmove', handleTouchMove);
            window.removeEventListener('touchend', handleTouchEnd);
        };
    }, [handleRefresh]);
    
    // Infinite scroll
    useEffect(() => {
        const handleObserver = (entities) => { 
            const target = entities[0]; 
            if (target.isIntersecting && hasMore && !isFetching) { 
                fetchPosts();
            } 
        };
        const observer = new IntersectionObserver(handleObserver, { rootMargin: '200px' });
        const currentLoader = loader.current;
        if (currentLoader) { observer.observe(currentLoader); }
        return () => { if (currentLoader) { observer.unobserve(currentLoader); } };
    }, [posts, hasMore, isFetching, fetchPosts]);

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