import React, { useState, useEffect, useCallback, useRef } from 'react';

// --- Вспомогательные компоненты ---

function PostCard({ post }) {
    const formatDate = (dateString) => new Date(dateString).toLocaleString('ru-RU', { day: 'numeric', month: 'long', hour: '2-digit', minute: '2-digit' });
    const getPostUrl = (p) => p.channel.username ? `https://t.me/${p.channel.username}/${p.message_id}` : `https://t.me/c/${String(p.channel.id).substring(4)}/${p.message_id}`;
    const postUrl = getPostUrl(post);
    const hasVisualMedia = post.media && post.media.some(item => item.type === 'photo' || item.type === 'video');
    const channelUrl = post.channel.username ? `https://t.me/${post.channel.username}` : '#';

    return (
        <div className={`post-card ${hasVisualMedia ? 'post-card-with-media' : ''}`}>
            {/* Блок с аватаром, названием и датой */}
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

            {/* Блок с картинками/видео/аудио */}
            <PostMedia media={post.media} />
            
            {/* Блок с текстом поста и кнопкой "Комментировать" */}
            {(post.text || postUrl) && (
                 <div className="post-content">
                    {/* Исправленное отображение текста с HTML */}
                    {post.text && <div className="post-text" dangerouslySetInnerHTML={{ __html: post.text }} />}
                    <a href={postUrl} target="_blank" rel="noopener noreferrer" className="comment-button">Комментировать</a>
                </div>
            )}

            {/* ИСПРАВЛЕННЫЙ ФУТЕР: он должен быть здесь, внутри .post-card */}
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
                        {/* Условное отображение просмотров */}
                        {post.views && `👁️ ${post.views}`}
                    </div>
                </div>
            )}
        </div>
    );
}

function PostMedia({ media }) {
    if (!media || media.length === 0) return null;
    const visualMedia = media.filter(item => item.type === 'photo' || item.type === 'video');
    const audioMedia = media.filter(item => item.type === 'audio');
    const [currentIndex, setCurrentIndex] = useState(0);

    if (visualMedia.length === 0 && audioMedia.length === 0) return null;

    const goToPrevious = () => setCurrentIndex(prev => (prev === 0 ? visualMedia.length - 1 : prev - 1));
    const goToNext = () => setCurrentIndex(prev => (prev === visualMedia.length - 1 ? 0 : prev + 1));

    return (
        <>
            {visualMedia.length > 0 && (
                <div className="post-media-gallery">
                    {visualMedia.map((item, index) => (
                        <div key={index} className={`slider-item ${index === currentIndex ? 'active' : ''}`}>
                            {item.type === 'photo' && <img src={item.url} className="post-media-visual" alt={`Изображение ${index + 1}`} loading="lazy" />}
                            {item.type === 'video' && <video controls muted playsInline className="post-media-visual"><source src={item.url} /></video>}
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
            {audioMedia.map((item, index) => ( <audio key={index} controls className="post-media-audio"><source src={item.url} /></audio> ))}
        </>
    );
}

function Header({ onRefresh, onScrollUp }) {
    const handleMobileTap = (e, action) => {
        e.preventDefault();
        const button = e.currentTarget;
        button.classList.add('button--active');
        action();
        setTimeout(() => button.classList.remove('button--active'), 150);
    };
    
    return (
        <header className="app-header">
            <button onClick={onScrollUp} onTouchEnd={(e) => handleMobileTap(e, onScrollUp)} className="header-button">Вверх ⬆️</button>
            <button onClick={onRefresh} onTouchEnd={(e) => handleMobileTap(e, onRefresh)} className="header-button">Обновить 🔄</button>
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


// --- Основной компонент приложения ---

function App() {
    const [posts, setPosts] = useState([]);
    const [error, setError] = useState(null);
    const [hasMore, setHasMore] = useState(true);
    const [isFetching, setIsFetching] = useState(false);
    const [initialLoading, setInitialLoading] = useState(true); // При запуске показываем лоадер
    const [isBackfilling, setIsBackfilling] = useState(false);
    
    const page = useRef(1);
    const loader = useRef(null);
    const isFetchingRef = useRef(false);
    const pullStartY = useRef(0);
    const pullDeltaY = useRef(0);
    const isPulling = useRef(false);
    

    const fetchPosts = useCallback(async (isRefresh = false) => {
        if (isFetchingRef.current) return;
        if (!hasMore && !isRefresh) return;
        
        isFetchingRef.current = true;
        setIsFetching(true);
        if (isRefresh) {
            page.current = 1;
            setPosts([]);
            setError(null);
            setHasMore(true);
            setIsBackfilling(false);
        }

        try {
            // Убедитесь, что URL правильный для вашего продакшн-окружения
            const response = await fetch(`https://telegram-feed-app-production.up.railway.app/api/feed/?page=${page.current}`, {
                headers: { 'Authorization': `tma ${window.Telegram.WebApp.initData}` }
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({ detail: `HTTP ошибка: ${response.status}` }));
                throw new Error(errorData.detail);
            }

            const { posts: newPosts, status } = await response.json();

            setPosts(prev => isRefresh ? newPosts : [...new Set([...prev, ...newPosts].map(p => JSON.stringify(p)))].map(s => JSON.parse(s)));
            page.current += 1;
            
            if (status === "backfilling") {
                setIsBackfilling(true);
                setHasMore(false);
            } else {
                setHasMore(newPosts.length > 0);
            }
        } catch (err) {
            setError(err.message);
            setHasMore(false);
        } finally {
            isFetchingRef.current = false;
            setIsFetching(false);
            if (isRefresh) setInitialLoading(false);
        }
    }, [hasMore]);

    const handleRefresh = useCallback(() => {
        if (window.Telegram.WebApp.HapticFeedback) {
            window.Telegram.WebApp.HapticFeedback.impactOccurred('light');
        }
        fetchPosts(true);
    }, [fetchPosts]);

    const scrollToTop = () => {
        window.scrollTo({ top: 0, behavior: 'smooth' });
    };

    useEffect(() => {
        const tg = window.Telegram.WebApp;

        const applyThemeClass = () => {
            // Устанавливаем класс 'light' или 'dark' для тега <body>
            document.body.className = tg.colorScheme;
        };

        // Устанавливаем тему при первой загрузке
        applyThemeClass();

        // Добавляем слушатель, чтобы тема менялась динамически
        tg.onEvent('themeChanged', applyThemeClass);

        // Убираем слушатель при размонтировании компонента
        return () => {
            tg.offEvent('themeChanged', applyThemeClass);
        };
    }, []);

    useEffect(() => {
        const tg = window.Telegram.WebApp;
        const init = () => {
            if (tg && tg.initData) {
                fetchPosts().finally(() => setInitialLoading(false));
            } else {
                setError("Не удалось определить пользователя Telegram. Откройте приложение через бота.");
                setInitialLoading(false);
            }
        };
        if (tg) {
            tg.ready();
            init();
        } else {
             setError("Не удалось загрузить Telegram Web App API.");
             setInitialLoading(false);
        }
    }, [fetchPosts]);

    useEffect(() => {
        const indicator = document.getElementById('refresh-indicator');
        if (!indicator) return;
        
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
                indicator.style.top = `${Math.min(delta / 2 - 50, 70)}px`;
            }
        };
        
        const handleTouchEnd = () => { 
            if (!isPulling.current) return; 
            if (pullDeltaY.current > 70) { 
                indicator.style.top = '20px'; 
                handleRefresh(); 
                setTimeout(() => { indicator.style.top = '-50px'; }, 1000); 
            } else { 
                indicator.style.top = '-50px'; 
            } 
            isPulling.current = false; 
            pullDeltaY.current = 0; 
        };
        
        window.addEventListener('touchstart', handleTouchStart);
        window.addEventListener('touchmove', handleTouchMove);
        window.addEventListener('touchend', handleTouchEnd);
        
        return () => { 
            window.removeEventListener('touchstart', handleTouchStart);
            window.removeEventListener('touchmove', handleTouchMove);
            window.removeEventListener('touchend', handleTouchEnd);
        };
    }, [handleRefresh]);
    
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
            <div className="status-message">
                <RadialLoader />
            </div>
        );
    }

    if (error) {
        return <div className="status-message">Ошибка: {error}</div>;
    }
    
    if (posts.length === 0 && !isFetching) {
        return <div className="status-message">Ваша лента пока пуста. Добавьте каналы через бота!</div>;
    }

    return (
        <>
            <Header onRefresh={handleRefresh} onScrollUp={scrollToTop} />
            
            {/* --- НАЧАЛО ИЗМЕНЕНИЙ --- */}
            
            {/* 1. Выносим индикатор обновления ИЗ контейнера ленты */}
            <div id="refresh-indicator" className="pull-to-refresh-indicator">
                <RadialLoader />
            </div>

            <div className="feed-container">
                {posts.map(post => <PostCard key={`${post.channel_id}-${post.message_id}`} post={post} />)}
                
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

                {/* Он больше не здесь */}
                {/* <div id="refresh-indicator" ... > */}

                <div ref={loader} />
            </div>
        </>
    );
}

export default App;