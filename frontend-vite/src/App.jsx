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
    // Внутренняя логика PostCard остается без изменений
    const formatDate = (dateString) => new Date(dateString).toLocaleString('ru-RU', { day: 'numeric', month: 'long', hour: '2-digit', minute: '2-digit' });
    const getPostUrl = (p) => p.channel.username ? `https://t.me/${p.channel.username}/${p.message_id}` : `https://t.me/c/${String(p.channel.id).replace("-100", "")}/${p.message_id}`;
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
                            <span key={reaction.emoticon || reaction.document_id} className="reaction-item">
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
    // Внутренняя логика PostMedia остается без изменений
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
                            {item.type === 'video' && (
                                <div className="video-container">
                                    <video 
                                        muted 
                                        playsInline 
                                        className="post-media-visual"
                                        preload="metadata"
                                        poster={item.thumbnail_url || undefined}
                                        controls={false}
                                        onClick={(e) => {
                                            const video = e.currentTarget;
                                            if (video.paused) {
                                                video.controls = true;
                                                video.play();
                                            }
                                        }}
                                    >
                                        <source src={item.url} type="video/mp4" />
                                        Your browser does not support the video tag.
                                    </video>
                                    {!item.thumbnail_url && (
                                         <div className="video-play-overlay">
                                            <div className="video-play-button">▶️</div>
                                        </div>
                                    )}
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
    // Внутренняя логика Header остается без изменений
    const handleButtonPress = (e, action) => {
        e.preventDefault();
        e.stopPropagation();
        const button = e.currentTarget;
        button.classList.add('button--active');
        window.Telegram?.WebApp?.HapticFeedback?.impactOccurred('light');
        action();
        setTimeout(() => button.classList.remove('button--active'), 150);
    };
    return (
        <header className="app-header">
            <button onTouchEnd={(e) => handleButtonPress(e, onScrollUp)} onClick={(e) => !('ontouchend' in document) && handleButtonPress(e, onScrollUp)} className="header-button">
                Вверх ⬆️
            </button>
            <button onTouchEnd={(e) => handleButtonPress(e, onRefresh)} onClick={(e) => !('ontouchend' in document) && handleButtonPress(e, onRefresh)} className="header-button">
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

// --- ОСНОВНОЙ КОМПОНЕНТ ---
function App() {
    const [posts, setPosts] = useState([]);
    const [error, setError] = useState(null);
    const [pageStatus, setPageStatus] = useState('initial_loading'); // 'initial_loading', 'loading_more', 'ready', 'backfilling', 'empty', 'error'

    const page = useRef(1);
    const loaderRef = useRef(null);
    const isFetchingRef = useRef(false);

    const fetchPosts = useCallback(async (isRefresh = false) => {
        if (isFetchingRef.current) return;
        if (pageStatus !== 'ready' && pageStatus !== 'initial_loading' && !isRefresh) return;

        isFetchingRef.current = true;
        
        if (isRefresh) {
            page.current = 1;
            setError(null);
            setPageStatus('initial_loading');
        } else if (page.current > 1) {
            setPageStatus('loading_more');
        }

        try {
            const apiUrl = `https://telegram-feed-app-production.up.railway.app/api/feed/?page=${page.current}`;
            const headers = { 'Authorization': `tma ${window.Telegram.WebApp.initData}` };
            
            const response = await fetch(apiUrl, { headers });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({ detail: `HTTP ошибка: ${response.status}` }));
                throw new Error(errorData.detail || 'Ошибка сети');
            }

            const { posts: newPosts, status: apiStatus } = await response.json();
            
            setPosts(prev => {
                const currentPosts = isRefresh ? [] : prev;
                const postMap = new Map(currentPosts.map(p => [p.message_id, p]));
                newPosts.forEach(p => postMap.set(p.message_id, p));
                return Array.from(postMap.values()).sort((a, b) => new Date(b.date) - new Date(a.date));
            });

            if (apiStatus === 'ok') {
                setPageStatus('ready');
                page.current += 1;
            } else { 
                setPageStatus(apiStatus); 
            }

        } catch (err) {
            console.error('Fetch history error:', err);
            setError(err.message);
            setPageStatus('error');
        } finally {
            isFetchingRef.current = false;
        }
    }, [pageStatus]);

    // Инициализация и подгрузка постов
    useEffect(() => {
        if (pageStatus === 'initial_loading') {
            fetchPosts();
        }
    }, [pageStatus, fetchPosts]);
    
    // Настройка Telegram Web App
    useEffect(() => {
        const tg = window.Telegram.WebApp;
        if (tg) {
            tg.ready();
            const applyTheme = () => document.body.className = tg.colorScheme;
            applyTheme();
            tg.onEvent('themeChanged', applyTheme);
            return () => tg.offEvent('themeChanged', applyTheme);
        }
    }, []);

    // Подключение к SSE
    useEffect(() => {
        const initData = window.Telegram?.WebApp?.initData;
        if (!initData || pageStatus === 'initial_loading') {
            return;
        }

        const sseUrl = `https://telegram-feed-app-production.up.railway.app/api/feed/stream/?authorization=tma ${encodeURIComponent(initData)}`;
        const eventSource = new EventSource(sseUrl);
        
        eventSource.onmessage = (event) => {
            try {
                const newPost = JSON.parse(event.data);
                setPosts(prevPosts => {
                    const postMap = new Map(prevPosts.map(p => [p.message_id, p]));
                    if (!postMap.has(newPost.message_id)) {
                         window.Telegram?.WebApp?.HapticFeedback?.notificationOccurred('success');
                    }
                    postMap.set(newPost.message_id, newPost);
                    const sortedPosts = Array.from(postMap.values()).sort((a, b) => new Date(b.date) - new Date(a.date));
                    if (pageStatus === 'empty' || pageStatus === 'backfilling') {
                        setPageStatus('ready');
                    }
                    return sortedPosts;
                });
            } catch (e) {
                console.error("Failed to parse SSE data:", e);
            }
        };

        eventSource.onerror = (err) => {
            console.error("EventSource failed:", err);
            eventSource.close();
        };

        return () => eventSource.close();
    }, [pageStatus]);

    // Infinite scroll
    useEffect(() => {
        const observer = new IntersectionObserver(
            (entries) => {
                if (entries[0].isIntersecting && pageStatus === 'ready') {
                    fetchPosts();
                }
            }, { rootMargin: '400px' }
        );
        const currentLoader = loaderRef.current;
        if (currentLoader) observer.observe(currentLoader);
        return () => { if (currentLoader) observer.unobserve(currentLoader); };
    }, [pageStatus, fetchPosts]);

    const handleRefresh = useCallback(() => fetchPosts(true), [fetchPosts]);
    const scrollToTop = () => window.scrollTo({ top: 0, behavior: 'smooth' });

    // --- ЛОГИКА РЕНДЕРИНГА ---
    const renderContent = () => {
        if (pageStatus === 'error') {
            return <div className="status-message">Ошибка: {error}</div>;
        }

        if (pageStatus === 'initial_loading' && posts.length === 0) {
            return [...Array(5)].map((_, i) => <SkeletonCard key={i} />);
        }
        
        if (pageStatus === 'backfilling' && posts.length === 0) {
             return (
                <div className="status-message">
                    <RadialLoader /><br />Загружаем вашу ленту...
                </div>
            );
        }
        
        if (pageStatus === 'empty' && posts.length === 0) {
             return <div className="status-message">Ваша лента пока пуста. Добавьте каналы через бота! 📱</div>;
        }
        
        return (
            <>
                {posts.map(post => (
                    <ErrorBoundary key={`${post.channel.id}-${post.message_id}`}>
                        <PostCard post={post} />
                    </ErrorBoundary>
                ))}
                
                {/* --- ИЗМЕНЕНИЯ ЗДЕСЬ --- */}
                {pageStatus === 'loading_more' && (
                    <div className="loader-container"><RadialLoader /></div>
                )}

                {/* Заменяем старое сообщение на новое, более информативное */}
                {pageStatus === 'backfilling' && posts.length > 0 && (
                    <div className="status-message">
                        Идет загрузка более старых постов... ⏳<br/>
                    </div>
                )}
                {/* ------------------------- */}
                
                <div ref={loaderRef} style={{ height: '1px' }}/>
            </>
        );
    };

    return (
        <>
            <Header onRefresh={handleRefresh} onScrollUp={scrollToTop} />
            <div className="feed-container">
                {renderContent()}
            </div>
        </>
    );
}

export default App;
