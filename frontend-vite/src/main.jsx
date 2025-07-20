import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App.jsx'; // Импортируем наш главный компонент
import './App.css';       // Импортируем наши стили

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
