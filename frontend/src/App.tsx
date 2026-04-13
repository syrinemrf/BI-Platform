import React, { useEffect } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { Provider, useSelector } from 'react-redux';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Toaster } from 'react-hot-toast';

import { store, RootState } from './store';
import { Header } from './components/Layout/Header';
import { Sidebar } from './components/Layout/Sidebar';
import ErrorBoundary from './components/common/ErrorBoundary';
import DashboardPage from './pages/DashboardPage';
import DataSourcesPage from './pages/DataSourcesPage';
import ETLPage from './pages/ETLPage';
import SchemaPage from './pages/SchemaPage';
import WarehousePage from './pages/WarehousePage';
import SettingsPage from './pages/SettingsPage';
import AboutPage from './pages/AboutPage';
import AuthPage from './pages/AuthPage';
import WelcomePage from './pages/WelcomePage';
import NotFoundPage from './pages/NotFoundPage';
import ETLLLMPage from './pages/ETLLLMPage';

import './i18n';
import { clsx } from 'clsx';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1000 * 60 * 5,
      retry: 1,
    },
  },
});

const ProtectedLayout: React.FC = () => {
  const { sidebarCollapsed, theme } = useSelector((state: RootState) => state.ui);

  useEffect(() => {
    const applyTheme = () => {
      const root = document.documentElement;
      if (theme === 'dark') {
        root.classList.add('dark');
      } else if (theme === 'light') {
        root.classList.remove('dark');
      } else {
        if (window.matchMedia('(prefers-color-scheme: dark)').matches) {
          root.classList.add('dark');
        } else {
          root.classList.remove('dark');
        }
      }
    };

    applyTheme();

    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
    const handleChange = () => {
      if (theme === 'system') applyTheme();
    };

    mediaQuery.addEventListener('change', handleChange);
    return () => mediaQuery.removeEventListener('change', handleChange);
  }, [theme]);

  return (
    <div className="min-h-screen bg-slate-50 dark:bg-slate-900 transition-colors">
      <Sidebar />
      <div
        className={clsx(
          'transition-all duration-300',
          sidebarCollapsed ? 'lg:pl-20' : 'lg:pl-64'
        )}
      >
        <Header />
        <main className="p-4 lg:p-6">
          <ErrorBoundary>
            <Routes>
              <Route path="/" element={<Navigate to="/data-sources" replace />} />
              <Route path="/data-sources" element={<DataSourcesPage />} />
              <Route path="/etl" element={<ETLPage />} />
              <Route path="/etl-llm" element={<ETLLLMPage />} />
              <Route path="/schema" element={<SchemaPage />} />
              <Route path="/warehouse" element={<WarehousePage />} />
              <Route path="/dashboard" element={<DashboardPage />} />
              <Route path="/settings" element={<SettingsPage />} />
              <Route path="/about" element={<AboutPage />} />
              <Route path="*" element={<NotFoundPage />} />
            </Routes>
          </ErrorBoundary>
        </main>
      </div>

    </div>
  );
};

const AppRouter: React.FC = () => {
  const { isAuthenticated, isGuest } = useSelector((state: RootState) => state.auth);

  // Not authenticated and not guest: show welcome or auth
  if (!isAuthenticated && !isGuest) {
    return (
      <Routes>
        <Route path="/auth" element={<AuthPage />} />
        <Route path="/" element={<WelcomePage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    );
  }

  // Authenticated or guest: full app access
  return (
    <Routes>
      <Route path="/auth" element={<Navigate to="/data-sources" replace />} />
      <Route path="/welcome" element={<Navigate to="/data-sources" replace />} />
      <Route path="/*" element={<ProtectedLayout />} />
    </Routes>
  );
};

const App: React.FC = () => {
  return (
    <Provider store={store}>
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <AppRouter />
          <Toaster
            position="top-right"
            toastOptions={{
              className: 'dark:bg-slate-800 dark:text-white',
              duration: 4000,
            }}
          />
        </BrowserRouter>
      </QueryClientProvider>
    </Provider>
  );
};

export default App;
