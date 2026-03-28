import React from 'react';
import { useTranslation } from 'react-i18next';
import { useDispatch, useSelector } from 'react-redux';
import { useQuery } from '@tanstack/react-query';
import {
  SunIcon,
  MoonIcon,
  ComputerDesktopIcon,
  CheckCircleIcon,
  XCircleIcon,
} from '@heroicons/react/24/outline';
import { Card, CardHeader } from '../components/common/Card';
import { LanguageToggle } from '../components/common/LanguageToggle';
import type { RootState } from '../store';
import { setTheme } from '../store/uiSlice';
import type { Theme } from '../types';
import { healthApi, llmApi } from '../services/api';
import { clsx } from 'clsx';

export const SettingsPage: React.FC = () => {
  const { t } = useTranslation();
  const dispatch = useDispatch();
  const theme = useSelector((state: RootState) => state.ui.theme);

  // Fetch health status
  const { data: health } = useQuery({
    queryKey: ['health'],
    queryFn: healthApi.check,
    refetchInterval: 30000,
  });

  // Fetch LLM status
  const { data: llmStatus } = useQuery({
    queryKey: ['llm-status'],
    queryFn: llmApi.getStatus,
    refetchInterval: 30000,
  });

  const themes: { value: Theme; icon: React.ReactNode; label: string }[] = [
    { value: 'light', icon: <SunIcon className="h-5 w-5" />, label: t('settings.themeLight') },
    { value: 'dark', icon: <MoonIcon className="h-5 w-5" />, label: t('settings.themeDark') },
    { value: 'system', icon: <ComputerDesktopIcon className="h-5 w-5" />, label: t('settings.themeSystem') },
  ];

  return (
    <div className="space-y-6 max-w-4xl">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-slate-900 dark:text-white">
          {t('settings.title')}
        </h1>
        <p className="mt-1 text-slate-500 dark:text-slate-400">
          {t('settings.subtitle')}
        </p>
      </div>

      {/* Appearance */}
      <Card variant="glass">
        <CardHeader title={t('settings.appearance')} />

        <div className="space-y-6">
          {/* Theme Selection */}
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-3">
              {t('settings.theme')}
            </label>
            <div className="grid grid-cols-3 gap-3">
              {themes.map((item) => (
                <button
                  key={item.value}
                  onClick={() => dispatch(setTheme(item.value))}
                  className={clsx(
                    'flex flex-col items-center gap-2 p-4 rounded-xl border-2 transition-all',
                    theme === item.value
                      ? 'border-primary-500 bg-primary-50 dark:bg-primary-900/20'
                      : 'border-slate-200 dark:border-slate-700 hover:border-slate-300 dark:hover:border-slate-600'
                  )}
                >
                  <div
                    className={clsx(
                      'p-3 rounded-lg',
                      theme === item.value
                        ? 'bg-primary-100 text-primary-600 dark:bg-primary-900/30 dark:text-primary-400'
                        : 'bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400'
                    )}
                  >
                    {item.icon}
                  </div>
                  <span
                    className={clsx(
                      'text-sm font-medium',
                      theme === item.value
                        ? 'text-primary-700 dark:text-primary-300'
                        : 'text-slate-600 dark:text-slate-400'
                    )}
                  >
                    {item.label}
                  </span>
                </button>
              ))}
            </div>
          </div>

          {/* Language Selection */}
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-3">
              {t('settings.language')}
            </label>
            <LanguageToggle variant="full" />
          </div>
        </div>
      </Card>

      {/* System Status */}
      <Card variant="glass">
        <CardHeader title={t('settings.general')} />

        <div className="space-y-4">
          {/* Database Status */}
          <div className="flex items-center justify-between p-4 bg-slate-50 dark:bg-slate-800/50 rounded-xl">
            <div className="flex items-center gap-3">
              <div
                className={clsx(
                  'p-2 rounded-lg',
                  health?.database === 'healthy'
                    ? 'bg-green-100 text-green-600 dark:bg-green-900/30 dark:text-green-400'
                    : 'bg-red-100 text-red-600 dark:bg-red-900/30 dark:text-red-400'
                )}
              >
                {health?.database === 'healthy' ? (
                  <CheckCircleIcon className="h-5 w-5" />
                ) : (
                  <XCircleIcon className="h-5 w-5" />
                )}
              </div>
              <div>
                <p className="font-medium text-slate-900 dark:text-white">
                  {t('settings.database')}
                </p>
                <p className="text-sm text-slate-500 dark:text-slate-400">
                  PostgreSQL
                </p>
              </div>
            </div>
            <span
              className={clsx(
                'px-3 py-1 rounded-full text-sm font-medium',
                health?.database === 'healthy'
                  ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300'
                  : 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300'
              )}
            >
              {health?.database === 'healthy' ? t('settings.dbConnected') : t('settings.dbDisconnected')}
            </span>
          </div>

          {/* LLM Status */}
          <div className="flex items-center justify-between p-4 bg-slate-50 dark:bg-slate-800/50 rounded-xl">
            <div className="flex items-center gap-3">
              <div
                className={clsx(
                  'p-2 rounded-lg',
                  llmStatus?.available
                    ? 'bg-green-100 text-green-600 dark:bg-green-900/30 dark:text-green-400'
                    : 'bg-amber-100 text-amber-600 dark:bg-amber-900/30 dark:text-amber-400'
                )}
              >
                {llmStatus?.available ? (
                  <CheckCircleIcon className="h-5 w-5" />
                ) : (
                  <XCircleIcon className="h-5 w-5" />
                )}
              </div>
              <div>
                <p className="font-medium text-slate-900 dark:text-white">
                  {t('settings.llm')}
                </p>
                <p className="text-sm text-slate-500 dark:text-slate-400">
                  {llmStatus?.model || 'Ollama (LLaMA 3)'}
                </p>
              </div>
            </div>
            <span
              className={clsx(
                'px-3 py-1 rounded-full text-sm font-medium',
                llmStatus?.available
                  ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300'
                  : 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
              )}
            >
              {llmStatus?.available ? t('settings.llmAvailable') : t('settings.llmUnavailable')}
            </span>
          </div>
        </div>
      </Card>

      {/* About */}
      <Card variant="glass">
        <CardHeader title={t('settings.about')} />

        <div className="space-y-4">
          <div className="flex items-center gap-4">
            <div className="h-16 w-16 rounded-2xl bg-gradient-to-br from-primary-500 to-primary-700 flex items-center justify-center shadow-lg shadow-primary-500/25">
              <span className="text-white font-bold text-2xl">BI</span>
            </div>
            <div>
              <h3 className="text-lg font-semibold text-slate-900 dark:text-white">
                BI Platform
              </h3>
              <p className="text-sm text-slate-500 dark:text-slate-400">
                {t('settings.version')}: {health?.version || '1.0.0'}
              </p>
            </div>
          </div>

          <div className="p-4 bg-slate-50 dark:bg-slate-800/50 rounded-xl">
            <p className="text-sm text-slate-600 dark:text-slate-400">
              A comprehensive Business Intelligence platform with automatic schema detection,
              ETL pipeline with data quality checks, star schema generation, and LLM-powered
              analytics.
            </p>
          </div>

          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <p className="text-slate-500 dark:text-slate-400">Backend</p>
              <p className="font-medium text-slate-900 dark:text-white">FastAPI + Python</p>
            </div>
            <div>
              <p className="text-slate-500 dark:text-slate-400">Frontend</p>
              <p className="font-medium text-slate-900 dark:text-white">React + TypeScript</p>
            </div>
            <div>
              <p className="text-slate-500 dark:text-slate-400">Database</p>
              <p className="font-medium text-slate-900 dark:text-white">PostgreSQL</p>
            </div>
            <div>
              <p className="text-slate-500 dark:text-slate-400">LLM</p>
              <p className="font-medium text-slate-900 dark:text-white">Ollama (LLaMA 3)</p>
            </div>
          </div>
        </div>
      </Card>
    </div>
  );
};

export default SettingsPage;
