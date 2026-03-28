import React from 'react';
import { useTranslation } from 'react-i18next';
import { useDispatch, useSelector } from 'react-redux';
import {
  Bars3Icon,
  SunIcon,
  MoonIcon,
  ComputerDesktopIcon,
} from '@heroicons/react/24/outline';
import { setTheme, toggleSidebar } from '../../store/uiSlice';
import type { RootState } from '../../store';
import type { Theme } from '../../types';
import LanguageToggle from '../common/LanguageToggle';
import { clsx } from 'clsx';

export const Header: React.FC = () => {
  const { t } = useTranslation();
  const dispatch = useDispatch();
  const theme = useSelector((state: RootState) => state.ui.theme);

  const themes: { value: Theme; icon: React.ReactNode; label: string }[] = [
    { value: 'light', icon: <SunIcon className="h-5 w-5" />, label: t('settings.themeLight') },
    { value: 'dark', icon: <MoonIcon className="h-5 w-5" />, label: t('settings.themeDark') },
    { value: 'system', icon: <ComputerDesktopIcon className="h-5 w-5" />, label: t('settings.themeSystem') },
  ];

  const currentThemeIndex = themes.findIndex((t) => t.value === theme);

  const cycleTheme = () => {
    const nextIndex = (currentThemeIndex + 1) % themes.length;
    dispatch(setTheme(themes[nextIndex].value));
  };

  return (
    <header className="sticky top-0 z-40 bg-white/80 dark:bg-slate-900/80 backdrop-blur-xl border-b border-slate-200 dark:border-slate-700">
      <div className="flex items-center justify-between h-16 px-4 lg:px-6">
        {/* Left side */}
        <div className="flex items-center gap-4">
          <button
            onClick={() => dispatch(toggleSidebar())}
            className="p-2 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors lg:hidden"
          >
            <Bars3Icon className="h-6 w-6 text-slate-600 dark:text-slate-300" />
          </button>

          <div className="hidden lg:flex items-center gap-3">
            <div className="h-8 w-8 rounded-lg bg-gradient-to-br from-primary-500 to-primary-700 flex items-center justify-center">
              <span className="text-white font-bold text-sm">BI</span>
            </div>
            <span className="font-semibold text-slate-900 dark:text-white">
              BI Platform
            </span>
          </div>
        </div>

        {/* Right side */}
        <div className="flex items-center gap-2">
          {/* Language Toggle */}
          <LanguageToggle variant="text" />

          {/* Theme Toggle */}
          <button
            onClick={cycleTheme}
            className={clsx(
              'p-2 rounded-lg transition-colors',
              'hover:bg-slate-100 dark:hover:bg-slate-800',
              'text-slate-600 dark:text-slate-300'
            )}
            title={themes[currentThemeIndex].label}
          >
            {themes[currentThemeIndex].icon}
          </button>
        </div>
      </div>
    </header>
  );
};

export default Header;
