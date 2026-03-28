import React from 'react';
import { useTranslation } from 'react-i18next';
import { useDispatch, useSelector } from 'react-redux';
import { setLanguage } from '../../store/uiSlice';
import type { RootState } from '../../store';
import type { Language } from '../../types';
import { clsx } from 'clsx';

interface LanguageToggleProps {
  className?: string;
  variant?: 'icon' | 'text' | 'full';
}

export const LanguageToggle: React.FC<LanguageToggleProps> = ({
  className,
  variant = 'text',
}) => {
  const { i18n } = useTranslation();
  const dispatch = useDispatch();
  const currentLanguage = useSelector((state: RootState) => state.ui.language);

  const languages: { code: Language; label: string; flag: string }[] = [
    { code: 'en', label: 'English', flag: '🇬🇧' },
    { code: 'fr', label: 'Français', flag: '🇫🇷' },
  ];

  const handleChange = (lang: Language) => {
    dispatch(setLanguage(lang));
    i18n.changeLanguage(lang);
  };

  if (variant === 'icon') {
    const current = languages.find((l) => l.code === currentLanguage);
    const next = languages.find((l) => l.code !== currentLanguage);

    return (
      <button
        onClick={() => handleChange(next!.code)}
        className={clsx(
          'p-2 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700 transition-colors',
          className
        )}
        title={`Switch to ${next?.label}`}
      >
        <span className="text-xl">{current?.flag}</span>
      </button>
    );
  }

  if (variant === 'text') {
    return (
      <div className={clsx('flex items-center gap-1', className)}>
        {languages.map((lang) => (
          <button
            key={lang.code}
            onClick={() => handleChange(lang.code)}
            className={clsx(
              'px-2 py-1 text-sm font-medium rounded transition-colors',
              currentLanguage === lang.code
                ? 'bg-primary-100 text-primary-700 dark:bg-primary-900/30 dark:text-primary-300'
                : 'text-slate-600 hover:bg-slate-100 dark:text-slate-400 dark:hover:bg-slate-700'
            )}
          >
            {lang.code.toUpperCase()}
          </button>
        ))}
      </div>
    );
  }

  return (
    <div className={clsx('flex items-center gap-2', className)}>
      {languages.map((lang) => (
        <button
          key={lang.code}
          onClick={() => handleChange(lang.code)}
          className={clsx(
            'flex items-center gap-2 px-3 py-2 rounded-lg transition-colors',
            currentLanguage === lang.code
              ? 'bg-primary-100 text-primary-700 dark:bg-primary-900/30 dark:text-primary-300'
              : 'text-slate-600 hover:bg-slate-100 dark:text-slate-400 dark:hover:bg-slate-700'
          )}
        >
          <span className="text-lg">{lang.flag}</span>
          <span className="text-sm font-medium">{lang.label}</span>
        </button>
      ))}
    </div>
  );
};

export default LanguageToggle;
