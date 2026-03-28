import React from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useSelector, useDispatch } from 'react-redux';
import {
  HomeIcon,
  CircleStackIcon,
  ArrowPathIcon,
  CubeTransparentIcon,
  ServerStackIcon,
  Cog6ToothIcon,
  XMarkIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
} from '@heroicons/react/24/outline';
import { toggleSidebar, toggleSidebarCollapsed } from '../../store/uiSlice';
import type { RootState } from '../../store';
import { clsx } from 'clsx';

interface NavItem {
  name: string;
  href: string;
  icon: React.ComponentType<React.SVGProps<SVGSVGElement>>;
}

export const Sidebar: React.FC = () => {
  const { t } = useTranslation();
  const dispatch = useDispatch();
  const location = useLocation();
  const { sidebarOpen, sidebarCollapsed } = useSelector((state: RootState) => state.ui);

  const navigation: NavItem[] = [
    { name: t('nav.dashboard'), href: '/', icon: HomeIcon },
    { name: t('nav.dataSources'), href: '/data-sources', icon: CircleStackIcon },
    { name: t('nav.etl'), href: '/etl', icon: ArrowPathIcon },
    { name: t('nav.schema'), href: '/schema', icon: CubeTransparentIcon },
    { name: t('nav.warehouse'), href: '/warehouse', icon: ServerStackIcon },
    { name: t('nav.settings'), href: '/settings', icon: Cog6ToothIcon },
  ];

  const isActive = (href: string) => {
    if (href === '/') {
      return location.pathname === '/';
    }
    return location.pathname.startsWith(href);
  };

  return (
    <>
      {/* Mobile overlay */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-slate-900/50 backdrop-blur-sm z-40 lg:hidden"
          onClick={() => dispatch(toggleSidebar())}
        />
      )}

      {/* Sidebar */}
      <aside
        className={clsx(
          'fixed inset-y-0 left-0 z-50 flex flex-col bg-white dark:bg-slate-900 border-r border-slate-200 dark:border-slate-700 transition-all duration-300',
          sidebarOpen ? 'translate-x-0' : '-translate-x-full lg:translate-x-0',
          sidebarCollapsed ? 'lg:w-20' : 'lg:w-64',
          'w-64'
        )}
      >
        {/* Logo */}
        <div className="flex items-center justify-between h-16 px-4 border-b border-slate-200 dark:border-slate-700">
          <div className={clsx('flex items-center gap-3', sidebarCollapsed && 'lg:justify-center lg:w-full')}>
            <div className="h-10 w-10 rounded-xl bg-gradient-to-br from-primary-500 to-primary-700 flex items-center justify-center shadow-lg shadow-primary-500/25">
              <span className="text-white font-bold text-lg">BI</span>
            </div>
            {!sidebarCollapsed && (
              <span className="font-bold text-xl text-slate-900 dark:text-white">
                Platform
              </span>
            )}
          </div>

          {/* Mobile close button */}
          <button
            onClick={() => dispatch(toggleSidebar())}
            className="p-2 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 lg:hidden"
          >
            <XMarkIcon className="h-5 w-5 text-slate-500" />
          </button>

          {/* Desktop collapse button */}
          <button
            onClick={() => dispatch(toggleSidebarCollapsed())}
            className={clsx(
              'hidden lg:flex p-1.5 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors',
              sidebarCollapsed && 'absolute -right-3 top-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 shadow-sm'
            )}
          >
            {sidebarCollapsed ? (
              <ChevronRightIcon className="h-4 w-4 text-slate-500" />
            ) : (
              <ChevronLeftIcon className="h-4 w-4 text-slate-500" />
            )}
          </button>
        </div>

        {/* Navigation */}
        <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
          {navigation.map((item) => {
            const active = isActive(item.href);
            return (
              <NavLink
                key={item.name}
                to={item.href}
                className={clsx(
                  'flex items-center gap-3 px-3 py-2.5 rounded-xl transition-all duration-200 group',
                  active
                    ? 'bg-primary-50 text-primary-700 dark:bg-primary-900/30 dark:text-primary-300'
                    : 'text-slate-600 hover:bg-slate-100 dark:text-slate-400 dark:hover:bg-slate-800',
                  sidebarCollapsed && 'lg:justify-center lg:px-2'
                )}
                onClick={() => {
                  if (window.innerWidth < 1024) {
                    dispatch(toggleSidebar());
                  }
                }}
              >
                <item.icon
                  className={clsx(
                    'h-5 w-5 flex-shrink-0 transition-colors',
                    active
                      ? 'text-primary-600 dark:text-primary-400'
                      : 'text-slate-400 group-hover:text-slate-600 dark:group-hover:text-slate-300'
                  )}
                />
                {!sidebarCollapsed && (
                  <span className="font-medium truncate">{item.name}</span>
                )}
              </NavLink>
            );
          })}
        </nav>

        {/* Footer */}
        <div className={clsx(
          'p-4 border-t border-slate-200 dark:border-slate-700',
          sidebarCollapsed && 'lg:px-2'
        )}>
          <div className={clsx(
            'flex items-center gap-3 px-3 py-2 rounded-xl bg-slate-50 dark:bg-slate-800/50',
            sidebarCollapsed && 'lg:justify-center lg:px-2'
          )}>
            <div className="h-8 w-8 rounded-lg bg-gradient-to-br from-accent-400 to-accent-600 flex items-center justify-center">
              <span className="text-white text-xs font-bold">v1</span>
            </div>
            {!sidebarCollapsed && (
              <div className="flex-1 min-w-0">
                <p className="text-xs font-medium text-slate-900 dark:text-white truncate">
                  BI Platform
                </p>
                <p className="text-xs text-slate-500 dark:text-slate-400">
                  v1.0.0
                </p>
              </div>
            )}
          </div>
        </div>
      </aside>
    </>
  );
};

export default Sidebar;
