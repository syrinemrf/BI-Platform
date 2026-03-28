import React from 'react';
import { clsx } from 'clsx';
import { ArrowUpIcon, ArrowDownIcon, MinusIcon } from '@heroicons/react/24/solid';
import { Card } from '../common/Card';

interface KPICardProps {
  title: string;
  value: number | string;
  subtitle?: string;
  change?: number;
  changeLabel?: string;
  icon?: React.ReactNode;
  color?: 'primary' | 'success' | 'warning' | 'danger' | 'info';
  format?: 'number' | 'currency' | 'percent';
}

export const KPICard: React.FC<KPICardProps> = ({
  title,
  value,
  subtitle,
  change,
  changeLabel,
  icon,
  color = 'primary',
  format = 'number',
}) => {
  const formatValue = (val: number | string) => {
    if (typeof val === 'string') return val;

    switch (format) {
      case 'currency':
        return new Intl.NumberFormat('en-US', {
          style: 'currency',
          currency: 'USD',
          minimumFractionDigits: 0,
          maximumFractionDigits: 0,
        }).format(val);
      case 'percent':
        return `${val.toFixed(1)}%`;
      default:
        return new Intl.NumberFormat('en-US', {
          maximumFractionDigits: 2,
        }).format(val);
    }
  };

  const colorClasses = {
    primary: 'from-primary-500 to-primary-600',
    success: 'from-green-500 to-green-600',
    warning: 'from-amber-500 to-amber-600',
    danger: 'from-red-500 to-red-600',
    info: 'from-blue-500 to-blue-600',
  };

  const getChangeIcon = () => {
    if (change === undefined || change === 0) {
      return <MinusIcon className="h-4 w-4 text-slate-400" />;
    }
    if (change > 0) {
      return <ArrowUpIcon className="h-4 w-4 text-green-500" />;
    }
    return <ArrowDownIcon className="h-4 w-4 text-red-500" />;
  };

  const getChangeColor = () => {
    if (change === undefined || change === 0) return 'text-slate-500';
    if (change > 0) return 'text-green-600 dark:text-green-400';
    return 'text-red-600 dark:text-red-400';
  };

  return (
    <Card variant="glass" className="relative overflow-hidden group">
      {/* Background gradient decoration */}
      <div
        className={clsx(
          'absolute top-0 right-0 w-32 h-32 rounded-full blur-3xl opacity-20 -translate-y-1/2 translate-x-1/2 bg-gradient-to-br',
          colorClasses[color]
        )}
      />

      <div className="relative">
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <p className="text-sm font-medium text-slate-500 dark:text-slate-400">
              {title}
            </p>
            <p className="mt-2 text-3xl font-bold text-slate-900 dark:text-white">
              {formatValue(value)}
            </p>
            {subtitle && (
              <p className="mt-1 text-sm text-slate-500 dark:text-slate-400">
                {subtitle}
              </p>
            )}
          </div>

          {icon && (
            <div
              className={clsx(
                'p-3 rounded-xl bg-gradient-to-br shadow-lg',
                colorClasses[color]
              )}
            >
              <div className="h-6 w-6 text-white">{icon}</div>
            </div>
          )}
        </div>

        {change !== undefined && (
          <div className="mt-4 flex items-center gap-2">
            {getChangeIcon()}
            <span className={clsx('text-sm font-medium', getChangeColor())}>
              {Math.abs(change).toFixed(1)}%
            </span>
            {changeLabel && (
              <span className="text-sm text-slate-500 dark:text-slate-400">
                {changeLabel}
              </span>
            )}
          </div>
        )}
      </div>
    </Card>
  );
};

export default KPICard;
