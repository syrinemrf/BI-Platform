import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useQuery } from '@tanstack/react-query';
import {
  ChartBarIcon,
  TableCellsIcon,
  CubeIcon,
  ClockIcon,
} from '@heroicons/react/24/outline';
import { Card, CardHeader } from '../components/common/Card';
import { LoadingSpinner } from '../components/common/LoadingSpinner';
import { KPICard } from '../components/Dashboard/KPICard';
import { ChartCard } from '../components/Dashboard/ChartCard';
import { dashboardApi, warehouseApi } from '../services/api';

export const DashboardPage: React.FC = () => {
  const { t } = useTranslation();
  const [selectedMeasure, setSelectedMeasure] = useState<string>('');
  const [granularity, setGranularity] = useState<string>('month');

  // Fetch dashboard summary
  const { data: summary, isLoading: summaryLoading } = useQuery({
    queryKey: ['dashboard-summary'],
    queryFn: dashboardApi.getSummary,
  });

  // Fetch KPIs
  const { data: kpisData, isLoading: kpisLoading } = useQuery({
    queryKey: ['dashboard-kpis'],
    queryFn: () => dashboardApi.getKPIs(),
  });

  // Fetch warehouse stats
  const { data: stats } = useQuery({
    queryKey: ['warehouse-stats'],
    queryFn: warehouseApi.getStats,
  });

  // Fetch time series when measure is selected
  const { data: timeSeriesData } = useQuery({
    queryKey: ['timeseries', selectedMeasure, granularity],
    queryFn: () => dashboardApi.getTimeSeries(selectedMeasure, 'sum', granularity),
    enabled: !!selectedMeasure,
  });

  // Set initial measure when KPIs load
  useEffect(() => {
    if (kpisData?.kpis?.length && !selectedMeasure) {
      setSelectedMeasure(kpisData.kpis[0].column);
    }
  }, [kpisData, selectedMeasure]);

  const isLoading = summaryLoading || kpisLoading;

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-96">
        <LoadingSpinner size="lg" text={t('common.loading')} />
      </div>
    );
  }

  const kpis = kpisData?.kpis || [];
  const hasData = kpis.length > 0;

  // Prepare time series chart data
  const chartData = timeSeriesData
    ? timeSeriesData.labels.map((label, index) => ({
        name: label,
        value: timeSeriesData.values[index],
      }))
    : [];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-slate-900 dark:text-white">
          {t('dashboard.title')}
        </h1>
        <p className="mt-1 text-slate-500 dark:text-slate-400">
          {t('dashboard.subtitle')}
        </p>
      </div>

      {!hasData ? (
        /* Empty State */
        <Card variant="glass" className="py-16">
          <div className="text-center">
            <ChartBarIcon className="mx-auto h-16 w-16 text-slate-300 dark:text-slate-600" />
            <h3 className="mt-4 text-lg font-medium text-slate-900 dark:text-white">
              {t('dashboard.noData')}
            </h3>
            <p className="mt-2 text-slate-500 dark:text-slate-400">
              {t('dashboard.noData')}
            </p>
          </div>
        </Card>
      ) : (
        <>
          {/* Stats Cards */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <KPICard
              title={t('dashboard.totalRecords')}
              value={summary?.total_records || 0}
              icon={<TableCellsIcon />}
              color="primary"
            />
            <KPICard
              title={t('dashboard.factTables')}
              value={stats?.fact_table_count || 0}
              icon={<CubeIcon />}
              color="success"
            />
            <KPICard
              title={t('dashboard.dimensions')}
              value={stats?.dimension_table_count || 0}
              icon={<ChartBarIcon />}
              color="info"
            />
            <KPICard
              title={t('dashboard.lastUpdated')}
              value={new Date().toLocaleDateString()}
              icon={<ClockIcon />}
              color="warning"
            />
          </div>

          {/* KPIs */}
          <div>
            <h2 className="text-lg font-semibold text-slate-900 dark:text-white mb-4">
              {t('dashboard.kpis')}
            </h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
              {kpis.slice(0, 8).map((kpi, index) => (
                <KPICard
                  key={kpi.column}
                  title={kpi.name}
                  value={kpi.total}
                  subtitle={`Avg: ${kpi.average.toFixed(2)}`}
                  color={
                    index % 4 === 0
                      ? 'primary'
                      : index % 4 === 1
                      ? 'success'
                      : index % 4 === 2
                      ? 'warning'
                      : 'info'
                  }
                />
              ))}
            </div>
          </div>

          {/* Charts */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Time Series */}
            <ChartCard
              title={t('dashboard.timeSeries')}
              subtitle={selectedMeasure}
              type="area"
              data={chartData}
              dataKey="value"
              action={
                <div className="flex gap-2">
                  <select
                    value={selectedMeasure}
                    onChange={(e) => setSelectedMeasure(e.target.value)}
                    className="text-sm border border-slate-200 dark:border-slate-700 rounded-lg px-3 py-1.5 bg-white dark:bg-slate-800 text-slate-900 dark:text-white"
                  >
                    {kpis.map((kpi) => (
                      <option key={kpi.column} value={kpi.column}>
                        {kpi.name}
                      </option>
                    ))}
                  </select>
                  <select
                    value={granularity}
                    onChange={(e) => setGranularity(e.target.value)}
                    className="text-sm border border-slate-200 dark:border-slate-700 rounded-lg px-3 py-1.5 bg-white dark:bg-slate-800 text-slate-900 dark:text-white"
                  >
                    <option value="day">{t('dashboard.day')}</option>
                    <option value="week">{t('dashboard.week')}</option>
                    <option value="month">{t('dashboard.month')}</option>
                    <option value="quarter">{t('dashboard.quarter')}</option>
                    <option value="year">{t('dashboard.year')}</option>
                  </select>
                </div>
              }
            />

            {/* Bar Chart */}
            <ChartCard
              title={t('dashboard.aggregations')}
              type="bar"
              data={chartData.slice(0, 10)}
              dataKey="value"
            />
          </div>

          {/* Table Stats */}
          {stats?.table_stats && (
            <Card variant="glass">
              <CardHeader title={t('warehouse.tables')} />
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
                  <thead>
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                        {t('common.name')}
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                        {t('common.type')}
                      </th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-slate-500 uppercase tracking-wider">
                        {t('dataSources.rows')}
                      </th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-slate-500 uppercase tracking-wider">
                        {t('common.size')}
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-200 dark:divide-slate-700">
                    {stats.table_stats.map((table: { name: string; type: string; row_count: number; size: string }) => (
                      <tr key={table.name} className="hover:bg-slate-50 dark:hover:bg-slate-800/50">
                        <td className="px-4 py-3 text-sm font-medium text-slate-900 dark:text-white">
                          {table.name}
                        </td>
                        <td className="px-4 py-3 text-sm">
                          <span
                            className={`px-2 py-1 rounded-full text-xs font-medium ${
                              table.type === 'fact'
                                ? 'bg-primary-100 text-primary-700 dark:bg-primary-900/30 dark:text-primary-300'
                                : 'bg-slate-100 text-slate-700 dark:bg-slate-700 dark:text-slate-300'
                            }`}
                          >
                            {table.type}
                          </span>
                        </td>
                        <td className="px-4 py-3 text-sm text-right text-slate-600 dark:text-slate-400">
                          {table.row_count.toLocaleString()}
                        </td>
                        <td className="px-4 py-3 text-sm text-right text-slate-600 dark:text-slate-400">
                          {table.size}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
        </>
      )}
    </div>
  );
};

export default DashboardPage;
