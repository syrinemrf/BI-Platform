import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useQuery, useMutation } from '@tanstack/react-query';
import {
  TableCellsIcon,
  PlayIcon,
  DocumentDuplicateIcon,
  ArrowPathIcon,
  ChevronRightIcon,
  MagnifyingGlassIcon,
  SparklesIcon,
  CubeIcon,
} from '@heroicons/react/24/outline';
import { toast } from 'react-hot-toast';
import { Card, CardHeader } from '../components/common/Card';
import { Button } from '../components/common/Button';
import { LoadingSpinner } from '../components/common/LoadingSpinner';
import { warehouseApi, llmApi } from '../services/api';
import { clsx } from 'clsx';

export const WarehousePage: React.FC = () => {
  const { t } = useTranslation();
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [sqlQuery, setSqlQuery] = useState<string>('');
  const [naturalQuery, setNaturalQuery] = useState<string>('');
  const [queryResults, setQueryResults] = useState<any>(null);
  const [activeTab, setActiveTab] = useState<'sql' | 'natural'>('sql');

  // Fetch tables
  const { data: tables, isLoading: tablesLoading, refetch: refetchTables } = useQuery({
    queryKey: ['warehouse-tables'],
    queryFn: warehouseApi.listTables,
  });

  // Fetch warehouse stats
  const { data: stats } = useQuery({
    queryKey: ['warehouse-stats'],
    queryFn: warehouseApi.getStats,
  });

  // Fetch table data when selected
  const { data: tableData, isLoading: tableDataLoading } = useQuery({
    queryKey: ['table-data', selectedTable],
    queryFn: () => warehouseApi.getTableData(selectedTable!, 100, 0),
    enabled: !!selectedTable,
  });

  // Execute SQL query
  const executeSqlMutation = useMutation({
    mutationFn: (sql: string) => warehouseApi.query(sql),
    onSuccess: (data) => {
      setQueryResults(data);
      toast.success(`Query executed: ${data.row_count} rows returned`);
    },
    onError: (error: Error) => {
      toast.error(error.message);
    },
  });

  // Natural language query
  const naturalQueryMutation = useMutation({
    mutationFn: (question: string) => llmApi.naturalQuery(question, true),
    onSuccess: (data) => {
      if (data.sql) {
        setSqlQuery(data.sql);
      }
      if (data.results) {
        setQueryResults({
          columns: data.results.length > 0 ? Object.keys(data.results[0]) : [],
          data: data.results,
          row_count: data.row_count || data.results.length,
        });
      }
      if (data.explanation) {
        toast.success(data.explanation.slice(0, 100) + '...');
      }
    },
    onError: (error: Error) => {
      toast.error(error.message);
    },
  });

  const handleExecuteQuery = () => {
    if (!sqlQuery.trim()) {
      toast.error('Please enter a SQL query');
      return;
    }
    executeSqlMutation.mutate(sqlQuery);
  };

  const handleNaturalQuery = () => {
    if (!naturalQuery.trim()) {
      toast.error('Please enter a question');
      return;
    }
    naturalQueryMutation.mutate(naturalQuery);
  };

  const handleTableSelect = (tableName: string) => {
    setSelectedTable(tableName);
    setSqlQuery(`SELECT * FROM ${tableName} LIMIT 100`);
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    toast.success('Copied to clipboard');
  };

  const factTables = tables?.filter((t: any) => t.table_type === 'fact') || [];
  const dimensionTables = tables?.filter((t: any) => t.table_type === 'dimension') || [];

  if (tablesLoading) {
    return (
      <div className="flex items-center justify-center h-96">
        <LoadingSpinner size="lg" text={t('common.loading')} />
      </div>
    );
  }

  const hasTables = tables && tables.length > 0;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 dark:text-white">
            {t('warehouse.title')}
          </h1>
          <p className="mt-1 text-slate-500 dark:text-slate-400">
            {t('warehouse.subtitle')}
          </p>
        </div>
        <Button
          variant="secondary"
          icon={<ArrowPathIcon className="h-5 w-5" />}
          onClick={() => refetchTables()}
        >
          {t('common.refresh')}
        </Button>
      </div>

      {!hasTables ? (
        /* Empty State */
        <Card variant="glass" className="py-16">
          <div className="text-center">
            <TableCellsIcon className="mx-auto h-16 w-16 text-slate-300 dark:text-slate-600" />
            <h3 className="mt-4 text-lg font-medium text-slate-900 dark:text-white">
              No warehouse tables found
            </h3>
            <p className="mt-2 text-slate-500 dark:text-slate-400">
              Run the ETL pipeline to populate the data warehouse
            </p>
          </div>
        </Card>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          {/* Left Sidebar - Tables */}
          <div className="space-y-4">
            {/* Stats */}
            <Card variant="glass" className="p-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <p className="text-xs text-slate-500 dark:text-slate-400">Fact Tables</p>
                  <p className="text-xl font-bold text-primary-600 dark:text-primary-400">
                    {stats?.fact_table_count || factTables.length}
                  </p>
                </div>
                <div>
                  <p className="text-xs text-slate-500 dark:text-slate-400">Dimensions</p>
                  <p className="text-xl font-bold text-emerald-600 dark:text-emerald-400">
                    {stats?.dimension_table_count || dimensionTables.length}
                  </p>
                </div>
              </div>
            </Card>

            {/* Fact Tables */}
            <Card variant="glass">
              <CardHeader title="Fact Tables" />
              <div className="space-y-1">
                {factTables.length === 0 ? (
                  <p className="text-sm text-slate-500 dark:text-slate-400 py-2">
                    No fact tables
                  </p>
                ) : (
                  factTables.map((table: any) => (
                    <button
                      key={table.name}
                      onClick={() => handleTableSelect(table.name)}
                      className={clsx(
                        'w-full flex items-center justify-between p-2 rounded-lg transition-all text-left',
                        selectedTable === table.name
                          ? 'bg-primary-50 dark:bg-primary-900/20 text-primary-700 dark:text-primary-300'
                          : 'hover:bg-slate-50 dark:hover:bg-slate-800'
                      )}
                    >
                      <div className="flex items-center gap-2">
                        <CubeIcon className="h-4 w-4 text-primary-500" />
                        <span className="text-sm font-medium truncate">{table.display_name}</span>
                      </div>
                      <span className="text-xs text-slate-500 dark:text-slate-400">
                        {table.row_count?.toLocaleString()}
                      </span>
                    </button>
                  ))
                )}
              </div>
            </Card>

            {/* Dimension Tables */}
            <Card variant="glass">
              <CardHeader title="Dimension Tables" />
              <div className="space-y-1 max-h-64 overflow-y-auto">
                {dimensionTables.length === 0 ? (
                  <p className="text-sm text-slate-500 dark:text-slate-400 py-2">
                    No dimension tables
                  </p>
                ) : (
                  dimensionTables.map((table: any) => (
                    <button
                      key={table.name}
                      onClick={() => handleTableSelect(table.name)}
                      className={clsx(
                        'w-full flex items-center justify-between p-2 rounded-lg transition-all text-left',
                        selectedTable === table.name
                          ? 'bg-emerald-50 dark:bg-emerald-900/20 text-emerald-700 dark:text-emerald-300'
                          : 'hover:bg-slate-50 dark:hover:bg-slate-800'
                      )}
                    >
                      <div className="flex items-center gap-2">
                        <TableCellsIcon className="h-4 w-4 text-emerald-500" />
                        <span className="text-sm font-medium truncate">{table.display_name}</span>
                      </div>
                      <span className="text-xs text-slate-500 dark:text-slate-400">
                        {table.row_count?.toLocaleString()}
                      </span>
                    </button>
                  ))
                )}
              </div>
            </Card>
          </div>

          {/* Main Content */}
          <div className="lg:col-span-3 space-y-6">
            {/* Query Interface */}
            <Card variant="glass">
              {/* Tab Navigation */}
              <div className="flex gap-2 p-2 bg-slate-100 dark:bg-slate-800 rounded-t-xl">
                <button
                  onClick={() => setActiveTab('sql')}
                  className={clsx(
                    'flex-1 flex items-center justify-center gap-2 px-4 py-2 rounded-lg font-medium text-sm transition-all',
                    activeTab === 'sql'
                      ? 'bg-white dark:bg-slate-700 text-primary-600 dark:text-primary-400 shadow'
                      : 'text-slate-600 dark:text-slate-400'
                  )}
                >
                  <TableCellsIcon className="h-4 w-4" />
                  SQL Query
                </button>
                <button
                  onClick={() => setActiveTab('natural')}
                  className={clsx(
                    'flex-1 flex items-center justify-center gap-2 px-4 py-2 rounded-lg font-medium text-sm transition-all',
                    activeTab === 'natural'
                      ? 'bg-white dark:bg-slate-700 text-primary-600 dark:text-primary-400 shadow'
                      : 'text-slate-600 dark:text-slate-400'
                  )}
                >
                  <SparklesIcon className="h-4 w-4" />
                  {t('warehouse.naturalLanguage')}
                </button>
              </div>

              <div className="p-4">
                {activeTab === 'sql' ? (
                  <div className="space-y-3">
                    <div className="relative">
                      <textarea
                        value={sqlQuery}
                        onChange={(e) => setSqlQuery(e.target.value)}
                        placeholder="SELECT * FROM fact_main LIMIT 100"
                        rows={4}
                        className="w-full px-4 py-3 bg-slate-900 text-green-400 font-mono text-sm rounded-lg border border-slate-700 focus:ring-2 focus:ring-primary-500 resize-none"
                      />
                      <button
                        onClick={() => copyToClipboard(sqlQuery)}
                        className="absolute top-2 right-2 p-1.5 text-slate-400 hover:text-white transition-colors"
                      >
                        <DocumentDuplicateIcon className="h-4 w-4" />
                      </button>
                    </div>
                    <div className="flex justify-end">
                      <Button
                        onClick={handleExecuteQuery}
                        icon={<PlayIcon className="h-5 w-5" />}
                        disabled={executeSqlMutation.isPending || !sqlQuery.trim()}
                      >
                        {executeSqlMutation.isPending ? t('common.loading') : t('warehouse.runQuery')}
                      </Button>
                    </div>
                  </div>
                ) : (
                  <div className="space-y-3">
                    <div className="relative">
                      <input
                        type="text"
                        value={naturalQuery}
                        onChange={(e) => setNaturalQuery(e.target.value)}
                        onKeyPress={(e) => e.key === 'Enter' && handleNaturalQuery()}
                        placeholder={t('warehouse.nlPlaceholder')}
                        className="w-full px-4 py-3 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg focus:ring-2 focus:ring-primary-500"
                      />
                      <MagnifyingGlassIcon className="absolute right-3 top-1/2 -translate-y-1/2 h-5 w-5 text-slate-400" />
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {[
                        'Show me total sales by month',
                        'What are the top 10 products?',
                        'Count records by category',
                      ].map((suggestion) => (
                        <button
                          key={suggestion}
                          onClick={() => setNaturalQuery(suggestion)}
                          className="px-3 py-1 text-xs bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 rounded-full hover:bg-slate-200 dark:hover:bg-slate-700 transition-colors"
                        >
                          {suggestion}
                        </button>
                      ))}
                    </div>
                    <div className="flex justify-end">
                      <Button
                        onClick={handleNaturalQuery}
                        icon={<SparklesIcon className="h-5 w-5" />}
                        disabled={naturalQueryMutation.isPending || !naturalQuery.trim()}
                      >
                        {naturalQueryMutation.isPending ? t('common.loading') : t('warehouse.generateSQL')}
                      </Button>
                    </div>
                  </div>
                )}
              </div>
            </Card>

            {/* Results */}
            <Card variant="glass">
              <CardHeader
                title={t('warehouse.results')}
                subtitle={queryResults ? `${queryResults.row_count} ${t('warehouse.rowsReturned')}` : undefined}
              />
              <div className="overflow-x-auto">
                {queryResults ? (
                  <table className="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
                    <thead className="bg-slate-50 dark:bg-slate-800/50">
                      <tr>
                        {queryResults.columns?.map((col: string) => (
                          <th
                            key={col}
                            className="px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider"
                          >
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-200 dark:divide-slate-700">
                      {queryResults.data?.slice(0, 50).map((row: any, rowIndex: number) => (
                        <tr key={rowIndex} className="hover:bg-slate-50 dark:hover:bg-slate-800/50">
                          {queryResults.columns?.map((col: string) => (
                            <td
                              key={col}
                              className="px-4 py-2 text-sm text-slate-600 dark:text-slate-400 whitespace-nowrap"
                            >
                              {row[col]?.toString() ?? 'NULL'}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : tableData ? (
                  <table className="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
                    <thead className="bg-slate-50 dark:bg-slate-800/50">
                      <tr>
                        {tableData.data?.[0] && Object.keys(tableData.data[0]).map((col: string) => (
                          <th
                            key={col}
                            className="px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider"
                          >
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-200 dark:divide-slate-700">
                      {tableData.data?.slice(0, 50).map((row: any, rowIndex: number) => (
                        <tr key={rowIndex} className="hover:bg-slate-50 dark:hover:bg-slate-800/50">
                          {Object.values(row).map((value: any, colIndex: number) => (
                            <td
                              key={colIndex}
                              className="px-4 py-2 text-sm text-slate-600 dark:text-slate-400 whitespace-nowrap"
                            >
                              {value?.toString() ?? 'NULL'}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <div className="text-center py-12">
                    <TableCellsIcon className="mx-auto h-12 w-12 text-slate-300 dark:text-slate-600" />
                    <p className="mt-4 text-slate-500 dark:text-slate-400">
                      Select a table or run a query to see results
                    </p>
                  </div>
                )}
              </div>
              {(queryResults?.row_count > 50 || tableData?.row_count > 50) && (
                <div className="p-4 border-t border-slate-200 dark:border-slate-700 text-center">
                  <p className="text-sm text-slate-500 dark:text-slate-400">
                    Showing first 50 rows of {queryResults?.row_count || tableData?.row_count}
                  </p>
                </div>
              )}
            </Card>

            {/* Table Schema */}
            {selectedTable && tableData && (
              <Card variant="glass">
                <CardHeader title={`${selectedTable} Schema`} />
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
                    <thead className="bg-slate-50 dark:bg-slate-800/50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase">
                          Column
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase">
                          Type
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-200 dark:divide-slate-700">
                      {tables?.find((t: any) => t.name === selectedTable)?.columns?.map((col: any) => (
                        <tr key={col.name} className="hover:bg-slate-50 dark:hover:bg-slate-800/50">
                          <td className="px-4 py-2 text-sm font-mono text-slate-900 dark:text-white">
                            {col.name}
                          </td>
                          <td className="px-4 py-2 text-sm text-slate-600 dark:text-slate-400">
                            {col.type}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Card>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default WarehousePage;
