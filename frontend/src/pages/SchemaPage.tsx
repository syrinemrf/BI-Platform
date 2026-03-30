import React from 'react';
import { useTranslation } from 'react-i18next';
import { useQuery } from '@tanstack/react-query';
import {
  CubeIcon,
  TableCellsIcon,
  KeyIcon,
  ArrowRightIcon,
  ArrowPathIcon,
} from '@heroicons/react/24/outline';
import { Card, CardHeader } from '../components/common/Card';
import { Button } from '../components/common/Button';
import { LoadingSpinner } from '../components/common/LoadingSpinner';
import { warehouseApi } from '../services/api';
import { clsx } from 'clsx';

export const SchemaPage: React.FC = () => {
  const { t } = useTranslation();

  // Fetch star schema
  const { data: schema, isLoading: schemaLoading, refetch } = useQuery({
    queryKey: ['star-schema'],
    queryFn: () => warehouseApi.getSchema(),
    retry: 1,
  });

  // Fetch tables
  const { data: tables, isLoading: tablesLoading } = useQuery({
    queryKey: ['warehouse-tables'],
    queryFn: warehouseApi.listTables,
  });

  // Fetch relationships
  const { data: relationshipsData } = useQuery({
    queryKey: ['warehouse-relationships'],
    queryFn: warehouseApi.getRelationships,
  });

  const isLoading = schemaLoading || tablesLoading;

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-96">
        <LoadingSpinner size="lg" text={t('common.loading')} />
      </div>
    );
  }

  const schemaDefinition = schema?.schema_definition || {};
  const factTable = schemaDefinition.fact_table;
  const dimensions = schemaDefinition.dimensions || [];
  const relationships = relationshipsData?.relationships || schemaDefinition.relationships || [];

  const factTableInfo = tables?.find((t: any) => t.name === factTable?.name);
  const dimensionTableInfos = dimensions.map((dim: any) =>
    tables?.find((t: any) => t.name === dim.name)
  ).filter(Boolean);

  const hasSchema = factTable && dimensions.length > 0;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 dark:text-white">
            {t('schema.title')}
          </h1>
          <p className="mt-1 text-slate-500 dark:text-slate-400">
            {t('schema.subtitle')}
          </p>
        </div>
        <div className="flex gap-2">
          <Button
            variant="secondary"
            icon={<ArrowPathIcon className="h-5 w-5" />}
            onClick={() => refetch()}
          >
            {t('common.refresh')}
          </Button>
        </div>
      </div>

      {!hasSchema ? (
        /* Empty State */
        <Card variant="glass" className="py-16">
          <div className="text-center">
            <CubeIcon className="mx-auto h-16 w-16 text-slate-300 dark:text-slate-600" />
            <h3 className="mt-4 text-lg font-medium text-slate-900 dark:text-white">
              {t('schema.noSchema')}
            </h3>
            <p className="mt-2 text-slate-500 dark:text-slate-400">
              Run the ETL pipeline to generate a star schema from your data.
            </p>
          </div>
        </Card>
      ) : (
        <>
          {/* Schema Stats */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <Card variant="glass" className="p-4">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-primary-100 dark:bg-primary-900/30 rounded-lg">
                  <CubeIcon className="h-6 w-6 text-primary-600 dark:text-primary-400" />
                </div>
                <div>
                  <p className="text-sm text-slate-500 dark:text-slate-400">Fact Tables</p>
                  <p className="text-2xl font-bold text-slate-900 dark:text-white">1</p>
                </div>
              </div>
            </Card>
            <Card variant="glass" className="p-4">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-emerald-100 dark:bg-emerald-900/30 rounded-lg">
                  <TableCellsIcon className="h-6 w-6 text-emerald-600 dark:text-emerald-400" />
                </div>
                <div>
                  <p className="text-sm text-slate-500 dark:text-slate-400">Dimensions</p>
                  <p className="text-2xl font-bold text-slate-900 dark:text-white">
                    {dimensions.length}
                  </p>
                </div>
              </div>
            </Card>
            <Card variant="glass" className="p-4">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-amber-100 dark:bg-amber-900/30 rounded-lg">
                  <KeyIcon className="h-6 w-6 text-amber-600 dark:text-amber-400" />
                </div>
                <div>
                  <p className="text-sm text-slate-500 dark:text-slate-400">Relationships</p>
                  <p className="text-2xl font-bold text-slate-900 dark:text-white">
                    {relationships.length}
                  </p>
                </div>
              </div>
            </Card>
            <Card variant="glass" className="p-4">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
                  <TableCellsIcon className="h-6 w-6 text-blue-600 dark:text-blue-400" />
                </div>
                <div>
                  <p className="text-sm text-slate-500 dark:text-slate-400">Total Rows</p>
                  <p className="text-2xl font-bold text-slate-900 dark:text-white">
                    {(factTableInfo?.row_count || 0).toLocaleString()}
                  </p>
                </div>
              </div>
            </Card>
          </div>

          {/* Star Schema Visualization */}
          <Card variant="glass">
            <CardHeader title="Star Schema Diagram" />
            <div className="p-4">
              {/* Fact Table in Center */}
              <div className="flex justify-center mb-8">
                <div className="relative">
                  <div className="absolute -inset-4 bg-gradient-to-r from-primary-500/20 to-purple-500/20 rounded-2xl blur-xl" />
                  <div className="relative bg-white dark:bg-slate-800 border-2 border-primary-500 rounded-xl p-6 min-w-[320px] shadow-xl">
                    <div className="flex items-center gap-3 mb-4">
                      <div className="p-2 bg-primary-100 dark:bg-primary-900/30 rounded-lg">
                        <CubeIcon className="h-6 w-6 text-primary-600 dark:text-primary-400" />
                      </div>
                      <div>
                        <h3 className="font-bold text-lg text-slate-900 dark:text-white">
                          {factTable?.display_name || factTable?.name || 'Fact Table'}
                        </h3>
                        <p className="text-sm text-slate-500 dark:text-slate-400">
                          {(factTableInfo?.row_count || 0).toLocaleString()} rows
                        </p>
                      </div>
                    </div>

                    {/* Measures */}
                    {factTable?.measures && factTable.measures.length > 0 && (
                      <div className="space-y-2 mb-4">
                        <p className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase">
                          Measures
                        </p>
                        <div className="space-y-1">
                          {factTable.measures.slice(0, 6).map((measure: string) => (
                            <div
                              key={measure}
                              className="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300 bg-amber-50 dark:bg-amber-900/20 px-3 py-1.5 rounded"
                            >
                              <div className="h-2 w-2 rounded-full bg-amber-500" />
                              <span className="font-mono text-xs">{measure}</span>
                            </div>
                          ))}
                          {factTable.measures.length > 6 && (
                            <p className="text-xs text-slate-500 dark:text-slate-400 px-3">
                              +{factTable.measures.length - 6} more
                            </p>
                          )}
                        </div>
                      </div>
                    )}

                    {/* Foreign Keys */}
                    {factTable?.dimension_keys && Object.keys(factTable.dimension_keys).length > 0 && (
                      <div className="space-y-2">
                        <p className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase">
                          Foreign Keys
                        </p>
                        <div className="space-y-1">
                          {Object.entries(factTable.dimension_keys).slice(0, 4).map(([dim, fk]: [string, any]) => (
                            <div
                              key={dim}
                              className="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300 bg-slate-50 dark:bg-slate-700/50 px-3 py-1.5 rounded"
                            >
                              <KeyIcon className="h-3 w-3 text-primary-500" />
                              <span className="font-mono text-xs">{fk}</span>
                              <ArrowRightIcon className="h-3 w-3 text-slate-400" />
                              <span className="text-xs">{dim}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              </div>

              {/* Dimension Tables */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                {dimensions.map((dim: any, index: number) => {
                  const dimTableInfo = dimensionTableInfos.find((t: any) => t?.name === dim.name);

                  return (
                    <div
                      key={dim.name}
                      className={clsx(
                        "bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl p-4 shadow-sm hover:shadow-lg transition-all"
                      )}
                    >
                      <div className="flex items-center gap-2 mb-3">
                        <div className={clsx(
                          "p-1.5 rounded-lg",
                          dim.is_time_dimension
                            ? "bg-blue-100 dark:bg-blue-900/30"
                            : "bg-emerald-100 dark:bg-emerald-900/30"
                        )}>
                          <TableCellsIcon className={clsx(
                            "h-4 w-4",
                            dim.is_time_dimension
                              ? "text-blue-600 dark:text-blue-400"
                              : "text-emerald-600 dark:text-emerald-400"
                          )} />
                        </div>
                        <div className="flex-1 min-w-0">
                          <h4 className="font-semibold text-sm text-slate-900 dark:text-white truncate">
                            {dim.display_name || dim.name}
                          </h4>
                          <p className="text-xs text-slate-500 dark:text-slate-400">
                            {(dimTableInfo?.row_count || dim.row_count || 0).toLocaleString()} rows
                          </p>
                        </div>
                        {dim.is_time_dimension && (
                          <span className="text-xs px-2 py-0.5 bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 rounded-full">
                            Time
                          </span>
                        )}
                      </div>

                      {/* Columns */}
                      {dim.columns && dim.columns.length > 0 && (
                        <div className="space-y-1">
                          {dim.columns.slice(0, 4).map((col: string) => (
                            <div
                              key={col}
                              className="text-xs text-slate-600 dark:text-slate-400 font-mono bg-slate-50 dark:bg-slate-700/50 px-2 py-1 rounded truncate"
                            >
                              {col}
                            </div>
                          ))}
                          {dim.columns.length > 4 && (
                            <p className="text-xs text-slate-500 dark:text-slate-400 px-2">
                              +{dim.columns.length - 4} more columns
                            </p>
                          )}
                        </div>
                      )}

                      {/* Surrogate Key */}
                      {dim.surrogate_key && (
                        <div className="mt-3 pt-3 border-t border-slate-200 dark:border-slate-700">
                          <div className="flex items-center gap-1 text-xs text-slate-500 dark:text-slate-400">
                            <KeyIcon className="h-3 w-3" />
                            <span className="font-mono">{dim.surrogate_key}</span>
                          </div>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          </Card>

          {/* Relationships */}
          {relationships.length > 0 && (
            <Card variant="glass">
              <CardHeader title={t('schema.relationships')} />
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
                  <thead className="bg-slate-50 dark:bg-slate-800/50">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase">
                        Fact Table
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase">
                        Foreign Key
                      </th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-slate-500 uppercase">
                        Relationship
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase">
                        Dimension Table
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase">
                        Primary Key
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-200 dark:divide-slate-700">
                    {relationships.map((rel: any, index: number) => (
                      <tr key={index} className="hover:bg-slate-50 dark:hover:bg-slate-800/50">
                        <td className="px-4 py-3 text-sm font-mono text-primary-600 dark:text-primary-400">
                          {rel.fact_table}
                        </td>
                        <td className="px-4 py-3 text-sm font-mono text-slate-600 dark:text-slate-400">
                          {rel.fact_column}
                        </td>
                        <td className="px-4 py-3 text-center">
                          <ArrowRightIcon className="h-4 w-4 mx-auto text-slate-400" />
                        </td>
                        <td className="px-4 py-3 text-sm font-mono text-emerald-600 dark:text-emerald-400">
                          {rel.dimension_table}
                        </td>
                        <td className="px-4 py-3 text-sm font-mono text-slate-600 dark:text-slate-400">
                          {rel.dimension_column}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}

          {/* Schema Info */}
          <Card variant="glass">
            <CardHeader title="Schema Information" />
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <h4 className="text-sm font-medium text-slate-700 dark:text-slate-300 mb-3">
                  ETL Job Details
                </h4>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-slate-500 dark:text-slate-400">Job ID:</span>
                    <span className="font-medium text-slate-900 dark:text-white">
                      #{schema?.etl_job_id || 'N/A'}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-slate-500 dark:text-slate-400">Created:</span>
                    <span className="font-medium text-slate-900 dark:text-white">
                      {schema?.created_at ? new Date(schema.created_at).toLocaleString() : 'N/A'}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-slate-500 dark:text-slate-400">Fact Table:</span>
                    <span className="font-medium text-slate-900 dark:text-white">
                      {schema?.fact_table_name || factTable?.name || 'N/A'}
                    </span>
                  </div>
                </div>
              </div>
              <div>
                <h4 className="text-sm font-medium text-slate-700 dark:text-slate-300 mb-3">
                  Dimension Tables
                </h4>
                <div className="space-y-1">
                  {dimensions.slice(0, 5).map((dim: any) => (
                    <div key={dim.name} className="flex items-center gap-2 text-sm">
                      <div className={clsx(
                        "h-2 w-2 rounded-full",
                        dim.is_time_dimension ? "bg-blue-500" : "bg-emerald-500"
                      )} />
                      <span className="text-slate-600 dark:text-slate-400">{dim.name}</span>
                    </div>
                  ))}
                  {dimensions.length > 5 && (
                    <p className="text-xs text-slate-500 dark:text-slate-400 ml-4">
                      +{dimensions.length - 5} more
                    </p>
                  )}
                </div>
              </div>
            </div>
          </Card>
        </>
      )}
    </div>
  );
};

export default SchemaPage;
