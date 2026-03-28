import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { CubeIcon, TableCellsIcon, KeyIcon, ArrowRightIcon } from '@heroicons/react/24/outline';
import { Card, CardHeader } from '../common/Card';
import { LoadingSpinner } from '../common/LoadingSpinner';
import { warehouseApi } from '../../services/api';
import { clsx } from 'clsx';

interface StarSchemaViewerProps {
  jobId?: number;
}

export const StarSchemaViewer: React.FC<StarSchemaViewerProps> = ({ jobId }) => {
  const { data: schema, isLoading, error } = useQuery({
    queryKey: ['star-schema', jobId],
    queryFn: () => warehouseApi.getSchema(jobId),
    retry: 1,
  });

  const { data: tables } = useQuery({
    queryKey: ['warehouse-tables'],
    queryFn: warehouseApi.listTables,
  });

  if (isLoading) {
    return (
      <Card variant="glass">
        <CardHeader title="Star Schema" />
        <div className="flex items-center justify-center py-12">
          <LoadingSpinner size="lg" />
        </div>
      </Card>
    );
  }

  if (error || !schema) {
    return (
      <Card variant="glass">
        <CardHeader title="Star Schema" />
        <div className="text-center py-12">
          <CubeIcon className="mx-auto h-12 w-12 text-slate-300 dark:text-slate-600" />
          <p className="mt-4 text-slate-500 dark:text-slate-400">
            No star schema generated yet. Run an ETL job first.
          </p>
        </div>
      </Card>
    );
  }

  const { fact_table, dimensions, relationships } = schema.schema_definition || {};

  const factTable = tables?.find((t: any) => t.name === fact_table?.name);
  const dimensionTables = dimensions?.map((dim: any) =>
    tables?.find((t: any) => t.name === dim.name)
  ).filter(Boolean) || [];

  return (
    <Card variant="glass">
      <CardHeader
        title="Star Schema"
        subtitle={`Generated from ETL Job #${schema.etl_job_id}`}
      />

      <div className="space-y-6">
        {/* Star Schema Diagram */}
        <div className="relative">
          {/* Center: Fact Table */}
          <div className="flex justify-center mb-8">
            <div className="relative">
              <div className="absolute -inset-4 bg-gradient-to-r from-primary-500/20 to-purple-500/20 rounded-2xl blur-xl" />
              <div className="relative bg-white dark:bg-slate-800 border-2 border-primary-500 rounded-xl p-6 min-w-[300px] shadow-lg">
                <div className="flex items-center gap-3 mb-4">
                  <div className="p-2 bg-primary-100 dark:bg-primary-900/30 rounded-lg">
                    <CubeIcon className="h-6 w-6 text-primary-600 dark:text-primary-400" />
                  </div>
                  <div>
                    <h3 className="font-bold text-lg text-slate-900 dark:text-white">
                      {factTable?.display_name || fact_table?.display_name || 'Fact Table'}
                    </h3>
                    <p className="text-sm text-slate-500 dark:text-slate-400">
                      {factTable?.row_count?.toLocaleString()} rows
                    </p>
                  </div>
                </div>

                {/* Measures */}
                {fact_table?.measures && fact_table.measures.length > 0 && (
                  <div className="space-y-2">
                    <p className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase">
                      Measures
                    </p>
                    <div className="space-y-1">
                      {fact_table.measures.slice(0, 5).map((measure: string) => (
                        <div
                          key={measure}
                          className="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300 bg-slate-50 dark:bg-slate-700/50 px-3 py-1.5 rounded"
                        >
                          <div className="h-2 w-2 rounded-full bg-amber-500" />
                          <span className="font-mono text-xs">{measure}</span>
                        </div>
                      ))}
                      {fact_table.measures.length > 5 && (
                        <p className="text-xs text-slate-500 dark:text-slate-400 px-3">
                          +{fact_table.measures.length - 5} more
                        </p>
                      )}
                    </div>
                  </div>
                )}

                {/* Foreign Keys */}
                {fact_table?.dimension_keys && Object.keys(fact_table.dimension_keys).length > 0 && (
                  <div className="mt-4 space-y-2">
                    <p className="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase">
                      Foreign Keys
                    </p>
                    <div className="space-y-1">
                      {Object.entries(fact_table.dimension_keys).slice(0, 3).map(([dim, fk]: [string, any]) => (
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

          {/* Dimensions around the fact table */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
            {dimensionTables.map((dimTable: any, index: number) => {
              const dim = dimensions?.find((d: any) => d.name === dimTable.name);

              return (
                <div
                  key={dimTable.name}
                  className={clsx(
                    "bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg p-4 shadow-sm hover:shadow-md transition-shadow"
                  )}
                >
                  <div className="flex items-center gap-2 mb-3">
                    <div className={clsx(
                      "p-1.5 rounded-lg",
                      dim?.is_time_dimension
                        ? "bg-blue-100 dark:bg-blue-900/30"
                        : "bg-emerald-100 dark:bg-emerald-900/30"
                    )}>
                      <TableCellsIcon className={clsx(
                        "h-4 w-4",
                        dim?.is_time_dimension
                          ? "text-blue-600 dark:text-blue-400"
                          : "text-emerald-600 dark:text-emerald-400"
                      )} />
                    </div>
                    <div className="flex-1 min-w-0">
                      <h4 className="font-semibold text-sm text-slate-900 dark:text-white truncate">
                        {dimTable.display_name}
                      </h4>
                      <p className="text-xs text-slate-500 dark:text-slate-400">
                        {dimTable.row_count?.toLocaleString()} rows
                      </p>
                    </div>
                  </div>

                  {/* Dimension Columns */}
                  {dim?.columns && dim.columns.length > 0 && (
                    <div className="space-y-1">
                      {dim.columns.slice(0, 3).map((col: string) => (
                        <div
                          key={col}
                          className="text-xs text-slate-600 dark:text-slate-400 font-mono bg-slate-50 dark:bg-slate-700/50 px-2 py-1 rounded"
                        >
                          {col}
                        </div>
                      ))}
                      {dim.columns.length > 3 && (
                        <p className="text-xs text-slate-500 dark:text-slate-400 px-2">
                          +{dim.columns.length - 3} more
                        </p>
                      )}
                    </div>
                  )}

                  {/* Surrogate Key */}
                  {dim?.surrogate_key && (
                    <div className="mt-2 pt-2 border-t border-slate-200 dark:border-slate-700">
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

        {/* Relationships Summary */}
        {relationships && relationships.length > 0 && (
          <div className="mt-6 pt-6 border-t border-slate-200 dark:border-slate-700">
            <h4 className="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-3">
              Relationships ({relationships.length})
            </h4>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {relationships.map((rel: any, index: number) => (
                <div
                  key={index}
                  className="flex items-center gap-3 text-sm bg-slate-50 dark:bg-slate-800/50 px-4 py-2 rounded-lg"
                >
                  <span className="font-mono text-xs text-primary-600 dark:text-primary-400">
                    {rel.fact_table}.{rel.fact_column}
                  </span>
                  <ArrowRightIcon className="h-4 w-4 text-slate-400" />
                  <span className="font-mono text-xs text-emerald-600 dark:text-emerald-400">
                    {rel.dimension_table}.{rel.dimension_column}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Schema Info */}
        <div className="mt-6 pt-6 border-t border-slate-200 dark:border-slate-700">
          <div className="flex items-center justify-between text-sm">
            <div className="flex items-center gap-6">
              <div>
                <span className="text-slate-500 dark:text-slate-400">Fact Tables:</span>
                <span className="ml-2 font-semibold text-slate-900 dark:text-white">
                  {fact_table ? 1 : 0}
                </span>
              </div>
              <div>
                <span className="text-slate-500 dark:text-slate-400">Dimensions:</span>
                <span className="ml-2 font-semibold text-slate-900 dark:text-white">
                  {dimensions?.length || 0}
                </span>
              </div>
              <div>
                <span className="text-slate-500 dark:text-slate-400">Relationships:</span>
                <span className="ml-2 font-semibold text-slate-900 dark:text-white">
                  {relationships?.length || 0}
                </span>
              </div>
            </div>
            <div className="text-xs text-slate-500 dark:text-slate-400">
              Created: {new Date(schema.created_at).toLocaleDateString()}
            </div>
          </div>
        </div>
      </div>
    </Card>
  );
};
