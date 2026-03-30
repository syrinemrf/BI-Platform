import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  DocumentIcon,
  TrashIcon,
  EyeIcon,
  ArrowPathIcon,
} from '@heroicons/react/24/outline';
import { toast } from 'react-hot-toast';
import { Card, CardHeader } from '../components/common/Card';
import { Button } from '../components/common/Button';
import { LoadingSpinner } from '../components/common/LoadingSpinner';
import { UploadZone } from '../components/DataUpload/UploadZone';
import { datasetApi } from '../services/api';
import type { Dataset } from '../types';
import { clsx } from 'clsx';

export const DataSourcesPage: React.FC = () => {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [selectedDataset, setSelectedDataset] = useState<Dataset | null>(null);
  const [previewData, setPreviewData] = useState<{ columns: string[]; data: Record<string, unknown>[] } | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);

  // Fetch datasets
  const { data: datasets, isLoading } = useQuery({
    queryKey: ['datasets'],
    queryFn: datasetApi.list,
  });

  // Upload mutation
  const uploadMutation = useMutation({
    mutationFn: (file: File) => datasetApi.upload(file),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['datasets'] });
      toast.success(t('dataSources.uploadSuccess'));
    },
    onError: (error: Error) => {
      toast.error(error.message || t('dataSources.uploadError'));
    },
  });

  // Delete mutation
  const deleteMutation = useMutation({
    mutationFn: datasetApi.delete,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['datasets'] });
      toast.success(t('dataSources.deleteSuccess'));
      if (selectedDataset) {
        setSelectedDataset(null);
        setPreviewData(null);
      }
    },
    onError: (error: Error) => {
      toast.error(error.message);
    },
  });

  const handlePreview = async (dataset: Dataset) => {
    setSelectedDataset(dataset);
    setPreviewLoading(true);
    try {
      const preview = await datasetApi.preview(dataset.id, 50);
      setPreviewData(preview);
    } catch (error) {
      toast.error('Failed to load preview');
    } finally {
      setPreviewLoading(false);
    }
  };

  const handleDelete = (dataset: Dataset) => {
    if (window.confirm(t('dataSources.deleteConfirm'))) {
      deleteMutation.mutate(dataset.id);
    }
  };

  const formatFileSize = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  const getFileTypeIcon = (type: string) => {
    const colors = {
      csv: 'bg-green-100 text-green-600 dark:bg-green-900/30 dark:text-green-400',
      excel: 'bg-emerald-100 text-emerald-600 dark:bg-emerald-900/30 dark:text-emerald-400',
      json: 'bg-amber-100 text-amber-600 dark:bg-amber-900/30 dark:text-amber-400',
    };
    return colors[type as keyof typeof colors] || 'bg-slate-100 text-slate-600';
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-slate-900 dark:text-white">
          {t('dataSources.title')}
        </h1>
        <p className="mt-1 text-slate-500 dark:text-slate-400">
          {t('dataSources.subtitle')}
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Upload Zone */}
        <div className="lg:col-span-1">
          <Card variant="glass">
            <CardHeader title={t('dataSources.upload')} />
            <UploadZone onUpload={(file) => { uploadMutation.mutateAsync(file); }} />
          </Card>
        </div>

        {/* Dataset List */}
        <div className="lg:col-span-2">
          <Card variant="glass">
            <CardHeader
              title={t('dataSources.datasets')}
              subtitle={`${datasets?.length || 0} datasets`}
            />

            {isLoading ? (
              <div className="py-12">
                <LoadingSpinner size="lg" />
              </div>
            ) : !datasets?.length ? (
              <div className="text-center py-12">
                <DocumentIcon className="mx-auto h-12 w-12 text-slate-300 dark:text-slate-600" />
                <p className="mt-4 text-slate-500 dark:text-slate-400">
                  {t('dataSources.noDatasets')}
                </p>
              </div>
            ) : (
              <div className="space-y-3">
                {datasets.map((dataset) => (
                  <div
                    key={dataset.id}
                    className={clsx(
                      'flex items-center gap-4 p-4 rounded-xl transition-all cursor-pointer',
                      selectedDataset?.id === dataset.id
                        ? 'bg-primary-50 dark:bg-primary-900/20 ring-2 ring-primary-500'
                        : 'bg-slate-50 dark:bg-slate-800/50 hover:bg-slate-100 dark:hover:bg-slate-800'
                    )}
                    onClick={() => handlePreview(dataset)}
                  >
                    <div
                      className={clsx(
                        'p-3 rounded-xl',
                        getFileTypeIcon(dataset.file_type)
                      )}
                    >
                      <DocumentIcon className="h-6 w-6" />
                    </div>

                    <div className="flex-1 min-w-0">
                      <h4 className="font-medium text-slate-900 dark:text-white truncate">
                        {dataset.name}
                      </h4>
                      <div className="flex items-center gap-3 mt-1 text-sm text-slate-500 dark:text-slate-400">
                        <span>{formatFileSize(dataset.file_size)}</span>
                        <span>•</span>
                        <span>
                          {dataset.row_count?.toLocaleString()} {t('dataSources.rows')}
                        </span>
                        <span>•</span>
                        <span>
                          {dataset.column_count} {t('dataSources.columns')}
                        </span>
                      </div>
                      <p className="text-xs text-slate-400 dark:text-slate-500 mt-1">
                        {t('dataSources.uploadedAt')}: {formatDate(dataset.created_at)}
                      </p>
                    </div>

                    <div className="flex items-center gap-2">
                      <Button
                        variant="ghost"
                        size="sm"
                        icon={<EyeIcon className="h-4 w-4" />}
                        onClick={(e) => {
                          e.stopPropagation();
                          handlePreview(dataset);
                        }}
                      >
                        {t('dataSources.preview')}
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        icon={<TrashIcon className="h-4 w-4" />}
                        onClick={(e) => {
                          e.stopPropagation();
                          handleDelete(dataset);
                        }}
                        className="text-red-500 hover:text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20"
                      />
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Card>
        </div>
      </div>

      {/* Preview Panel */}
      {selectedDataset && (
        <Card variant="glass">
          <CardHeader
            title={`${t('dataSources.preview')}: ${selectedDataset.name}`}
            action={
              <Button
                variant="ghost"
                size="sm"
                icon={<ArrowPathIcon className="h-4 w-4" />}
                onClick={() => handlePreview(selectedDataset)}
              >
                {t('common.refresh')}
              </Button>
            }
          />

          {previewLoading ? (
            <div className="py-12">
              <LoadingSpinner size="lg" />
            </div>
          ) : previewData ? (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
                <thead className="bg-slate-50 dark:bg-slate-800/50">
                  <tr>
                    {previewData.columns.map((col) => (
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
                  {previewData.data.slice(0, 20).map((row, idx) => (
                    <tr key={idx} className="hover:bg-slate-50 dark:hover:bg-slate-800/50">
                      {previewData.columns.map((col) => (
                        <td
                          key={col}
                          className="px-4 py-3 text-sm text-slate-600 dark:text-slate-400 whitespace-nowrap"
                        >
                          {String(row[col] ?? '')}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}
        </Card>
      )}
    </div>
  );
};

export default DataSourcesPage;
