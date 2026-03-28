import React, { useCallback, useState } from 'react';
import { useDropzone } from 'react-dropzone';
import { useTranslation } from 'react-i18next';
import {
  CloudArrowUpIcon,
  DocumentIcon,
  XMarkIcon,
} from '@heroicons/react/24/outline';
import { clsx } from 'clsx';
import Button from '../common/Button';

interface UploadZoneProps {
  onUpload: (file: File) => Promise<void>;
  accept?: Record<string, string[]>;
  maxSize?: number;
  className?: string;
}

export const UploadZone: React.FC<UploadZoneProps> = ({
  onUpload,
  accept = {
    'text/csv': ['.csv'],
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
    'application/vnd.ms-excel': ['.xls'],
    'application/json': ['.json'],
  },
  maxSize = 100 * 1024 * 1024, // 100MB
  className,
}) => {
  const { t } = useTranslation();
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const onDrop = useCallback((acceptedFiles: File[]) => {
    setError(null);
    if (acceptedFiles.length > 0) {
      setSelectedFile(acceptedFiles[0]);
    }
  }, []);

  const onDropRejected = useCallback(() => {
    setError('Invalid file type or size. Please check and try again.');
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    onDropRejected,
    accept,
    maxSize,
    multiple: false,
  });

  const handleUpload = async () => {
    if (!selectedFile) return;

    setUploading(true);
    setError(null);

    try {
      await onUpload(selectedFile);
      setSelectedFile(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Upload failed');
    } finally {
      setUploading(false);
    }
  };

  const handleRemove = () => {
    setSelectedFile(null);
    setError(null);
  };

  const formatFileSize = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  return (
    <div className={className}>
      {!selectedFile ? (
        <div
          {...getRootProps()}
          className={clsx(
            'relative border-2 border-dashed rounded-2xl p-8 transition-all duration-200 cursor-pointer',
            isDragActive
              ? 'border-primary-500 bg-primary-50 dark:bg-primary-900/20'
              : 'border-slate-300 dark:border-slate-700 hover:border-primary-400 hover:bg-slate-50 dark:hover:bg-slate-800/50'
          )}
        >
          <input {...getInputProps()} />

          <div className="flex flex-col items-center text-center">
            <div
              className={clsx(
                'p-4 rounded-2xl mb-4 transition-colors',
                isDragActive
                  ? 'bg-primary-100 dark:bg-primary-900/30'
                  : 'bg-slate-100 dark:bg-slate-800'
              )}
            >
              <CloudArrowUpIcon
                className={clsx(
                  'h-12 w-12 transition-colors',
                  isDragActive
                    ? 'text-primary-600'
                    : 'text-slate-400'
                )}
              />
            </div>

            <h3 className="text-lg font-semibold text-slate-900 dark:text-white mb-2">
              {t('dataSources.upload')}
            </h3>

            <p className="text-slate-500 dark:text-slate-400 mb-4">
              {t('dataSources.uploadDesc')}
            </p>

            <div className="flex flex-wrap gap-2 justify-center mb-2">
              {['.csv', '.xlsx', '.xls', '.json'].map((ext) => (
                <span
                  key={ext}
                  className="px-2 py-1 text-xs font-medium bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 rounded"
                >
                  {ext}
                </span>
              ))}
            </div>

            <p className="text-xs text-slate-400 dark:text-slate-500">
              {t('dataSources.maxSize')}
            </p>
          </div>

          {isDragActive && (
            <div className="absolute inset-0 bg-primary-500/10 rounded-2xl flex items-center justify-center">
              <p className="text-primary-600 font-medium">
                Drop file here
              </p>
            </div>
          )}
        </div>
      ) : (
        <div className="border-2 border-slate-200 dark:border-slate-700 rounded-2xl p-6">
          <div className="flex items-center gap-4">
            <div className="p-3 bg-primary-100 dark:bg-primary-900/30 rounded-xl">
              <DocumentIcon className="h-8 w-8 text-primary-600" />
            </div>

            <div className="flex-1 min-w-0">
              <p className="font-medium text-slate-900 dark:text-white truncate">
                {selectedFile.name}
              </p>
              <p className="text-sm text-slate-500 dark:text-slate-400">
                {formatFileSize(selectedFile.size)}
              </p>
            </div>

            <button
              onClick={handleRemove}
              className="p-2 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg transition-colors"
              disabled={uploading}
            >
              <XMarkIcon className="h-5 w-5 text-slate-500" />
            </button>
          </div>

          <div className="mt-4 flex gap-3">
            <Button
              onClick={handleUpload}
              loading={uploading}
              className="flex-1"
            >
              {uploading ? t('common.loading') : t('dataSources.upload')}
            </Button>
            <Button
              variant="outline"
              onClick={handleRemove}
              disabled={uploading}
            >
              {t('common.cancel')}
            </Button>
          </div>
        </div>
      )}

      {error && (
        <div className="mt-4 p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl">
          <p className="text-sm text-red-600 dark:text-red-400">{error}</p>
        </div>
      )}
    </div>
  );
};

export default UploadZone;
