import React, { useState, useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useSelector, useDispatch } from 'react-redux';
import {
  PlayIcon,
  CheckCircleIcon,
  XCircleIcon,
  ExclamationTriangleIcon,
  ArrowPathIcon,
  SparklesIcon,
  ChatBubbleBottomCenterTextIcon,
  WrenchScrewdriverIcon,
  DocumentMagnifyingGlassIcon,
  PaperAirplaneIcon,
  ChevronRightIcon,
  ClockIcon,
  BeakerIcon,
  CubeTransparentIcon,
  CircleStackIcon,
  ShieldCheckIcon,
  TableCellsIcon,
  ArrowDownTrayIcon,
  ArrowsRightLeftIcon,
} from '@heroicons/react/24/outline';
import { toast } from 'react-hot-toast';
import { Card, CardHeader } from '../components/common/Card';
import { Button } from '../components/common/Button';
import { datasetApi, etlApi } from '../services/api';
import { setSelectedDatasetId } from '../store/dataSlice';
import type { RootState } from '../store';
import type { Dataset, ETLConfig, ETLProgress, DataQualityReport } from '../types';
import { clsx } from 'clsx';

interface ImprovementSuggestion {
  type: 'critical' | 'warning' | 'info';
  column: string | null;
  issue: string;
  action: string;
  description: string;
}

interface ChatMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
}

const ETL_STEPS = [
  { key: 'extract', icon: ArrowDownTrayIcon, color: 'text-blue-500' },
  { key: 'quality_check', icon: ShieldCheckIcon, color: 'text-purple-500' },
  { key: 'clean', icon: SparklesIcon, color: 'text-amber-500' },
  { key: 'analyze_schema', icon: DocumentMagnifyingGlassIcon, color: 'text-cyan-500' },
  { key: 'generate_star_schema', icon: CubeTransparentIcon, color: 'text-indigo-500' },
  { key: 'generate_ddl', icon: TableCellsIcon, color: 'text-emerald-500' },
  { key: 'load', icon: CircleStackIcon, color: 'text-green-500' },
];

export const ETLPage: React.FC = () => {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const chatEndRef = useRef<HTMLDivElement>(null);
  const dispatch = useDispatch();

  // Use Redux global state for dataset selection (syncs across pages)
  const globalDatasetId = useSelector((state: RootState) => state.data.selectedDatasetId);
  const [selectedDatasetId, setLocalSelectedDatasetId] = useState<number | null>(() => {
    return globalDatasetId || null;
  });

  // Sync local state from Redux
  useEffect(() => {
    if (globalDatasetId && globalDatasetId !== selectedDatasetId) {
      setLocalSelectedDatasetId(globalDatasetId);
    }
  }, [globalDatasetId]);

  // Sync Redux from local state
  const setSelectedDatasetIdSync = (id: number | null) => {
    setLocalSelectedDatasetId(id);
    dispatch(setSelectedDatasetId(id));
  };
  const [currentJobId, setCurrentJobId] = useState<number | null>(() => {
    const saved = localStorage.getItem('etl_current_job');
    return saved ? Number(saved) : null;
  });
  const [jobName, setJobName] = useState('');
  const [progress, setProgress] = useState<ETLProgress | null>(null);
  const [preQualityReport, setPreQualityReport] = useState<DataQualityReport | null>(null);
  const [postQualityReport, setPostQualityReport] = useState<DataQualityReport | null>(null);
  const [activeTab, setActiveTab] = useState<'config' | 'quality' | 'improve' | 'chat'>('config');
  const [improvementSuggestions, setImprovementSuggestions] = useState<ImprovementSuggestion[]>([]);
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([]);
  const [chatInput, setChatInput] = useState('');
  const [isChatLoading, setIsChatLoading] = useState(false);
  const [completedSteps, setCompletedSteps] = useState<Set<string>>(new Set());
  const [isRunningPreCheck, setIsRunningPreCheck] = useState(false);

  const [config, setConfig] = useState<Partial<ETLConfig>>({
    handle_missing: 'drop',
    remove_duplicates: true,
    normalize_strings: true,
    generate_time_dimension: true,
  });

  const { data: datasets } = useQuery({
    queryKey: ['datasets'],
    queryFn: datasetApi.list,
  });

  const { data: etlJobs, refetch: refetchJobs } = useQuery({
    queryKey: ['etl-jobs'],
    queryFn: () => etlApi.listJobs(),
  });

  useEffect(() => {
    if (currentJobId) {
      localStorage.setItem('etl_current_job', String(currentJobId));
    }
  }, [currentJobId]);

  useEffect(() => {
    if (selectedDatasetId) {
      etlApi.getImprovementSuggestions(selectedDatasetId)
        .then(data => setImprovementSuggestions(data.suggestions || []))
        .catch(() => setImprovementSuggestions([]));
    }
  }, [selectedDatasetId]);

  // Run pre-ETL quality check
  const handlePreQualityCheck = async () => {
    if (!selectedDatasetId) return;
    setIsRunningPreCheck(true);
    try {
      const report = await etlApi.qualityCheck(selectedDatasetId);
      setPreQualityReport(report);
      toast.success(t('etl.qualityReport') + ' - ' + t('etl.preETL'));
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Error';
      toast.error(msg);
    } finally {
      setIsRunningPreCheck(false);
    }
  };

  const runETLMutation = useMutation({
    mutationFn: etlApi.run,
    onSuccess: (job) => {
      setCurrentJobId(job.id);
      setCompletedSteps(new Set());
      toast.success(t('etl.etlRunning'));
    },
    onError: (error: Error) => {
      toast.error(error.message);
    },
  });

  const improveDataMutation = useMutation({
    mutationFn: ({ action, column }: { action: string; column?: string; params?: Record<string, unknown> }) => {
      if (!selectedDatasetId) throw new Error('No dataset selected');
      return etlApi.improveData(selectedDatasetId, action, column);
    },
    onSuccess: () => {
      toast.success('Data improved successfully');
      if (selectedDatasetId) {
        etlApi.getImprovementSuggestions(selectedDatasetId)
          .then(data => setImprovementSuggestions(data.suggestions || []));
      }
      queryClient.invalidateQueries({ queryKey: ['datasets'] });
    },
    onError: (error: Error) => {
      toast.error(error.message);
    },
  });

  // Poll job status
  useEffect(() => {
    if (!currentJobId) return;

    const pollStatus = async () => {
      try {
        const status = await etlApi.getStatus(currentJobId);
        setProgress(status);

        const stepIndex = ETL_STEPS.findIndex(s => s.key === status.current_step);
        if (stepIndex > 0) {
          setCompletedSteps(new Set(ETL_STEPS.slice(0, stepIndex).map(s => s.key)));
        }

        if (status.status === 'completed') {
          setCompletedSteps(new Set(ETL_STEPS.map(s => s.key)));
          toast.success(t('etl.etlComplete'));
          refetchJobs();
          localStorage.removeItem('etl_current_job');
          try {
            const report = await etlApi.getQualityReport(currentJobId);
            setPostQualityReport(report);
          } catch { /* quality report might not be available */ }
          return true;
        } else if (status.status === 'failed') {
          toast.error(t('etl.etlFailed'));
          refetchJobs();
          localStorage.removeItem('etl_current_job');
          return true;
        }
        return false;
      } catch {
        return true;
      }
    };

    pollStatus();
    const interval = setInterval(async () => {
      const shouldStop = await pollStatus();
      if (shouldStop) clearInterval(interval);
    }, 2000);

    return () => clearInterval(interval);
  }, [currentJobId, refetchJobs, t]);

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [chatMessages]);

  const handleRunETL = () => {
    if (!selectedDatasetId) {
      toast.error(t('etl.selectDataset'));
      return;
    }
    setPostQualityReport(null);
    setProgress(null);
    setCompletedSteps(new Set());

    runETLMutation.mutate({
      dataset_id: selectedDatasetId,
      handle_missing: config.handle_missing || 'drop',
      remove_duplicates: config.remove_duplicates ?? true,
      normalize_strings: config.normalize_strings ?? true,
      generate_time_dimension: config.generate_time_dimension ?? true,
    });
  };

  const handleApplyImprovement = (suggestion: ImprovementSuggestion) => {
    improveDataMutation.mutate({
      action: suggestion.action,
      column: suggestion.column || undefined,
    });
  };

  const handleSendChatMessage = async () => {
    if (!chatInput.trim() || !selectedDatasetId) return;

    const userMessage: ChatMessage = {
      id: Date.now().toString(),
      role: 'user',
      content: chatInput,
      timestamp: new Date(),
    };
    setChatMessages(prev => [...prev, userMessage]);
    const messageToSend = chatInput;
    setChatInput('');
    setIsChatLoading(true);

    try {
      const response = await etlApi.chat(selectedDatasetId, messageToSend);
      const assistantMessage: ChatMessage = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: response.response || 'No response received',
        timestamp: new Date(),
      };
      setChatMessages(prev => [...prev, assistantMessage]);
    } catch (error: unknown) {
      const errorDetails = error instanceof Error ? error.message : 'Unknown error';
      setChatMessages(prev => [...prev, {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: `Error: ${errorDetails}`,
        timestamp: new Date(),
      }]);
    } finally {
      setIsChatLoading(false);
    }
  };

  const handleSuggestedPrompt = async (prompt: string) => {
    if (!selectedDatasetId || isChatLoading) return;
    setChatInput('');
    const userMessage: ChatMessage = {
      id: Date.now().toString(),
      role: 'user',
      content: prompt,
      timestamp: new Date(),
    };
    setChatMessages(prev => [...prev, userMessage]);
    setIsChatLoading(true);

    try {
      const response = await etlApi.chat(selectedDatasetId, prompt);
      setChatMessages(prev => [...prev, {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: response.response || 'No response received',
        timestamp: new Date(),
      }]);
    } catch (error: unknown) {
      const errorDetails = error instanceof Error ? error.message : 'Unknown error';
      setChatMessages(prev => [...prev, {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: `Error: ${errorDetails}`,
        timestamp: new Date(),
      }]);
    } finally {
      setIsChatLoading(false);
    }
  };

  const handleDownloadCleaned = () => {
    if (!selectedDatasetId) return;
    const url = etlApi.downloadCleanedData(selectedDatasetId);
    const token = localStorage.getItem('bi_token');
    // Open in new tab with auth
    const a = document.createElement('a');
    a.href = url + (token ? `?token=${token}` : '');
    a.download = '';
    a.click();
  };

  const getStepStatus = (stepKey: string) => {
    if (completedSteps.has(stepKey)) return 'completed';
    if (progress?.current_step === stepKey) return 'current';
    return 'pending';
  };

  const getScoreColor = (score: number) => {
    if (score >= 0.9) return 'text-green-600 dark:text-green-400';
    if (score >= 0.7) return 'text-amber-600 dark:text-amber-400';
    return 'text-red-600 dark:text-red-400';
  };

  const getScoreBgColor = (score: number) => {
    if (score >= 0.9) return 'bg-green-500';
    if (score >= 0.7) return 'bg-amber-500';
    return 'bg-red-500';
  };

  const renderQualityScores = (report: DataQualityReport, label: string) => (
    <div className="space-y-3">
      <div className={clsx(
        'p-3 rounded-xl flex items-center gap-3',
        report.passed ? 'bg-green-50 dark:bg-green-900/20' : 'bg-amber-50 dark:bg-amber-900/20'
      )}>
        {report.passed ? (
          <CheckCircleIcon className="h-6 w-6 text-green-500" />
        ) : (
          <ExclamationTriangleIcon className="h-6 w-6 text-amber-500" />
        )}
        <div>
          <span className="text-sm font-semibold text-slate-700 dark:text-slate-300">{label}</span>
          <p className="text-xs text-slate-500 dark:text-slate-400">
            Score: {(report.overall_score * 100).toFixed(0)}%
          </p>
        </div>
      </div>
      {[
        { label: t('etl.completeness'), value: report.completeness_score },
        { label: t('etl.validity'), value: report.validity_score },
        { label: t('etl.uniqueness'), value: report.uniqueness_score },
      ].map((metric) => (
        <div key={metric.label}>
          <div className="flex justify-between text-xs mb-1">
            <span className="text-slate-600 dark:text-slate-400">{metric.label}</span>
            <span className={clsx('font-medium', getScoreColor(metric.value))}>
              {(metric.value * 100).toFixed(0)}%
            </span>
          </div>
          <div className="h-1.5 bg-slate-200 dark:bg-slate-700 rounded-full overflow-hidden">
            <div
              className={clsx('h-full rounded-full transition-all', getScoreBgColor(metric.value))}
              style={{ width: `${metric.value * 100}%` }}
            />
          </div>
        </div>
      ))}
    </div>
  );

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 dark:text-white">
            {t('etl.title')}
          </h1>
          <p className="mt-1 text-slate-500 dark:text-slate-400">
            {t('etl.subtitle')}
          </p>
        </div>
        {progress && (
          <div className="flex items-center gap-3">
            {progress.status === 'running' && (
              <div className="flex items-center gap-2 px-4 py-2 bg-primary-50 dark:bg-primary-900/20 rounded-full">
                <ArrowPathIcon className="h-5 w-5 text-primary-500 animate-spin" />
                <span className="text-sm font-medium text-primary-700 dark:text-primary-300">
                  {progress.progress_percent}%
                </span>
              </div>
            )}
            {progress.status === 'completed' && (
              <div className="flex items-center gap-2 px-4 py-2 bg-green-50 dark:bg-green-900/20 rounded-full">
                <CheckCircleIcon className="h-5 w-5 text-green-500" />
                <span className="text-sm font-medium text-green-700 dark:text-green-300">
                  {t('etl.completed')}
                </span>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Pipeline Steps Visualization */}
      <Card variant="glass" className="overflow-hidden">
        <div className="p-4">
          <div className="flex items-center justify-between overflow-x-auto pb-2">
            {ETL_STEPS.map((step, index) => {
              const status = getStepStatus(step.key);
              const Icon = step.icon;
              return (
                <React.Fragment key={step.key}>
                  <div className="flex flex-col items-center min-w-[100px]">
                    <div
                      className={clsx(
                        'relative flex items-center justify-center w-14 h-14 rounded-2xl transition-all duration-500',
                        status === 'completed' && 'bg-gradient-to-br from-green-400 to-green-600 shadow-lg shadow-green-500/25',
                        status === 'current' && 'bg-gradient-to-br from-primary-400 to-primary-600 shadow-lg shadow-primary-500/25 animate-pulse',
                        status === 'pending' && 'bg-slate-100 dark:bg-slate-800'
                      )}
                    >
                      {status === 'completed' ? (
                        <CheckCircleIcon className="h-7 w-7 text-white" />
                      ) : (
                        <Icon className={clsx('h-7 w-7 transition-colors', status === 'current' ? 'text-white' : step.color)} />
                      )}
                      {status === 'current' && (
                        <div className="absolute inset-0 rounded-2xl bg-white/20 animate-ping" />
                      )}
                    </div>
                    <span className={clsx(
                      'mt-2 text-xs font-medium text-center',
                      status === 'completed' && 'text-green-600 dark:text-green-400',
                      status === 'current' && 'text-primary-600 dark:text-primary-400',
                      status === 'pending' && 'text-slate-500 dark:text-slate-400'
                    )}>
                      {t(`etl.step.${step.key.replace('_', '')}`)}
                    </span>
                  </div>
                  {index < ETL_STEPS.length - 1 && (
                    <div className="flex-1 mx-2">
                      <div className={clsx(
                        'h-1 rounded-full transition-all duration-500',
                        completedSteps.has(ETL_STEPS[index + 1].key) || completedSteps.has(step.key)
                          ? 'bg-gradient-to-r from-green-400 to-green-600'
                          : progress?.current_step === ETL_STEPS[index + 1].key
                          ? 'bg-gradient-to-r from-green-400 via-primary-500 to-slate-200 dark:to-slate-700'
                          : 'bg-slate-200 dark:bg-slate-700'
                      )} />
                    </div>
                  )}
                </React.Fragment>
              );
            })}
          </div>
        </div>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left Column - Tabs */}
        <div className="lg:col-span-2 space-y-6">
          {/* Tab Navigation */}
          <div className="flex gap-1 p-1 bg-slate-100 dark:bg-slate-800 rounded-xl overflow-x-auto">
            {[
              { key: 'config' as const, icon: BeakerIcon, label: t('etl.configuration') },
              { key: 'quality' as const, icon: ShieldCheckIcon, label: t('etl.qualityComparison') },
              { key: 'improve' as const, icon: WrenchScrewdriverIcon, label: t('etl.improvements'), badge: improvementSuggestions.length },
              { key: 'chat' as const, icon: ChatBubbleBottomCenterTextIcon, label: t('etl.dataAssistant') },
            ].map((tab) => (
              <button
                key={tab.key}
                onClick={() => setActiveTab(tab.key)}
                className={clsx(
                  'flex-1 flex items-center justify-center gap-2 px-3 py-2.5 rounded-lg font-medium text-sm transition-all whitespace-nowrap',
                  activeTab === tab.key
                    ? 'bg-white dark:bg-slate-700 text-primary-600 dark:text-primary-400 shadow'
                    : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'
                )}
              >
                <tab.icon className="h-4 w-4" />
                <span className="hidden sm:inline">{tab.label}</span>
                {tab.badge ? (
                  <span className="px-1.5 py-0.5 text-xs bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300 rounded-full">
                    {tab.badge}
                  </span>
                ) : null}
              </button>
            ))}
          </div>

          {/* Config Tab */}
          {activeTab === 'config' && (
            <Card variant="glass">
              <CardHeader title={t('etl.configuration')} />
              <div className="space-y-5">
                <div>
                  <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">
                    {t('etl.selectDataset')}
                  </label>
                  <select
                    value={selectedDatasetId || ''}
                    onChange={(e) => {
                      setSelectedDatasetIdSync(Number(e.target.value));
                      setPreQualityReport(null);
                      setPostQualityReport(null);
                    }}
                    className="w-full px-4 py-3 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl focus:ring-2 focus:ring-primary-500 transition-all"
                  >
                    <option value="">{t('etl.chooseDataset')}</option>
                    {datasets?.map((dataset: Dataset) => (
                      <option key={dataset.id} value={dataset.id}>
                        {dataset.name} ({dataset.row_count?.toLocaleString()} rows)
                      </option>
                    ))}
                  </select>
                </div>

                {/* Job Name */}
                <div>
                  <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">
                    {t('etl.jobName')}
                  </label>
                  <input
                    type="text"
                    value={jobName}
                    onChange={(e) => setJobName(e.target.value)}
                    placeholder={t('etl.jobNamePlaceholder')}
                    className="w-full px-4 py-3 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl focus:ring-2 focus:ring-primary-500 transition-all"
                  />
                </div>

                {/* Config Options */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <label className="flex items-center gap-3 p-4 bg-slate-50 dark:bg-slate-800/50 rounded-xl cursor-pointer hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors">
                    <input type="checkbox" checked={config.remove_duplicates} onChange={(e) => setConfig({ ...config, remove_duplicates: e.target.checked })} className="w-5 h-5 rounded border-slate-300 text-primary-600 focus:ring-primary-500" />
                    <div>
                      <span className="text-sm font-medium text-slate-700 dark:text-slate-300">{t('etl.removeDuplicates')}</span>
                      <p className="text-xs text-slate-500 dark:text-slate-400">Remove duplicate rows</p>
                    </div>
                  </label>
                  <label className="flex items-center gap-3 p-4 bg-slate-50 dark:bg-slate-800/50 rounded-xl cursor-pointer hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors">
                    <input type="checkbox" checked={config.normalize_strings} onChange={(e) => setConfig({ ...config, normalize_strings: e.target.checked })} className="w-5 h-5 rounded border-slate-300 text-primary-600 focus:ring-primary-500" />
                    <div>
                      <span className="text-sm font-medium text-slate-700 dark:text-slate-300">{t('etl.normalizeStrings')}</span>
                      <p className="text-xs text-slate-500 dark:text-slate-400">Trim & normalize text</p>
                    </div>
                  </label>
                  <label className="flex items-center gap-3 p-4 bg-slate-50 dark:bg-slate-800/50 rounded-xl cursor-pointer hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors">
                    <input type="checkbox" checked={config.generate_time_dimension} onChange={(e) => setConfig({ ...config, generate_time_dimension: e.target.checked })} className="w-5 h-5 rounded border-slate-300 text-primary-600 focus:ring-primary-500" />
                    <div>
                      <span className="text-sm font-medium text-slate-700 dark:text-slate-300">{t('etl.generateTimeDimension')}</span>
                      <p className="text-xs text-slate-500 dark:text-slate-400">Auto-generate time table</p>
                    </div>
                  </label>
                  <div className="p-4 bg-slate-50 dark:bg-slate-800/50 rounded-xl">
                    <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">{t('etl.handleMissing')}</label>
                    <select value={config.handle_missing} onChange={(e) => setConfig({ ...config, handle_missing: e.target.value })} className="w-full px-3 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg focus:ring-2 focus:ring-primary-500 text-sm">
                      <option value="drop">{t('etl.dropMissing')}</option>
                      <option value="fill">{t('etl.fillMissing')}</option>
                      <option value="keep">{t('etl.keepMissing')}</option>
                    </select>
                  </div>
                </div>

                <div className="flex gap-3">
                  <Button onClick={handlePreQualityCheck} variant="secondary" icon={<ShieldCheckIcon className="h-5 w-5" />} disabled={!selectedDatasetId || isRunningPreCheck} className="flex-1">
                    {isRunningPreCheck ? t('common.loading') : t('etl.runQualityCheck')}
                  </Button>
                  <Button onClick={handleRunETL} icon={<PlayIcon className="h-5 w-5" />} disabled={!selectedDatasetId || runETLMutation.isPending} className="flex-1">
                    {runETLMutation.isPending ? t('common.loading') : t('etl.runETL')}
                  </Button>
                </div>

                {selectedDatasetId && (
                  <Button onClick={handleDownloadCleaned} variant="secondary" icon={<ArrowDownTrayIcon className="h-5 w-5" />} className="w-full">
                    {t('etl.downloadCleaned')}
                  </Button>
                )}
              </div>
            </Card>
          )}

          {/* Quality Comparison Tab */}
          {activeTab === 'quality' && (
            <Card variant="glass">
              <CardHeader title={t('etl.qualityComparison')} subtitle={preQualityReport && postQualityReport ? 'Before vs After ETL' : undefined} />
              <div className="space-y-6">
                {!preQualityReport && !postQualityReport ? (
                  <div className="text-center py-12">
                    <ArrowsRightLeftIcon className="mx-auto h-12 w-12 text-slate-300 dark:text-slate-600" />
                    <p className="mt-4 text-slate-500 dark:text-slate-400">{t('etl.noQualityReport')}</p>
                    <Button onClick={handlePreQualityCheck} variant="secondary" className="mt-4" disabled={!selectedDatasetId || isRunningPreCheck}>
                      {t('etl.runQualityCheck')}
                    </Button>
                  </div>
                ) : (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    {preQualityReport && (
                      <div>
                        <h4 className="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-3 flex items-center gap-2">
                          <div className="h-2 w-2 rounded-full bg-amber-500" />
                          {t('etl.preETL')}
                        </h4>
                        {renderQualityScores(preQualityReport, t('etl.preETL'))}
                      </div>
                    )}
                    {postQualityReport && (
                      <div>
                        <h4 className="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-3 flex items-center gap-2">
                          <div className="h-2 w-2 rounded-full bg-green-500" />
                          {t('etl.postETL')}
                        </h4>
                        {renderQualityScores(postQualityReport, t('etl.postETL'))}
                      </div>
                    )}
                  </div>
                )}

                {/* Improvement delta */}
                {preQualityReport && postQualityReport && (
                  <div className="p-4 bg-primary-50 dark:bg-primary-900/20 rounded-xl">
                    <h4 className="text-sm font-semibold text-primary-700 dark:text-primary-300 mb-2">Improvement Summary</h4>
                    <div className="grid grid-cols-3 gap-4 text-center">
                      {[
                        { label: t('etl.completeness'), before: preQualityReport.completeness_score, after: postQualityReport.completeness_score },
                        { label: t('etl.validity'), before: preQualityReport.validity_score, after: postQualityReport.validity_score },
                        { label: t('etl.uniqueness'), before: preQualityReport.uniqueness_score, after: postQualityReport.uniqueness_score },
                      ].map((m) => {
                        const delta = ((m.after - m.before) * 100).toFixed(1);
                        const positive = m.after >= m.before;
                        return (
                          <div key={m.label}>
                            <p className="text-xs text-slate-500 dark:text-slate-400">{m.label}</p>
                            <p className={clsx('text-lg font-bold', positive ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400')}>
                              {positive ? '+' : ''}{delta}%
                            </p>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}
              </div>
            </Card>
          )}

          {/* Improve Tab */}
          {activeTab === 'improve' && (
            <Card variant="glass">
              <CardHeader title={t('etl.improvements')} subtitle={`${improvementSuggestions.length} suggestions`} />
              <div className="space-y-4">
                {!selectedDatasetId ? (
                  <div className="text-center py-8">
                    <DocumentMagnifyingGlassIcon className="mx-auto h-12 w-12 text-slate-300 dark:text-slate-600" />
                    <p className="mt-4 text-slate-500 dark:text-slate-400">Select a dataset to see suggestions</p>
                  </div>
                ) : improvementSuggestions.length === 0 ? (
                  <div className="text-center py-8">
                    <CheckCircleIcon className="mx-auto h-12 w-12 text-green-400" />
                    <p className="mt-4 text-green-600 dark:text-green-400 font-medium">No issues found! Your data looks good.</p>
                  </div>
                ) : (
                  improvementSuggestions.map((suggestion, index) => (
                    <div key={index} className={clsx('p-4 rounded-xl border-l-4 transition-all hover:shadow-md',
                      suggestion.type === 'critical' && 'bg-red-50 dark:bg-red-900/20 border-red-500',
                      suggestion.type === 'warning' && 'bg-amber-50 dark:bg-amber-900/20 border-amber-500',
                      suggestion.type === 'info' && 'bg-blue-50 dark:bg-blue-900/20 border-blue-500'
                    )}>
                      <div className="flex items-start justify-between gap-4">
                        <div className="flex-1">
                          <div className="flex items-center gap-2 mb-1">
                            {suggestion.type === 'critical' && <XCircleIcon className="h-5 w-5 text-red-500" />}
                            {suggestion.type === 'warning' && <ExclamationTriangleIcon className="h-5 w-5 text-amber-500" />}
                            {suggestion.type === 'info' && <SparklesIcon className="h-5 w-5 text-blue-500" />}
                            <span className="font-medium text-slate-700 dark:text-slate-300">{suggestion.column || 'Dataset'}</span>
                            <span className="text-xs px-2 py-0.5 rounded-full bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400">{suggestion.issue}</span>
                          </div>
                          <p className="text-sm text-slate-600 dark:text-slate-400">{suggestion.description}</p>
                        </div>
                        <Button size="sm" variant="secondary" onClick={() => handleApplyImprovement(suggestion)} disabled={improveDataMutation.isPending} className="shrink-0">
                          {improveDataMutation.isPending ? <ArrowPathIcon className="h-4 w-4 animate-spin" /> : <><WrenchScrewdriverIcon className="h-4 w-4 mr-1" />Fix</>}
                        </Button>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </Card>
          )}

          {/* Chat Tab */}
          {activeTab === 'chat' && (
            <Card variant="glass" className="flex flex-col h-[500px]">
              <CardHeader title={t('etl.dataAssistant')} subtitle="Ask questions, request improvements, or generate SQL/Python" />
              <div className="flex-1 overflow-y-auto space-y-4 px-1">
                {chatMessages.length === 0 ? (
                  <div className="text-center py-12 text-slate-500 dark:text-slate-400">
                    <ChatBubbleBottomCenterTextIcon className="mx-auto h-12 w-12 mb-4 text-slate-300 dark:text-slate-600" />
                    <p className="font-medium">Start a conversation</p>
                    <p className="text-sm mt-1">Ask about data quality, suggest transformations, generate SQL or Python code</p>
                  </div>
                ) : (
                  chatMessages.map((message) => (
                    <div key={message.id} className={clsx('flex', message.role === 'user' ? 'justify-end' : 'justify-start')}>
                      <div className={clsx('max-w-[80%] px-4 py-3 rounded-2xl',
                        message.role === 'user' ? 'bg-primary-500 text-white rounded-br-none' : 'bg-slate-100 dark:bg-slate-800 text-slate-900 dark:text-white rounded-bl-none'
                      )}>
                        <p className="text-sm whitespace-pre-wrap">{message.content}</p>
                        <span className="text-xs opacity-60 mt-1 block">{message.timestamp.toLocaleTimeString()}</span>
                      </div>
                    </div>
                  ))
                )}
                {isChatLoading && (
                  <div className="flex justify-start">
                    <div className="bg-slate-100 dark:bg-slate-800 px-4 py-3 rounded-2xl rounded-bl-none">
                      <div className="flex items-center gap-2">
                        <ArrowPathIcon className="h-4 w-4 animate-spin text-primary-500" />
                        <span className="text-sm text-slate-500">Thinking...</span>
                      </div>
                    </div>
                  </div>
                )}
                <div ref={chatEndRef} />
              </div>
              <div className="pt-4 border-t border-slate-200 dark:border-slate-700">
                <div className="flex gap-2">
                  <input
                    type="text"
                    value={chatInput}
                    onChange={(e) => setChatInput(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && handleSendChatMessage()}
                    placeholder={selectedDatasetId ? 'Ask about your data, request SQL, Python code...' : 'Select a dataset first'}
                    disabled={!selectedDatasetId || isChatLoading}
                    className="flex-1 px-4 py-3 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl focus:ring-2 focus:ring-primary-500 transition-all"
                  />
                  <Button onClick={handleSendChatMessage} disabled={!chatInput.trim() || !selectedDatasetId || isChatLoading} icon={<PaperAirplaneIcon className="h-5 w-5" />}>
                    Send
                  </Button>
                </div>
                <div className="flex flex-wrap gap-2 mt-3">
                  {['Show data summary', 'Check for issues', 'Generate SQL query for totals', 'Suggest Python analysis code', 'Describe columns', 'Suggest improvements'].map((prompt) => (
                    <button key={prompt} onClick={() => handleSuggestedPrompt(prompt)} disabled={!selectedDatasetId || isChatLoading}
                      className="px-3 py-1.5 text-xs bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 rounded-full hover:bg-slate-200 dark:hover:bg-slate-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed">
                      {prompt}
                    </button>
                  ))}
                </div>
              </div>
            </Card>
          )}
        </div>

        {/* Right Column - Job History */}
        <div className="space-y-6">
          {/* Quick Quality */}
          {preQualityReport && !postQualityReport && (
            <Card variant="glass">
              <CardHeader title={t('etl.preETL') + ' ' + t('etl.qualityReport')} />
              {renderQualityScores(preQualityReport, t('etl.preETL'))}
            </Card>
          )}

          {postQualityReport && (
            <Card variant="glass">
              <CardHeader title={t('etl.postETL') + ' ' + t('etl.qualityReport')} />
              {renderQualityScores(postQualityReport, t('etl.postETL'))}
            </Card>
          )}

          {/* Job History */}
          {etlJobs && etlJobs.length > 0 && (
            <Card variant="glass">
              <CardHeader title={t('etl.jobHistory')} />
              <div className="space-y-2">
                {etlJobs.slice(0, 10).map((job: any) => (
                  <div
                    key={job.id}
                    onClick={() => setCurrentJobId(job.id)}
                    className={clsx(
                      'flex items-center justify-between p-3 rounded-lg cursor-pointer transition-all',
                      currentJobId === job.id
                        ? 'bg-primary-50 dark:bg-primary-900/20 ring-1 ring-primary-500'
                        : 'bg-slate-50 dark:bg-slate-800/50 hover:bg-slate-100 dark:hover:bg-slate-800'
                    )}
                  >
                    <div className="flex items-center gap-3">
                      {job.status === 'completed' && <CheckCircleIcon className="h-5 w-5 text-green-500" />}
                      {job.status === 'failed' && <XCircleIcon className="h-5 w-5 text-red-500" />}
                      {job.status === 'running' && <ArrowPathIcon className="h-5 w-5 text-primary-500 animate-spin" />}
                      {job.status === 'pending' && <ClockIcon className="h-5 w-5 text-slate-400" />}
                      <div>
                        <p className="text-sm font-medium text-slate-900 dark:text-white">
                          {job.job_name || `Job #${job.id}`}
                        </p>
                        <p className="text-xs text-slate-500 dark:text-slate-400">
                          {datasets?.find((d: Dataset) => d.id === job.dataset_id)?.name || `Dataset #${job.dataset_id}`}
                        </p>
                      </div>
                    </div>
                    <ChevronRightIcon className="h-4 w-4 text-slate-400" />
                  </div>
                ))}
              </div>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
};

export default ETLPage;
