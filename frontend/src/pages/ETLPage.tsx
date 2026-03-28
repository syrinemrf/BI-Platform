import React, { useState, useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
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
} from '@heroicons/react/24/outline';
import { toast } from 'react-hot-toast';
import { Card, CardHeader } from '../components/common/Card';
import { Button } from '../components/common/Button';
import { datasetApi, etlApi } from '../services/api';
import type { Dataset, ETLConfig, ETLProgress, DataQualityReport } from '../types';
import { clsx } from 'clsx';

// Types for improvement suggestions
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

// ETL Steps definition with icons
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

  // State
  const [selectedDatasetId, setSelectedDatasetId] = useState<number | null>(() => {
    const saved = localStorage.getItem('etl_selected_dataset');
    return saved ? Number(saved) : null;
  });
  const [currentJobId, setCurrentJobId] = useState<number | null>(() => {
    const saved = localStorage.getItem('etl_current_job');
    return saved ? Number(saved) : null;
  });
  const [progress, setProgress] = useState<ETLProgress | null>(null);
  const [qualityReport, setQualityReport] = useState<DataQualityReport | null>(null);
  const [activeTab, setActiveTab] = useState<'config' | 'improve' | 'chat'>('config');
  const [improvementSuggestions, setImprovementSuggestions] = useState<ImprovementSuggestion[]>([]);
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([]);
  const [chatInput, setChatInput] = useState('');
  const [isChatLoading, setIsChatLoading] = useState(false);
  const [completedSteps, setCompletedSteps] = useState<Set<string>>(new Set());

  // ETL Configuration
  const [config, setConfig] = useState<Partial<ETLConfig>>({
    handle_missing: 'drop',
    remove_duplicates: true,
    normalize_strings: true,
    generate_time_dimension: true,
  });

  // Fetch datasets
  const { data: datasets } = useQuery({
    queryKey: ['datasets'],
    queryFn: datasetApi.list,
  });

  // Fetch ETL jobs
  const { data: etlJobs, refetch: refetchJobs } = useQuery({
    queryKey: ['etl-jobs'],
    queryFn: () => etlApi.listJobs(),
  });

  // Persist selected dataset
  useEffect(() => {
    if (selectedDatasetId) {
      localStorage.setItem('etl_selected_dataset', String(selectedDatasetId));
    }
  }, [selectedDatasetId]);

  // Persist current job
  useEffect(() => {
    if (currentJobId) {
      localStorage.setItem('etl_current_job', String(currentJobId));
    }
  }, [currentJobId]);

  // Fetch improvement suggestions when dataset is selected
  useEffect(() => {
    if (selectedDatasetId) {
      etlApi.getImprovementSuggestions(selectedDatasetId)
        .then(data => setImprovementSuggestions(data.suggestions || []))
        .catch(() => setImprovementSuggestions([]));
    }
  }, [selectedDatasetId]);

  // Run ETL mutation
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

  // Improve data mutation
  const improveDataMutation = useMutation({
    mutationFn: ({ action, column, params }: { action: string; column?: string; params?: Record<string, unknown> }) => {
      if (!selectedDatasetId) throw new Error('No dataset selected');
      return etlApi.improveData(selectedDatasetId, action, column, params);
    },
    onSuccess: (result) => {
      toast.success(`Data improved: ${result.rows_removed} rows removed, ${result.nulls_fixed} nulls fixed`);
      // Refresh suggestions
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

  // Poll for status when job is running
  useEffect(() => {
    if (!currentJobId) return;

    const pollStatus = async () => {
      try {
        const status = await etlApi.getStatus(currentJobId);
        setProgress(status);

        // Update completed steps
        const stepIndex = ETL_STEPS.findIndex(s => s.key === status.current_step);
        if (stepIndex > 0) {
          const newCompleted = new Set(ETL_STEPS.slice(0, stepIndex).map(s => s.key));
          setCompletedSteps(newCompleted);
        }

        if (status.status === 'completed') {
          setCompletedSteps(new Set(ETL_STEPS.map(s => s.key)));
          toast.success(t('etl.etlComplete'));
          refetchJobs();
          localStorage.removeItem('etl_current_job');

          // Fetch quality report
          try {
            const report = await etlApi.getQualityReport(currentJobId);
            setQualityReport(report);
          } catch {
            // Quality report might not be available
          }
          return true; // Stop polling
        } else if (status.status === 'failed') {
          toast.error(t('etl.etlFailed'));
          refetchJobs();
          localStorage.removeItem('etl_current_job');
          return true; // Stop polling
        }
        return false;
      } catch {
        return true; // Stop polling on error
      }
    };

    // Initial poll
    pollStatus();

    // Set up interval
    const interval = setInterval(async () => {
      const shouldStop = await pollStatus();
      if (shouldStop) {
        clearInterval(interval);
      }
    }, 2000);

    return () => clearInterval(interval);
  }, [currentJobId, refetchJobs, t]);

  // Scroll chat to bottom
  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [chatMessages]);

  const handleRunETL = () => {
    if (!selectedDatasetId) {
      toast.error(t('etl.selectDataset'));
      return;
    }

    setQualityReport(null);
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

      // Handle the response even if LLM is not available
      const assistantMessage: ChatMessage = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: response.response || 'No response received',
        timestamp: new Date(),
      };
      setChatMessages(prev => [...prev, assistantMessage]);

      // Show warning if LLM was not available
      if (response.available === false) {
        console.log('LLM service not available - showing basic data info');
      }
    } catch (error: unknown) {
      console.error('Chat error:', error);
      const errorDetails = error instanceof Error ? error.message : 'Unknown error';
      const errorMessage: ChatMessage = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: `Sorry, I encountered an error: ${errorDetails}\n\nPlease make sure:\n1. A dataset is selected\n2. The backend server is running\n3. Check the browser console for more details`,
        timestamp: new Date(),
      };
      setChatMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsChatLoading(false);
    }
  };

  const handleSuggestedPrompt = async (prompt: string) => {
    if (!selectedDatasetId || isChatLoading) return;

    // Set input and immediately send
    setChatInput(prompt);

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
      const assistantMessage: ChatMessage = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: response.response || 'No response received',
        timestamp: new Date(),
      };
      setChatMessages(prev => [...prev, assistantMessage]);
    } catch (error: unknown) {
      console.error('Chat error:', error);
      const errorDetails = error instanceof Error ? error.message : 'Unknown error';
      const errorMessage: ChatMessage = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: `Error: ${errorDetails}`,
        timestamp: new Date(),
      };
      setChatMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsChatLoading(false);
      setChatInput('');
    }
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
                        <Icon
                          className={clsx(
                            'h-7 w-7 transition-colors',
                            status === 'current' ? 'text-white' : step.color
                          )}
                        />
                      )}
                      {status === 'current' && (
                        <div className="absolute inset-0 rounded-2xl bg-white/20 animate-ping" />
                      )}
                    </div>
                    <span
                      className={clsx(
                        'mt-2 text-xs font-medium text-center',
                        status === 'completed' && 'text-green-600 dark:text-green-400',
                        status === 'current' && 'text-primary-600 dark:text-primary-400',
                        status === 'pending' && 'text-slate-500 dark:text-slate-400'
                      )}
                    >
                      {t(`etl.step.${step.key.replace('_', '')}`)}
                    </span>
                  </div>
                  {index < ETL_STEPS.length - 1 && (
                    <div className="flex-1 mx-2">
                      <div
                        className={clsx(
                          'h-1 rounded-full transition-all duration-500',
                          completedSteps.has(ETL_STEPS[index + 1].key) || completedSteps.has(step.key)
                            ? 'bg-gradient-to-r from-green-400 to-green-600'
                            : progress?.current_step === ETL_STEPS[index + 1].key
                            ? 'bg-gradient-to-r from-green-400 via-primary-500 to-slate-200 dark:to-slate-700'
                            : 'bg-slate-200 dark:bg-slate-700'
                        )}
                      />
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
          <div className="flex gap-2 p-1 bg-slate-100 dark:bg-slate-800 rounded-xl">
            <button
              onClick={() => setActiveTab('config')}
              className={clsx(
                'flex-1 flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg font-medium text-sm transition-all',
                activeTab === 'config'
                  ? 'bg-white dark:bg-slate-700 text-primary-600 dark:text-primary-400 shadow'
                  : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'
              )}
            >
              <BeakerIcon className="h-5 w-5" />
              {t('etl.configuration')}
            </button>
            <button
              onClick={() => setActiveTab('improve')}
              className={clsx(
                'flex-1 flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg font-medium text-sm transition-all',
                activeTab === 'improve'
                  ? 'bg-white dark:bg-slate-700 text-primary-600 dark:text-primary-400 shadow'
                  : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'
              )}
            >
              <WrenchScrewdriverIcon className="h-5 w-5" />
              {t('etl.improvements')}
              {improvementSuggestions.length > 0 && (
                <span className="px-2 py-0.5 text-xs bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300 rounded-full">
                  {improvementSuggestions.length}
                </span>
              )}
            </button>
            <button
              onClick={() => setActiveTab('chat')}
              className={clsx(
                'flex-1 flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg font-medium text-sm transition-all',
                activeTab === 'chat'
                  ? 'bg-white dark:bg-slate-700 text-primary-600 dark:text-primary-400 shadow'
                  : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'
              )}
            >
              <ChatBubbleBottomCenterTextIcon className="h-5 w-5" />
              {t('etl.dataAssistant')}
            </button>
          </div>

          {/* Tab Content */}
          {activeTab === 'config' && (
            <Card variant="glass">
              <CardHeader title={t('etl.configuration')} />
              <div className="space-y-5">
                {/* Dataset Selection */}
                <div>
                  <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">
                    {t('etl.selectDataset')}
                  </label>
                  <select
                    value={selectedDatasetId || ''}
                    onChange={(e) => setSelectedDatasetId(Number(e.target.value))}
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

                {/* Config Options */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <label className="flex items-center gap-3 p-4 bg-slate-50 dark:bg-slate-800/50 rounded-xl cursor-pointer hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors">
                    <input
                      type="checkbox"
                      checked={config.remove_duplicates}
                      onChange={(e) => setConfig({ ...config, remove_duplicates: e.target.checked })}
                      className="w-5 h-5 rounded border-slate-300 text-primary-600 focus:ring-primary-500"
                    />
                    <div>
                      <span className="text-sm font-medium text-slate-700 dark:text-slate-300">
                        {t('etl.removeDuplicates')}
                      </span>
                      <p className="text-xs text-slate-500 dark:text-slate-400">
                        Remove duplicate rows
                      </p>
                    </div>
                  </label>

                  <label className="flex items-center gap-3 p-4 bg-slate-50 dark:bg-slate-800/50 rounded-xl cursor-pointer hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors">
                    <input
                      type="checkbox"
                      checked={config.normalize_strings}
                      onChange={(e) => setConfig({ ...config, normalize_strings: e.target.checked })}
                      className="w-5 h-5 rounded border-slate-300 text-primary-600 focus:ring-primary-500"
                    />
                    <div>
                      <span className="text-sm font-medium text-slate-700 dark:text-slate-300">
                        {t('etl.normalizeStrings')}
                      </span>
                      <p className="text-xs text-slate-500 dark:text-slate-400">
                        Trim & normalize text
                      </p>
                    </div>
                  </label>

                  <label className="flex items-center gap-3 p-4 bg-slate-50 dark:bg-slate-800/50 rounded-xl cursor-pointer hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors">
                    <input
                      type="checkbox"
                      checked={config.generate_time_dimension}
                      onChange={(e) => setConfig({ ...config, generate_time_dimension: e.target.checked })}
                      className="w-5 h-5 rounded border-slate-300 text-primary-600 focus:ring-primary-500"
                    />
                    <div>
                      <span className="text-sm font-medium text-slate-700 dark:text-slate-300">
                        {t('etl.generateTimeDimension')}
                      </span>
                      <p className="text-xs text-slate-500 dark:text-slate-400">
                        Auto-generate time table
                      </p>
                    </div>
                  </label>

                  <div className="p-4 bg-slate-50 dark:bg-slate-800/50 rounded-xl">
                    <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">
                      {t('etl.handleMissing')}
                    </label>
                    <select
                      value={config.handle_missing}
                      onChange={(e) => setConfig({ ...config, handle_missing: e.target.value })}
                      className="w-full px-3 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg focus:ring-2 focus:ring-primary-500 text-sm"
                    >
                      <option value="drop">{t('etl.dropMissing')}</option>
                      <option value="fill">{t('etl.fillMissing')}</option>
                      <option value="keep">{t('etl.keepMissing')}</option>
                    </select>
                  </div>
                </div>

                <Button
                  onClick={handleRunETL}
                  icon={<PlayIcon className="h-5 w-5" />}
                  disabled={!selectedDatasetId || runETLMutation.isPending}
                  className="w-full py-3"
                >
                  {runETLMutation.isPending ? t('common.loading') : t('etl.runETL')}
                </Button>
              </div>
            </Card>
          )}

          {activeTab === 'improve' && (
            <Card variant="glass">
              <CardHeader
                title={t('etl.improvements')}
                subtitle={`${improvementSuggestions.length} suggestions available`}
              />
              <div className="space-y-4">
                {!selectedDatasetId ? (
                  <div className="text-center py-8">
                    <DocumentMagnifyingGlassIcon className="mx-auto h-12 w-12 text-slate-300 dark:text-slate-600" />
                    <p className="mt-4 text-slate-500 dark:text-slate-400">
                      Select a dataset to see improvement suggestions
                    </p>
                  </div>
                ) : improvementSuggestions.length === 0 ? (
                  <div className="text-center py-8">
                    <CheckCircleIcon className="mx-auto h-12 w-12 text-green-400" />
                    <p className="mt-4 text-green-600 dark:text-green-400 font-medium">
                      No issues found! Your data looks good.
                    </p>
                  </div>
                ) : (
                  improvementSuggestions.map((suggestion, index) => (
                    <div
                      key={index}
                      className={clsx(
                        'p-4 rounded-xl border-l-4 transition-all hover:shadow-md',
                        suggestion.type === 'critical' && 'bg-red-50 dark:bg-red-900/20 border-red-500',
                        suggestion.type === 'warning' && 'bg-amber-50 dark:bg-amber-900/20 border-amber-500',
                        suggestion.type === 'info' && 'bg-blue-50 dark:bg-blue-900/20 border-blue-500'
                      )}
                    >
                      <div className="flex items-start justify-between gap-4">
                        <div className="flex-1">
                          <div className="flex items-center gap-2 mb-1">
                            {suggestion.type === 'critical' && (
                              <XCircleIcon className="h-5 w-5 text-red-500" />
                            )}
                            {suggestion.type === 'warning' && (
                              <ExclamationTriangleIcon className="h-5 w-5 text-amber-500" />
                            )}
                            {suggestion.type === 'info' && (
                              <SparklesIcon className="h-5 w-5 text-blue-500" />
                            )}
                            <span className={clsx(
                              'font-medium',
                              suggestion.type === 'critical' && 'text-red-700 dark:text-red-300',
                              suggestion.type === 'warning' && 'text-amber-700 dark:text-amber-300',
                              suggestion.type === 'info' && 'text-blue-700 dark:text-blue-300'
                            )}>
                              {suggestion.column || 'Dataset'}
                            </span>
                            <span className={clsx(
                              'text-xs px-2 py-0.5 rounded-full',
                              suggestion.type === 'critical' && 'bg-red-100 dark:bg-red-900/30 text-red-600 dark:text-red-400',
                              suggestion.type === 'warning' && 'bg-amber-100 dark:bg-amber-900/30 text-amber-600 dark:text-amber-400',
                              suggestion.type === 'info' && 'bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400'
                            )}>
                              {suggestion.issue}
                            </span>
                          </div>
                          <p className="text-sm text-slate-600 dark:text-slate-400">
                            {suggestion.description}
                          </p>
                        </div>
                        <Button
                          size="sm"
                          variant="secondary"
                          onClick={() => handleApplyImprovement(suggestion)}
                          disabled={improveDataMutation.isPending}
                          className="shrink-0"
                        >
                          {improveDataMutation.isPending ? (
                            <ArrowPathIcon className="h-4 w-4 animate-spin" />
                          ) : (
                            <>
                              <WrenchScrewdriverIcon className="h-4 w-4 mr-1" />
                              Fix
                            </>
                          )}
                        </Button>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </Card>
          )}

          {activeTab === 'chat' && (
            <Card variant="glass" className="flex flex-col h-[500px]">
              <CardHeader
                title={t('etl.dataAssistant')}
                subtitle="Ask questions about your data or request transformations"
              />
              <div className="flex-1 overflow-y-auto space-y-4 px-1">
                {chatMessages.length === 0 ? (
                  <div className="text-center py-12 text-slate-500 dark:text-slate-400">
                    <ChatBubbleBottomCenterTextIcon className="mx-auto h-12 w-12 mb-4 text-slate-300 dark:text-slate-600" />
                    <p className="font-medium">Start a conversation</p>
                    <p className="text-sm mt-1">
                      Ask about data quality, suggest transformations, or explore your dataset
                    </p>
                  </div>
                ) : (
                  chatMessages.map((message) => (
                    <div
                      key={message.id}
                      className={clsx(
                        'flex',
                        message.role === 'user' ? 'justify-end' : 'justify-start'
                      )}
                    >
                      <div
                        className={clsx(
                          'max-w-[80%] px-4 py-3 rounded-2xl',
                          message.role === 'user'
                            ? 'bg-primary-500 text-white rounded-br-none'
                            : 'bg-slate-100 dark:bg-slate-800 text-slate-900 dark:text-white rounded-bl-none'
                        )}
                      >
                        <p className="text-sm whitespace-pre-wrap">{message.content}</p>
                        <span className="text-xs opacity-60 mt-1 block">
                          {message.timestamp.toLocaleTimeString()}
                        </span>
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
                    onKeyPress={(e) => e.key === 'Enter' && handleSendChatMessage()}
                    placeholder={selectedDatasetId ? "Ask about your data..." : "Select a dataset first"}
                    disabled={!selectedDatasetId || isChatLoading}
                    className="flex-1 px-4 py-3 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl focus:ring-2 focus:ring-primary-500 transition-all"
                  />
                  <Button
                    onClick={handleSendChatMessage}
                    disabled={!chatInput.trim() || !selectedDatasetId || isChatLoading}
                    icon={<PaperAirplaneIcon className="h-5 w-5" />}
                  >
                    Send
                  </Button>
                </div>
                <div className="flex flex-wrap gap-2 mt-3">
                  {['Show data summary', 'Check for issues', 'Suggest improvements', 'Describe columns'].map((prompt) => (
                    <button
                      key={prompt}
                      onClick={() => handleSuggestedPrompt(prompt)}
                      disabled={!selectedDatasetId || isChatLoading}
                      className="px-3 py-1.5 text-xs bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 rounded-full hover:bg-slate-200 dark:hover:bg-slate-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      {prompt}
                    </button>
                  ))}
                </div>
              </div>
            </Card>
          )}
        </div>

        {/* Right Column - Quality Report */}
        <div className="space-y-6">
          {/* Quality Report */}
          <Card variant="glass">
            <CardHeader title={t('etl.qualityReport')} />
            {qualityReport ? (
              <div className="space-y-4">
                {/* Overall Status */}
                <div
                  className={clsx(
                    'p-4 rounded-xl flex items-center gap-3',
                    qualityReport.passed
                      ? 'bg-green-50 dark:bg-green-900/20'
                      : 'bg-amber-50 dark:bg-amber-900/20'
                  )}
                >
                  {qualityReport.passed ? (
                    <CheckCircleIcon className="h-8 w-8 text-green-500" />
                  ) : (
                    <ExclamationTriangleIcon className="h-8 w-8 text-amber-500" />
                  )}
                  <div>
                    <span
                      className={clsx(
                        'font-semibold',
                        qualityReport.passed
                          ? 'text-green-700 dark:text-green-300'
                          : 'text-amber-700 dark:text-amber-300'
                      )}
                    >
                      {qualityReport.passed ? t('etl.qualityPassed') : t('etl.qualityWarning')}
                    </span>
                    <p className="text-xs mt-0.5 text-slate-500 dark:text-slate-400">
                      Score: {(qualityReport.overall_score * 100).toFixed(0)}%
                    </p>
                  </div>
                </div>

                {/* Score Breakdown */}
                <div className="space-y-3">
                  {[
                    { label: t('etl.completeness'), value: qualityReport.completeness_score },
                    { label: t('etl.validity'), value: qualityReport.validity_score },
                    { label: t('etl.uniqueness'), value: qualityReport.uniqueness_score },
                  ].map((metric) => (
                    <div key={metric.label}>
                      <div className="flex justify-between text-sm mb-1">
                        <span className="text-slate-600 dark:text-slate-400">{metric.label}</span>
                        <span className={clsx('font-medium', getScoreColor(metric.value))}>
                          {(metric.value * 100).toFixed(0)}%
                        </span>
                      </div>
                      <div className="h-2 bg-slate-200 dark:bg-slate-700 rounded-full overflow-hidden">
                        <div
                          className={clsx('h-full rounded-full transition-all', getScoreBgColor(metric.value))}
                          style={{ width: `${metric.value * 100}%` }}
                        />
                      </div>
                    </div>
                  ))}
                </div>

                {/* Critical Issues */}
                {qualityReport.critical_issues && qualityReport.critical_issues.length > 0 && (
                  <div>
                    <h4 className="text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">
                      {t('etl.criticalIssues')}
                    </h4>
                    <div className="space-y-2">
                      {qualityReport.critical_issues.slice(0, 3).map((issue, idx) => (
                        <div
                          key={idx}
                          className="p-2 bg-red-50 dark:bg-red-900/20 rounded-lg text-xs text-red-700 dark:text-red-300"
                        >
                          <strong>{issue.column}:</strong> {issue.issue}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <div className="text-center py-8">
                <ShieldCheckIcon className="mx-auto h-12 w-12 text-slate-300 dark:text-slate-600" />
                <p className="mt-4 text-slate-500 dark:text-slate-400 text-sm">
                  {t('etl.noQualityReport')}
                </p>
              </div>
            )}
          </Card>

          {/* Recent Jobs */}
          {etlJobs && etlJobs.length > 0 && (
            <Card variant="glass">
              <CardHeader title="Recent Jobs" />
              <div className="space-y-2">
                {etlJobs.slice(0, 5).map((job: any) => (
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
                      {job.status === 'completed' && (
                        <CheckCircleIcon className="h-5 w-5 text-green-500" />
                      )}
                      {job.status === 'failed' && (
                        <XCircleIcon className="h-5 w-5 text-red-500" />
                      )}
                      {job.status === 'running' && (
                        <ArrowPathIcon className="h-5 w-5 text-primary-500 animate-spin" />
                      )}
                      {job.status === 'pending' && (
                        <ClockIcon className="h-5 w-5 text-slate-400" />
                      )}
                      <div>
                        <p className="text-sm font-medium text-slate-900 dark:text-white">
                          Job #{job.id}
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
