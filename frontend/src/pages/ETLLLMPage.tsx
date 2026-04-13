import React, { useState, useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import axios from 'axios';

interface PipelineResult {
  pipeline_id: string;
  source_name: string;
  rows_ingested: number;
  schema_fingerprint: string;
  mapping_confidence: number;
  cleaning_confidence: number;
  requires_human_review: boolean;
  review_job_id: string | null;
  tables_created: string[];
  rows_loaded: number;
  lineage_markdown: string;
  errors: string[];
}

interface ReviewJob {
  job_id: string;
  created_at: string;
  status: string;
  assessment: {
    confidence_score: number;
    reasons: string[];
    review_items: { item_type: string; description: string; suggestion: string; risk_level: string }[];
  };
}

const ETLLLMPage: React.FC = () => {
  const { t } = useTranslation();
  const [file, setFile] = useState<File | null>(null);
  const [sourceType, setSourceType] = useState('csv');
  const [autoApprove, setAutoApprove] = useState(false);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<PipelineResult | null>(null);
  const [reviews, setReviews] = useState<ReviewJob[]>([]);
  const [error, setError] = useState('');

  const runPipeline = useCallback(async () => {
    if (!file) return;
    setLoading(true);
    setError('');
    setResult(null);
    try {
      const formData = new FormData();
      formData.append('file', file);
      const { data } = await axios.post(
        `/api/etl-llm/run?source_type=${sourceType}&auto_approve=${autoApprove}`,
        formData,
        { headers: { 'Content-Type': 'multipart/form-data' } }
      );
      setResult(data);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err.message || 'Pipeline failed');
    } finally {
      setLoading(false);
    }
  }, [file, sourceType, autoApprove]);

  const fetchReviews = useCallback(async () => {
    try {
      const { data } = await axios.get('/api/etl-llm/review-queue');
      setReviews(data);
    } catch {
      /* ignore */
    }
  }, []);

  const approveReview = useCallback(async (jobId: string) => {
    await axios.post(`/api/etl-llm/approve/${jobId}`);
    fetchReviews();
  }, [fetchReviews]);

  const rejectReview = useCallback(async (jobId: string) => {
    await axios.post(`/api/etl-llm/reject/${jobId}?reason=manual`);
    fetchReviews();
  }, [fetchReviews]);

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-slate-900 dark:text-white">
        {t('etlLlm.title', 'LLM-Powered ETL Pipeline')}
      </h1>
      <p className="text-slate-600 dark:text-slate-400">
        {t('etlLlm.subtitle', 'Upload a data source and let the AI build your star schema automatically.')}
      </p>

      {/* Upload section */}
      <div className="bg-white dark:bg-slate-800 rounded-xl shadow p-6 space-y-4">
        <h2 className="text-lg font-semibold text-slate-900 dark:text-white">
          {t('etlLlm.upload', 'Upload & Configure')}
        </h2>

        <div className="flex flex-wrap gap-4 items-end">
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
              {t('etlLlm.file', 'Data File')}
            </label>
            <input
              type="file"
              accept=".csv,.xlsx,.json,.xml"
              onChange={(e) => setFile(e.target.files?.[0] || null)}
              className="block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-semibold file:bg-primary-50 file:text-primary-700 hover:file:bg-primary-100 dark:file:bg-primary-900/30 dark:file:text-primary-400"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
              {t('etlLlm.sourceType', 'Source Type')}
            </label>
            <select
              value={sourceType}
              onChange={(e) => setSourceType(e.target.value)}
              className="rounded-lg border-slate-300 dark:border-slate-600 dark:bg-slate-700 dark:text-white text-sm"
            >
              <option value="csv">CSV</option>
              <option value="excel">Excel</option>
              <option value="json">JSON</option>
              <option value="xml">XML</option>
            </select>
          </div>

          <label className="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300">
            <input
              type="checkbox"
              checked={autoApprove}
              onChange={(e) => setAutoApprove(e.target.checked)}
              className="rounded border-slate-300 text-primary-600"
            />
            {t('etlLlm.autoApprove', 'Auto-approve (skip HITL)')}
          </label>

          <button
            onClick={runPipeline}
            disabled={!file || loading}
            className="px-6 py-2 bg-primary-600 text-white rounded-lg font-medium hover:bg-primary-700 disabled:opacity-50 disabled:cursor-not-allowed transition"
          >
            {loading ? t('etlLlm.running', 'Running...') : t('etlLlm.run', 'Run Pipeline')}
          </button>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl p-4 text-red-700 dark:text-red-400">
          {error}
        </div>
      )}

      {/* Results */}
      {result && (
        <div className="bg-white dark:bg-slate-800 rounded-xl shadow p-6 space-y-4">
          <h2 className="text-lg font-semibold text-slate-900 dark:text-white">
            {t('etlLlm.results', 'Pipeline Results')}
          </h2>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <Stat label={t('etlLlm.rowsIngested', 'Rows Ingested')} value={result.rows_ingested} />
            <Stat label={t('etlLlm.mappingConf', 'Mapping Confidence')} value={`${(result.mapping_confidence * 100).toFixed(0)}%`} />
            <Stat label={t('etlLlm.cleaningConf', 'Cleaning Confidence')} value={`${(result.cleaning_confidence * 100).toFixed(0)}%`} />
            <Stat label={t('etlLlm.rowsLoaded', 'Rows Loaded')} value={result.rows_loaded} />
          </div>

          {result.tables_created.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
                {t('etlLlm.tablesCreated', 'Tables Created')}
              </h3>
              <div className="flex flex-wrap gap-2">
                {result.tables_created.map((t) => (
                  <span key={t} className="px-3 py-1 bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400 rounded-full text-sm">
                    {t}
                  </span>
                ))}
              </div>
            </div>
          )}

          {result.requires_human_review && result.review_job_id && (
            <div className="bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-lg p-3 text-amber-700 dark:text-amber-400">
              ⚠️ {t('etlLlm.needsReview', 'This pipeline requires human review.')} Job ID: {result.review_job_id}
            </div>
          )}

          {result.errors.length > 0 && (
            <div className="space-y-1">
              {result.errors.map((err, i) => (
                <p key={i} className="text-red-600 dark:text-red-400 text-sm">• {err}</p>
              ))}
            </div>
          )}

          {result.lineage_markdown && (
            <details className="mt-4">
              <summary className="cursor-pointer text-sm font-medium text-slate-700 dark:text-slate-300">
                {t('etlLlm.lineage', 'Data Lineage')}
              </summary>
              <pre className="mt-2 p-4 bg-slate-50 dark:bg-slate-900 rounded-lg text-xs overflow-x-auto whitespace-pre-wrap">
                {result.lineage_markdown}
              </pre>
            </details>
          )}
        </div>
      )}

      {/* HITL Review Queue */}
      <div className="bg-white dark:bg-slate-800 rounded-xl shadow p-6 space-y-4">
        <div className="flex items-center justify-between">
          <h2 className="text-lg font-semibold text-slate-900 dark:text-white">
            {t('etlLlm.reviewQueue', 'Human Review Queue')}
          </h2>
          <button
            onClick={fetchReviews}
            className="px-4 py-1.5 text-sm bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-lg hover:bg-slate-200 dark:hover:bg-slate-600 transition"
          >
            {t('etlLlm.refresh', 'Refresh')}
          </button>
        </div>

        {reviews.length === 0 ? (
          <p className="text-slate-500 dark:text-slate-400 text-sm">
            {t('etlLlm.noReviews', 'No pending reviews.')}
          </p>
        ) : (
          <div className="space-y-3">
            {reviews.map((rev) => (
              <div key={rev.job_id} className="border border-slate-200 dark:border-slate-700 rounded-lg p-4">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm font-mono text-slate-600 dark:text-slate-400">
                    {rev.job_id.slice(0, 8)}...
                  </span>
                  <span className="text-sm">
                    Confidence: {(rev.assessment.confidence_score * 100).toFixed(0)}%
                  </span>
                </div>
                {rev.assessment.reasons.map((r, i) => (
                  <p key={i} className="text-sm text-amber-600 dark:text-amber-400">• {r}</p>
                ))}
                <div className="flex gap-2 mt-3">
                  <button
                    onClick={() => approveReview(rev.job_id)}
                    className="px-4 py-1.5 text-sm bg-green-600 text-white rounded-lg hover:bg-green-700 transition"
                  >
                    {t('etlLlm.approve', 'Approve')}
                  </button>
                  <button
                    onClick={() => rejectReview(rev.job_id)}
                    className="px-4 py-1.5 text-sm bg-red-600 text-white rounded-lg hover:bg-red-700 transition"
                  >
                    {t('etlLlm.reject', 'Reject')}
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

const Stat: React.FC<{ label: string; value: string | number }> = ({ label, value }) => (
  <div className="bg-slate-50 dark:bg-slate-900/50 rounded-lg p-3">
    <p className="text-xs text-slate-500 dark:text-slate-400">{label}</p>
    <p className="text-xl font-bold text-slate-900 dark:text-white">{value}</p>
  </div>
);

export default ETLLLMPage;
