import axios, { AxiosError } from 'axios';
import type {
  Dataset,
  ETLJob,
  ETLConfig,
  ETLProgress,
  DataQualityReport,
  TableInfo,
  StarSchema,
  KPI,
  TimeSeriesData,
  FilterOption,
  AggregateRequest,
  LLMQueryResponse,
  AuthResponse,
  LoginRequest,
  RegisterRequest,
  UserProject,
} from '../types';

const API_BASE = '/api';

const api = axios.create({
  baseURL: API_BASE,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add auth token and session ID to requests
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('bi_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  // Send guest session ID for data isolation
  const sessionId = sessionStorage.getItem('bi_session_id');
  if (sessionId && !token) {
    config.headers['X-Session-Id'] = sessionId;
  }
  return config;
});

// Extract error messages from API responses
api.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    if (error.response) {
      // Handle expired/invalid token
      if (error.response.status === 401) {
        localStorage.removeItem('bi_token');
        localStorage.removeItem('bi_user');
        window.location.href = '/auth';
        return Promise.reject(new Error('Session expired. Please log in again.'));
      }
      const detail = (error.response.data as { detail?: string })?.detail;
      return Promise.reject(new Error(detail || 'An error occurred'));
    }
    return Promise.reject(error);
  }
);

// Auth API
export const authApi = {
  register: async (data: RegisterRequest): Promise<AuthResponse> => {
    const response = await api.post('/auth/register', data);
    return response.data;
  },

  login: async (data: LoginRequest): Promise<AuthResponse> => {
    const response = await api.post('/auth/login', data);
    return response.data;
  },

  getProfile: async () => {
    const response = await api.get('/auth/me');
    return response.data;
  },

  listProjects: async (): Promise<UserProject[]> => {
    const response = await api.get('/auth/projects');
    return response.data;
  },

  createProject: async (data: Partial<UserProject>): Promise<UserProject> => {
    const response = await api.post('/auth/projects', data);
    return response.data;
  },

  updateProject: async (id: number, data: Partial<UserProject>): Promise<UserProject> => {
    const response = await api.put(`/auth/projects/${id}`, data);
    return response.data;
  },

  deleteProject: async (id: number): Promise<void> => {
    await api.delete(`/auth/projects/${id}`);
  },
};

// Dataset API
export const datasetApi = {
  upload: async (file: File, name?: string): Promise<Dataset> => {
    const formData = new FormData();
    formData.append('file', file);
    if (name) {
      formData.append('name', name);
    }
    const response = await api.post('/datasets/upload', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data;
  },

  list: async (): Promise<Dataset[]> => {
    const response = await api.get('/datasets');
    return response.data;
  },

  get: async (id: number): Promise<Dataset> => {
    const response = await api.get(`/datasets/${id}`);
    return response.data;
  },

  preview: async (id: number, rows = 100): Promise<{ columns: string[]; data: Record<string, unknown>[]; total_rows: number }> => {
    const response = await api.get(`/datasets/${id}/preview`, { params: { rows } });
    return response.data;
  },

  getSchema: async (id: number) => {
    const response = await api.get(`/datasets/${id}/schema`);
    return response.data;
  },

  delete: async (id: number): Promise<void> => {
    await api.delete(`/datasets/${id}`);
  },

  reanalyze: async (id: number) => {
    const response = await api.post(`/datasets/${id}/reanalyze`);
    return response.data;
  },
};

// ETL API
export const etlApi = {
  analyze: async (datasetId: number) => {
    const response = await api.post(`/etl/analyze/${datasetId}`);
    return response.data;
  },

  qualityCheck: async (datasetId: number): Promise<DataQualityReport> => {
    const response = await api.post(`/etl/quality-check/${datasetId}`);
    return response.data;
  },

  run: async (config: ETLConfig): Promise<ETLJob> => {
    const response = await api.post('/etl/run', config);
    return response.data;
  },

  getStatus: async (jobId: number): Promise<ETLProgress> => {
    const response = await api.get(`/etl/status/${jobId}`);
    return response.data;
  },

  getJob: async (jobId: number): Promise<ETLJob> => {
    const response = await api.get(`/etl/job/${jobId}`);
    return response.data;
  },

  getQualityReport: async (jobId: number): Promise<DataQualityReport> => {
    const response = await api.get(`/etl/quality-report/${jobId}`);
    return response.data;
  },

  listJobs: async (datasetId?: number, status?: string): Promise<ETLJob[]> => {
    const response = await api.get('/etl/jobs', { params: { dataset_id: datasetId, status } });
    return response.data;
  },

  cancelJob: async (jobId: number): Promise<void> => {
    await api.delete(`/etl/job/${jobId}`);
  },

  improveData: async (
    datasetId: number,
    action: string,
    column?: string,
    params?: Record<string, unknown>
  ) => {
    const response = await api.post(
      `/etl/improve-data/${datasetId}`,
      params || {},
      { params: { action, column } }
    );
    return response.data;
  },

  chat: async (datasetId: number, message: string) => {
    const response = await api.post('/etl/chat', null, {
      params: { dataset_id: datasetId, message },
    });
    return response.data;
  },

  getImprovementSuggestions: async (datasetId: number) => {
    const response = await api.get(`/etl/improvement-suggestions/${datasetId}`);
    return response.data;
  },

  downloadCleanedData: (datasetId: number) => {
    return `${API_BASE}/etl/download-cleaned/${datasetId}`;
  },
};

// Warehouse API
export const warehouseApi = {
  listTables: async (): Promise<TableInfo[]> => {
    const response = await api.get('/warehouse/tables');
    return response.data;
  },

  getSchema: async (jobId?: number): Promise<StarSchema> => {
    const response = await api.get('/warehouse/schema', { params: { job_id: jobId } });
    return response.data;
  },

  getTableData: async (tableName: string, limit = 1000, offset = 0) => {
    const response = await api.get(`/warehouse/table/${tableName}`, { params: { limit, offset } });
    return response.data;
  },

  query: async (sql: string, params?: Record<string, unknown>, limit = 1000) => {
    const response = await api.post('/warehouse/query', { sql, params, limit });
    return response.data;
  },

  getDimensionValues: async (dimName: string, column?: string, search?: string) => {
    const response = await api.get(`/warehouse/dimensions/${dimName}/values`, {
      params: { column, search },
    });
    return response.data;
  },

  getRelationships: async () => {
    const response = await api.get('/warehouse/relationships');
    return response.data;
  },

  getStats: async () => {
    const response = await api.get('/warehouse/stats');
    return response.data;
  },
};

// Dashboard API
export const dashboardApi = {
  getKPIs: async (factTable?: string): Promise<{ kpis: KPI[]; fact_table: string }> => {
    const response = await api.get('/dashboard/kpis', { params: { fact_table: factTable } });
    return response.data;
  },

  aggregate: async (request: AggregateRequest) => {
    const response = await api.post('/dashboard/aggregate', request);
    return response.data;
  },

  getTimeSeries: async (
    measure: string,
    aggregation = 'sum',
    granularity = 'day'
  ): Promise<TimeSeriesData> => {
    const response = await api.get('/dashboard/timeseries', {
      params: { measure, aggregation, granularity },
    });
    return response.data;
  },

  getFilters: async (): Promise<{ filters: FilterOption[] }> => {
    const response = await api.get('/dashboard/filters');
    return response.data;
  },

  filter: async (filters: Record<string, unknown>, measures?: string[], limit = 1000) => {
    const response = await api.post('/dashboard/filter', { filters, measures, limit });
    return response.data;
  },

  getSummary: async () => {
    const response = await api.get('/dashboard/summary');
    return response.data;
  },

  saveQuery: async (name: string, queryType: string, configuration: Record<string, unknown>, description?: string) => {
    const response = await api.post('/dashboard/saved-query', configuration, {
      params: { name, query_type: queryType, description },
    });
    return response.data;
  },

  listSavedQueries: async () => {
    const response = await api.get('/dashboard/saved-queries');
    return response.data;
  },
};

// LLM API
export const llmApi = {
  getStatus: async (): Promise<{ available: boolean; model: string; base_url: string }> => {
    const response = await api.get('/llm/status');
    return response.data;
  },

  query: async (question: string, context?: string): Promise<LLMQueryResponse> => {
    const response = await api.post('/llm/query', { question, context });
    return response.data;
  },

  schemaAssist: async (datasetId: number, question: string) => {
    const response = await api.post('/llm/schema-assist', { dataset_id: datasetId, question });
    return response.data;
  },

  suggestTransformations: async (datasetId: number) => {
    const response = await api.post(`/llm/transformation-suggest?dataset_id=${datasetId}`);
    return response.data;
  },

  explain: async (question: string, context?: string) => {
    const response = await api.post('/llm/explain', null, { params: { question, context } });
    return response.data;
  },

  naturalQuery: async (question: string, execute = false) => {
    const response = await api.post('/llm/natural-query', null, { params: { question, execute } });
    return response.data;
  },
};

// Health API
export const healthApi = {
  check: async () => {
    const response = await api.get('/health');
    return response.data;
  },
};

export default api;
