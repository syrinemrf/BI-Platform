// Dataset types
export interface Dataset {
  id: number;
  name: string;
  original_filename: string;
  file_type: string;
  file_size: number;
  row_count: number | null;
  column_count: number | null;
  schema_info: SchemaInfo | null;
  created_at: string;
}

export interface SchemaInfo {
  measures: ColumnAnalysis[];
  dimensions: ColumnAnalysis[];
  date_columns: ColumnAnalysis[];
  potential_keys: string[];
  suggested_entities: SuggestedEntity[];
  total_rows: number;
  total_columns: number;
}

export interface ColumnAnalysis {
  name: string;
  original_dtype: string;
  inferred_type: 'measure' | 'dimension' | 'date';
  non_null_count: number;
  null_count: number;
  unique_count: number;
  sample_values: unknown[];
  is_potential_key: boolean;
  statistics: Record<string, unknown>;
}

export interface SuggestedEntity {
  name: string;
  display_name: string;
  columns: string[];
  suggested_key: string | null;
  is_time_dimension: boolean;
}

// ETL types
export interface ETLJob {
  id: number;
  dataset_id: number;
  status: 'pending' | 'running' | 'completed' | 'failed';
  started_at: string | null;
  completed_at: string | null;
  error_message: string | null;
  created_at: string;
}

export interface ETLProgress {
  job_id: number;
  status: string;
  current_step: string;
  progress_percent: number;
  message: string;
}

export interface ETLConfig {
  dataset_id: number;
  handle_missing: 'drop' | 'fill_mean' | 'fill_median' | 'fill_mode' | 'fill_value';
  fill_value?: unknown;
  remove_duplicates: boolean;
  normalize_strings: boolean;
  generate_time_dimension: boolean;
}

// Data Quality types
export interface DataQualityReport {
  overall_score: number;
  completeness_score: number;
  uniqueness_score: number;
  validity_score: number;
  consistency_score: number;
  total_rows: number;
  duplicate_rows: number;
  column_reports: ColumnQualityReport[];
  critical_issues: QualityIssue[];
  warnings: QualityIssue[];
  recommendations: string[];
  passed: boolean;
}

export interface ColumnQualityReport {
  column_name: string;
  completeness: number;
  uniqueness: number;
  validity: number;
  data_type: string;
  null_count: number;
  issues: string[];
}

export interface QualityIssue {
  column: string;
  issue: string;
  severity: 'critical' | 'warning';
}

// Warehouse types
export interface TableInfo {
  name: string;
  display_name: string;
  table_type: 'fact' | 'dimension' | 'other';
  columns: ColumnInfo[];
  row_count: number;
}

export interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
}

export interface StarSchema {
  fact_table_name: string;
  dimension_tables: DimensionInfo[];
  schema_definition: Record<string, unknown>;
  relationships: Relationship[];
}

export interface DimensionInfo {
  name: string;
  display_name: string;
  columns: string[];
  surrogate_key: string;
  is_time_dimension: boolean;
  row_count: number;
}

export interface Relationship {
  fact_table: string;
  fact_column: string;
  dimension_table: string;
  dimension_column: string;
}

// Dashboard types
export interface KPI {
  name: string;
  column: string;
  total: number;
  average: number;
  count: number;
  min: number;
  max: number;
}

export interface TimeSeriesData {
  labels: string[];
  values: number[];
  measure_name: string;
  granularity: string;
}

export interface FilterOption {
  dimension: string;
  column: string;
  display_name: string;
  values: unknown[];
  count: number;
}

export interface AggregateRequest {
  measures: string[];
  dimensions: string[];
  aggregations: Record<string, 'sum' | 'avg' | 'count' | 'min' | 'max'>;
  filters?: Record<string, unknown>;
  order_by?: string;
  limit?: number;
}

// LLM types
export interface LLMQueryResponse {
  answer: string;
  sql_query: string | null;
  confidence: number;
  suggestions: string[];
}

// UI types
export interface Toast {
  id: string;
  type: 'success' | 'error' | 'info' | 'warning';
  message: string;
}

export type Theme = 'light' | 'dark' | 'system';

export type Language = 'en' | 'fr';
