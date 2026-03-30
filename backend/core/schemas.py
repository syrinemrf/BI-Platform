"""
Pydantic schemas for API request/response validation.
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


# ========== Enums ==========

class FileType(str, Enum):
    CSV = "csv"
    EXCEL = "excel"
    JSON = "json"


class ColumnType(str, Enum):
    MEASURE = "measure"
    DIMENSION = "dimension"
    DATE = "date"
    KEY = "key"


class ETLStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class AggregationType(str, Enum):
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    MIN = "min"
    MAX = "max"


# ========== Dataset Schemas ==========

class DatasetBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)


class DatasetCreate(DatasetBase):
    pass


class DatasetResponse(DatasetBase):
    id: int
    original_filename: str
    file_type: str
    file_size: int
    row_count: Optional[int]
    column_count: Optional[int]
    schema_info: Optional[Dict[str, Any]]
    user_id: Optional[int] = None
    session_id: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class DatasetPreview(BaseModel):
    columns: List[str]
    data: List[Dict[str, Any]]
    total_rows: int


# ========== Schema Analysis Schemas ==========

class ColumnAnalysis(BaseModel):
    name: str
    dtype: str
    column_type: ColumnType
    non_null_count: int
    null_count: int
    unique_count: int
    sample_values: List[Any]
    is_potential_key: bool = False
    statistics: Optional[Dict[str, Any]] = None


class SchemaAnalysisResult(BaseModel):
    measures: List[ColumnAnalysis]
    dimensions: List[ColumnAnalysis]
    date_columns: List[ColumnAnalysis]
    potential_keys: List[str]
    suggested_entities: List[str]
    total_rows: int
    total_columns: int


# ========== Star Schema Schemas ==========

class DimensionDefinition(BaseModel):
    name: str
    source_columns: List[str]
    surrogate_key: str = "sk_id"
    natural_key: Optional[str] = None
    is_time_dimension: bool = False
    attributes: List[str] = []


class FactDefinition(BaseModel):
    name: str
    measures: List[str]
    dimension_keys: Dict[str, str]  # dimension_name -> fk_column


class StarSchemaDefinition(BaseModel):
    fact_table: FactDefinition
    dimensions: List[DimensionDefinition]


class GeneratedStarSchema(BaseModel):
    fact_table: Dict[str, Any]
    dimension_tables: List[Dict[str, Any]]
    ddl_script: str
    relationships: List[Dict[str, str]]


# ========== ETL Schemas ==========

class ETLConfig(BaseModel):
    dataset_id: int
    handle_missing: str = "drop"  # drop, fill_mean, fill_median, fill_mode, fill_value
    fill_value: Optional[Any] = None
    remove_duplicates: bool = True
    normalize_strings: bool = True
    generate_time_dimension: bool = True
    custom_transformations: Optional[List[Dict[str, Any]]] = None


class ETLJobResponse(BaseModel):
    id: int
    dataset_id: int
    user_id: Optional[int] = None
    session_id: Optional[str] = None
    job_name: Optional[str] = None
    status: ETLStatus
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    error_message: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


class ETLProgress(BaseModel):
    job_id: int
    status: ETLStatus
    current_step: str
    progress_percent: int
    message: str


# ========== Data Quality Schemas ==========

class ColumnQualityReport(BaseModel):
    column_name: str
    completeness: float
    uniqueness: float
    validity: float
    data_type: str
    null_count: int
    duplicate_count: int
    invalid_count: int
    issues: List[str]


class DataQualityReportResponse(BaseModel):
    overall_score: float
    completeness_score: float
    uniqueness_score: float
    validity_score: float
    consistency_score: float
    column_reports: List[ColumnQualityReport]
    issues: List[Dict[str, Any]]
    recommendations: List[str]
    passed: bool


# ========== Warehouse Schemas ==========

class TableInfo(BaseModel):
    name: str
    display_name: str
    table_type: str  # fact or dimension
    columns: List[Dict[str, str]]
    row_count: int


class QueryRequest(BaseModel):
    sql: str
    params: Optional[Dict[str, Any]] = None
    limit: int = 1000


class QueryResponse(BaseModel):
    columns: List[str]
    data: List[Dict[str, Any]]
    row_count: int
    execution_time_ms: float


# ========== Dashboard Schemas ==========

class KPIRequest(BaseModel):
    measure: str
    aggregation: AggregationType
    filters: Optional[Dict[str, Any]] = None


class KPIResponse(BaseModel):
    name: str
    value: float
    previous_value: Optional[float] = None
    change_percent: Optional[float] = None
    trend: Optional[str] = None  # up, down, stable


class AggregateRequest(BaseModel):
    measures: List[str]
    dimensions: List[str]
    aggregations: Dict[str, AggregationType]
    filters: Optional[Dict[str, Any]] = None
    order_by: Optional[str] = None
    limit: int = 100


class TimeSeriesRequest(BaseModel):
    measure: str
    aggregation: AggregationType
    time_column: str
    granularity: str = "day"  # day, week, month, quarter, year
    filters: Optional[Dict[str, Any]] = None


class TimeSeriesResponse(BaseModel):
    labels: List[str]
    values: List[float]
    measure_name: str
    granularity: str


class FilterOptions(BaseModel):
    dimension: str
    values: List[Any]


class DashboardData(BaseModel):
    kpis: List[KPIResponse]
    charts: List[Dict[str, Any]]
    filters: List[FilterOptions]


# ========== LLM Schemas ==========

class LLMQueryRequest(BaseModel):
    question: str
    context: Optional[str] = None


class LLMQueryResponse(BaseModel):
    answer: str
    sql_query: Optional[str] = None
    confidence: float
    suggestions: List[str] = []


class SchemaAssistRequest(BaseModel):
    dataset_id: int
    question: str


# ========== Health Check ==========

class HealthCheck(BaseModel):
    status: str
    version: str
    database: str
    llm: str
