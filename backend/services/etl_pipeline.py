"""
ETL Pipeline Service.

Complete Extract-Transform-Load pipeline with data quality checks.
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
import logging
import traceback

from services.schema_analyzer import SchemaAnalyzer, analyze_schema
from services.star_schema_generator import StarSchemaGenerator, StarSchema
from services.data_quality import DataQualityChecker, DataQualityReport
from services.ddl_generator import DDLGenerator
from core.database import dataframe_to_table, execute_ddl, table_exists
from utils.file_handlers import load_file
from utils.validators import DataValidator, sanitize_for_json

logger = logging.getLogger(__name__)


class ETLStatus(str, Enum):
    PENDING = "pending"
    EXTRACTING = "extracting"
    QUALITY_CHECK = "quality_check"
    TRANSFORMING = "transforming"
    LOADING = "loading"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ETLStep:
    """Represents a single ETL step."""
    name: str
    status: str = "pending"
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ETLResult:
    """Result of ETL pipeline execution."""
    success: bool
    status: ETLStatus
    steps: List[ETLStep]
    quality_report: Optional[Dict[str, Any]] = None
    star_schema: Optional[Dict[str, Any]] = None
    tables_created: List[str] = field(default_factory=list)
    rows_processed: int = 0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)


class ETLPipeline:
    """
    Complete ETL pipeline for data warehouse loading.
    """

    def __init__(
            self,
            source_path: Optional[str] = None,
            source_df: Optional[pd.DataFrame] = None,
            config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize ETL pipeline.

        Args:
            source_path: Path to source file
            source_df: Source DataFrame (alternative to file)
            config: ETL configuration options
        """
        self.source_path = source_path
        self.source_df = source_df
        self.config = config or {}
        self.df: Optional[pd.DataFrame] = None
        self.star_schema: Optional[StarSchema] = None
        self.quality_report: Optional[DataQualityReport] = None

        # Configuration with defaults
        self.handle_missing = self.config.get('handle_missing', 'drop')
        self.fill_value = self.config.get('fill_value', None)
        self.remove_duplicates = self.config.get('remove_duplicates', True)
        self.normalize_strings = self.config.get('normalize_strings', True)
        self.generate_time_dim = self.config.get('generate_time_dimension', True)
        self.fact_table_name = self.config.get('fact_table_name', 'fact_main')
        self.dimension_config = self.config.get('dimension_config', None)
        self.skip_quality_check = self.config.get('skip_quality_check', False)
        self.fail_on_quality_issues = self.config.get('fail_on_quality_issues', False)

        # Pipeline state
        self.steps: List[ETLStep] = []
        self.result: Optional[ETLResult] = None

    def run(self) -> ETLResult:
        """
        Execute the complete ETL pipeline.

        Returns:
            ETLResult object with execution details
        """
        started_at = datetime.now()
        tables_created = []
        warnings = []

        try:
            # Step 1: Extract
            step_extract = self._run_step("extract", self._extract)
            if step_extract.status == "failed":
                raise Exception(step_extract.error)

            # Step 2: Quality Check
            if not self.skip_quality_check:
                step_quality = self._run_step("quality_check", self._quality_check)
                if step_quality.status == "failed" and self.fail_on_quality_issues:
                    raise Exception("Data quality check failed")
                if self.quality_report and not self.quality_report.passed:
                    warnings.append("Data quality issues detected - check quality report")

            # Step 3: Transform (Clean)
            step_clean = self._run_step("clean", self._clean_data)
            if step_clean.status == "failed":
                raise Exception(step_clean.error)

            # Step 4: Analyze Schema
            step_analyze = self._run_step("analyze_schema", self._analyze_schema)
            if step_analyze.status == "failed":
                raise Exception(step_analyze.error)

            # Step 5: Generate Star Schema
            step_star = self._run_step("generate_star_schema", self._generate_star_schema)
            if step_star.status == "failed":
                raise Exception(step_star.error)

            # Step 6: Generate DDL
            step_ddl = self._run_step("generate_ddl", self._generate_ddl)
            if step_ddl.status == "failed":
                raise Exception(step_ddl.error)

            # Step 7: Load to Database
            step_load = self._run_step("load", self._load_to_database)
            if step_load.status == "failed":
                raise Exception(step_load.error)

            tables_created = step_load.metadata.get('tables_created', [])

            self.result = ETLResult(
                success=True,
                status=ETLStatus.COMPLETED,
                steps=self.steps,
                quality_report=sanitize_for_json(asdict(self.quality_report)) if self.quality_report else None,
                star_schema=self._star_schema_to_dict(),
                tables_created=tables_created,
                rows_processed=len(self.df) if self.df is not None else 0,
                started_at=started_at,
                completed_at=datetime.now(),
                warnings=warnings
            )

        except Exception as e:
            logger.error(f"ETL Pipeline failed: {str(e)}")
            logger.error(traceback.format_exc())

            self.result = ETLResult(
                success=False,
                status=ETLStatus.FAILED,
                steps=self.steps,
                quality_report=sanitize_for_json(asdict(self.quality_report)) if self.quality_report else None,
                star_schema=None,
                tables_created=tables_created,
                rows_processed=len(self.df) if self.df is not None else 0,
                started_at=started_at,
                completed_at=datetime.now(),
                error=str(e),
                warnings=warnings
            )

        return self.result

    def _run_step(self, name: str, func: Callable) -> ETLStep:
        """Execute a single pipeline step."""
        step = ETLStep(name=name, status="running", started_at=datetime.now())
        self.steps.append(step)

        try:
            result = func()
            step.status = "completed"
            step.completed_at = datetime.now()
            if isinstance(result, dict):
                step.metadata = result
        except Exception as e:
            step.status = "failed"
            step.error = str(e)
            step.completed_at = datetime.now()
            logger.error(f"Step '{name}' failed: {str(e)}")

        return step

    def _extract(self) -> Dict[str, Any]:
        """Extract data from source."""
        if self.source_df is not None:
            self.df = self.source_df.copy()
        elif self.source_path:
            self.df = load_file(self.source_path)
        else:
            raise ValueError("No data source provided")

        return {
            'rows': len(self.df),
            'columns': len(self.df.columns),
            'column_names': list(self.df.columns)
        }

    def _quality_check(self) -> Dict[str, Any]:
        """Run data quality checks."""
        checker = DataQualityChecker(self.df)
        self.quality_report = checker.run_checks()

        return {
            'overall_score': self.quality_report.overall_score,
            'passed': self.quality_report.passed,
            'critical_issues': len(self.quality_report.critical_issues)
        }

    def _clean_data(self) -> Dict[str, Any]:
        """Clean and transform data."""
        original_rows = len(self.df)
        issues_fixed = 0

        # Handle missing values
        if self.handle_missing == 'drop':
            # Drop rows with any null values
            self.df = self.df.dropna()
        elif self.handle_missing == 'drop_columns':
            # Drop columns with too many nulls (>50%)
            threshold = len(self.df) * 0.5
            self.df = self.df.dropna(axis=1, thresh=threshold)
        elif self.handle_missing in ('fill', 'fill_mean'):
            # Fill numeric columns with mean, others with mode
            numeric_cols = self.df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                self.df[col] = self.df[col].fillna(self.df[col].mean())
            for col in self.df.select_dtypes(include=['object']).columns:
                mode_val = self.df[col].mode()
                if len(mode_val) > 0:
                    self.df[col] = self.df[col].fillna(mode_val[0])
        elif self.handle_missing == 'fill_median':
            # Fill numeric columns with median
            numeric_cols = self.df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                self.df[col] = self.df[col].fillna(self.df[col].median())
        elif self.handle_missing == 'fill_mode':
            # Fill with mode (most frequent value)
            for col in self.df.columns:
                mode_val = self.df[col].mode()
                if len(mode_val) > 0:
                    self.df[col] = self.df[col].fillna(mode_val[0])
        elif self.handle_missing == 'fill_value' and self.fill_value is not None:
            self.df = self.df.fillna(self.fill_value)
        elif self.handle_missing == 'keep':
            pass  # Keep missing values as-is

        rows_after_missing = len(self.df)
        issues_fixed += original_rows - rows_after_missing

        # Remove duplicates
        if self.remove_duplicates:
            before_dedup = len(self.df)
            self.df = self.df.drop_duplicates()
            issues_fixed += before_dedup - len(self.df)

        # Normalize strings
        if self.normalize_strings:
            string_cols = self.df.select_dtypes(include=['object']).columns
            for col in string_cols:
                # Strip whitespace
                self.df[col] = self.df[col].astype(str).str.strip()
                # Replace multiple spaces with single space
                self.df[col] = self.df[col].str.replace(r'\s+', ' ', regex=True)

        # Standardize column names
        self.df.columns = [DataValidator.validate_column_name(col) for col in self.df.columns]

        # Convert date columns
        for col in self.df.columns:
            if 'date' in col.lower() or 'time' in col.lower():
                try:
                    self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                except Exception:
                    pass

        return {
            'original_rows': original_rows,
            'final_rows': len(self.df),
            'rows_removed': original_rows - len(self.df),
            'issues_fixed': issues_fixed
        }

    def _analyze_schema(self) -> Dict[str, Any]:
        """Analyze schema for star schema generation."""
        self.schema_analysis = analyze_schema(self.df)
        return {
            'measures': len(self.schema_analysis.get('measures', [])),
            'dimensions': len(self.schema_analysis.get('dimensions', [])),
            'date_columns': len(self.schema_analysis.get('date_columns', []))
        }

    def _generate_star_schema(self) -> Dict[str, Any]:
        """Generate star schema from analyzed data."""
        generator = StarSchemaGenerator(self.df, self.schema_analysis)
        self.star_schema = generator.generate(
            fact_name=self.fact_table_name,
            dimension_config=self.dimension_config,
            generate_time_dim=self.generate_time_dim
        )

        return {
            'fact_table': self.star_schema.fact_table.name,
            'dimensions': [d.name for d in self.star_schema.dimensions],
            'relationships': len(self.star_schema.relationships)
        }

    def _generate_ddl(self) -> Dict[str, Any]:
        """Generate DDL scripts for star schema."""
        if self.star_schema is None:
            raise ValueError("Star schema not generated")

        self.ddl_generator = DDLGenerator(self.star_schema)
        self.ddl_script = self.ddl_generator.generate_all()

        return {
            'ddl_length': len(self.ddl_script),
            'tables': len(self.star_schema.dimensions) + 1
        }

    def _load_to_database(self) -> Dict[str, Any]:
        """Load star schema tables to PostgreSQL."""
        if self.star_schema is None:
            raise ValueError("Star schema not generated")

        tables_created = []

        try:
            # Execute DDL to create tables
            execute_ddl(self.ddl_script)

            # Load dimension tables
            for dim in self.star_schema.dimensions:
                if dim.dataframe is not None and len(dim.dataframe) > 0:
                    dataframe_to_table(dim.dataframe, dim.name, if_exists='replace')
                    tables_created.append(dim.name)
                    logger.info(f"Loaded dimension table: {dim.name} ({len(dim.dataframe)} rows)")

            # Load fact table
            if self.star_schema.fact_table.dataframe is not None:
                dataframe_to_table(
                    self.star_schema.fact_table.dataframe,
                    self.star_schema.fact_table.name,
                    if_exists='replace'
                )
                tables_created.append(self.star_schema.fact_table.name)
                logger.info(f"Loaded fact table: {self.star_schema.fact_table.name}")

        except Exception as e:
            logger.error(f"Failed to load tables: {str(e)}")
            raise

        return {
            'tables_created': tables_created,
            'total_tables': len(tables_created)
        }

    def _star_schema_to_dict(self) -> Optional[Dict[str, Any]]:
        """Convert star schema to dictionary."""
        if self.star_schema is None:
            return None

        return {
            'fact_table': {
                'name': self.star_schema.fact_table.name,
                'display_name': self.star_schema.fact_table.display_name,
                'measures': self.star_schema.fact_table.measures,
                'dimension_keys': self.star_schema.fact_table.dimension_keys,
                'row_count': len(self.star_schema.fact_table.dataframe) if self.star_schema.fact_table.dataframe is not None else 0
            },
            'dimensions': [
                {
                    'name': dim.name,
                    'display_name': dim.display_name,
                    'columns': dim.columns,
                    'surrogate_key': dim.surrogate_key,
                    'is_time_dimension': dim.is_time_dimension,
                    'row_count': len(dim.dataframe) if dim.dataframe is not None else 0
                }
                for dim in self.star_schema.dimensions
            ],
            'relationships': self.star_schema.relationships
        }

    def get_status(self) -> Dict[str, Any]:
        """Get current pipeline status."""
        current_step = None
        progress = 0

        if self.steps:
            completed_steps = sum(1 for s in self.steps if s.status == 'completed')
            total_steps = 7  # Total number of pipeline steps
            progress = int((completed_steps / total_steps) * 100)

            running_steps = [s for s in self.steps if s.status == 'running']
            if running_steps:
                current_step = running_steps[0].name

        return {
            'steps': [
                {
                    'name': s.name,
                    'status': s.status,
                    'started_at': s.started_at.isoformat() if s.started_at else None,
                    'completed_at': s.completed_at.isoformat() if s.completed_at else None,
                    'error': s.error
                }
                for s in self.steps
            ],
            'current_step': current_step,
            'progress_percent': progress
        }


def run_etl_pipeline(
        source_path: str = None,
        source_df: pd.DataFrame = None,
        config: Dict[str, Any] = None
) -> ETLResult:
    """
    Convenience function to run ETL pipeline.

    Args:
        source_path: Path to source file
        source_df: Source DataFrame
        config: ETL configuration

    Returns:
        ETLResult object
    """
    pipeline = ETLPipeline(
        source_path=source_path,
        source_df=source_df,
        config=config
    )
    return pipeline.run()
