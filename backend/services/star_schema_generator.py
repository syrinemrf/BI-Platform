"""
Star Schema Generator Service.

Generates dimension and fact tables from analyzed schema.
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import hashlib
import logging

from services.schema_analyzer import SchemaAnalyzer, SchemaAnalysis
from utils.validators import DataValidator

logger = logging.getLogger(__name__)


@dataclass
class DimensionTable:
    """Definition for a dimension table."""
    name: str
    display_name: str
    columns: List[str]
    surrogate_key: str
    natural_key: Optional[str]
    is_time_dimension: bool
    attributes: List[Dict[str, Any]] = field(default_factory=list)
    dataframe: Optional[pd.DataFrame] = None


@dataclass
class FactTable:
    """Definition for a fact table."""
    name: str
    display_name: str
    measures: List[str]
    dimension_keys: Dict[str, str]  # dim_name -> fk_column
    degenerate_dimensions: List[str] = field(default_factory=list)
    dataframe: Optional[pd.DataFrame] = None


@dataclass
class StarSchema:
    """Complete star schema definition."""
    fact_table: FactTable
    dimensions: List[DimensionTable]
    source_columns: List[str]
    relationships: List[Dict[str, str]]


class StarSchemaGenerator:
    """
    Generates star schema from source DataFrame.
    """

    def __init__(self, df: pd.DataFrame, analysis: Optional[Dict[str, Any]] = None):
        """
        Initialize generator.

        Args:
            df: Source DataFrame
            analysis: Pre-computed schema analysis (optional)
        """
        self.df = df.copy()
        self.analysis = analysis or SchemaAnalyzer(df).to_dict()

    def generate(
            self,
            fact_name: str = "fact_main",
            dimension_config: Optional[List[Dict[str, Any]]] = None,
            generate_time_dim: bool = True
    ) -> StarSchema:
        """
        Generate complete star schema.

        Args:
            fact_name: Name for the fact table
            dimension_config: Custom dimension configuration
            generate_time_dim: Whether to auto-generate time dimension

        Returns:
            StarSchema object with all tables
        """
        # Use suggested entities or custom config
        dim_config = dimension_config or self.analysis.get('suggested_entities', [])

        dimensions = []
        dimension_keys = {}
        used_columns = set()

        # Generate dimension tables (skip time dimensions - handled below)
        for config in dim_config:
            if config.get('is_time_dimension', False):
                continue  # Time dimensions need special handling
            dim = self._create_dimension(config)
            if dim and dim.dataframe is not None and len(dim.dataframe) > 0:
                dimensions.append(dim)
                dimension_keys[dim.name] = f"fk_{dim.name}"
                used_columns.update(config.get('columns', []))

        # Generate time dimension if needed
        if generate_time_dim:
            date_cols = [c['name'] for c in self.analysis.get('date_columns', [])]
            # Also check suggested entities for date columns
            for config in dim_config:
                if config.get('is_time_dimension', False):
                    for col in config.get('columns', []):
                        if col not in date_cols:
                            date_cols.append(col)
            if date_cols:
                time_dim = self._create_time_dimension(date_cols[0])
                if time_dim:
                    dimensions.append(time_dim)
                    dimension_keys[time_dim.name] = f"fk_{time_dim.name}"

        # Generate fact table
        measures = [m['name'] for m in self.analysis.get('measures', [])]
        fact_table = self._create_fact_table(
            fact_name,
            measures,
            dimensions,
            dimension_keys
        )

        # Build relationships
        relationships = []
        for dim in dimensions:
            relationships.append({
                'fact_table': fact_table.name,
                'fact_column': f"fk_{dim.name}",
                'dimension_table': dim.name,
                'dimension_column': dim.surrogate_key
            })

        return StarSchema(
            fact_table=fact_table,
            dimensions=dimensions,
            source_columns=list(self.df.columns),
            relationships=relationships
        )

    def _create_dimension(self, config: Dict[str, Any]) -> Optional[DimensionTable]:
        """
        Create a dimension table from configuration.

        Args:
            config: Dimension configuration dict

        Returns:
            DimensionTable object or None
        """
        name = config.get('name', 'dim_unknown')
        display_name = config.get('display_name', name.replace('dim_', '').title())
        columns = config.get('columns', [])
        suggested_key = config.get('suggested_key')
        is_time_dim = config.get('is_time_dimension', False)

        if not columns:
            return None

        # Filter to existing columns
        columns = [c for c in columns if c in self.df.columns]
        if not columns:
            return None

        # Create dimension DataFrame with unique values
        dim_df = self.df[columns].drop_duplicates().reset_index(drop=True)

        # Add surrogate key
        sk_name = f"sk_{name.replace('dim_', '')}"
        dim_df.insert(0, sk_name, range(1, len(dim_df) + 1))

        # Determine natural key
        natural_key = suggested_key if suggested_key in columns else columns[0]

        # Build attribute definitions
        attributes = []
        for col in columns:
            dtype = str(dim_df[col].dtype)
            attributes.append({
                'name': col,
                'data_type': self._map_dtype_to_sql(dtype),
                'is_natural_key': col == natural_key
            })

        return DimensionTable(
            name=name,
            display_name=display_name,
            columns=columns,
            surrogate_key=sk_name,
            natural_key=natural_key,
            is_time_dimension=is_time_dim,
            attributes=attributes,
            dataframe=dim_df
        )

    def _create_time_dimension(self, date_column: str) -> Optional[DimensionTable]:
        """
        Create a time dimension table from date column.

        Args:
            date_column: Name of date column

        Returns:
            DimensionTable for time dimension
        """
        if date_column not in self.df.columns:
            return None

        # Convert to datetime
        dates = pd.to_datetime(self.df[date_column], errors='coerce').dropna()
        if len(dates) == 0:
            return None

        # Get date range
        min_date = dates.min().date()
        max_date = dates.max().date()

        # Generate all dates in range
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')

        # Build time dimension
        time_df = pd.DataFrame({
            'sk_time': range(1, len(date_range) + 1),
            'date_key': date_range.strftime('%Y%m%d').astype(int),
            'full_date': date_range.date,
            'year': date_range.year,
            'quarter': date_range.quarter,
            'month': date_range.month,
            'month_name': date_range.strftime('%B'),
            'week': date_range.isocalendar().week.astype(int),
            'day_of_month': date_range.day,
            'day_of_week': date_range.dayofweek + 1,
            'day_name': date_range.strftime('%A'),
            'is_weekend': date_range.dayofweek >= 5,
            'is_month_start': date_range.is_month_start,
            'is_month_end': date_range.is_month_end,
            'fiscal_year': date_range.year,  # Can be customized
            'fiscal_quarter': date_range.quarter
        })

        attributes = [
            {'name': 'sk_time', 'data_type': 'INTEGER', 'is_natural_key': False},
            {'name': 'date_key', 'data_type': 'INTEGER', 'is_natural_key': True},
            {'name': 'full_date', 'data_type': 'DATE', 'is_natural_key': False},
            {'name': 'year', 'data_type': 'INTEGER', 'is_natural_key': False},
            {'name': 'quarter', 'data_type': 'INTEGER', 'is_natural_key': False},
            {'name': 'month', 'data_type': 'INTEGER', 'is_natural_key': False},
            {'name': 'month_name', 'data_type': 'VARCHAR(20)', 'is_natural_key': False},
            {'name': 'week', 'data_type': 'INTEGER', 'is_natural_key': False},
            {'name': 'day_of_month', 'data_type': 'INTEGER', 'is_natural_key': False},
            {'name': 'day_of_week', 'data_type': 'INTEGER', 'is_natural_key': False},
            {'name': 'day_name', 'data_type': 'VARCHAR(20)', 'is_natural_key': False},
            {'name': 'is_weekend', 'data_type': 'BOOLEAN', 'is_natural_key': False},
            {'name': 'is_month_start', 'data_type': 'BOOLEAN', 'is_natural_key': False},
            {'name': 'is_month_end', 'data_type': 'BOOLEAN', 'is_natural_key': False},
            {'name': 'fiscal_year', 'data_type': 'INTEGER', 'is_natural_key': False},
            {'name': 'fiscal_quarter', 'data_type': 'INTEGER', 'is_natural_key': False},
        ]

        return DimensionTable(
            name='dim_time',
            display_name='Time',
            columns=['full_date', 'year', 'quarter', 'month', 'week', 'day_of_month'],
            surrogate_key='sk_time',
            natural_key='date_key',
            is_time_dimension=True,
            attributes=attributes,
            dataframe=time_df
        )

    def _create_fact_table(
            self,
            name: str,
            measures: List[str],
            dimensions: List[DimensionTable],
            dimension_keys: Dict[str, str]
    ) -> FactTable:
        """
        Create fact table with foreign keys to dimensions.

        Args:
            name: Fact table name
            measures: List of measure columns
            dimensions: List of dimension tables
            dimension_keys: Mapping of dimension names to FK columns

        Returns:
            FactTable object
        """
        fact_df = self.df.copy()

        # Add foreign keys for each dimension
        for dim in dimensions:
            if dim.dataframe is None:
                continue

            fk_column = dimension_keys.get(dim.name)
            if not fk_column:
                continue

            if dim.is_time_dimension:
                # Join time dimension on date
                date_cols = [c['name'] for c in self.analysis.get('date_columns', [])]
                if date_cols:
                    date_col = date_cols[0]
                    try:
                        fact_df[date_col] = pd.to_datetime(fact_df[date_col], errors='coerce')

                        # Create date_key for joining
                        fact_df['_temp_date_key'] = fact_df[date_col].dt.strftime('%Y%m%d')
                        fact_df['_temp_date_key'] = pd.to_numeric(fact_df['_temp_date_key'], errors='coerce')

                        # Merge with time dimension
                        time_lookup = dim.dataframe[['sk_time', 'date_key']].copy()
                        fact_df = fact_df.merge(
                            time_lookup,
                            left_on='_temp_date_key',
                            right_on='date_key',
                            how='left'
                        )
                        fact_df[fk_column] = fact_df['sk_time']
                        # Drop temp columns safely
                        drop_cols = [c for c in ['_temp_date_key', 'sk_time', 'date_key'] if c in fact_df.columns]
                        fact_df.drop(drop_cols, axis=1, inplace=True, errors='ignore')
                    except Exception as e:
                        logger.warning(f"Time dimension join failed for {date_col}: {e}")
                        fact_df[fk_column] = -1
            else:
                # Join regular dimension
                if dim.natural_key and dim.natural_key in fact_df.columns:
                    try:
                        lookup = dim.dataframe[[dim.surrogate_key, dim.natural_key]].copy()
                        fact_df = fact_df.merge(
                            lookup,
                            left_on=dim.natural_key,
                            right_on=dim.natural_key,
                            how='left',
                            suffixes=('', '_dim')
                        )
                        fact_df[fk_column] = fact_df[dim.surrogate_key]
                        fact_df.drop([dim.surrogate_key], axis=1, inplace=True, errors='ignore')
                    except Exception as e:
                        logger.warning(f"Dimension join failed for {dim.name}: {e}")
                        fact_df[fk_column] = -1

        # Determine columns to keep
        keep_cols = []

        # If no measures detected, use all numeric columns as measures
        actual_measures = measures.copy()
        if not actual_measures:
            numeric_cols = fact_df.select_dtypes(include=[np.number]).columns.tolist()
            actual_measures = [c for c in numeric_cols if not c.startswith('fk_') and c != 'sk_fact']

        # Collect FK columns
        fk_cols = [col for col in fact_df.columns if col.startswith('fk_')]

        # If no dimensions/measures at all, keep ALL original columns (flat fact table)
        if not actual_measures and not fk_cols:
            # Keep all columns from original data
            keep_cols = list(fact_df.columns)
            actual_measures = [c for c in keep_cols if c in self.df.columns]
        else:
            # Normal case: measures + FK columns
            for col in fact_df.columns:
                if col in actual_measures or col.startswith('fk_'):
                    keep_cols.append(col)

        # Add a surrogate key for the fact table
        fact_df.insert(0, 'sk_fact', range(1, len(fact_df) + 1))
        keep_cols.insert(0, 'sk_fact')

        # Ensure we have at least the sk_fact column
        keep_cols = [c for c in keep_cols if c in fact_df.columns]
        if not keep_cols:
            keep_cols = ['sk_fact']

        fact_df = fact_df[keep_cols]

        # Fill NaN foreign keys with -1 (unknown)
        for col in fact_df.columns:
            if col.startswith('fk_'):
                fact_df[col] = fact_df[col].fillna(-1).astype(int)

        return FactTable(
            name=name,
            display_name=name.replace('fact_', '').replace('_', ' ').title(),
            measures=actual_measures,
            dimension_keys=dimension_keys,
            dataframe=fact_df
        )

    def _map_dtype_to_sql(self, dtype: str) -> str:
        """Map pandas dtype to SQL type."""
        dtype_lower = dtype.lower()

        if 'int' in dtype_lower:
            return 'INTEGER'
        elif 'float' in dtype_lower:
            return 'DECIMAL(18,4)'
        elif 'bool' in dtype_lower:
            return 'BOOLEAN'
        elif 'datetime' in dtype_lower:
            return 'TIMESTAMP'
        elif 'date' in dtype_lower:
            return 'DATE'
        else:
            return 'VARCHAR(255)'

    def to_dict(self) -> Dict[str, Any]:
        """Convert star schema to dictionary format."""
        schema = self.generate()

        return {
            'fact_table': {
                'name': schema.fact_table.name,
                'display_name': schema.fact_table.display_name,
                'measures': schema.fact_table.measures,
                'dimension_keys': schema.fact_table.dimension_keys,
                'row_count': len(schema.fact_table.dataframe) if schema.fact_table.dataframe is not None else 0
            },
            'dimensions': [
                {
                    'name': dim.name,
                    'display_name': dim.display_name,
                    'columns': dim.columns,
                    'surrogate_key': dim.surrogate_key,
                    'natural_key': dim.natural_key,
                    'is_time_dimension': dim.is_time_dimension,
                    'attributes': dim.attributes,
                    'row_count': len(dim.dataframe) if dim.dataframe is not None else 0
                }
                for dim in schema.dimensions
            ],
            'relationships': schema.relationships,
            'source_columns': schema.source_columns
        }


def generate_star_schema(
        df: pd.DataFrame,
        fact_name: str = "fact_main",
        dimension_config: Optional[List[Dict[str, Any]]] = None
) -> StarSchema:
    """
    Convenience function to generate star schema.

    Args:
        df: Source DataFrame
        fact_name: Name for fact table
        dimension_config: Custom dimension configuration

    Returns:
        StarSchema object
    """
    generator = StarSchemaGenerator(df)
    return generator.generate(fact_name=fact_name, dimension_config=dimension_config)
