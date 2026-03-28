"""
Schema Analyzer Service.

Automatically detects column types, measures, dimensions, and suggests star schema entities.
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import re

from utils.validators import DataValidator


@dataclass
class ColumnProfile:
    """Profile information for a single column."""
    name: str
    original_dtype: str
    inferred_type: str  # measure, dimension, date, key
    non_null_count: int
    null_count: int
    null_percent: float
    unique_count: int
    unique_percent: float
    sample_values: List[Any]
    is_potential_key: bool
    is_potential_fk: bool
    statistics: Dict[str, Any]
    semantic_type: Optional[str]  # customer_id, product_name, date, etc.


@dataclass
class SchemaAnalysis:
    """Complete schema analysis result."""
    measures: List[ColumnProfile]
    dimensions: List[ColumnProfile]
    date_columns: List[ColumnProfile]
    potential_keys: List[str]
    potential_foreign_keys: List[str]
    suggested_entities: List[Dict[str, Any]]
    total_rows: int
    total_columns: int
    memory_usage_mb: float


class SchemaAnalyzer:
    """
    Analyzes DataFrame schema to detect column types and suggest star schema structure.
    """

    # Patterns for semantic type detection
    SEMANTIC_PATTERNS = {
        'customer': re.compile(r'customer|client|buyer|user|member', re.I),
        'product': re.compile(r'product|item|sku|article|goods', re.I),
        'order': re.compile(r'order|transaction|sale|purchase', re.I),
        'date': re.compile(r'date|time|timestamp|created|updated|modified', re.I),
        'location': re.compile(r'location|city|country|region|state|address|zip|postal', re.I),
        'price': re.compile(r'price|cost|amount|total|value|revenue|sales', re.I),
        'quantity': re.compile(r'quantity|qty|count|number|units', re.I),
        'id': re.compile(r'_id$|^id$|_key$|_sk$|_pk$', re.I),
        'name': re.compile(r'name|title|description|label', re.I),
        'email': re.compile(r'email|mail', re.I),
        'phone': re.compile(r'phone|tel|mobile|fax', re.I),
        'category': re.compile(r'category|type|class|group|segment', re.I),
        'status': re.compile(r'status|state|flag|active|enabled', re.I),
    }

    # Common dimension entity keywords
    ENTITY_KEYWORDS = {
        'customer': ['customer', 'client', 'buyer', 'user', 'member', 'account'],
        'product': ['product', 'item', 'sku', 'article', 'goods', 'service'],
        'location': ['location', 'city', 'country', 'region', 'state', 'store', 'branch'],
        'time': ['date', 'time', 'year', 'month', 'day', 'quarter'],
        'employee': ['employee', 'staff', 'worker', 'agent', 'rep'],
        'category': ['category', 'type', 'class', 'segment', 'group'],
        'channel': ['channel', 'source', 'medium', 'platform'],
        'promotion': ['promotion', 'discount', 'campaign', 'coupon'],
    }

    def __init__(self, df: pd.DataFrame):
        """
        Initialize analyzer with DataFrame.

        Args:
            df: pandas DataFrame to analyze
        """
        self.df = df
        self.row_count = len(df)
        self.column_count = len(df.columns)

    def analyze(self) -> SchemaAnalysis:
        """
        Perform complete schema analysis.

        Returns:
            SchemaAnalysis object with all findings
        """
        profiles = [self._profile_column(col) for col in self.df.columns]

        measures = [p for p in profiles if p.inferred_type == 'measure']
        dimensions = [p for p in profiles if p.inferred_type == 'dimension']
        date_columns = [p for p in profiles if p.inferred_type == 'date']

        potential_keys = [p.name for p in profiles if p.is_potential_key]
        potential_fks = [p.name for p in profiles if p.is_potential_fk]

        suggested_entities = self._suggest_entities(profiles)

        memory_mb = self.df.memory_usage(deep=True).sum() / (1024 * 1024)

        return SchemaAnalysis(
            measures=measures,
            dimensions=dimensions,
            date_columns=date_columns,
            potential_keys=potential_keys,
            potential_foreign_keys=potential_fks,
            suggested_entities=suggested_entities,
            total_rows=self.row_count,
            total_columns=self.column_count,
            memory_usage_mb=round(memory_mb, 2)
        )

    def _profile_column(self, column_name: str) -> ColumnProfile:
        """
        Create detailed profile for a single column.

        Args:
            column_name: Name of column to profile

        Returns:
            ColumnProfile object
        """
        series = self.df[column_name]
        dtype = str(series.dtype)

        # Basic statistics
        non_null = series.count()
        null_count = series.isna().sum()
        null_percent = (null_count / self.row_count * 100) if self.row_count > 0 else 0
        unique_count = series.nunique()
        unique_percent = (unique_count / non_null * 100) if non_null > 0 else 0

        # Sample values (non-null) - convert to native Python types
        non_null_values = series.dropna()
        sample_list = non_null_values.head(5).tolist() if len(non_null_values) > 0 else []
        # Ensure all values are JSON serializable
        sample_values = []
        for v in sample_list:
            if hasattr(v, 'item'):
                sample_values.append(v.item())
            elif isinstance(v, (np.integer, np.floating)):
                sample_values.append(v.item())
            elif pd.isna(v):
                sample_values.append(None)
            else:
                sample_values.append(v)

        # Infer column type
        inferred_type = self._infer_column_type(series, column_name)

        # Check if potential key
        is_potential_key = self._is_potential_key(series, unique_count, null_count)
        is_potential_fk = self._is_potential_foreign_key(column_name, series)

        # Calculate statistics based on type
        statistics = self._calculate_statistics(series, inferred_type)

        # Detect semantic type
        semantic_type = self._detect_semantic_type(column_name, series)

        return ColumnProfile(
            name=column_name,
            original_dtype=dtype,
            inferred_type=inferred_type,
            non_null_count=int(non_null),
            null_count=int(null_count),
            null_percent=round(null_percent, 2),
            unique_count=int(unique_count),
            unique_percent=round(unique_percent, 2),
            sample_values=sample_values,
            is_potential_key=is_potential_key,
            is_potential_fk=is_potential_fk,
            statistics=statistics,
            semantic_type=semantic_type
        )

    def _infer_column_type(self, series: pd.Series, column_name: str) -> str:
        """
        Infer whether column is measure, dimension, or date.

        Args:
            series: pandas Series
            column_name: Name of the column

        Returns:
            'measure', 'dimension', or 'date'
        """
        dtype = series.dtype

        # Check for datetime
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return 'date'

        # Check for date-like strings
        if dtype == 'object':
            sample = series.dropna().head(100)
            if len(sample) > 0:
                date_count = sum(1 for v in sample if DataValidator.is_valid_date(v))
                if date_count / len(sample) > 0.8:
                    return 'date'

        # Check column name for date hints
        if self.SEMANTIC_PATTERNS['date'].search(column_name):
            if dtype == 'object' or pd.api.types.is_datetime64_any_dtype(dtype):
                return 'date'

        # Check for numeric types (potential measures)
        if pd.api.types.is_numeric_dtype(dtype):
            # But check if it's likely an ID column
            if self._looks_like_id(series, column_name):
                return 'dimension'

            # Check cardinality - low cardinality numeric might be categorical
            unique_ratio = series.nunique() / max(series.count(), 1)
            if unique_ratio < 0.01 and series.nunique() < 20:
                return 'dimension'

            return 'measure'

        # Boolean as dimension
        if pd.api.types.is_bool_dtype(dtype):
            return 'dimension'

        # Default: treat as dimension
        return 'dimension'

    def _looks_like_id(self, series: pd.Series, column_name: str) -> bool:
        """Check if numeric column looks like an ID field."""
        # Check name patterns
        if self.SEMANTIC_PATTERNS['id'].search(column_name):
            return True

        # Check if all integers
        if pd.api.types.is_integer_dtype(series.dtype):
            # Check if sequential-ish
            unique_vals = series.dropna().unique()
            if len(unique_vals) > 10:
                sorted_vals = sorted(unique_vals)
                # Check if reasonably sequential
                if sorted_vals[0] >= 0 and sorted_vals[-1] < len(self.df) * 2:
                    return True

        return False

    def _is_potential_key(self, series: pd.Series, unique_count: int, null_count: int) -> bool:
        """
        Check if column could be a primary key.

        Args:
            series: pandas Series
            unique_count: Number of unique values
            null_count: Number of null values

        Returns:
            True if column could be a primary key
        """
        if null_count > 0:
            return False

        if unique_count == self.row_count:
            return True

        return False

    def _is_potential_foreign_key(self, column_name: str, series: pd.Series) -> bool:
        """
        Check if column could be a foreign key.

        Args:
            column_name: Name of the column
            series: pandas Series

        Returns:
            True if column could be a foreign key
        """
        # Check naming patterns
        if self.SEMANTIC_PATTERNS['id'].search(column_name):
            # Check that it's not a primary key (has duplicates)
            if series.duplicated().any():
                return True

        return False

    def _calculate_statistics(self, series: pd.Series, inferred_type: str) -> Dict[str, Any]:
        """
        Calculate relevant statistics based on column type.

        Args:
            series: pandas Series
            inferred_type: Inferred column type

        Returns:
            Dictionary of statistics
        """
        stats = {}

        if inferred_type == 'measure':
            numeric_series = pd.to_numeric(series, errors='coerce')
            stats = {
                'min': float(numeric_series.min()) if not pd.isna(numeric_series.min()) else None,
                'max': float(numeric_series.max()) if not pd.isna(numeric_series.max()) else None,
                'mean': float(numeric_series.mean()) if not pd.isna(numeric_series.mean()) else None,
                'median': float(numeric_series.median()) if not pd.isna(numeric_series.median()) else None,
                'std': float(numeric_series.std()) if not pd.isna(numeric_series.std()) else None,
                'sum': float(numeric_series.sum()) if not pd.isna(numeric_series.sum()) else None,
            }

        elif inferred_type == 'dimension':
            value_counts = series.value_counts().head(10)
            # Convert numpy types to Python native types
            top_values = {str(k): int(v) for k, v in value_counts.items()}
            mode_val = series.mode().iloc[0] if len(series.mode()) > 0 else None
            # Convert mode to native Python type
            if hasattr(mode_val, 'item'):
                mode_val = mode_val.item()
            elif isinstance(mode_val, (np.integer, np.floating)):
                mode_val = mode_val.item()
            stats = {
                'cardinality': int(series.nunique()),
                'top_values': top_values,
                'mode': mode_val,
            }

        elif inferred_type == 'date':
            try:
                date_series = pd.to_datetime(series, errors='coerce')
                stats = {
                    'min_date': str(date_series.min()) if not pd.isna(date_series.min()) else None,
                    'max_date': str(date_series.max()) if not pd.isna(date_series.max()) else None,
                    'date_range_days': (date_series.max() - date_series.min()).days if not pd.isna(date_series.min()) else None,
                }
            except Exception:
                stats = {}

        return stats

    def _detect_semantic_type(self, column_name: str, series: pd.Series) -> Optional[str]:
        """
        Detect semantic type of column based on name and content.

        Args:
            column_name: Name of the column
            series: pandas Series

        Returns:
            Semantic type string or None
        """
        column_lower = column_name.lower()

        # Check patterns
        for semantic_type, pattern in self.SEMANTIC_PATTERNS.items():
            if pattern.search(column_lower):
                return semantic_type

        # Check content for email/phone
        if series.dtype == 'object':
            sample = series.dropna().head(50).astype(str)
            if len(sample) > 0:
                email_count = sum(1 for v in sample if DataValidator.is_valid_email(v))
                if email_count / len(sample) > 0.8:
                    return 'email'

                phone_count = sum(1 for v in sample if DataValidator.is_valid_phone(v))
                if phone_count / len(sample) > 0.8:
                    return 'phone'

        return None

    def _suggest_entities(self, profiles: List[ColumnProfile]) -> List[Dict[str, Any]]:
        """
        Suggest dimension entities based on column analysis.

        Args:
            profiles: List of column profiles

        Returns:
            List of suggested entity definitions
        """
        entities = []
        used_columns = set()

        # Group columns by semantic type / entity
        for entity_name, keywords in self.ENTITY_KEYWORDS.items():
            entity_columns = []

            for profile in profiles:
                if profile.name in used_columns:
                    continue

                col_lower = profile.name.lower()

                # Check if column name contains entity keywords
                for keyword in keywords:
                    if keyword in col_lower:
                        entity_columns.append(profile.name)
                        used_columns.add(profile.name)
                        break

            if entity_columns:
                # Determine the key column
                key_col = None
                for col in entity_columns:
                    if '_id' in col.lower() or col.lower().endswith('id'):
                        key_col = col
                        break

                entities.append({
                    'name': f'dim_{entity_name}',
                    'display_name': entity_name.title(),
                    'columns': entity_columns,
                    'suggested_key': key_col,
                    'is_time_dimension': entity_name == 'time'
                })

        # Add time dimension if we have date columns
        date_cols = [p.name for p in profiles if p.inferred_type == 'date']
        if date_cols and not any(e['name'] == 'dim_time' for e in entities):
            entities.append({
                'name': 'dim_time',
                'display_name': 'Time',
                'columns': date_cols,
                'suggested_key': date_cols[0],
                'is_time_dimension': True
            })

        return entities

    def to_dict(self) -> Dict[str, Any]:
        """Convert analysis results to dictionary."""
        analysis = self.analyze()
        return {
            'measures': [asdict(m) for m in analysis.measures],
            'dimensions': [asdict(d) for d in analysis.dimensions],
            'date_columns': [asdict(d) for d in analysis.date_columns],
            'potential_keys': analysis.potential_keys,
            'potential_foreign_keys': analysis.potential_foreign_keys,
            'suggested_entities': analysis.suggested_entities,
            'total_rows': analysis.total_rows,
            'total_columns': analysis.total_columns,
            'memory_usage_mb': analysis.memory_usage_mb
        }


def analyze_schema(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Convenience function to analyze DataFrame schema.

    Args:
        df: pandas DataFrame

    Returns:
        Dictionary with analysis results
    """
    analyzer = SchemaAnalyzer(df)
    return analyzer.to_dict()
