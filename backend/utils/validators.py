"""
Input validation utilities.
"""
import re
from typing import Any, List, Optional, Dict
from datetime import datetime
import pandas as pd
import numpy as np


class DataValidator:
    """Validation utilities for data processing."""

    # Common patterns
    EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    PHONE_PATTERN = re.compile(r'^[\d\s\-\+\(\)]{7,20}$')
    URL_PATTERN = re.compile(r'^https?://[^\s/$.?#].[^\s]*$')
    UUID_PATTERN = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)

    @staticmethod
    def is_valid_email(value: str) -> bool:
        """Check if value is a valid email address."""
        if not isinstance(value, str):
            return False
        return bool(DataValidator.EMAIL_PATTERN.match(value))

    @staticmethod
    def is_valid_phone(value: str) -> bool:
        """Check if value is a valid phone number."""
        if not isinstance(value, str):
            return False
        return bool(DataValidator.PHONE_PATTERN.match(value))

    @staticmethod
    def is_valid_url(value: str) -> bool:
        """Check if value is a valid URL."""
        if not isinstance(value, str):
            return False
        return bool(DataValidator.URL_PATTERN.match(value))

    @staticmethod
    def is_valid_uuid(value: str) -> bool:
        """Check if value is a valid UUID."""
        if not isinstance(value, str):
            return False
        return bool(DataValidator.UUID_PATTERN.match(value))

    @staticmethod
    def is_valid_date(value: Any, formats: List[str] = None) -> bool:
        """Check if value can be parsed as a date."""
        if pd.isna(value):
            return False

        if isinstance(value, (datetime, pd.Timestamp)):
            return True

        if not isinstance(value, str):
            return False

        default_formats = [
            '%Y-%m-%d',
            '%Y/%m/%d',
            '%d-%m-%Y',
            '%d/%m/%Y',
            '%m-%d-%Y',
            '%m/%d/%Y',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%SZ',
        ]

        formats = formats or default_formats

        for fmt in formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue

        return False

    @staticmethod
    def is_numeric_string(value: str) -> bool:
        """Check if string represents a number."""
        if not isinstance(value, str):
            return False
        try:
            float(value.replace(',', ''))
            return True
        except ValueError:
            return False

    @staticmethod
    def validate_column_name(name: str) -> str:
        """
        Validate and sanitize column name for SQL compatibility.

        Returns sanitized column name.
        """
        # Remove special characters, keep alphanumeric and underscores
        sanitized = re.sub(r'[^\w]', '_', str(name).lower())
        # Remove consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        # Ensure it doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = 'col_' + sanitized
        # Default name if empty
        return sanitized or 'unnamed_column'

    @staticmethod
    def validate_table_name(name: str) -> str:
        """
        Validate and sanitize table name for SQL compatibility.

        Returns sanitized table name.
        """
        sanitized = DataValidator.validate_column_name(name)
        # Avoid SQL reserved words
        reserved_words = {'select', 'from', 'where', 'table', 'index', 'create', 'drop',
                          'insert', 'update', 'delete', 'order', 'group', 'by', 'join'}
        if sanitized.lower() in reserved_words:
            sanitized = f"tbl_{sanitized}"
        return sanitized


class SchemaValidator:
    """Validation for schema-related operations."""

    @staticmethod
    def validate_star_schema(schema: Dict) -> List[str]:
        """
        Validate star schema definition.

        Returns list of validation errors (empty if valid).
        """
        errors = []

        # Check fact table
        if 'fact_table' not in schema:
            errors.append("Missing fact table definition")
        else:
            fact = schema['fact_table']
            if 'name' not in fact:
                errors.append("Fact table missing name")
            if 'measures' not in fact or not fact['measures']:
                errors.append("Fact table must have at least one measure")

        # Check dimensions
        if 'dimensions' not in schema or not schema['dimensions']:
            errors.append("Schema must have at least one dimension")
        else:
            for i, dim in enumerate(schema['dimensions']):
                if 'name' not in dim:
                    errors.append(f"Dimension {i} missing name")
                if 'source_columns' not in dim:
                    errors.append(f"Dimension '{dim.get('name', i)}' missing source columns")

        return errors

    @staticmethod
    def validate_query(query: str) -> List[str]:
        """
        Basic SQL query validation.

        Returns list of validation errors (empty if valid).
        """
        errors = []

        # Check for dangerous operations
        dangerous_keywords = ['DROP', 'TRUNCATE', 'DELETE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']

        query_upper = query.upper()
        for keyword in dangerous_keywords:
            if re.search(rf'\b{keyword}\b', query_upper):
                errors.append(f"Query contains forbidden keyword: {keyword}")

        # Basic syntax check
        if not query_upper.strip().startswith('SELECT'):
            errors.append("Only SELECT queries are allowed")

        return errors


class FileValidator:
    """Validation for file uploads."""

    ALLOWED_EXTENSIONS = {'.csv', '.xlsx', '.xls', '.json'}
    MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB

    @classmethod
    def validate_extension(cls, filename: str) -> bool:
        """Check if file extension is allowed."""
        ext = '.' + filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
        return ext in cls.ALLOWED_EXTENSIONS

    @classmethod
    def validate_size(cls, size: int, max_size: int = None) -> bool:
        """Check if file size is within limits."""
        max_size = max_size or cls.MAX_FILE_SIZE
        return size <= max_size

    @classmethod
    def validate_file(cls, filename: str, size: int) -> List[str]:
        """
        Validate uploaded file.

        Returns list of validation errors (empty if valid).
        """
        errors = []

        if not cls.validate_extension(filename):
            errors.append(f"File extension not allowed. Allowed: {', '.join(cls.ALLOWED_EXTENSIONS)}")

        if not cls.validate_size(size):
            max_mb = cls.MAX_FILE_SIZE / (1024 * 1024)
            errors.append(f"File size exceeds maximum limit of {max_mb}MB")

        return errors


def sanitize_for_json(obj: Any) -> Any:
    """
    Sanitize object for JSON serialization.

    Handles pandas types, numpy types, and datetime objects.
    """
    # Handle None
    if obj is None:
        return None

    # Handle numpy arrays
    if isinstance(obj, np.ndarray):
        return [sanitize_for_json(v) for v in obj.tolist()]

    # Handle pandas NA/NaT
    try:
        if pd.isna(obj) and not isinstance(obj, (list, dict, np.ndarray)):
            return None
    except (ValueError, TypeError):
        # pd.isna raises ValueError for arrays
        pass

    # Handle timestamps
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()

    # Handle numpy scalars
    if isinstance(obj, (np.integer, np.floating)):
        return obj.item()
    if hasattr(obj, 'item') and callable(obj.item):
        try:
            return obj.item()
        except (ValueError, TypeError):
            pass

    # Handle numpy bool
    if isinstance(obj, np.bool_):
        return bool(obj)

    # Handle dict
    if isinstance(obj, dict):
        return {str(k): sanitize_for_json(v) for k, v in obj.items()}

    # Handle list/tuple
    if isinstance(obj, (list, tuple)):
        return [sanitize_for_json(v) for v in obj]

    # Handle pandas Series
    if isinstance(obj, pd.Series):
        return sanitize_for_json(obj.tolist())

    return obj
