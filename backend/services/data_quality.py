"""
Data Quality Service.

Comprehensive data quality checks for ETL pipelines.
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import re

from config import settings
from utils.validators import DataValidator


@dataclass
class ColumnQualityMetrics:
    """Quality metrics for a single column."""
    column_name: str
    data_type: str

    # Completeness
    total_count: int
    non_null_count: int
    null_count: int
    completeness_score: float

    # Uniqueness
    unique_count: int
    duplicate_count: int
    uniqueness_score: float

    # Validity
    valid_count: int
    invalid_count: int
    validity_score: float
    validity_issues: List[str]

    # Statistics
    statistics: Dict[str, Any]

    # Overall
    overall_score: float
    issues: List[str]
    recommendations: List[str]


@dataclass
class DataQualityReport:
    """Complete data quality report."""
    # Overall scores
    overall_score: float
    completeness_score: float
    uniqueness_score: float
    validity_score: float
    consistency_score: float

    # Row-level metrics
    total_rows: int
    duplicate_rows: int
    complete_rows: int

    # Column reports
    column_reports: List[ColumnQualityMetrics]

    # Issues and recommendations
    critical_issues: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]
    recommendations: List[str]

    # Status
    passed: bool
    check_timestamp: str


class DataQualityChecker:
    """
    Performs comprehensive data quality checks.
    """

    def __init__(
            self,
            df: pd.DataFrame,
            completeness_threshold: float = None,
            uniqueness_threshold: float = None,
            validity_threshold: float = None
    ):
        """
        Initialize quality checker.

        Args:
            df: DataFrame to check
            completeness_threshold: Minimum completeness score (0-1)
            uniqueness_threshold: Minimum uniqueness score (0-1)
            validity_threshold: Minimum validity score (0-1)
        """
        self.df = df
        self.completeness_threshold = completeness_threshold or settings.DQ_COMPLETENESS_THRESHOLD
        self.uniqueness_threshold = uniqueness_threshold or settings.DQ_UNIQUENESS_THRESHOLD
        self.validity_threshold = validity_threshold or settings.DQ_VALIDITY_THRESHOLD

    def run_checks(self) -> DataQualityReport:
        """
        Run all quality checks and generate report.

        Returns:
            DataQualityReport object
        """
        # Column-level checks
        column_reports = [self._check_column(col) for col in self.df.columns]

        # Row-level checks
        total_rows = len(self.df)
        duplicate_rows = self.df.duplicated().sum()
        complete_rows = len(self.df.dropna())

        # Calculate overall scores
        completeness_score = np.mean([c.completeness_score for c in column_reports])
        uniqueness_score = 1 - (duplicate_rows / total_rows) if total_rows > 0 else 1
        validity_score = np.mean([c.validity_score for c in column_reports])
        consistency_score = self._check_consistency()

        overall_score = np.mean([
            completeness_score,
            uniqueness_score,
            validity_score,
            consistency_score
        ])

        # Collect issues
        critical_issues = []
        warnings = []

        for report in column_reports:
            for issue in report.issues:
                if 'critical' in issue.lower() or report.overall_score < 0.5:
                    critical_issues.append({
                        'column': report.column_name,
                        'issue': issue,
                        'severity': 'critical'
                    })
                else:
                    warnings.append({
                        'column': report.column_name,
                        'issue': issue,
                        'severity': 'warning'
                    })

        # Row-level issues
        if duplicate_rows > 0:
            dup_percent = (duplicate_rows / total_rows) * 100
            if dup_percent > 10:
                critical_issues.append({
                    'column': '_all_',
                    'issue': f'{duplicate_rows} duplicate rows ({dup_percent:.1f}%)',
                    'severity': 'critical'
                })
            else:
                warnings.append({
                    'column': '_all_',
                    'issue': f'{duplicate_rows} duplicate rows ({dup_percent:.1f}%)',
                    'severity': 'warning'
                })

        # Generate recommendations
        recommendations = self._generate_recommendations(
            column_reports,
            duplicate_rows,
            total_rows
        )

        # Determine pass/fail
        passed = (
                completeness_score >= self.completeness_threshold and
                validity_score >= self.validity_threshold and
                len(critical_issues) == 0
        )

        return DataQualityReport(
            overall_score=round(overall_score, 4),
            completeness_score=round(completeness_score, 4),
            uniqueness_score=round(uniqueness_score, 4),
            validity_score=round(validity_score, 4),
            consistency_score=round(consistency_score, 4),
            total_rows=total_rows,
            duplicate_rows=int(duplicate_rows),
            complete_rows=complete_rows,
            column_reports=column_reports,
            critical_issues=critical_issues,
            warnings=warnings,
            recommendations=recommendations,
            passed=passed,
            check_timestamp=datetime.now().isoformat()
        )

    def _check_column(self, column_name: str) -> ColumnQualityMetrics:
        """
        Perform quality checks on a single column.

        Args:
            column_name: Name of column to check

        Returns:
            ColumnQualityMetrics object
        """
        series = self.df[column_name]
        dtype = str(series.dtype)
        total_count = len(series)

        # Completeness
        non_null_count = series.count()
        null_count = series.isna().sum()
        completeness_score = non_null_count / total_count if total_count > 0 else 0

        # Uniqueness
        unique_count = series.nunique()
        duplicate_count = int((series.duplicated() & series.notna()).sum())
        uniqueness_score = unique_count / non_null_count if non_null_count > 0 else 1

        # Validity
        valid_count, invalid_count, validity_issues = self._check_validity(series, dtype)
        validity_score = valid_count / non_null_count if non_null_count > 0 else 1

        # Statistics
        statistics = self._calculate_statistics(series, dtype)

        # Overall score (weighted average)
        overall_score = (
                completeness_score * 0.4 +
                validity_score * 0.4 +
                min(uniqueness_score, 1.0) * 0.2
        )

        # Collect issues
        issues = []
        if completeness_score < self.completeness_threshold:
            issues.append(f"Low completeness: {completeness_score:.1%} (threshold: {self.completeness_threshold:.1%})")

        if invalid_count > 0:
            issues.append(f"{invalid_count} invalid values detected")

        issues.extend(validity_issues)

        # Generate recommendations
        recommendations = []
        if null_count > 0:
            recommendations.append(f"Handle {null_count} missing values")
        if duplicate_count > total_count * 0.1:
            recommendations.append(f"Review {duplicate_count} duplicate values")

        return ColumnQualityMetrics(
            column_name=column_name,
            data_type=dtype,
            total_count=total_count,
            non_null_count=int(non_null_count),
            null_count=int(null_count),
            completeness_score=round(completeness_score, 4),
            unique_count=int(unique_count),
            duplicate_count=duplicate_count,
            uniqueness_score=round(uniqueness_score, 4),
            valid_count=valid_count,
            invalid_count=invalid_count,
            validity_score=round(validity_score, 4),
            validity_issues=validity_issues,
            statistics=statistics,
            overall_score=round(overall_score, 4),
            issues=issues,
            recommendations=recommendations
        )

    def _check_validity(self, series: pd.Series, dtype: str) -> Tuple[int, int, List[str]]:
        """
        Check validity of values in a column.

        Returns:
            Tuple of (valid_count, invalid_count, issues)
        """
        non_null = series.dropna()
        total_non_null = len(non_null)
        invalid_count = 0
        issues = []

        if total_non_null == 0:
            return 0, 0, issues

        # Type-specific validity checks
        if pd.api.types.is_numeric_dtype(series.dtype):
            # Check for infinite values
            inf_count = np.isinf(non_null).sum() if pd.api.types.is_float_dtype(series.dtype) else 0
            if inf_count > 0:
                invalid_count += inf_count
                issues.append(f"{inf_count} infinite values")

            # Check for negative values where unexpected
            if series.name and any(kw in series.name.lower() for kw in ['price', 'quantity', 'amount', 'count', 'age']):
                neg_count = (non_null < 0).sum()
                if neg_count > 0:
                    invalid_count += neg_count
                    issues.append(f"{neg_count} negative values in '{series.name}'")

            # Check for outliers (using IQR method)
            q1 = non_null.quantile(0.25)
            q3 = non_null.quantile(0.75)
            iqr = q3 - q1
            outlier_count = ((non_null < q1 - 3 * iqr) | (non_null > q3 + 3 * iqr)).sum()
            if outlier_count > total_non_null * 0.05:
                issues.append(f"{outlier_count} potential outliers (>5% of data)")

        elif dtype == 'object':
            # String validation
            sample = non_null.astype(str)

            # Check for empty strings
            empty_count = (sample.str.strip() == '').sum()
            if empty_count > 0:
                invalid_count += empty_count
                issues.append(f"{empty_count} empty strings")

            # Check for whitespace issues
            whitespace_count = (sample != sample.str.strip()).sum()
            if whitespace_count > 0:
                issues.append(f"{whitespace_count} values with leading/trailing whitespace")

            # Email validation
            if series.name and 'email' in series.name.lower():
                invalid_emails = sum(1 for v in sample if not DataValidator.is_valid_email(str(v)))
                if invalid_emails > 0:
                    invalid_count += invalid_emails
                    issues.append(f"{invalid_emails} invalid email addresses")

            # Phone validation
            if series.name and any(kw in series.name.lower() for kw in ['phone', 'tel', 'mobile']):
                invalid_phones = sum(1 for v in sample if not DataValidator.is_valid_phone(str(v)))
                if invalid_phones > 0:
                    invalid_count += invalid_phones
                    issues.append(f"{invalid_phones} invalid phone numbers")

        valid_count = total_non_null - invalid_count
        return int(valid_count), int(invalid_count), issues

    def _calculate_statistics(self, series: pd.Series, dtype: str) -> Dict[str, Any]:
        """Calculate statistics for a column."""
        stats = {}

        if pd.api.types.is_numeric_dtype(series.dtype):
            numeric = series.dropna()
            if len(numeric) > 0:
                stats = {
                    'min': float(numeric.min()),
                    'max': float(numeric.max()),
                    'mean': float(numeric.mean()),
                    'median': float(numeric.median()),
                    'std': float(numeric.std()) if len(numeric) > 1 else 0,
                    'skewness': float(numeric.skew()) if len(numeric) > 2 else 0,
                }
        elif dtype == 'object':
            non_null = series.dropna().astype(str)
            if len(non_null) > 0:
                lengths = non_null.str.len()
                stats = {
                    'min_length': int(lengths.min()),
                    'max_length': int(lengths.max()),
                    'avg_length': float(lengths.mean()),
                    'mode': non_null.mode().iloc[0] if len(non_null.mode()) > 0 else None,
                }

        return stats

    def _check_consistency(self) -> float:
        """
        Check cross-column consistency.

        Returns:
            Consistency score (0-1)
        """
        consistency_issues = 0
        total_checks = 0

        # Check date consistency (start < end)
        date_cols = [col for col in self.df.columns if 'date' in col.lower()]
        start_cols = [col for col in date_cols if any(kw in col.lower() for kw in ['start', 'begin', 'from'])]
        end_cols = [col for col in date_cols if any(kw in col.lower() for kw in ['end', 'finish', 'to', 'until'])]

        for start_col in start_cols:
            for end_col in end_cols:
                if start_col in self.df.columns and end_col in self.df.columns:
                    try:
                        start_dates = pd.to_datetime(self.df[start_col], errors='coerce')
                        end_dates = pd.to_datetime(self.df[end_col], errors='coerce')
                        invalid = (start_dates > end_dates).sum()
                        total_checks += len(self.df)
                        consistency_issues += invalid
                    except Exception:
                        pass

        # Check numeric consistency (e.g., quantity * price = total)
        if 'quantity' in [c.lower() for c in self.df.columns] and 'price' in [c.lower() for c in self.df.columns]:
            qty_col = [c for c in self.df.columns if c.lower() == 'quantity']
            price_col = [c for c in self.df.columns if c.lower() == 'price']
            total_cols = [c for c in self.df.columns if 'total' in c.lower()]

            if qty_col and price_col and total_cols:
                try:
                    expected = self.df[qty_col[0]] * self.df[price_col[0]]
                    actual = self.df[total_cols[0]]
                    mismatches = (abs(expected - actual) > 0.01).sum()
                    total_checks += len(self.df)
                    consistency_issues += mismatches
                except Exception:
                    pass

        if total_checks == 0:
            return 1.0

        return 1 - (consistency_issues / total_checks)

    def _generate_recommendations(
            self,
            column_reports: List[ColumnQualityMetrics],
            duplicate_rows: int,
            total_rows: int
    ) -> List[str]:
        """Generate actionable recommendations."""
        recommendations = []

        # Missing value recommendations
        high_null_cols = [r.column_name for r in column_reports if r.completeness_score < 0.9]
        if high_null_cols:
            recommendations.append(
                f"Address missing values in columns: {', '.join(high_null_cols[:5])}"
            )

        # Duplicate recommendations
        if duplicate_rows > 0:
            recommendations.append(
                f"Remove or investigate {duplicate_rows} duplicate rows"
            )

        # Low validity recommendations
        low_validity_cols = [r.column_name for r in column_reports if r.validity_score < 0.95]
        if low_validity_cols:
            recommendations.append(
                f"Review data quality in columns: {', '.join(low_validity_cols[:5])}"
            )

        # Type-specific recommendations
        for report in column_reports:
            if 'outliers' in str(report.issues).lower():
                recommendations.append(
                    f"Investigate outliers in '{report.column_name}'"
                )

        return recommendations[:10]  # Limit to top 10

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary."""
        report = self.run_checks()
        return {
            'overall_score': report.overall_score,
            'completeness_score': report.completeness_score,
            'uniqueness_score': report.uniqueness_score,
            'validity_score': report.validity_score,
            'consistency_score': report.consistency_score,
            'total_rows': report.total_rows,
            'duplicate_rows': report.duplicate_rows,
            'complete_rows': report.complete_rows,
            'column_reports': [asdict(c) for c in report.column_reports],
            'critical_issues': report.critical_issues,
            'warnings': report.warnings,
            'recommendations': report.recommendations,
            'passed': report.passed,
            'check_timestamp': report.check_timestamp
        }


def check_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Convenience function to check data quality.

    Args:
        df: DataFrame to check

    Returns:
        Dictionary with quality report
    """
    checker = DataQualityChecker(df)
    return checker.to_dict()
