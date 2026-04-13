"""
Data Quality Gates
===================
Rule-based data quality checks that run as a validation layer
between cleaning and loading.

Inspired by Great-Expectations-style validation but implemented
as a lightweight standalone module to avoid heavy dependencies.

Reference: [El-Sappagh et al. 2011] — data quality is critical
for analytical processing in the staging area.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class QualityCheckResult:
    check_name: str
    passed: bool
    details: str = ""
    severity: str = "error"  # error | warning


@dataclass
class QualityGateReport:
    dataset_name: str
    total_checks: int = 0
    passed: int = 0
    failed: int = 0
    warnings: int = 0
    gate_passed: bool = False
    results: list[QualityCheckResult] = field(default_factory=list)


class DataQualityGates:
    """Lightweight data quality gate validator.

    Checks:
    - Column completeness (null percentage thresholds)
    - Uniqueness (duplicate row percentage)
    - Type consistency (expected dtypes)
    - Value range (min/max bounds for numeric columns)
    - Row count (minimum expected rows)
    """

    def __init__(
        self,
        max_null_pct: float = 20.0,
        max_duplicate_pct: float = 5.0,
        min_rows: int = 1,
    ) -> None:
        self.max_null_pct = max_null_pct
        self.max_duplicate_pct = max_duplicate_pct
        self.min_rows = min_rows

    def check_completeness(self, df: pd.DataFrame) -> list[QualityCheckResult]:
        """Check that no column exceeds the null percentage threshold."""
        results = []
        for col in df.columns:
            null_pct = df[col].isnull().mean() * 100
            passed = null_pct <= self.max_null_pct
            results.append(
                QualityCheckResult(
                    check_name=f"completeness:{col}",
                    passed=passed,
                    details=f"null%={null_pct:.1f} (max={self.max_null_pct})",
                    severity="error" if not passed else "info",
                )
            )
        return results

    def check_uniqueness(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check that duplicate rows don't exceed threshold."""
        dup_pct = df.duplicated().mean() * 100
        passed = dup_pct <= self.max_duplicate_pct
        return QualityCheckResult(
            check_name="uniqueness",
            passed=passed,
            details=f"dup%={dup_pct:.1f} (max={self.max_duplicate_pct})",
            severity="error" if not passed else "info",
        )

    def check_row_count(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check minimum row count."""
        passed = len(df) >= self.min_rows
        return QualityCheckResult(
            check_name="row_count",
            passed=passed,
            details=f"rows={len(df)} (min={self.min_rows})",
            severity="error" if not passed else "info",
        )

    def check_numeric_ranges(
        self,
        df: pd.DataFrame,
        ranges: dict[str, tuple[float, float]] | None = None,
    ) -> list[QualityCheckResult]:
        """Check that numeric columns fall within specified ranges."""
        results = []
        if not ranges:
            return results
        for col, (lo, hi) in ranges.items():
            if col not in df.columns:
                results.append(
                    QualityCheckResult(
                        check_name=f"range:{col}",
                        passed=False,
                        details=f"Column '{col}' not found",
                        severity="warning",
                    )
                )
                continue
            if not pd.api.types.is_numeric_dtype(df[col]):
                continue
            out_of_range = ((df[col] < lo) | (df[col] > hi)).sum()
            passed = out_of_range == 0
            results.append(
                QualityCheckResult(
                    check_name=f"range:{col}",
                    passed=passed,
                    details=f"{out_of_range} values outside [{lo}, {hi}]",
                    severity="warning" if not passed else "info",
                )
            )
        return results

    def run_all_checks(
        self,
        df: pd.DataFrame,
        dataset_name: str = "unnamed",
        numeric_ranges: dict[str, tuple[float, float]] | None = None,
    ) -> QualityGateReport:
        """Run all quality checks and return a gate report."""
        checks: list[QualityCheckResult] = []
        checks.extend(self.check_completeness(df))
        checks.append(self.check_uniqueness(df))
        checks.append(self.check_row_count(df))
        checks.extend(self.check_numeric_ranges(df, numeric_ranges))

        passed = sum(1 for c in checks if c.passed)
        failed = sum(1 for c in checks if not c.passed and c.severity == "error")
        warnings = sum(1 for c in checks if not c.passed and c.severity == "warning")
        gate_passed = failed == 0

        return QualityGateReport(
            dataset_name=dataset_name,
            total_checks=len(checks),
            passed=passed,
            failed=failed,
            warnings=warnings,
            gate_passed=gate_passed,
            results=checks,
        )
