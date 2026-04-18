"""
Schema profiling module — Layer 1 of the ETL pipeline.
Operates WITHOUT any LLM call. Implements source area characterization
(El-Sappagh et al. 2011) and schema context construction (Annam 2025).
"""
import hashlib
import json
import time
from dataclasses import dataclass, field, asdict
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class ColumnProfile:
    name: str
    dtype: str
    null_pct: float
    unique_count: int
    sample_values: list
    is_candidate_key: bool = False
    is_candidate_measure: bool = False
    is_candidate_dimension: bool = False
    is_candidate_date: bool = False
    min_val: Optional[str] = None
    max_val: Optional[str] = None
    mean_val: Optional[float] = None


@dataclass
class SchemaContext:
    dataset_name: str
    num_rows: int
    num_columns: int
    columns: list[ColumnProfile] = field(default_factory=list)
    fingerprint: str = ""
    profiling_time_ms: float = 0.0

    def to_prompt_string(self) -> str:
        """Convert to a string suitable for LLM prompt injection."""
        lines = [
            f"Dataset: {self.dataset_name}",
            f"Rows: {self.num_rows}, Columns: {self.num_columns}",
            "",
            "Column details:",
        ]
        for col in self.columns:
            role = []
            if col.is_candidate_key:
                role.append("KEY")
            if col.is_candidate_measure:
                role.append("MEASURE")
            if col.is_candidate_dimension:
                role.append("DIMENSION")
            if col.is_candidate_date:
                role.append("DATE")
            role_str = ",".join(role) if role else "UNKNOWN"
            lines.append(
                f"  - {col.name} ({col.dtype}) | null={col.null_pct:.1%} "
                f"| unique={col.unique_count} | role={role_str} "
                f"| samples={col.sample_values[:3]}"
            )
        return "\n".join(lines)


@dataclass
class DriftReport:
    is_drifted: bool
    columns_added: list[str] = field(default_factory=list)
    columns_removed: list[str] = field(default_factory=list)
    type_changes: dict = field(default_factory=dict)
    fingerprint_old: str = ""
    fingerprint_new: str = ""


class SchemaProfiler:
    """Automated schema profiler — no LLM required."""

    NUMERIC_TYPES = {"int64", "float64", "int32", "float32"}
    DATE_KEYWORDS = {"date", "time", "timestamp", "ts", "created", "updated",
                     "issued", "due", "admission", "discharge", "paid"}

    def profile(self, df: pd.DataFrame, dataset_name: str = "unknown") -> SchemaContext:
        """Profile a DataFrame and return a SchemaContext."""
        start = time.perf_counter()
        columns = []

        for col_name in df.columns:
            series = df[col_name]
            dtype_str = str(series.dtype)
            null_pct = float(series.isna().mean())
            unique_count = int(series.nunique())
            samples = series.dropna().head(5).astype(str).tolist()

            cp = ColumnProfile(
                name=col_name,
                dtype=dtype_str,
                null_pct=null_pct,
                unique_count=unique_count,
                sample_values=samples,
            )

            # Classify column role
            cp.is_candidate_key = self._is_key(series, df.shape[0], col_name)
            cp.is_candidate_measure = self._is_measure(series, dtype_str, col_name)
            cp.is_candidate_dimension = self._is_dimension(
                series, dtype_str, unique_count, df.shape[0], col_name
            )
            cp.is_candidate_date = self._is_date(col_name, series)

            # Stats for numeric columns
            if dtype_str in self.NUMERIC_TYPES:
                cp.min_val = str(series.min())
                cp.max_val = str(series.max())
                cp.mean_val = float(series.mean())

            columns.append(cp)

        elapsed_ms = (time.perf_counter() - start) * 1000
        fingerprint = self._compute_fingerprint(df)

        return SchemaContext(
            dataset_name=dataset_name,
            num_rows=df.shape[0],
            num_columns=df.shape[1],
            columns=columns,
            fingerprint=fingerprint,
            profiling_time_ms=elapsed_ms,
        )

    def detect_drift(self, old_ctx: SchemaContext, new_ctx: SchemaContext) -> DriftReport:
        """Compare two SchemaContexts and report drift."""
        old_cols = {c.name for c in old_ctx.columns}
        new_cols = {c.name for c in new_ctx.columns}
        added = sorted(new_cols - old_cols)
        removed = sorted(old_cols - new_cols)

        type_changes = {}
        common = old_cols & new_cols
        old_map = {c.name: c.dtype for c in old_ctx.columns}
        new_map = {c.name: c.dtype for c in new_ctx.columns}
        for col in common:
            if old_map[col] != new_map[col]:
                type_changes[col] = {"old": old_map[col], "new": new_map[col]}

        is_drifted = bool(added or removed or type_changes)
        return DriftReport(
            is_drifted=is_drifted,
            columns_added=added,
            columns_removed=removed,
            type_changes=type_changes,
            fingerprint_old=old_ctx.fingerprint,
            fingerprint_new=new_ctx.fingerprint,
        )

    # ── private helpers ────────────────────────────────────
    @staticmethod
    def _compute_fingerprint(df: pd.DataFrame) -> str:
        """Deterministic fingerprint from column names and types."""
        schema_str = "|".join(
            f"{col}:{str(df[col].dtype)}" for col in sorted(df.columns)
        )
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]

    @staticmethod
    def _is_key(series: pd.Series, n_rows: int, col_name: str) -> bool:
        col_lower = col_name.lower()
        if "id" in col_lower or "code" in col_lower:
            if series.nunique() > 0.9 * n_rows:
                return True
        return False

    def _is_measure(self, series: pd.Series, dtype_str: str, col_name: str) -> bool:
        col_lower = col_name.lower()
        measure_kw = {"price", "cost", "amount", "total", "quantity", "qty",
                      "discount", "paid", "covered", "refund", "vat", "subtotal"}
        if dtype_str in self.NUMERIC_TYPES:
            if any(kw in col_lower for kw in measure_kw):
                return True
            # High cardinality numeric → likely measure
            if series.nunique() > 20:
                return True
        return False

    def _is_dimension(self, series: pd.Series, dtype_str: str,
                      unique_count: int, n_rows: int, col_name: str) -> bool:
        col_lower = col_name.lower()
        dim_kw = {"name", "category", "status", "type", "method", "region",
                  "country", "city", "department", "gender", "segment",
                  "device", "os", "blood", "severity", "outcome", "currency"}
        if any(kw in col_lower for kw in dim_kw):
            return True
        if dtype_str == "object" and unique_count < 0.5 * n_rows:
            return True
        return False

    def _is_date(self, col_name: str, series: pd.Series) -> bool:
        col_lower = col_name.lower()
        if any(kw in col_lower for kw in self.DATE_KEYWORDS):
            return True
        if str(series.dtype).startswith("datetime"):
            return True
        return False
