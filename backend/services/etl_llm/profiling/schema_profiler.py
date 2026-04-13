"""
Automated Schema Profiling (Layer 1)
=====================================
Analyses ingested DataFrames to produce structured column profiles
without any LLM calls. Pure statistical profiling using pandas.

References:
  [El-Sappagh et al. 2011] — Source/destination/mapping area framework.
    Column classification (key, measure, dimension) mirrors the conceptual
    model's mapping area that identifies roles before transformation.
  [Annam 2025] — Probabilistic reasoning for data quality: column statistics
    (null rate, cardinality, type) feed downstream LLM agents.

Innovation #3 — Schema Fingerprinting:
  A SHA-256 hash of sorted column semantics (name + dtype + first sample)
  enables deterministic drift detection between pipeline runs.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field


# ── Pydantic models ─────────────────────────────────────────────


class ColumnProfile(BaseModel):
    """Statistical profile of a single DataFrame column."""

    name: str
    dtype: str
    null_pct: float = Field(ge=0.0, le=1.0)
    unique_count: int
    cardinality_ratio: float = Field(ge=0.0, le=1.0)
    samples: list[str] = Field(default_factory=list, max_length=5)
    min_val: str | None = None
    max_val: str | None = None
    mean_val: str | None = None
    is_candidate_key: bool = False
    is_candidate_measure: bool = False
    is_candidate_dimension: bool = False
    is_candidate_date: bool = False


class SchemaContext(BaseModel):
    """Full schema profile for one ingested DataFrame.

    The ``schema_fingerprint`` is a SHA-256 hash computed over sorted
    column semantics — see Innovation #3 in the paper.
    """

    source_name: str
    row_count: int
    column_count: int
    columns: list[ColumnProfile]
    schema_fingerprint: str
    profiled_at: datetime


# ── Profiler ─────────────────────────────────────────────────────


class SchemaProfiler:
    """Compute column-level statistics and classify columns.

    No LLM is used — all classification is rule-based using statistical
    thresholds derived from [El-Sappagh et al. 2011] and [Annam 2025].
    """

    _DATE_KEYWORDS = {"date", "time", "timestamp", "datetime", "dt", "day", "month", "year"}

    def profile(self, df: pd.DataFrame, source_name: str) -> SchemaContext:
        """Profile every column in *df* and return a :class:`SchemaContext`.

        Classification rules:
        * **candidate key** — cardinality ratio > 0.95
        * **candidate measure** — numeric dtype AND cardinality ratio > 0.1
        * **candidate dimension** — object/category dtype AND cardinality ratio < 0.5
        * **candidate date** — name contains date/time keywords OR datetime dtype
        """
        row_count = len(df)
        profiles: list[ColumnProfile] = []

        for col in df.columns:
            series = df[col]
            null_count = int(series.isna().sum())
            null_pct = null_count / row_count if row_count > 0 else 0.0
            unique_count = int(series.nunique())
            card_ratio = unique_count / row_count if row_count > 0 else 0.0

            # Representative samples (non-NaN)
            non_null = series.dropna()
            samples = [str(v) for v in non_null.head(5).tolist()]

            # Min / Max / Mean
            min_val: str | None = None
            max_val: str | None = None
            mean_val: str | None = None

            if pd.api.types.is_numeric_dtype(series):
                min_val = str(non_null.min()) if len(non_null) else None
                max_val = str(non_null.max()) if len(non_null) else None
                mean_val = str(round(non_null.mean(), 4)) if len(non_null) else None
            elif len(non_null) > 0:
                min_val = str(non_null.min())
                max_val = str(non_null.max())

            # Classification
            dtype_str = str(series.dtype)
            is_numeric = pd.api.types.is_numeric_dtype(series)
            is_object = dtype_str.startswith("object") or dtype_str == "string" or dtype_str.startswith("str")
            is_dt = pd.api.types.is_datetime64_any_dtype(series)

            col_lower = col.lower()
            has_date_keyword = any(kw in col_lower for kw in self._DATE_KEYWORDS)

            profiles.append(
                ColumnProfile(
                    name=col,
                    dtype=dtype_str,
                    null_pct=round(null_pct, 4),
                    unique_count=unique_count,
                    cardinality_ratio=round(card_ratio, 4),
                    samples=samples,
                    min_val=min_val,
                    max_val=max_val,
                    mean_val=mean_val,
                    is_candidate_key=card_ratio > 0.95,
                    is_candidate_measure=is_numeric and card_ratio > 0.1,
                    is_candidate_dimension=is_object and card_ratio < 0.5,
                    is_candidate_date=is_dt or has_date_keyword,
                )
            )

        fingerprint = self._compute_fingerprint(profiles)

        return SchemaContext(
            source_name=source_name,
            row_count=row_count,
            column_count=len(df.columns),
            columns=profiles,
            schema_fingerprint=fingerprint,
            profiled_at=datetime.utcnow(),
        )

    @staticmethod
    def _compute_fingerprint(columns: list[ColumnProfile]) -> str:
        """Compute SHA-256 schema fingerprint (Innovation #3).

        ``fingerprint = SHA256(sorted([name|dtype|sample0]))``
        """
        parts = []
        for c in columns:
            sample0 = c.samples[0] if c.samples else ""
            parts.append(f"{c.name}|{c.dtype}|{sample0}")
        canonical = "\n".join(sorted(parts))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
