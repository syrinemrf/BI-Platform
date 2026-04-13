"""Tests for schema profiling and drift detection (Layer 1)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from services.etl_llm.profiling.schema_profiler import SchemaContext, SchemaProfiler
from services.etl_llm.profiling.drift_detector import SchemaDriftDetector


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=20, freq="D"),
            "product": (["Widget A", "Widget B", "Widget C", "Widget A", "Widget B"] * 4),
            "quantity": [10, 5, 8, 12, 3, 15, 7, 9, 11, 6, 14, 2, 18, 4, 13, 1, 16, 20, 8, 10],
            "price": ([19.99, 29.99, 9.99, 19.99, 29.99] * 4),
            "region": (["North", "South", "East", "West"] * 5),
        }
    )


@pytest.fixture
def profiler() -> SchemaProfiler:
    return SchemaProfiler()


class TestSchemaProfiler:
    def test_profile_returns_schema_context(self, profiler: SchemaProfiler, sample_df: pd.DataFrame):
        ctx = profiler.profile(sample_df, "test_source")
        assert isinstance(ctx, SchemaContext)
        assert ctx.source_name == "test_source"
        assert ctx.row_count == 20
        assert ctx.column_count == 5
        assert len(ctx.columns) == 5
        assert len(ctx.schema_fingerprint) == 64  # SHA-256 hex digest

    def test_column_classification_measure(self, profiler: SchemaProfiler, sample_df: pd.DataFrame):
        ctx = profiler.profile(sample_df, "test_source")
        qty_col = next(c for c in ctx.columns if c.name == "quantity")
        assert qty_col.is_candidate_measure is True
        assert qty_col.is_candidate_dimension is False

    def test_column_classification_dimension(self, profiler: SchemaProfiler, sample_df: pd.DataFrame):
        ctx = profiler.profile(sample_df, "test_source")
        region_col = next(c for c in ctx.columns if c.name == "region")
        assert region_col.is_candidate_dimension is True
        assert region_col.is_candidate_measure is False

    def test_schema_fingerprint_deterministic(self, profiler: SchemaProfiler, sample_df: pd.DataFrame):
        ctx1 = profiler.profile(sample_df, "test_source")
        ctx2 = profiler.profile(sample_df, "test_source")
        assert ctx1.schema_fingerprint == ctx2.schema_fingerprint

    def test_schema_fingerprint_changes_on_schema_change(
        self, profiler: SchemaProfiler, sample_df: pd.DataFrame
    ):
        ctx1 = profiler.profile(sample_df, "test_source")
        df2 = sample_df.drop(columns=["region"])
        ctx2 = profiler.profile(df2, "test_source")
        assert ctx1.schema_fingerprint != ctx2.schema_fingerprint


class TestDriftDetector:
    def test_drift_detector_detects_new_column(
        self, profiler: SchemaProfiler, sample_df: pd.DataFrame, tmp_path: Path
    ):
        store = tmp_path / "fp.json"
        detector = SchemaDriftDetector(store_path=str(store))

        # First run: new source
        ctx1 = profiler.profile(sample_df, "sales")
        report1 = detector.check_drift(ctx1)
        assert report1.is_new is True
        assert report1.is_drifted is False

        # Second run: same schema → no drift
        ctx2 = profiler.profile(sample_df, "sales")
        report2 = detector.check_drift(ctx2)
        assert report2.is_new is False
        assert report2.is_drifted is False

        # Third run: add column → drift
        df3 = sample_df.copy()
        df3["new_column"] = 1
        ctx3 = profiler.profile(df3, "sales")
        report3 = detector.check_drift(ctx3)
        assert report3.is_drifted is True
        assert "new_column" in report3.columns_added
