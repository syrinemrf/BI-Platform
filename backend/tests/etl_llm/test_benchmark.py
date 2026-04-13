"""Tests for the benchmarking framework."""

from __future__ import annotations

import pandas as pd
import pytest

from services.etl_llm.agents.schema_mapper import (
    DimensionTableSpec,
    FactTableSpec,
    SchemaMappingResult,
)
from services.etl_llm.evaluation.benchmark import (
    BenchmarkResult,
    ETLBenchmark,
)


class TestETLBenchmark:
    def test_perfect_mapping_accuracy(self):
        mapping = SchemaMappingResult(
            fact_table=FactTableSpec(name="fact", measures=["qty", "price"], foreign_keys=[]),
            dimension_tables=[
                DimensionTableSpec(name="dim_product", source_columns=["product"], surrogate_key="sk"),
            ],
            confidence=0.9,
            model_used="test",
            reasoning="",
        )
        acc = ETLBenchmark.evaluate_mapping_accuracy(
            mapping,
            ground_truth_fact_measures=["qty", "price"],
            ground_truth_dim_names=["dim_product"],
        )
        assert acc.precision == 1.0
        assert acc.recall == 1.0
        assert acc.f1_score == 1.0

    def test_partial_mapping_accuracy(self):
        mapping = SchemaMappingResult(
            fact_table=FactTableSpec(name="fact", measures=["qty"], foreign_keys=[]),
            dimension_tables=[],
            confidence=0.5,
            model_used="test",
            reasoning="",
        )
        acc = ETLBenchmark.evaluate_mapping_accuracy(
            mapping,
            ground_truth_fact_measures=["qty", "price"],
            ground_truth_dim_names=["dim_product"],
        )
        assert acc.precision == 1.0  # qty is correct
        assert acc.recall < 1.0  # missed price and dim_product
        assert 0 < acc.f1_score < 1.0

    def test_data_quality_improvement(self):
        before = pd.DataFrame({"a": [1, None, 3, 3], "b": ["x", "y", "z", "z"]})
        after = pd.DataFrame({"a": [1, 0, 3], "b": ["x", "y", "z"]})
        dq = ETLBenchmark.evaluate_data_quality(before, after)
        assert dq.null_pct_after < dq.null_pct_before
        assert dq.quality_improvement > 0

    def test_format_results_table(self):
        r = BenchmarkResult(dataset_name="test")
        r.mapping_accuracy.f1_score = 0.85
        r.code_quality.sql_syntax_valid = True
        r.latency.total_sec = 2.5
        table = ETLBenchmark.format_results_table([r])
        assert "test" in table
        assert "0.850" in table
        assert "✓" in table
