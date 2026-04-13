"""Tests for data quality gates."""

from __future__ import annotations

import pandas as pd
import pytest

from services.etl_llm.validation.quality_gates import DataQualityGates


class TestDataQualityGates:
    def test_clean_data_passes(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        gates = DataQualityGates()
        report = gates.run_all_checks(df, "test")
        assert report.gate_passed is True
        assert report.failed == 0

    def test_high_null_fails(self):
        df = pd.DataFrame({"a": [None, None, None, 1], "b": [1, 2, 3, 4]})
        gates = DataQualityGates(max_null_pct=50.0)
        report = gates.run_all_checks(df, "test")
        assert report.gate_passed is False

    def test_duplicates_fail(self):
        df = pd.DataFrame({"a": [1, 1, 1, 1], "b": [2, 2, 2, 2]})
        gates = DataQualityGates(max_duplicate_pct=1.0)
        report = gates.run_all_checks(df, "test")
        assert report.gate_passed is False

    def test_range_check(self):
        df = pd.DataFrame({"price": [10, 20, -5, 100]})
        gates = DataQualityGates()
        report = gates.run_all_checks(df, "test", numeric_ranges={"price": (0, 50)})
        range_results = [r for r in report.results if r.check_name.startswith("range:")]
        assert any(not r.passed for r in range_results)

    def test_empty_df_fails_row_count(self):
        df = pd.DataFrame({"a": []})
        gates = DataQualityGates(min_rows=1)
        report = gates.run_all_checks(df, "test")
        assert report.gate_passed is False
