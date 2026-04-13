"""Tests for the Cleaning Rules Agent (Agent 2)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from services.etl_llm.agents.cleaning_agent import (
    CleaningPlan,
    CleaningRule,
    CleaningRulesAgent,
)
from services.etl_llm.profiling.schema_profiler import SchemaProfiler


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "quantity": [10, 5, None, 12, 3, 15, 7, None, 11, 6],
            "product": ["Widget A", "  Widget B  ", "WIDGET C", "widget a", "Widget B",
                        "Widget C", "Widget A", "Widget B", "Widget C", "Widget A"],
            "price": [19.99, 29.99, 9.99, 19.99, 29.99, 9.99, 19.99, 29.99, 200.0, 19.99],
        }
    )


@pytest.fixture
def agent() -> CleaningRulesAgent:
    return CleaningRulesAgent()


class TestCleaningRulesAgent:
    def test_generate_cleaning_plan_returns_valid_model(self, agent: CleaningRulesAgent):
        schema = SchemaProfiler().profile(
            pd.DataFrame({"a": [1, 2, None], "b": ["x", "y", "z"]}), "test"
        )
        mock_response = {
            "rules": [
                {"column": "a", "rule_type": "fill_null", "params": {"method": "median"},
                 "priority": 1, "justification": "20% nulls"}
            ],
            "estimated_quality_improvement": 0.15,
            "confidence": 0.85,
        }
        with patch.object(agent, "_call_llama", return_value=(mock_response, 0.85)):
            plan = agent.generate_cleaning_plan(schema)
        assert isinstance(plan, CleaningPlan)
        assert len(plan.rules) == 1
        assert plan.confidence == 0.85

    def test_apply_fill_null_median(self, agent: CleaningRulesAgent, sample_df: pd.DataFrame):
        plan = CleaningPlan(
            rules=[
                CleaningRule(
                    column="quantity",
                    rule_type="fill_null",
                    params={"method": "median"},
                    priority=1,
                    justification="Fill nulls",
                )
            ],
            estimated_quality_improvement=0.1,
            confidence=0.9,
            model_used="test",
        )
        cleaned, report = agent.apply_cleaning_plan(sample_df, plan)
        assert cleaned["quantity"].isna().sum() == 0
        assert report["rules_applied"][0]["rows_affected"] == 2

    def test_apply_normalize_text(self, agent: CleaningRulesAgent, sample_df: pd.DataFrame):
        plan = CleaningPlan(
            rules=[
                CleaningRule(
                    column="product",
                    rule_type="normalize_text",
                    params={},
                    priority=2,
                    justification="Normalize casing",
                )
            ],
            estimated_quality_improvement=0.05,
            confidence=0.9,
            model_used="test",
        )
        cleaned, report = agent.apply_cleaning_plan(sample_df, plan)
        assert all(v == v.lower().strip() for v in cleaned["product"])

    def test_apply_clip_outliers(self, agent: CleaningRulesAgent, sample_df: pd.DataFrame):
        plan = CleaningPlan(
            rules=[
                CleaningRule(
                    column="price",
                    rule_type="clip_outliers",
                    params={},
                    priority=2,
                    justification="Remove outliers",
                )
            ],
            estimated_quality_improvement=0.05,
            confidence=0.9,
            model_used="test",
        )
        cleaned, report = agent.apply_cleaning_plan(sample_df, plan)
        assert cleaned["price"].max() < 200.0  # The 200.0 outlier should be clipped

    def test_cleaning_report_counts_rows_affected(
        self, agent: CleaningRulesAgent, sample_df: pd.DataFrame
    ):
        plan = CleaningPlan(
            rules=[
                CleaningRule(column="quantity", rule_type="fill_null",
                             params={"method": "median"}, priority=1, justification=""),
                CleaningRule(column="product", rule_type="normalize_text",
                             params={}, priority=2, justification=""),
            ],
            estimated_quality_improvement=0.15,
            confidence=0.9,
            model_used="test",
        )
        _, report = agent.apply_cleaning_plan(sample_df, plan)
        assert len(report["rules_applied"]) == 2
        assert report["total_rows_affected"] > 0
