"""Tests for the ETL Code Generator Agent (Agent 3)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from services.etl_llm.agents.code_generator import (
    ETLCodeGeneratorAgent,
    GeneratedETLCode,
)
from services.etl_llm.agents.cleaning_agent import CleaningPlan, CleaningRule
from services.etl_llm.agents.schema_mapper import (
    DimensionTableSpec,
    FactTableSpec,
    SchemaMappingResult,
)
from services.etl_llm.profiling.schema_profiler import SchemaProfiler


@pytest.fixture
def agent() -> ETLCodeGeneratorAgent:
    return ETLCodeGeneratorAgent()


@pytest.fixture
def sample_schema():
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    return SchemaProfiler().profile(df, "test")


@pytest.fixture
def sample_mapping() -> SchemaMappingResult:
    return SchemaMappingResult(
        fact_table=FactTableSpec(name="fact", measures=["a"], foreign_keys=["b"]),
        dimension_tables=[
            DimensionTableSpec(name="dim_b", source_columns=["b"], surrogate_key="b_sk")
        ],
        confidence=0.9,
        model_used="test",
        reasoning="test",
    )


@pytest.fixture
def sample_plan() -> CleaningPlan:
    return CleaningPlan(
        rules=[CleaningRule(column="a", rule_type="fill_null", params={"method": "median"},
                            priority=1, justification="test")],
        estimated_quality_improvement=0.1,
        confidence=0.9,
        model_used="test",
    )


class TestETLCodeGenerator:
    def test_validate_sql_valid(self, agent: ETLCodeGeneratorAgent):
        valid, err = agent.validate_sql("SELECT 1")
        assert valid is True
        assert err == ""

    def test_validate_sql_invalid(self, agent: ETLCodeGeneratorAgent):
        valid, err = agent.validate_sql("SELECTT 1 FROMM")
        # sqlfluff should report parse errors
        assert valid is False or "PRS" in err or len(err) > 0

    def test_validate_python_valid(self, agent: ETLCodeGeneratorAgent):
        valid, err = agent.validate_python("x = 1\nprint(x)")
        assert valid is True
        assert err == ""

    def test_validate_python_invalid(self, agent: ETLCodeGeneratorAgent):
        valid, err = agent.validate_python("def foo(\n  pass")
        assert valid is False
        assert "SyntaxError" in err

    def test_self_correct_called_on_invalid_sql(
        self, agent, sample_schema, sample_mapping, sample_plan
    ):
        generated = GeneratedETLCode(
            extraction_code="import pandas",
            transformation_code="df = df.copy()",
            loading_code="SELECTT 1 FROMM",  # invalid
            full_pipeline_code="x = 1",
            final_confidence=0.8,
        )
        with patch.object(agent, "generate_code", return_value=generated):
            with patch.object(agent, "self_correct", return_value="SELECT 1") as mock_correct:
                result = agent.run_with_self_correction(
                    sample_schema, sample_mapping, sample_plan
                )
                assert mock_correct.called

    def test_correction_attempts_tracked(
        self, agent, sample_schema, sample_mapping, sample_plan
    ):
        generated = GeneratedETLCode(
            extraction_code="",
            transformation_code="",
            loading_code="",
            full_pipeline_code="x = 1",
            final_confidence=0.8,
        )
        with patch.object(agent, "generate_code", return_value=generated):
            result = agent.run_with_self_correction(
                sample_schema, sample_mapping, sample_plan
            )
            assert isinstance(result.correction_attempts, int)

    def test_max_3_correction_attempts(
        self, agent, sample_schema, sample_mapping, sample_plan
    ):
        generated = GeneratedETLCode(
            extraction_code="",
            transformation_code="",
            loading_code="SELECTT bad sql",
            full_pipeline_code="def foo(\n  pass",  # also invalid
            final_confidence=0.8,
        )
        # self_correct always returns invalid code too
        with patch.object(agent, "generate_code", return_value=generated):
            with patch.object(agent, "self_correct", side_effect=lambda c, e, l, a: c):
                result = agent.run_with_self_correction(
                    sample_schema, sample_mapping, sample_plan
                )
                # Should not exceed 6 total (3 SQL + 3 Python)
                assert result.correction_attempts <= 6
