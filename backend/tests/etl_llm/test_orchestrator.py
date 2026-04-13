"""Tests for the full pipeline orchestrator (Layer 1→5)."""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from services.etl_llm.agents.cleaning_agent import CleaningPlan, CleaningRule
from services.etl_llm.agents.code_generator import GeneratedETLCode
from services.etl_llm.agents.schema_mapper import (
    DimensionTableSpec,
    FactTableSpec,
    SchemaMappingResult,
)
from services.etl_llm.orchestrator.pipeline_orchestrator import (
    ETLPipelineOrchestrator,
    PipelineResult,
)


@pytest.fixture
def csv_path(tmp_path):
    """Create a temporary CSV file."""
    p = tmp_path / "test.csv"
    df = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-02"],
        "product": ["A", "B"],
        "quantity": [10, 20],
        "price": [1.5, 2.5],
    })
    df.to_csv(p, index=False)
    return str(p)


def _mock_mapping():
    return SchemaMappingResult(
        fact_table=FactTableSpec(name="fact_sales", measures=["quantity", "price"], foreign_keys=["product"]),
        dimension_tables=[DimensionTableSpec(name="dim_product", source_columns=["product"], surrogate_key="sk")],
        confidence=0.9,
        model_used="llama3:8b",
        reasoning="test",
    )


def _mock_plan():
    return CleaningPlan(
        rules=[],
        estimated_quality_improvement=0.0,
        confidence=0.95,
        model_used="llama3:8b",
    )


def _mock_code():
    return GeneratedETLCode(
        loading_code="CREATE TABLE IF NOT EXISTS fact_sales (quantity INT, price REAL);",
        correction_attempts=0,
    )


class TestPipelineOrchestrator:
    @pytest.mark.asyncio
    async def test_pipeline_ingestion_failure(self, tmp_path):
        orch = ETLPipelineOrchestrator(db_path=str(tmp_path / "wh.db"), drift_store_path=str(tmp_path / "fp.json"))
        result = await orch.run_pipeline("/nonexistent/file.csv", "csv")
        assert len(result.errors) > 0
        assert "Ingestion" in result.errors[0] or "ingestion" in result.errors[0].lower()

    @pytest.mark.asyncio
    async def test_pipeline_layers_1_2_no_mocks(self, csv_path, tmp_path):
        """Test that layers 1 and 2 run without LLM calls."""
        orch = ETLPipelineOrchestrator(
            db_path=str(tmp_path / "wh.db"),
            drift_store_path=str(tmp_path / "fp.json"),
        )
        # Only run layers 1-2 by mocking the mapper to raise
        with patch.object(orch.mapper, "map_schema", side_effect=Exception("No LLM")):
            result = await orch.run_pipeline(csv_path, "csv")
        assert result.rows_ingested == 2
        assert result.schema_fingerprint != ""
        assert "Mapping failed" in result.errors[0]

    @pytest.mark.asyncio
    async def test_full_pipeline_auto_approve(self, csv_path, tmp_path):
        """Full pipeline with mocked LLM calls and auto-approve."""
        orch = ETLPipelineOrchestrator(
            db_path=str(tmp_path / "wh.db"),
            drift_store_path=str(tmp_path / "fp.json"),
        )
        orch.mapper.map_schema = MagicMock(return_value=_mock_mapping())
        orch.cleaner.generate_cleaning_plan = MagicMock(return_value=_mock_plan())
        orch.code_gen.run_with_self_correction = MagicMock(return_value=_mock_code())

        result = await orch.run_pipeline(csv_path, "csv", auto_approve=True)

        assert result.rows_ingested == 2
        assert result.mapping_confidence == 0.9
        assert result.requires_human_review is False
        assert len(result.errors) == 0
        assert len(result.tables_created) >= 1

    @pytest.mark.asyncio
    async def test_pipeline_hitl_escalation(self, csv_path, tmp_path):
        """Pipeline pauses when confidence is low."""
        orch = ETLPipelineOrchestrator(
            db_path=str(tmp_path / "wh.db"),
            drift_store_path=str(tmp_path / "fp.json"),
        )
        low_conf_mapping = _mock_mapping()
        low_conf_mapping.confidence = 0.5
        orch.mapper.map_schema = MagicMock(return_value=low_conf_mapping)
        orch.cleaner.generate_cleaning_plan = MagicMock(return_value=_mock_plan())

        result = await orch.run_pipeline(csv_path, "csv", auto_approve=False)

        assert result.requires_human_review is True
        assert result.review_job_id is not None
        assert result.tables_created == []
