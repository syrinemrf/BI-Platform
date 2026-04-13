"""Shared fixtures for ETL LLM tests."""

from __future__ import annotations

import os
import tempfile

import pandas as pd
import pytest

from services.etl_llm.agents.cleaning_agent import CleaningPlan, CleaningRule
from services.etl_llm.agents.code_generator import GeneratedETLCode
from services.etl_llm.agents.schema_mapper import (
    DimensionTableSpec,
    FactTableSpec,
    SchemaMappingResult,
)


@pytest.fixture
def sample_df():
    """A standard 4-column sales dataset."""
    return pd.DataFrame({
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "product": ["Widget", "Gadget", "Widget"],
        "quantity": [10, 20, 15],
        "price": [1.5, 2.5, 1.5],
    })


@pytest.fixture
def sample_csv(tmp_path, sample_df):
    """Write sample_df to a temp CSV and return the path."""
    p = tmp_path / "sample.csv"
    sample_df.to_csv(p, index=False)
    return str(p)


@pytest.fixture
def mock_mapping():
    return SchemaMappingResult(
        fact_table=FactTableSpec(
            name="fact_sales",
            measures=["quantity", "price"],
            foreign_keys=["product"],
        ),
        dimension_tables=[
            DimensionTableSpec(
                name="dim_product",
                source_columns=["product"],
                surrogate_key="product_sk",
            ),
        ],
        confidence=0.9,
        model_used="llama3:8b",
        reasoning="test fixture",
    )


@pytest.fixture
def mock_cleaning_plan():
    return CleaningPlan(
        rules=[
            CleaningRule(
                column="quantity",
                rule_type="fill_null",
                params={"strategy": "median"},
                priority=2,
                justification="fill missing quantities",
            )
        ],
        estimated_quality_improvement=0.05,
        confidence=0.92,
        model_used="llama3:8b",
    )


@pytest.fixture
def mock_generated_code():
    return GeneratedETLCode(
        loading_code="CREATE TABLE IF NOT EXISTS fact_sales (quantity INTEGER, price REAL);",
        correction_attempts=0,
    )
