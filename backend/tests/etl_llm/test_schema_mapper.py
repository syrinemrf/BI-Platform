"""Tests for the Schema Mapping Agent (Agent 1)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from services.etl_llm.agents.schema_mapper import (
    SchemaMappingAgent,
    SchemaMappingResult,
)
from services.etl_llm.profiling.schema_profiler import SchemaContext, SchemaProfiler
from services.etl_llm.rag.schema_store import SchemaVectorStore


@pytest.fixture
def sample_schema() -> SchemaContext:
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=10, freq="D"),
            "product": ["A"] * 10,
            "quantity": list(range(10)),
            "price": [9.99] * 10,
            "region": ["N"] * 10,
        }
    )
    return SchemaProfiler().profile(df, "test_sales")


@pytest.fixture
def mock_vector_store(tmp_path: Path) -> SchemaVectorStore:
    store = MagicMock(spec=SchemaVectorStore)
    store.retrieve_similar.return_value = [
        {
            "schema_text": "date:datetime64 | product:object",
            "mapping": {"fact_table": {"name": "sales_fact"}},
            "approved_by_human": True,
            "similarity_score": 0.9,
        }
    ]
    store.build_few_shot_prompt.return_value = "Example 1 (human-approved): ..."
    return store


def _ollama_response(confidence: float = 0.9) -> dict:
    return {
        "fact_table": {
            "name": "sales_fact",
            "measures": ["quantity", "price"],
            "foreign_keys": ["product", "region"],
        },
        "dimension_tables": [
            {"name": "product_dim", "source_columns": ["product"], "surrogate_key": "product_sk"},
            {"name": "region_dim", "source_columns": ["region"], "surrogate_key": "region_sk"},
        ],
        "confidence": confidence,
        "reasoning": "Numeric cols are measures, categorical cols are dimensions.",
    }


class TestSchemaMappingAgent:
    def test_build_prompt_contains_schema_context(
        self, mock_vector_store: SchemaVectorStore, sample_schema: SchemaContext
    ):
        agent = SchemaMappingAgent(mock_vector_store)
        prompt = agent.build_prompt(sample_schema, "")
        assert "quantity" in prompt
        assert "price" in prompt
        assert "star schema" in prompt.lower()

    def test_build_prompt_contains_few_shot(
        self, mock_vector_store: SchemaVectorStore, sample_schema: SchemaContext
    ):
        agent = SchemaMappingAgent(mock_vector_store)
        prompt = agent.build_prompt(sample_schema, "Example 1 (human-approved): ...")
        assert "human-approved" in prompt

    @patch("services.etl_llm.agents.schema_mapper.httpx.post")
    def test_routing_uses_llama_for_high_confidence(
        self, mock_post, mock_vector_store, sample_schema
    ):
        resp_data = _ollama_response(confidence=0.9)
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"response": json.dumps(resp_data)}
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        agent = SchemaMappingAgent(mock_vector_store, anthropic_key="test")
        result = agent.map_schema(sample_schema)

        assert isinstance(result, SchemaMappingResult)
        assert result.model_used == "llama3:8b"
        assert result.confidence == 0.9

    @patch("services.etl_llm.agents.schema_mapper.httpx.post")
    def test_routing_falls_back_to_claude(
        self, mock_post, mock_vector_store, sample_schema
    ):
        # LLaMA returns low confidence
        resp_data = _ollama_response(confidence=0.3)
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"response": json.dumps(resp_data)}
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        claude_resp = _ollama_response(confidence=0.95)

        agent = SchemaMappingAgent(mock_vector_store, anthropic_key="test")

        with patch.object(agent, "call_claude", return_value=(claude_resp, 0.95)):
            result = agent.map_schema(sample_schema)

        assert result.model_used == "claude-3-5-sonnet"
        assert result.confidence == 0.95

    @patch("services.etl_llm.agents.schema_mapper.httpx.post")
    def test_output_is_valid_pydantic_model(
        self, mock_post, mock_vector_store, sample_schema
    ):
        resp_data = _ollama_response(confidence=0.85)
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"response": json.dumps(resp_data)}
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        agent = SchemaMappingAgent(mock_vector_store)
        result = agent.map_schema(sample_schema)

        assert isinstance(result, SchemaMappingResult)
        assert result.fact_table.name == "sales_fact"
        assert len(result.dimension_tables) == 2
        assert 0 <= result.confidence <= 1
