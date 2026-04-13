"""Tests for FAISS schema vector store (RAG layer)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from services.etl_llm.profiling.schema_profiler import SchemaContext, SchemaProfiler
from services.etl_llm.rag.schema_store import SchemaVectorStore


@pytest.fixture
def sample_schema() -> SchemaContext:
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=10, freq="D"),
            "product": ["A", "B", "C", "A", "B", "C", "A", "B", "C", "A"],
            "quantity": [10, 5, 8, 12, 3, 15, 7, 9, 11, 6],
            "price": [19.99, 29.99, 9.99, 19.99, 29.99, 9.99, 19.99, 29.99, 9.99, 19.99],
            "region": ["N", "S", "E", "W", "N", "S", "E", "W", "N", "S"],
        }
    )
    return SchemaProfiler().profile(df, "test_sales")


@pytest.fixture
def vector_store(tmp_path: Path) -> SchemaVectorStore:
    return SchemaVectorStore(index_path=str(tmp_path / "test.index"))


class TestSchemaVectorStore:
    def test_embed_schema_returns_vector(
        self, vector_store: SchemaVectorStore, sample_schema: SchemaContext
    ):
        vec = vector_store.embed_schema(sample_schema)
        assert isinstance(vec, np.ndarray)
        assert vec.shape == (384,)

    def test_add_and_retrieve_schema(
        self, vector_store: SchemaVectorStore, sample_schema: SchemaContext
    ):
        mapping = {"fact_table": "sales_fact", "dimensions": ["product_dim"]}
        vector_store.add_schema(sample_schema, mapping, approved_by_human=True)

        results = vector_store.retrieve_similar(sample_schema, k=1)
        assert len(results) == 1
        assert results[0]["mapping"] == mapping
        assert results[0]["approved_by_human"] is True

    def test_retrieve_filters_human_approved(
        self, vector_store: SchemaVectorStore, sample_schema: SchemaContext
    ):
        vector_store.add_schema(
            sample_schema, {"auto": True}, approved_by_human=False
        )
        vector_store.add_schema(
            sample_schema, {"human": True}, approved_by_human=True
        )

        results = vector_store.retrieve_similar(sample_schema, k=3)
        # Should prefer approved
        assert all(r["approved_by_human"] for r in results)

    def test_few_shot_prompt_format(
        self, vector_store: SchemaVectorStore, sample_schema: SchemaContext
    ):
        similar = [
            {
                "schema_text": "date:datetime64 | product:object",
                "mapping": {"fact": "sales_fact"},
                "approved_by_human": True,
                "similarity_score": 0.95,
            }
        ]
        prompt = vector_store.build_few_shot_prompt(similar)
        assert "Example 1 (human-approved):" in prompt
        assert "sales_fact" in prompt
