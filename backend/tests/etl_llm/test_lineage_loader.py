"""Tests for lineage tracker and star schema loader (Layers 4-5)."""

from __future__ import annotations

import os
import sqlite3
import tempfile

import pandas as pd
import pytest

from services.etl_llm.agents.code_generator import GeneratedETLCode
from services.etl_llm.lineage.lineage_tracker import (
    DataLineageTracker,
    LineageNode,
)
from services.etl_llm.loader.star_schema_loader import StarSchemaLoader


class TestDataLineageTracker:
    def test_start_pipeline_returns_id(self):
        tracker = DataLineageTracker()
        pid = tracker.start_pipeline("sales.csv")
        assert pid is not None
        assert len(pid) > 10

    def test_add_and_get_nodes(self):
        tracker = DataLineageTracker()
        pid = tracker.start_pipeline("test")
        tracker.add_node(pid, LineageNode(node_type="source", rows_in=0, rows_out=100))
        tracker.add_node(pid, LineageNode(node_type="profile", rows_in=100, rows_out=100))

        graph = tracker.get_lineage(pid)
        assert graph is not None
        assert len(graph.nodes) == 2
        assert graph.nodes[0].node_type == "source"
        assert graph.nodes[1].node_type == "profile"

    def test_export_markdown(self):
        tracker = DataLineageTracker()
        pid = tracker.start_pipeline("test")
        tracker.add_node(pid, LineageNode(node_type="source", rows_in=0, rows_out=50, model_used="llama3:8b", confidence=0.9))
        md = tracker.export_lineage_markdown(pid)
        assert "Step 1" in md
        assert "SOURCE" in md
        assert "llama3:8b" in md


class TestStarSchemaLoader:
    def test_execute_ddl_creates_tables(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        loader = StarSchemaLoader(db_path=db_path)
        code = GeneratedETLCode(
            loading_code="CREATE TABLE IF NOT EXISTS fact_sales (id INTEGER, amount REAL); CREATE TABLE IF NOT EXISTS dim_product (id INTEGER, name TEXT);",
            correction_attempts=0,
        )
        tables = loader.execute_ddl(code)
        assert len(tables) >= 2

    def test_load_dataframe(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        loader = StarSchemaLoader(db_path=db_path)
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        count = loader.load_dataframe(df, "test_table")
        assert count == 3

    def test_query(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        loader = StarSchemaLoader(db_path=db_path)
        df = pd.DataFrame({"val": [10, 20, 30]})
        loader.load_dataframe(df, "numbers")
        result = loader.query("SELECT SUM(val) as total FROM numbers")
        assert result[0]["total"] == 60
