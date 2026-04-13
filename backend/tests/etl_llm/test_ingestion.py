"""Tests for multi-source ingestion (Layer 0).

Reference: [El-Sappagh et al. 2011] — source area extraction validation.
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

import pandas as pd
import pytest

from services.etl_llm.profiling.ingestion import MultiSourceIngester

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def ingester() -> MultiSourceIngester:
    return MultiSourceIngester()


# ── CSV tests ────────────────────────────────────────────────────
class TestLoadCSV:
    def test_load_csv_comma_delimiter(self, ingester: MultiSourceIngester):
        df = ingester.load_csv(str(FIXTURES / "sample.csv"))
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "product" in df.columns

    def test_load_csv_semicolon_delimiter(self, ingester: MultiSourceIngester, tmp_path: Path):
        # Create a semicolon-separated file
        out = tmp_path / "semi.csv"
        out.write_text(
            "date;product;quantity;price;region\n"
            "2024-01-01;Widget A;10;19.99;North\n"
            "2024-01-02;Widget B;5;29.99;South\n"
        )
        df = ingester.load_csv(str(out))
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "product" in df.columns


# ── Excel tests ──────────────────────────────────────────────────
class TestLoadExcel:
    def test_load_excel_multiple_sheets(self, ingester: MultiSourceIngester):
        result = ingester.load_excel(str(FIXTURES / "sample.xlsx"))
        assert isinstance(result, dict)
        assert "Sales" in result
        assert "Returns" in result
        assert isinstance(result["Sales"], pd.DataFrame)
        assert len(result["Sales"]) > 0
        assert len(result["Returns"]) > 0


# ── JSON tests ───────────────────────────────────────────────────
class TestLoadJSON:
    def test_load_json_nested(self, ingester: MultiSourceIngester):
        df = ingester.load_json(str(FIXTURES / "sample.json"))
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        # Nested fields should be flattened
        assert any("customer" in col for col in df.columns)


# ── XML tests ────────────────────────────────────────────────────
class TestLoadXML:
    def test_load_xml(self, ingester: MultiSourceIngester):
        df = ingester.load_xml(str(FIXTURES / "sample.xml"))
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "name" in df.columns


# ── Router tests ─────────────────────────────────────────────────
class TestIngestRouter:
    def test_ingest_router_csv(self, ingester: MultiSourceIngester):
        result = ingester.ingest({"type": "csv", "path": str(FIXTURES / "sample.csv")})
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_ingest_router_excel(self, ingester: MultiSourceIngester):
        result = ingester.ingest({"type": "excel", "path": str(FIXTURES / "sample.xlsx")})
        assert isinstance(result, dict)
        assert "Sales" in result
