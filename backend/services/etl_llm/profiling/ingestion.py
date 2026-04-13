"""
Multi-Source Data Ingestion Module
===================================
Implements Layer 0 of the LLM-powered ETL pipeline.

Supports: CSV, Excel, JSON, SQL, REST API, PDF, XML

References:
  [El-Sappagh et al. 2011] Source area of the ETL conceptual model —
    data is extracted from heterogeneous operational sources.
  [Park et al. 2024] Dataverse — open-source ETL supporting diverse data formats
    for LLM training pipelines (NAACL 2025).
  [Annam 2025] Multi-source ingestion as pre-processing for LLM-driven transformation.
"""

from __future__ import annotations

import io
import json
import logging
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from lxml import etree

logger = logging.getLogger(__name__)


class MultiSourceIngester:
    """Router-based ingestion for heterogeneous data sources.

    Each loader normalises its source into a ``pd.DataFrame`` so that
    downstream profiling (Layer 1) receives a uniform tabular interface.

    Reference: [El-Sappagh et al. 2011] — the *source area* reads data from
    operational systems regardless of format.
    """

    # ------------------------------------------------------------------
    # CSV
    # ------------------------------------------------------------------
    def load_csv(self, path: str) -> pd.DataFrame:
        """Load a CSV file with automatic delimiter detection.

        Tries comma, semicolon, and tab — picks the delimiter that produces
        the most columns.

        Reference: [Park et al. 2024] — Dataverse ingests CSV as a primary
        format for LLM training data pipelines.
        """
        path = str(path)
        best_df: pd.DataFrame | None = None
        best_cols = 0

        for sep in [",", ";", "\t"]:
            try:
                df = pd.read_csv(path, sep=sep, nrows=5)
                if len(df.columns) > best_cols:
                    best_cols = len(df.columns)
                    best_sep = sep
            except Exception:
                continue

        if best_cols == 0:
            raise ValueError(f"Could not parse CSV file: {path}")

        return pd.read_csv(path, sep=best_sep)

    # ------------------------------------------------------------------
    # Excel
    # ------------------------------------------------------------------
    def load_excel(self, path: str) -> dict[str, pd.DataFrame]:
        """Load all sheets from an Excel workbook using openpyxl.

        Returns a dict mapping sheet names to DataFrames.

        Reference: [Annam 2025] — enterprise data commonly resides in
        spreadsheets requiring multi-sheet extraction.
        """
        return pd.read_excel(path, sheet_name=None, engine="openpyxl")

    # ------------------------------------------------------------------
    # JSON
    # ------------------------------------------------------------------
    def load_json(self, path: str) -> pd.DataFrame:
        """Load a JSON file, flattening nested structures up to depth 3.

        Uses ``pandas.json_normalize`` for nested objects/arrays.

        Reference: [Park et al. 2024] — JSON normalisation is a standard
        step in heterogeneous ETL pipelines.
        """
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            return pd.json_normalize(data, max_level=3)
        elif isinstance(data, dict):
            # Find the first list value to normalise
            for key, value in data.items():
                if isinstance(value, list):
                    return pd.json_normalize(value, max_level=3)
            # Flat dict — wrap in list
            return pd.json_normalize([data], max_level=3)
        else:
            raise ValueError("JSON root must be a list or dict")

    # ------------------------------------------------------------------
    # SQL
    # ------------------------------------------------------------------
    def load_sql(self, connection_url: str, query: str) -> pd.DataFrame:
        """Load data from SQL database via SQLAlchemy.

        Reference: [El-Sappagh et al. 2011] — relational databases are the
        primary operational source in enterprise ETL.
        """
        from sqlalchemy import create_engine

        engine = create_engine(connection_url)
        return pd.read_sql(query, engine)

    # ------------------------------------------------------------------
    # REST API
    # ------------------------------------------------------------------
    def load_rest_api(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """Fetch data from a REST API and flatten the JSON response.

        Reference: [Annam 2025] — API ingestion enables real-time ETL from
        cloud-based operational systems.
        """
        response = httpx.get(url, headers=headers or {}, params=params or {}, timeout=30)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, list):
            return pd.json_normalize(data, max_level=3)
        elif isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, list):
                    return pd.json_normalize(value, max_level=3)
            return pd.json_normalize([data], max_level=3)
        else:
            raise ValueError("REST API response must be a JSON list or dict")

    # ------------------------------------------------------------------
    # PDF
    # ------------------------------------------------------------------
    def load_pdf(self, path: str) -> pd.DataFrame:
        """Extract tables from a PDF using PyMuPDF (fitz).

        Falls back to raw text extraction (one row per line) when no
        structured tables are detected.

        Reference: [Park et al. 2024] — PDF parsing for unstructured
        document ETL in the Dataverse pipeline.
        """
        import fitz  # PyMuPDF

        doc = fitz.open(path)
        all_tables: list[pd.DataFrame] = []

        for page in doc:
            tables = page.find_tables()
            for table in tables:
                df = table.to_pandas()
                if len(df) > 0:
                    all_tables.append(df)

        if all_tables:
            return pd.concat(all_tables, ignore_index=True)

        # Fallback: text extraction
        lines: list[str] = []
        for page in doc:
            text = page.get_text()
            lines.extend([line.strip() for line in text.split("\n") if line.strip()])

        if not lines:
            raise ValueError(f"No content found in PDF: {path}")

        return pd.DataFrame({"text": lines})

    # ------------------------------------------------------------------
    # XML
    # ------------------------------------------------------------------
    def load_xml(self, path: str) -> pd.DataFrame:
        """Parse an XML file and convert to DataFrame.

        Auto-detects the most repeated element tag as the record tag.

        Reference: [El-Sappagh et al. 2011] — XML is a common exchange
        format in enterprise source systems.
        """
        tree = etree.parse(path)
        root = tree.getroot()

        # Count element occurrences to find the record tag
        tag_counts: dict[str, int] = {}
        for elem in root.iter():
            tag = etree.QName(elem.tag).localname if "}" in elem.tag else elem.tag
            tag_counts[tag] = tag_counts.get(tag, 0) + 1

        # Remove root tag from candidates
        root_tag = etree.QName(root.tag).localname if "}" in root.tag else root.tag
        tag_counts.pop(root_tag, None)

        if not tag_counts:
            raise ValueError(f"No repeated elements found in XML: {path}")

        record_tag = max(tag_counts, key=tag_counts.get)

        records: list[dict[str, str]] = []
        for elem in root.iter():
            tag = etree.QName(elem.tag).localname if "}" in elem.tag else elem.tag
            if tag == record_tag:
                record: dict[str, str] = dict(elem.attrib)
                for child in elem:
                    child_tag = (
                        etree.QName(child.tag).localname
                        if "}" in child.tag
                        else child.tag
                    )
                    record[child_tag] = child.text or ""
                records.append(record)

        if not records:
            raise ValueError(f"No records found for tag '{record_tag}' in XML: {path}")

        return pd.DataFrame(records)

    # ------------------------------------------------------------------
    # Router
    # ------------------------------------------------------------------
    def ingest(self, source_config: dict[str, Any]) -> pd.DataFrame | dict[str, pd.DataFrame]:
        """Route ingestion based on ``source_config["type"]``.

        Args:
            source_config: Dict with ``type`` key and source-specific params.
                Examples::

                    {"type": "csv", "path": "/data/sales.csv"}
                    {"type": "excel", "path": "/data/sales.xlsx"}
                    {"type": "json", "path": "/data/orders.json"}
                    {"type": "sql", "connection_url": "...", "query": "SELECT ..."}
                    {"type": "rest", "url": "https://api.example.com/data"}
                    {"type": "pdf", "path": "/data/report.pdf"}
                    {"type": "xml", "path": "/data/catalog.xml"}

        Reference: [El-Sappagh et al. 2011] — the source area must handle
        multiple heterogeneous operational systems.
        """
        source_type = source_config.get("type", "").lower()
        logger.info(f"Ingesting source type: {source_type}")

        if source_type == "csv":
            return self.load_csv(source_config["path"])
        elif source_type == "excel":
            return self.load_excel(source_config["path"])
        elif source_type == "json":
            return self.load_json(source_config["path"])
        elif source_type == "sql":
            return self.load_sql(source_config["connection_url"], source_config["query"])
        elif source_type == "rest":
            return self.load_rest_api(
                source_config["url"],
                source_config.get("headers"),
                source_config.get("params"),
            )
        elif source_type == "pdf":
            return self.load_pdf(source_config["path"])
        elif source_type == "xml":
            return self.load_xml(source_config["path"])
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
