"""
Multi-source data ingestion module.
Supports CSV, JSON (flat and nested), and XML formats.
"""
import csv
import json
import os
from typing import Any

import pandas as pd
from lxml import etree


class MultiSourceIngester:
    """Ingest datasets from CSV, JSON, and XML into pandas DataFrames."""

    SUPPORTED = {".csv", ".json", ".xml"}

    def __init__(self, data_dir: str = None):
        if data_dir is None:
            data_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "data", "raw"
            )
        self.data_dir = data_dir

    # ── public API ─────────────────────────────────────────
    def ingest(self, filename: str) -> pd.DataFrame:
        """Load a single file by name and return a DataFrame."""
        path = os.path.join(self.data_dir, filename)
        ext = os.path.splitext(filename)[1].lower()
        if ext == ".csv":
            return self._read_csv(path)
        elif ext == ".json":
            return self._read_json(path)
        elif ext == ".xml":
            return self._read_xml(path)
        else:
            raise ValueError(f"Unsupported format: {ext}")

    def ingest_all(self) -> dict[str, pd.DataFrame]:
        """Load every supported file in data_dir."""
        result = {}
        for fname in sorted(os.listdir(self.data_dir)):
            ext = os.path.splitext(fname)[1].lower()
            if ext in self.SUPPORTED:
                result[fname] = self.ingest(fname)
        return result

    # ── private readers ────────────────────────────────────
    @staticmethod
    def _read_csv(path: str) -> pd.DataFrame:
        return pd.read_csv(path, encoding="utf-8")

    @staticmethod
    def _read_json(path: str) -> pd.DataFrame:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return pd.json_normalize(data, sep=".")

    @staticmethod
    def _read_xml(path: str) -> pd.DataFrame:
        tree = etree.parse(path)
        root = tree.getroot()
        records = []
        for invoice in root.findall("invoice"):
            rec = {}
            for child in invoice:
                if len(child) == 0:
                    rec[child.tag] = child.text
                else:
                    # nested element
                    for sub in child:
                        if len(sub) == 0:
                            rec[f"{child.tag}.{sub.tag}"] = sub.text
                        else:
                            # line_items: flatten first item summary
                            items = []
                            for item in sub:
                                item_d = {}
                                for field in item:
                                    item_d[field.tag] = field.text
                                items.append(item_d)
                            rec[f"{child.tag}.{sub.tag}"] = items
            # Flatten line_items to count + first item description
            if "line_items.item" in rec and isinstance(rec["line_items.item"], list):
                rec["line_items_count"] = len(rec["line_items.item"])
                rec.pop("line_items.item")
            records.append(rec)
        return pd.DataFrame(records)
