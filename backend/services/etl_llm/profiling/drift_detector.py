"""
Schema Drift Detection
=======================
Persists schema fingerprints across pipeline runs and detects when
the schema of a recurring source changes (columns added/removed).

Innovation #3 — Schema Fingerprinting:
  If the SHA-256 fingerprint of column semantics changes between runs,
  the system raises a drift alert and can trigger HITL escalation.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from services.etl_llm.profiling.schema_profiler import SchemaContext

logger = logging.getLogger(__name__)


class DriftReport(BaseModel):
    """Result of a schema drift check."""

    source_name: str
    is_new: bool = False
    is_drifted: bool = False
    previous_fingerprint: str | None = None
    current_fingerprint: str = ""
    columns_added: list[str] = Field(default_factory=list)
    columns_removed: list[str] = Field(default_factory=list)


class SchemaDriftDetector:
    """Detect schema drift by comparing fingerprints across pipeline runs.

    Fingerprints and column lists are persisted to a JSON file so they
    survive process restarts.
    """

    def __init__(self, store_path: str = "schema_fingerprints.json") -> None:
        self._store_path = Path(store_path)
        self._store: dict[str, Any] = {}
        if self._store_path.exists():
            try:
                self._store = json.loads(self._store_path.read_text(encoding="utf-8"))
            except Exception:
                logger.warning("Could not load fingerprint store — starting fresh")
                self._store = {}

    def _save(self) -> None:
        self._store_path.write_text(json.dumps(self._store, indent=2), encoding="utf-8")

    def check_drift(self, schema: SchemaContext) -> DriftReport:
        """Compare *schema* against the last-seen fingerprint for the same source.

        Returns a :class:`DriftReport` indicating whether the schema is new,
        unchanged, or drifted.
        """
        current_cols = {c.name for c in schema.columns}
        current_fp = schema.schema_fingerprint

        previous = self._store.get(schema.source_name)

        if previous is None:
            # First time seeing this source
            self._store[schema.source_name] = {
                "fingerprint": current_fp,
                "columns": sorted(current_cols),
            }
            self._save()
            return DriftReport(
                source_name=schema.source_name,
                is_new=True,
                current_fingerprint=current_fp,
            )

        prev_fp = previous["fingerprint"]
        prev_cols = set(previous.get("columns", []))

        if current_fp == prev_fp:
            return DriftReport(
                source_name=schema.source_name,
                is_new=False,
                is_drifted=False,
                previous_fingerprint=prev_fp,
                current_fingerprint=current_fp,
            )

        # Drift detected
        added = sorted(current_cols - prev_cols)
        removed = sorted(prev_cols - current_cols)

        # Update stored fingerprint
        self._store[schema.source_name] = {
            "fingerprint": current_fp,
            "columns": sorted(current_cols),
        }
        self._save()

        logger.warning(
            f"Schema drift detected for '{schema.source_name}': "
            f"added={added}, removed={removed}"
        )

        return DriftReport(
            source_name=schema.source_name,
            is_new=False,
            is_drifted=True,
            previous_fingerprint=prev_fp,
            current_fingerprint=current_fp,
            columns_added=added,
            columns_removed=removed,
        )
