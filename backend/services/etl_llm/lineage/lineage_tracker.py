"""
Data Lineage Tracker (Layer 4)
===============================
Records every transformation step in a lineage graph for full
auditability and auto-documentation.

Reference: [Annam 2025] — "LLMs can generate data dictionaries, pipeline
documentation, and lineage graphs automatically as transformations are written."
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class LineageNode(BaseModel):
    """A single node in the data lineage graph."""

    node_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    node_type: Literal["source", "profile", "mapping", "cleaning", "loading"]
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    input_schema_fingerprint: str = ""
    output_schema_fingerprint: str = ""
    model_used: str | None = None
    confidence: float | None = None
    rows_in: int = 0
    rows_out: int = 0
    metadata: dict = Field(default_factory=dict)


class LineageGraph(BaseModel):
    """Complete lineage for one pipeline run."""

    pipeline_id: str
    nodes: list[LineageNode] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class DataLineageTracker:
    """Track data lineage for every pipeline execution.

    Reference: [Annam 2025] — automatic lineage graph generation.
    """

    def __init__(self) -> None:
        self._graphs: dict[str, LineageGraph] = {}

    def start_pipeline(self, source_name: str) -> str:
        """Start a new pipeline and return the pipeline_id."""
        pid = str(uuid.uuid4())
        self._graphs[pid] = LineageGraph(pipeline_id=pid)
        return pid

    def add_node(self, pipeline_id: str, node: LineageNode) -> None:
        """Append a lineage node to the pipeline graph."""
        graph = self._graphs.get(pipeline_id)
        if graph:
            graph.nodes.append(node)

    def get_lineage(self, pipeline_id: str) -> LineageGraph | None:
        return self._graphs.get(pipeline_id)

    def export_lineage_markdown(self, pipeline_id: str) -> str:
        """Export lineage as a Markdown timeline for auto-documentation."""
        graph = self._graphs.get(pipeline_id)
        if not graph:
            return ""

        lines = [f"# Data Lineage — Pipeline {pipeline_id[:8]}", ""]
        for i, node in enumerate(graph.nodes, 1):
            lines.append(f"## Step {i}: {node.node_type.upper()}")
            lines.append(f"- **Timestamp**: {node.timestamp.isoformat()}")
            lines.append(f"- **Rows in**: {node.rows_in} → **Rows out**: {node.rows_out}")
            if node.model_used:
                lines.append(f"- **Model**: {node.model_used}")
            if node.confidence is not None:
                lines.append(f"- **Confidence**: {node.confidence:.2f}")
            lines.append("")
        return "\n".join(lines)

    def export_lineage_json(self, pipeline_id: str) -> dict:
        """Export lineage as a JSON dict for data catalog integration."""
        graph = self._graphs.get(pipeline_id)
        if not graph:
            return {}
        return graph.model_dump(mode="json")
