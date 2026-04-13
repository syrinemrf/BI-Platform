"""
Full ETL Pipeline Orchestrator (Layers 1→5)
=============================================
Chains all 5 layers: Ingest → Profile → LLM Agents → HITL → Load.

Reference: [Annam 2025] — "An end-to-end LLM-driven ETL pipeline that
automates schema mapping, data cleaning, and star schema generation."
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from services.etl_llm.agents.cleaning_agent import CleaningRulesAgent
from services.etl_llm.agents.code_generator import ETLCodeGeneratorAgent
from services.etl_llm.agents.schema_mapper import SchemaMappingAgent
from services.etl_llm.lineage.lineage_tracker import (
    DataLineageTracker,
    LineageNode,
)
from services.etl_llm.loader.star_schema_loader import StarSchemaLoader
from services.etl_llm.profiling.drift_detector import SchemaDriftDetector
from services.etl_llm.profiling.ingestion import MultiSourceIngester
from services.etl_llm.profiling.schema_profiler import SchemaProfiler
from services.etl_llm.rag.schema_store import SchemaVectorStore
from services.etl_llm.validation.hitl_validator import HITLValidator

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Combined outcome of a full pipeline run."""

    pipeline_id: str = ""
    source_name: str = ""
    rows_ingested: int = 0
    schema_fingerprint: str = ""
    mapping_confidence: float = 0.0
    cleaning_confidence: float = 0.0
    requires_human_review: bool = False
    review_job_id: str | None = None
    tables_created: list[str] = field(default_factory=list)
    rows_loaded: int = 0
    lineage_markdown: str = ""
    errors: list[str] = field(default_factory=list)


class ETLPipelineOrchestrator:
    """Orchestrate the complete 5-layer LLM ETL pipeline.

    Layer 1: Multi-source ingestion
    Layer 2: Schema profiling + drift detection
    Layer 3: LLM agents (mapping, cleaning, code generation)
    Layer 4: HITL validation
    Layer 5: Star schema loading + lineage tracking
    """

    def __init__(
        self,
        db_path: str = "warehouse.db",
        drift_store_path: str = "schema_fingerprints.json",
    ) -> None:
        self.ingester = MultiSourceIngester()
        self.profiler = SchemaProfiler()
        self.drift_detector = SchemaDriftDetector(store_path=drift_store_path)
        self.schema_store = SchemaVectorStore()
        self.mapper = SchemaMappingAgent(vector_store=self.schema_store)
        self.cleaner = CleaningRulesAgent()
        self.code_gen = ETLCodeGeneratorAgent()
        self.validator = HITLValidator()
        self.loader = StarSchemaLoader(db_path=db_path)
        self.lineage = DataLineageTracker()

    async def run_pipeline(
        self,
        source_path: str,
        source_type: str = "csv",
        auto_approve: bool = False,
    ) -> PipelineResult:
        """Execute layers 1→5 end-to-end.

        If auto_approve=False and HITL escalates, the pipeline pauses
        at layer 4 and returns a review_job_id for human interaction.
        """
        result = PipelineResult(source_name=source_path)
        pid = self.lineage.start_pipeline(source_path)
        result.pipeline_id = pid

        # ── Layer 1: Ingest ──────────────────────────────────────
        try:
            source_config = {"type": source_type, "path": source_path}
            df = self.ingester.ingest(source_config)
            result.rows_ingested = len(df)
            self.lineage.add_node(
                pid,
                LineageNode(node_type="source", rows_in=0, rows_out=len(df)),
            )
        except Exception as e:
            result.errors.append(f"Ingestion failed: {e}")
            return result

        # ── Layer 2: Profile + drift ─────────────────────────────
        try:
            context = self.profiler.profile(df, source_name=source_path)
            result.schema_fingerprint = context.schema_fingerprint
            drift = self.drift_detector.check_drift(context)
            self.lineage.add_node(
                pid,
                LineageNode(
                    node_type="profile",
                    rows_in=len(df),
                    rows_out=len(df),
                    input_schema_fingerprint=context.schema_fingerprint,
                ),
            )
        except Exception as e:
            result.errors.append(f"Profiling failed: {e}")
            return result

        # ── Layer 3a: Schema mapping ─────────────────────────────
        try:
            mapping = self.mapper.map_schema(context)
            result.mapping_confidence = mapping.confidence
            self.lineage.add_node(
                pid,
                LineageNode(
                    node_type="mapping",
                    rows_in=len(df),
                    rows_out=len(df),
                    model_used=mapping.model_used,
                    confidence=mapping.confidence,
                ),
            )
        except Exception as e:
            result.errors.append(f"Mapping failed: {e}")
            return result

        # ── Layer 3b: Cleaning rules ─────────────────────────────
        try:
            plan = self.cleaner.generate_cleaning_plan(context)
            df, _cleaning_report = self.cleaner.apply_cleaning_plan(df, plan)
            result.cleaning_confidence = plan.confidence
            self.lineage.add_node(
                pid,
                LineageNode(
                    node_type="cleaning",
                    rows_in=result.rows_ingested,
                    rows_out=len(df),
                    model_used=plan.model_used,
                    confidence=plan.confidence,
                ),
            )
        except Exception as e:
            result.errors.append(f"Cleaning failed: {e}")
            return result

        # ── Layer 4: HITL ────────────────────────────────────────
        assessment = self.validator.assess_confidence(
            mapping,
            plan,
            drift_report=drift,
            column_count=len(context.columns),
        )
        result.requires_human_review = assessment.requires_human_review

        if assessment.requires_human_review and not auto_approve:
            job_id = self.validator.enqueue_review(mapping, plan, assessment)
            result.review_job_id = job_id
            result.lineage_markdown = self.lineage.export_lineage_markdown(pid)
            return result

        # ── Layer 3c: Code generation ────────────────────────────
        try:
            code = self.code_gen.run_with_self_correction(context, mapping)
        except Exception as e:
            result.errors.append(f"Code generation failed: {e}")
            return result

        # ── Layer 5: Load ────────────────────────────────────────
        try:
            tables = self.loader.execute_ddl(code)
            result.tables_created = tables
            if tables:
                count = self.loader.load_dataframe(df, tables[0])
                result.rows_loaded = count
            self.lineage.add_node(
                pid,
                LineageNode(
                    node_type="loading",
                    rows_in=len(df),
                    rows_out=result.rows_loaded,
                ),
            )
        except Exception as e:
            result.errors.append(f"Loading failed: {e}")

        # Store approved mapping in FAISS for adaptive few-shot
        self.schema_store.add_schema(context, mapping.model_dump(), approved_by_human=auto_approve)

        result.lineage_markdown = self.lineage.export_lineage_markdown(pid)
        return result
