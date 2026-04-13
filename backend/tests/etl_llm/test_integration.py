"""
Integration tests for the full LLM ETL pipeline.

These tests verify that components wire together correctly
(with LLM calls mocked).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from services.etl_llm.orchestrator.pipeline_orchestrator import ETLPipelineOrchestrator
from services.etl_llm.profiling.ingestion import MultiSourceIngester
from services.etl_llm.profiling.schema_profiler import SchemaProfiler
from services.etl_llm.profiling.drift_detector import SchemaDriftDetector
from services.etl_llm.validation.hitl_validator import HITLValidator
from services.etl_llm.validation.quality_gates import DataQualityGates
from services.etl_llm.lineage.lineage_tracker import DataLineageTracker
from services.etl_llm.loader.star_schema_loader import StarSchemaLoader
from services.etl_llm.evaluation.benchmark import ETLBenchmark


class TestIngestionToProfile:
    """Layer 1 → Layer 2 integration."""

    def test_csv_ingest_then_profile(self, sample_csv):
        ingester = MultiSourceIngester()
        df = ingester.ingest({"type": "csv", "path": sample_csv})
        assert len(df) == 3

        profiler = SchemaProfiler()
        ctx = profiler.profile(df, source_name="test")
        assert ctx.row_count == 3
        assert ctx.column_count == 4
        assert ctx.schema_fingerprint != ""

    def test_profile_then_drift(self, sample_csv, tmp_path):
        ingester = MultiSourceIngester()
        df = ingester.ingest({"type": "csv", "path": sample_csv})
        profiler = SchemaProfiler()
        ctx = profiler.profile(df, source_name="integration_test")

        detector = SchemaDriftDetector(store_path=str(tmp_path / "fp.json"))
        report1 = detector.check_drift(ctx)
        assert report1.is_new is True

        report2 = detector.check_drift(ctx)
        assert report2.is_new is False
        assert report2.is_drifted is False


class TestHITLIntegration:
    """Layer 3 → Layer 4 integration."""

    def test_high_conf_auto_approves(self, mock_mapping, mock_cleaning_plan):
        validator = HITLValidator()
        assessment = validator.assess_confidence(
            mock_mapping,
            mock_cleaning_plan,
        )
        assert assessment.auto_approved is True

    def test_low_conf_creates_review_job(self, mock_mapping, mock_cleaning_plan):
        mock_mapping.confidence = 0.5
        validator = HITLValidator()
        assessment = validator.assess_confidence(mock_mapping, mock_cleaning_plan)
        assert assessment.requires_human_review is True

        job_id = validator.enqueue_review(mock_mapping, mock_cleaning_plan, assessment)
        assert job_id is not None
        pending = validator.get_pending_reviews()
        assert any(j.job_id == job_id for j in pending)


class TestLoaderIntegration:
    """Layer 5 integration."""

    def test_ddl_then_load(self, tmp_path, sample_df, mock_generated_code):
        loader = StarSchemaLoader(db_path=str(tmp_path / "wh.db"))
        tables = loader.execute_ddl(mock_generated_code)
        assert len(tables) >= 1

        count = loader.load_dataframe(sample_df, tables[0])
        assert count == 3

        rows = loader.query(f"SELECT * FROM [{tables[0]}]")
        assert len(rows) == 3


class TestQualityGateIntegration:
    """Quality gates on real data."""

    def test_sample_passes_gates(self, sample_df):
        gates = DataQualityGates()
        report = gates.run_all_checks(sample_df, "sample")
        assert report.gate_passed is True


class TestLineageIntegration:
    """Lineage tracking across layers."""

    def test_lineage_records_all_steps(self):
        from services.etl_llm.lineage.lineage_tracker import LineageNode

        tracker = DataLineageTracker()
        pid = tracker.start_pipeline("test")
        tracker.add_node(pid, LineageNode(node_type="source", rows_out=100))
        tracker.add_node(pid, LineageNode(node_type="profile", rows_in=100, rows_out=100))
        tracker.add_node(pid, LineageNode(node_type="mapping", rows_in=100, rows_out=100))
        tracker.add_node(pid, LineageNode(node_type="cleaning", rows_in=100, rows_out=95))
        tracker.add_node(pid, LineageNode(node_type="loading", rows_in=95, rows_out=95))

        graph = tracker.get_lineage(pid)
        assert len(graph.nodes) == 5
        md = tracker.export_lineage_markdown(pid)
        assert "Step 5" in md


class TestEndToEndWithMocks:
    """Full pipeline with all LLM calls mocked."""

    @pytest.mark.asyncio
    async def test_full_pipeline_mocked(
        self, sample_csv, tmp_path, mock_mapping, mock_cleaning_plan, mock_generated_code
    ):
        orch = ETLPipelineOrchestrator(
            db_path=str(tmp_path / "wh.db"),
            drift_store_path=str(tmp_path / "fp.json"),
        )
        orch.mapper.map_schema = MagicMock(return_value=mock_mapping)
        orch.cleaner.generate_cleaning_plan = MagicMock(return_value=mock_cleaning_plan)
        orch.code_gen.run_with_self_correction = MagicMock(return_value=mock_generated_code)

        result = await orch.run_pipeline(sample_csv, "csv", auto_approve=True)

        assert result.rows_ingested == 3
        assert result.mapping_confidence == 0.9
        assert result.cleaning_confidence == 0.92
        assert len(result.tables_created) >= 1
        assert result.rows_loaded > 0
        assert result.lineage_markdown != ""
        assert len(result.errors) == 0
