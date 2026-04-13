"""Tests for HITL validation (Layer 3)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from services.etl_llm.agents.cleaning_agent import CleaningPlan, CleaningRule
from services.etl_llm.agents.schema_mapper import (
    DimensionTableSpec,
    FactTableSpec,
    SchemaMappingResult,
)
from services.etl_llm.profiling.drift_detector import DriftReport
from services.etl_llm.validation.hitl_validator import HITLValidator


def _mapping(confidence: float = 0.9) -> SchemaMappingResult:
    return SchemaMappingResult(
        fact_table=FactTableSpec(name="fact", measures=["qty"], foreign_keys=["product"]),
        dimension_tables=[DimensionTableSpec(name="dim", source_columns=["product"], surrogate_key="sk")],
        confidence=confidence,
        model_used="llama3:8b",
        reasoning="test",
    )


def _plan(priority: int = 3, confidence: float = 0.9) -> CleaningPlan:
    return CleaningPlan(
        rules=[CleaningRule(column="a", rule_type="fill_null", params={}, priority=priority, justification="")],
        estimated_quality_improvement=0.1,
        confidence=confidence,
        model_used="test",
    )


class TestHITLValidator:
    def test_auto_approve_high_confidence(self):
        v = HITLValidator()
        result = v.assess_confidence(_mapping(0.9), _plan(3, 0.9))
        assert result.auto_approved is True
        assert result.requires_human_review is False

    def test_escalate_low_confidence(self):
        v = HITLValidator()
        result = v.assess_confidence(_mapping(0.5), _plan(3, 0.9))
        assert result.requires_human_review is True
        assert any("confidence" in r.lower() for r in result.reasons)

    def test_escalate_complex_schema(self):
        v = HITLValidator()
        result = v.assess_confidence(_mapping(0.9), _plan(3, 0.9), column_count=55)
        assert result.requires_human_review is True
        assert any("complex" in r.lower() or "columns" in r.lower() for r in result.reasons)

    def test_escalate_schema_drift(self):
        v = HITLValidator()
        drift = DriftReport(
            source_name="test",
            is_drifted=True,
            current_fingerprint="abc",
            previous_fingerprint="xyz",
            columns_added=["new_col"],
        )
        result = v.assess_confidence(_mapping(0.9), _plan(3, 0.9), drift_report=drift)
        assert result.requires_human_review is True
        assert any("drift" in r.lower() for r in result.reasons)

    def test_approve_stores_to_faiss(self):
        v = HITLValidator()
        mapping = _mapping(0.5)
        plan = _plan(1, 0.5)
        assessment = v.assess_confidence(mapping, plan)
        job_id = v.enqueue_review(mapping, plan, assessment)

        job = v.approve_review(job_id)
        assert job is not None
        assert job.status == "approved"
