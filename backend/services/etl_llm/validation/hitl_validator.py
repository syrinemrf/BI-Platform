"""
Human-in-the-Loop (HITL) Validation (Layer 3)
===============================================
Assesses pipeline outputs and decides whether human review is needed
based on confidence scores, schema complexity, and drift status.

Auto-approval conditions:
  - mapping confidence >= 0.85
  - no critical cleaning rules (priority 1)
  - model_used == claude-3-5-sonnet (high trust)

Escalation triggers:
  - any confidence < 0.75
  - critical cleaning rule affecting >10% rows
  - schema has >50 columns
  - schema drift detected
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from services.etl_llm.agents.cleaning_agent import CleaningPlan
from services.etl_llm.agents.schema_mapper import SchemaMappingResult
from services.etl_llm.profiling.drift_detector import DriftReport

logger = logging.getLogger(__name__)


# ── Pydantic models ─────────────────────────────────────────────


class ReviewItem(BaseModel):
    item_type: Literal["mapping", "cleaning_rule", "code"]
    description: str
    suggestion: str
    risk_level: Literal["low", "medium", "high"]


class ValidationAssessment(BaseModel):
    requires_human_review: bool = False
    confidence_score: float = Field(ge=0.0, le=1.0, default=0.0)
    reasons: list[str] = Field(default_factory=list)
    auto_approved: bool = False
    review_items: list[ReviewItem] = Field(default_factory=list)


# ── In-memory review queue ───────────────────────────────────────


class ReviewJob(BaseModel):
    job_id: str
    created_at: datetime
    mapping: SchemaMappingResult
    cleaning_plan: CleaningPlan
    assessment: ValidationAssessment
    status: Literal["pending", "approved", "rejected", "modified"] = "pending"
    human_feedback: str = ""


_review_queue: dict[str, ReviewJob] = {}


# ── Validator ────────────────────────────────────────────────────


class HITLValidator:
    """Human-in-the-Loop confidence assessor.

    Decides whether a pipeline result can be auto-approved or must
    be routed to a human reviewer.
    """

    def assess_confidence(
        self,
        mapping: SchemaMappingResult,
        cleaning_plan: CleaningPlan,
        drift_report: DriftReport | None = None,
        column_count: int = 0,
    ) -> ValidationAssessment:
        """Evaluate pipeline outputs and produce a validation assessment."""
        reasons: list[str] = []
        review_items: list[ReviewItem] = []
        min_conf = min(mapping.confidence, cleaning_plan.confidence)

        # ── Escalation checks ────────────────────────────────────
        if min_conf < 0.75:
            reasons.append(f"Low confidence: {min_conf:.2f}")
            review_items.append(
                ReviewItem(
                    item_type="mapping",
                    description=f"Confidence {min_conf:.2f} below threshold",
                    suggestion="Verify mapping correctness manually",
                    risk_level="high",
                )
            )

        critical_rules = [r for r in cleaning_plan.rules if r.priority == 1]
        if critical_rules:
            reasons.append(f"{len(critical_rules)} critical cleaning rules")
            for r in critical_rules:
                review_items.append(
                    ReviewItem(
                        item_type="cleaning_rule",
                        description=f"Critical rule on '{r.column}': {r.rule_type}",
                        suggestion=r.justification,
                        risk_level="high",
                    )
                )

        if column_count > 50:
            reasons.append(f"Complex schema: {column_count} columns")
            review_items.append(
                ReviewItem(
                    item_type="mapping",
                    description=f"Schema has {column_count} columns (>50)",
                    suggestion="Review dimension/fact assignment for large schemas",
                    risk_level="medium",
                )
            )

        if drift_report and drift_report.is_drifted:
            reasons.append("Schema drift detected")
            review_items.append(
                ReviewItem(
                    item_type="mapping",
                    description=f"Added: {drift_report.columns_added}, Removed: {drift_report.columns_removed}",
                    suggestion="Verify mapping still valid after schema change",
                    risk_level="high",
                )
            )

        # ── Auto-approval ────────────────────────────────────────
        auto_approved = (
            len(reasons) == 0
            and mapping.confidence >= 0.85
            and not critical_rules
        )

        return ValidationAssessment(
            requires_human_review=not auto_approved,
            confidence_score=min_conf,
            reasons=reasons,
            auto_approved=auto_approved,
            review_items=review_items,
        )

    def enqueue_review(
        self,
        mapping: SchemaMappingResult,
        cleaning_plan: CleaningPlan,
        assessment: ValidationAssessment,
    ) -> str:
        """Add a job to the review queue and return the job_id."""
        job_id = str(uuid.uuid4())
        _review_queue[job_id] = ReviewJob(
            job_id=job_id,
            created_at=datetime.utcnow(),
            mapping=mapping,
            cleaning_plan=cleaning_plan,
            assessment=assessment,
        )
        return job_id

    @staticmethod
    def get_pending_reviews() -> list[ReviewJob]:
        return [j for j in _review_queue.values() if j.status == "pending"]

    @staticmethod
    def get_review(job_id: str) -> ReviewJob | None:
        return _review_queue.get(job_id)

    @staticmethod
    def approve_review(job_id: str) -> ReviewJob | None:
        job = _review_queue.get(job_id)
        if job:
            job.status = "approved"
        return job

    @staticmethod
    def reject_review(job_id: str, reason: str = "") -> ReviewJob | None:
        job = _review_queue.get(job_id)
        if job:
            job.status = "rejected"
            job.human_feedback = reason
        return job

    @staticmethod
    def modify_review(job_id: str, updated_mapping: dict) -> ReviewJob | None:
        job = _review_queue.get(job_id)
        if job:
            job.status = "modified"
            # Update mapping from dict
            from services.etl_llm.agents.schema_mapper import SchemaMappingResult
            job.mapping = SchemaMappingResult(**updated_mapping)
        return job
