"""
Human-in-the-Loop (HITL) Validator — Layer 3 of the ETL pipeline.
Implements confidence thresholding and Adaptive Few-Shot Memory (Innovation #2).
"""
import random
from dataclasses import dataclass, field
from typing import Optional

from .schema_mapper import MappingResult


@dataclass
class HITLAssessment:
    dataset_name: str
    requires_human_review: bool
    confidence_score: float
    reasons: list[str] = field(default_factory=list)
    auto_approved: bool = False
    escalation_category: str = ""  # "low_confidence", "complex_schema", "schema_drift"


@dataclass
class ApprovedExample:
    dataset_name: str
    schema_fingerprint: str
    mapping: dict
    approved_by_human: bool
    timestamp: str = ""


class HITLValidator:
    """Assess mapping confidence and manage human review escalation."""

    def __init__(self, confidence_threshold: float = 0.75):
        self.confidence_threshold = confidence_threshold
        self.approved_examples: list[ApprovedExample] = []
        self._assessment_log: list[HITLAssessment] = []

    @property
    def assessment_log(self) -> list[HITLAssessment]:
        return self._assessment_log

    def assess_confidence(
        self,
        mapping: MappingResult,
        schema_complexity: str = "medium",
        has_drift: bool = False,
    ) -> HITLAssessment:
        """Determine if a mapping result requires human review."""
        reasons = []

        # Check confidence threshold
        if mapping.confidence < self.confidence_threshold:
            reasons.append(
                f"Low confidence ({mapping.confidence:.2f} < {self.confidence_threshold})"
            )

        # Check schema complexity
        complexity_thresholds = {
            "easy": 0.60,
            "medium": 0.70,
            "medium_hard": 0.80,
            "hard": 0.85,
        }
        adj_threshold = complexity_thresholds.get(schema_complexity, 0.75)
        if mapping.confidence < adj_threshold:
            reasons.append(
                f"Complex schema requires higher confidence "
                f"({mapping.confidence:.2f} < {adj_threshold} for {schema_complexity})"
            )

        # Check for schema drift
        if has_drift:
            reasons.append("Schema drift detected — human verification recommended")

        # Check for fallback usage
        if mapping.fallback_reason:
            reasons.append(f"LLM fallback triggered: {mapping.fallback_reason}")

        # Check mapping completeness
        if not mapping.fact_table:
            reasons.append("No fact table identified")
        if len(mapping.dimensions) < 2:
            reasons.append("Too few dimensions identified (< 2)")

        requires_review = len(reasons) > 0

        # Determine escalation category
        if not mapping.fact_table or len(mapping.dimensions) < 2:
            category = "complex_schema"
        elif has_drift:
            category = "schema_drift"
        else:
            category = "low_confidence"

        assessment = HITLAssessment(
            dataset_name=mapping.dataset_name,
            requires_human_review=requires_review,
            confidence_score=mapping.confidence,
            reasons=reasons,
            auto_approved=not requires_review,
            escalation_category=category if requires_review else "auto_approved",
        )
        self._assessment_log.append(assessment)
        return assessment

    def simulate_human_approval(
        self, mapping: MappingResult, fingerprint: str
    ) -> ApprovedExample:
        """Simulate a human approving a mapping (for research purposes)."""
        example = ApprovedExample(
            dataset_name=mapping.dataset_name,
            schema_fingerprint=fingerprint,
            mapping={
                "fact_table": mapping.fact_table,
                "dimensions": mapping.dimensions,
                "measures": mapping.measures,
            },
            approved_by_human=True,
        )
        self.approved_examples.append(example)
        return example

    def get_approved_examples_for_schema(
        self, fingerprint: str
    ) -> list[ApprovedExample]:
        """Retrieve approved examples matching a schema fingerprint (Innovation #2)."""
        return [
            ex for ex in self.approved_examples
            if ex.schema_fingerprint == fingerprint
        ]

    def compute_escalation_rate(self) -> float:
        """Compute overall escalation rate."""
        if not self._assessment_log:
            return 0.0
        escalated = sum(1 for a in self._assessment_log if a.requires_human_review)
        return escalated / len(self._assessment_log)

    def compute_workload_distribution(self) -> dict:
        """Break down HITL workload by category."""
        dist = {"auto_approved": 0, "low_confidence": 0,
                "complex_schema": 0, "schema_drift": 0}
        for a in self._assessment_log:
            cat = a.escalation_category
            if cat in dist:
                dist[cat] += 1
        total = len(self._assessment_log) or 1
        return {k: v / total for k, v in dist.items()}
