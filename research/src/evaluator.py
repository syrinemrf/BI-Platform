"""
Evaluation module for the LLM-powered ETL pipeline.
Computes all metrics: mapping accuracy, cleaning recall, DQ improvement,
SQL validity, and produces an EvaluationReport.
"""
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel

try:
    from fuzzywuzzy import fuzz
except ImportError:
    # Fallback: simple substring matching
    class _FuzzFallback:
        @staticmethod
        def partial_ratio(a: str, b: str) -> int:
            a, b = a.lower(), b.lower()
            if a in b or b in a:
                return 90
            common = set(a.split(":")).intersection(set(b.split(":")))
            return 80 if common else 30
    fuzz = _FuzzFallback()


class EvaluationReport(BaseModel):
    per_dataset: dict  # metrics for each dataset
    overall_mapping_accuracy: float
    overall_cleaning_recall: float
    overall_dq_improvement: float
    routing_distribution: dict  # {llama: N, claude: N, fallback: N}
    avg_latency_llama_ms: float
    avg_latency_claude_ms: float
    avg_correction_attempts: float
    hitl_escalation_rate: float
    confidence_scores: list[float]  # all confidence values recorded

    class Config:
        arbitrary_types_allowed = True


class ETLEvaluator:
    """Evaluate all aspects of the ETL pipeline."""

    def mapping_accuracy(self, predicted: dict, ground_truth: dict) -> float:
        """
        Compute mapping accuracy as mean of:
        - fact_table match (fuzzy)
        - dimension Jaccard similarity
        - measure Jaccard similarity
        """
        # Fact table match
        pred_fact = predicted.get("fact_table", "").lower().strip()
        gt_fact = ground_truth.get("fact_table", "").lower().strip()
        fact_match = 1.0 if pred_fact == gt_fact else (
            0.5 if fuzz.partial_ratio(pred_fact, gt_fact) > 70 else 0.0
        )

        # Dimension Jaccard
        pred_dims = set(d.lower().strip() for d in predicted.get("dimensions", []))
        gt_dims = set(d.lower().strip() for d in ground_truth.get("dimensions", []))
        if gt_dims or pred_dims:
            dim_jaccard = len(pred_dims & gt_dims) / len(pred_dims | gt_dims)
        else:
            dim_jaccard = 1.0

        # Measure Jaccard
        pred_meas = set(m.lower().strip() for m in predicted.get("measures", []))
        gt_meas = set(m.lower().strip() for m in ground_truth.get("measures", []))
        if gt_meas or pred_meas:
            meas_jaccard = len(pred_meas & gt_meas) / len(pred_meas | gt_meas)
        else:
            meas_jaccard = 1.0

        return float(np.mean([fact_match, dim_jaccard, meas_jaccard]))

    def cleaning_recall(
        self, predicted_rules: list[str], expected_rules: list[str]
    ) -> float:
        """
        How many expected cleaning rules were detected?
        Uses fuzzy string matching (partial_ratio > 70).
        """
        if not expected_rules:
            return 1.0

        detected = 0
        for expected in expected_rules:
            for predicted in predicted_rules:
                if fuzz.partial_ratio(expected, predicted) > 70:
                    detected += 1
                    break

        return detected / len(expected_rules)

    def compute_dq_score(self, df: pd.DataFrame) -> float:
        """
        Data Quality score: mean(1 - null_pct) across all columns.
        """
        null_pcts = df.isnull().mean()
        return float((1 - null_pcts).mean())

    def compute_dq_improvement(
        self, df_before: pd.DataFrame, df_after: pd.DataFrame
    ) -> float:
        """
        DQ improvement ratio: (DQ_after / DQ_before) - 1.
        Positive = improvement.
        """
        dq_before = self.compute_dq_score(df_before)
        dq_after = self.compute_dq_score(df_after)
        if dq_before == 0:
            return 0.0
        return (dq_after / dq_before) - 1.0

    def sql_validity(self, sql: str) -> bool:
        """Check SQL validity using basic structural checks."""
        if not sql.strip():
            return False
        sql_upper = sql.upper()
        if "CREATE TABLE" not in sql_upper and "INSERT" not in sql_upper:
            return False
        if sql.count("(") != sql.count(")"):
            return False
        if sql.count("'") % 2 != 0:
            return False
        return True

    def run_full_evaluation(self, results: list[dict]) -> EvaluationReport:
        """Aggregate all metrics across all datasets."""
        per_dataset = {}
        all_mapping_acc = []
        all_cleaning_recall = []
        all_dq_improvement = []
        all_confidence = []
        llama_count = 0
        claude_count = 0
        fallback_count = 0
        llama_latencies = []
        claude_latencies = []
        correction_attempts = []
        escalated = 0
        total = 0

        for r in results:
            ds_name = r.get("dataset_name", "unknown")
            ds_metrics = {}

            # Mapping accuracy
            if "mapping_accuracy" in r:
                ds_metrics["mapping_accuracy"] = r["mapping_accuracy"]
                all_mapping_acc.append(r["mapping_accuracy"])

            # Cleaning recall
            if "cleaning_recall" in r:
                ds_metrics["cleaning_recall"] = r["cleaning_recall"]
                all_cleaning_recall.append(r["cleaning_recall"])

            # DQ improvement
            if "dq_improvement" in r:
                ds_metrics["dq_improvement"] = r["dq_improvement"]
                all_dq_improvement.append(r["dq_improvement"])

            # Routing
            model = r.get("model_used", "")
            if "llama" in model.lower():
                llama_count += 1
            if "claude" in model.lower():
                claude_count += 1
            if r.get("fallback_reason"):
                fallback_count += 1

            # Latency
            if r.get("latency_ms"):
                if "llama" in model.lower():
                    llama_latencies.append(r["latency_ms"])
                elif "claude" in model.lower():
                    claude_latencies.append(r["latency_ms"])

            # Confidence
            if "confidence" in r:
                all_confidence.append(r["confidence"])

            # Correction attempts
            if "correction_attempts" in r:
                correction_attempts.append(r["correction_attempts"])

            # HITL
            if "requires_human_review" in r:
                total += 1
                if r["requires_human_review"]:
                    escalated += 1

            per_dataset[ds_name] = ds_metrics

        return EvaluationReport(
            per_dataset=per_dataset,
            overall_mapping_accuracy=float(np.mean(all_mapping_acc)) if all_mapping_acc else 0.0,
            overall_cleaning_recall=float(np.mean(all_cleaning_recall)) if all_cleaning_recall else 0.0,
            overall_dq_improvement=float(np.mean(all_dq_improvement)) if all_dq_improvement else 0.0,
            routing_distribution={
                "llama": llama_count,
                "claude": claude_count,
                "fallback": fallback_count,
            },
            avg_latency_llama_ms=float(np.mean(llama_latencies)) if llama_latencies else 0.0,
            avg_latency_claude_ms=float(np.mean(claude_latencies)) if claude_latencies else 0.0,
            avg_correction_attempts=float(np.mean(correction_attempts)) if correction_attempts else 0.0,
            hitl_escalation_rate=escalated / total if total > 0 else 0.0,
            confidence_scores=all_confidence,
        )
