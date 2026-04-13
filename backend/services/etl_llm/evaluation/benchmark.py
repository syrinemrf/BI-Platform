"""
Benchmarking Framework for Academic Evaluation
===============================================
Provides structured evaluation of the LLM ETL pipeline across
four dimensions:

1. Schema Mapping Accuracy (precision, recall, F1)
2. Data Quality Improvement (null%, duplicate%, type mismatch)
3. Code Generation Quality (syntax valid, semantic correct, efficiency)
4. End-to-End Latency (per layer and total)

All metrics use ground-truth comparisons to enable reproducible
evaluation for the research paper.

Reference: [Gong et al. 2020] — evaluation metrics for ETL benchmarks.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from services.etl_llm.agents.schema_mapper import SchemaMappingResult


@dataclass
class MappingAccuracy:
    """Precision / recall / F1 for schema mapping vs ground truth."""

    true_positives: int = 0
    false_positives: int = 0
    false_negatives: int = 0
    precision: float = 0.0
    recall: float = 0.0
    f1_score: float = 0.0


@dataclass
class DataQualityMetrics:
    """Before/after comparison of data quality indicators."""

    null_pct_before: float = 0.0
    null_pct_after: float = 0.0
    duplicate_pct_before: float = 0.0
    duplicate_pct_after: float = 0.0
    quality_improvement: float = 0.0


@dataclass
class CodeQualityMetrics:
    """Metrics for generated ETL code quality."""

    sql_syntax_valid: bool = False
    python_syntax_valid: bool = False
    correction_attempts: int = 0
    final_confidence: float = 0.0


@dataclass
class LatencyMetrics:
    """Per-layer and total pipeline latency in seconds."""

    ingestion_sec: float = 0.0
    profiling_sec: float = 0.0
    mapping_sec: float = 0.0
    cleaning_sec: float = 0.0
    code_gen_sec: float = 0.0
    loading_sec: float = 0.0
    total_sec: float = 0.0


@dataclass
class BenchmarkResult:
    """Complete benchmark result for a single dataset."""

    dataset_name: str = ""
    mapping_accuracy: MappingAccuracy = field(default_factory=MappingAccuracy)
    data_quality: DataQualityMetrics = field(default_factory=DataQualityMetrics)
    code_quality: CodeQualityMetrics = field(default_factory=CodeQualityMetrics)
    latency: LatencyMetrics = field(default_factory=LatencyMetrics)


class ETLBenchmark:
    """Evaluate the LLM ETL pipeline against ground truth data."""

    @staticmethod
    def evaluate_mapping_accuracy(
        predicted: SchemaMappingResult,
        ground_truth_fact_measures: list[str],
        ground_truth_dim_names: list[str],
    ) -> MappingAccuracy:
        """Compare predicted mapping against ground truth.

        Computes precision, recall, F1 for both fact measures and
        dimension table assignments.
        """
        pred_measures = set(predicted.fact_table.measures)
        gt_measures = set(ground_truth_fact_measures)
        pred_dims = {d.name for d in predicted.dimension_tables}
        gt_dims = set(ground_truth_dim_names)

        # Combined TP/FP/FN across measures and dims
        all_pred = pred_measures | pred_dims
        all_gt = gt_measures | gt_dims

        tp = len(all_pred & all_gt)
        fp = len(all_pred - all_gt)
        fn = len(all_gt - all_pred)

        p = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        r = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * p * r / (p + r) if (p + r) > 0 else 0.0

        return MappingAccuracy(
            true_positives=tp,
            false_positives=fp,
            false_negatives=fn,
            precision=round(p, 4),
            recall=round(r, 4),
            f1_score=round(f1, 4),
        )

    @staticmethod
    def evaluate_data_quality(
        df_before: pd.DataFrame,
        df_after: pd.DataFrame,
    ) -> DataQualityMetrics:
        """Compare data quality before and after cleaning."""
        null_before = df_before.isnull().mean().mean() * 100
        null_after = df_after.isnull().mean().mean() * 100
        dup_before = df_before.duplicated().mean() * 100
        dup_after = df_after.duplicated().mean() * 100

        improvement = (null_before - null_after) + (dup_before - dup_after)

        return DataQualityMetrics(
            null_pct_before=round(null_before, 2),
            null_pct_after=round(null_after, 2),
            duplicate_pct_before=round(dup_before, 2),
            duplicate_pct_after=round(dup_after, 2),
            quality_improvement=round(improvement, 2),
        )

    @staticmethod
    def format_results_table(results: list[BenchmarkResult]) -> str:
        """Produce a Markdown table of benchmark results for the paper."""
        lines = [
            "| Dataset | F1 | Quality Δ | SQL Valid | Corr. Attempts | Total (s) |",
            "|---------|-----|-----------|-----------|----------------|-----------|",
        ]
        for r in results:
            lines.append(
                f"| {r.dataset_name} "
                f"| {r.mapping_accuracy.f1_score:.3f} "
                f"| {r.data_quality.quality_improvement:+.1f}% "
                f"| {'✓' if r.code_quality.sql_syntax_valid else '✗'} "
                f"| {r.code_quality.correction_attempts} "
                f"| {r.latency.total_sec:.2f} |"
            )
        return "\n".join(lines)
