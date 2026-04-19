"""
Script to run the benchmark and get precision/recall/F1 against ground truth.

Usage (from backend/ with .venv activated):
    python run_benchmark.py

Requires Ollama running with llama3:8b.
"""

import asyncio
import time

from services.etl_llm.evaluation.benchmark import ETLBenchmark, LatencyMetrics, CodeQualityMetrics
from services.etl_llm.evaluation.ground_truth import get_ground_truth
from services.etl_llm.orchestrator.pipeline_orchestrator import ETLPipelineOrchestrator


async def main():
    print("=" * 60)
    print("ETL-LLM BENCHMARK — Running against ground truth")
    print("=" * 60)
    print("⏳ This will call Llama 3 3 times — expect 2–10 minutes...\n")

    csv_path = "uploads/439748d0-4635-4ca2-a1b5-657366d9f598.csv"
    gt = get_ground_truth("test_sales")

    orch = ETLPipelineOrchestrator(
        db_path="benchmark_warehouse.db",
        drift_store_path="benchmark_fingerprints.json",
    )

    t0 = time.time()
    result = await orch.run_pipeline(csv_path, "csv", auto_approve=True)
    total_sec = time.time() - t0

    print(f"\n✅ Pipeline finished in {total_sec:.1f} seconds")
    print(f"   Rows ingested : {result.rows_ingested}")
    print(f"   Tables created: {result.tables_created}")
    print(f"   Mapping conf  : {result.mapping_confidence:.2f}")
    print(f"   Cleaning conf : {result.cleaning_confidence:.2f}")

    if result.errors:
        print(f"\n⚠️  Errors: {result.errors}")
        return

    # We need the actual mapping object — re-run just the mapper for metrics
    # (the orchestrator doesn't expose it publicly; we compute manually)
    print("\n" + "=" * 60)
    print("BENCHMARK RESULTS")
    print("=" * 60)
    print(f"Pipeline latency : {total_sec:.1f}s")
    print(f"Rows ingested    : {result.rows_ingested}")
    print(f"Tables created   : {result.tables_created}")
    print(f"\nMapping confidence (self-assessed) : {result.mapping_confidence:.2f}")
    print(f"Cleaning confidence (self-assessed): {result.cleaning_confidence:.2f}")
    print(f"\nGround truth expected tables: {gt['expected_tables']}")
    print(f"Tables created by pipeline  : {result.tables_created}")

    # Precision/recall on tables created vs expected
    pred_set = set(t.lower() for t in result.tables_created)
    gt_set = set(t.lower() for t in gt["expected_tables"])
    tp = len(pred_set & gt_set)
    fp = len(pred_set - gt_set)
    fn = len(gt_set - pred_set)
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    print(f"\n--- Schema Mapping Accuracy ---")
    print(f"True Positives  : {tp}")
    print(f"False Positives : {fp}")
    print(f"False Negatives : {fn}")
    print(f"Precision       : {precision:.2f}")
    print(f"Recall          : {recall:.2f}")
    print(f"F1 Score        : {f1:.2f}")
    print("\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
