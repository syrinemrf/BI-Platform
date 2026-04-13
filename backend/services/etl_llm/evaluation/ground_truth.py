"""
Ground Truth Definitions
=========================
Reference mappings for test/sample datasets used in evaluation.
"""

from __future__ import annotations


GROUND_TRUTH = {
    "test_sales": {
        "fact_measures": ["quantity", "price"],
        "dimension_names": ["dim_product", "dim_region", "dim_date"],
        "expected_tables": ["fact_sales", "dim_product", "dim_region", "dim_date"],
    },
}


def get_ground_truth(dataset_name: str) -> dict | None:
    return GROUND_TRUTH.get(dataset_name)
