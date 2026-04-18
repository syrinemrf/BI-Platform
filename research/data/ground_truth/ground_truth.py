"""
Ground truth definitions for all 4 research datasets.
Used for evaluating LLM schema mapping, cleaning rule detection,
and end-to-end ETL pipeline accuracy.
"""

GROUND_TRUTH = {
    "dataset1_retail_sales": {
        "fact_table": "sales_fact",
        "dimensions": ["date_dim", "customer_dim", "product_dim", "sales_rep_dim"],
        "measures": ["unit_price", "quantity", "discount_pct", "total_amount"],
        "expected_cleaning_rules": [
            "standardize_date_format:order_date",
            "fill_null:discount_pct:0",
            "normalize_text:customer_country",
            "remove_negative:quantity",
            "strip_currency_symbol:unit_price",
            "drop_duplicates:order_id",
        ],
        "difficulty": "easy",
        "expected_llm_routing": "llama3_alone",
        "expected_hitl": False,
    },
    "dataset2_hospital_records": {
        "fact_table": "hospital_visit_fact",
        "dimensions": [
            "patient_dim",
            "doctor_dim",
            "department_dim",
            "diagnosis_dim",
            "date_dim",
        ],
        "measures": [
            "length_of_stay_days",
            "total_cost",
            "insurance_covered",
            "patient_paid",
        ],
        "expected_cleaning_rules": [
            "normalize_text:patient.gender",
            "fill_null:discharge_date:still_admitted",
            "fix_inconsistency:total_cost",
            "normalize_text:diagnosis.severity",
        ],
        "difficulty": "medium",
        "expected_llm_routing": "llama3_with_fallback",
        "expected_hitl": True,
    },
    "dataset3_supplier_invoices": {
        "fact_table": "invoice_fact",
        "dimensions": ["supplier_dim", "buyer_department_dim", "date_dim"],
        "measures": [
            "subtotal_ht",
            "vat_amount",
            "total_ttc",
            "payment_delay_days",
        ],
        "expected_cleaning_rules": [
            "normalize_text:status",
            "fix_vat_computation",
            "standardize_date_format:paid_on",
            "drop_duplicates:invoice_id",
        ],
        "difficulty": "medium_hard",
        "expected_llm_routing": "claude_fallback",
        "expected_hitl": True,
    },
    "dataset4_ecommerce_events": {
        "fact_table": "purchase_fact",
        "dimensions": ["user_dim", "product_dim", "date_dim", "device_dim"],
        "measures": ["price", "quantity", "total", "refund_amount"],
        "expected_cleaning_rules": [
            "fill_null:user.uid:anonymous",
            "fix_timestamp_order",
            "normalize_text:user.country",
            "flag_orphan_refunds",
        ],
        "difficulty": "hard",
        "expected_llm_routing": "always_claude",
        "expected_hitl": True,
    },
}
