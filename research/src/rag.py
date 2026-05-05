"""
RAG Adaptive Few-Shot Memory — FAISS-backed schema store (Innovation #2).

Mirrors the production SchemaVectorStore
(backend/services/etl_llm/rag/schema_store.py) for isolated research evaluation.

Architecture:
  * FAISS IndexFlatL2 stores 384-dim embedding vectors of schema descriptions.
  * Parallel JSON metadata file stores mapping, approval status, source name.
  * sentence-transformers MiniLM-L6-v2 for schema text encoding.
  * Falls back to a hash-based TF-IDF embedding when sentence-transformers
    is unavailable (so the module works without GPU / heavy ML deps).

References:
  [Colombo et al. 2025] — Knowledge graph enrichment with retrieval-augmented
    prompting for LLM-driven ETL pipelines.
  [Birjega 2025] — Semantic-RAG architecture for schema-aware retrieval.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, List, Optional

import numpy as np

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Embedding helpers (sentence-transformers or TF-IDF fallback)
# ─────────────────────────────────────────────────────────────────────────────

_EMBED_MODEL = None
_USE_TRANSFORMERS: Optional[bool] = None  # None = not yet probed


def _probe_transformers() -> bool:
    global _USE_TRANSFORMERS
    if _USE_TRANSFORMERS is None:
        try:
            from sentence_transformers import SentenceTransformer  # noqa: F401
            _USE_TRANSFORMERS = True
        except ImportError:
            _USE_TRANSFORMERS = False
            logger.warning(
                "sentence-transformers not installed — RAGSchemaStore will use "
                "a hash-based TF-IDF fallback.  Install with: "
                "pip install sentence-transformers"
            )
    return _USE_TRANSFORMERS


def _get_embed_model():
    global _EMBED_MODEL
    if _probe_transformers() and _EMBED_MODEL is None:
        from sentence_transformers import SentenceTransformer
        _EMBED_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
    return _EMBED_MODEL


def _tfidf_embed(text: str, dim: int = 384) -> np.ndarray:
    """Hash-based bag-of-words embedding (no ML deps required)."""
    import hashlib
    tokens = text.lower().split()
    vec = np.zeros(dim, dtype="float32")
    for tok in tokens:
        idx = int(hashlib.md5(tok.encode()).hexdigest(), 16) % dim
        vec[idx] += 1.0
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec /= norm
    return vec


def embed_text(text: str) -> np.ndarray:
    """Embed a schema text to a 384-dim float32 vector."""
    model = _get_embed_model()
    if model is not None:
        return model.encode([text])[0].astype("float32")
    return _tfidf_embed(text)


# ─────────────────────────────────────────────────────────────────────────────
# Schema → text helper
# ─────────────────────────────────────────────────────────────────────────────

def schema_to_text(schema: Any) -> str:
    """Convert a schema description to a plain text string for embedding.

    Accepts:
      - a SchemaContext object (has ``.columns`` with ``.name`` / ``.dtype``)
      - a list of dicts with ``name`` / ``dtype`` keys
      - a plain ``{column: dtype}`` dict
      - a plain str (passed through unchanged)
    """
    if isinstance(schema, str):
        return schema
    if hasattr(schema, "columns"):          # SchemaContext
        return " | ".join(f"{c.name}:{c.dtype}" for c in schema.columns)
    if isinstance(schema, list):
        if schema and isinstance(schema[0], dict):
            return " | ".join(f"{c['name']}:{c.get('dtype', '?')}" for c in schema)
        return " | ".join(str(c) for c in schema)
    if isinstance(schema, dict):
        return " | ".join(f"{k}:{v}" for k, v in schema.items())
    return str(schema)


# ─────────────────────────────────────────────────────────────────────────────
# Curated seed examples (Innovation #2 — Adaptive Few-Shot Memory bootstrap)
# 20 expert-validated examples covering all benchmark domains:
#   Retail/Sales · Healthcare · Procurement · E-commerce · HR · Finance
# Each entry has schema_text, star-schema mapping, and cleaning_rules.
# ─────────────────────────────────────────────────────────────────────────────

_SEED_EXAMPLES: List[dict] = [
    # ── Retail / Sales (4 examples) ──────────────────────────────────────────
    {
        "source_name": "retail_orders_daily",
        "schema_text": "order_id:int64 | order_date:object | customer_id:int64 | customer_name:object | product_id:int64 | product_name:object | category:object | quantity:int64 | unit_price:object | discount:float64 | total_amount:object | store_id:int64 | region:object | payment_method:object",
        "mapping": {
            "fact_table": "sales_fact",
            "dimensions": ["date_dim", "customer_dim", "product_dim", "store_dim", "payment_dim"],
            "measures": ["quantity", "unit_price", "discount", "total_amount"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:unit_price",
            "strip_currency_symbol:total_amount",
            "standardize_date_format:order_date",
            "fill_null:discount",
            "drop_duplicates:order_id",
            "remove_negative:quantity",
        ],
    },
    {
        "source_name": "retail_transactions_pos",
        "schema_text": "transaction_id:object | sale_date:object | cashier_id:int64 | product_sku:object | product_desc:object | department:object | qty_sold:int64 | sale_price:object | cost_price:float64 | margin:float64 | store_code:object | city:object | country:object",
        "mapping": {
            "fact_table": "transaction_fact",
            "dimensions": ["date_dim", "product_dim", "store_dim", "cashier_dim"],
            "measures": ["qty_sold", "sale_price", "cost_price", "margin"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:sale_price",
            "standardize_date_format:sale_date",
            "normalize_text:department",
            "fill_null:margin",
            "drop_duplicates:transaction_id",
        ],
    },
    {
        "source_name": "ecommerce_web_orders",
        "schema_text": "order_number:object | placed_at:object | user_id:int64 | email:object | item_id:int64 | item_name:object | brand:object | units:int64 | list_price:float64 | promo_code:object | promo_discount:float64 | shipping_fee:float64 | grand_total:float64 | fulfillment_center:object | delivery_status:object",
        "mapping": {
            "fact_table": "order_fact",
            "dimensions": ["date_dim", "customer_dim", "product_dim", "promotion_dim", "fulfillment_dim"],
            "measures": ["units", "list_price", "promo_discount", "shipping_fee", "grand_total"],
        },
        "cleaning_rules": [
            "standardize_date_format:placed_at",
            "fill_null:promo_code",
            "fill_null:promo_discount",
            "normalize_text:delivery_status",
            "drop_duplicates:order_number",
        ],
    },
    {
        "source_name": "retail_returns",
        "schema_text": "return_id:object | return_date:object | original_order_id:object | customer_id:int64 | product_id:int64 | return_reason:object | qty_returned:int64 | refund_amount:object | refund_status:object | store_id:int64",
        "mapping": {
            "fact_table": "returns_fact",
            "dimensions": ["date_dim", "customer_dim", "product_dim", "store_dim", "return_reason_dim"],
            "measures": ["qty_returned", "refund_amount"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:refund_amount",
            "standardize_date_format:return_date",
            "normalize_text:return_reason",
            "fill_null:refund_status",
            "flag_orphan_refunds:original_order_id",
        ],
    },
    # ── Healthcare / Hospital (4 examples) ───────────────────────────────────
    {
        "source_name": "hospital_admissions",
        "schema_text": "admission_id:int64 | admit_date:object | discharge_date:object | patient_id:int64 | patient_name:object | dob:object | gender:object | ward:object | diagnosis_code:object | diagnosis_desc:object | attending_physician:object | treatment_cost:object | insurance_provider:object | bed_days:int64",
        "mapping": {
            "fact_table": "admission_fact",
            "dimensions": ["date_dim", "patient_dim", "ward_dim", "diagnosis_dim", "physician_dim", "insurance_dim"],
            "measures": ["treatment_cost", "bed_days"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:treatment_cost",
            "standardize_date_format:admit_date",
            "standardize_date_format:discharge_date",
            "standardize_date_format:dob",
            "normalize_text:gender",
            "fill_null:insurance_provider",
            "drop_duplicates:admission_id",
        ],
    },
    {
        "source_name": "clinical_lab_results",
        "schema_text": "test_id:int64 | test_date:object | patient_id:int64 | test_type:object | test_name:object | result_value:float64 | unit:object | reference_min:float64 | reference_max:float64 | flag:object | lab_technician:object | department:object",
        "mapping": {
            "fact_table": "lab_result_fact",
            "dimensions": ["date_dim", "patient_dim", "test_type_dim", "department_dim", "technician_dim"],
            "measures": ["result_value", "reference_min", "reference_max"],
        },
        "cleaning_rules": [
            "standardize_date_format:test_date",
            "normalize_text:flag",
            "fill_null:flag",
            "remove_negative:result_value",
        ],
    },
    {
        "source_name": "pharmacy_prescriptions",
        "schema_text": "prescription_id:object | prescription_date:object | patient_id:int64 | doctor_id:int64 | drug_code:object | drug_name:object | dosage_mg:float64 | qty_dispensed:int64 | unit_cost:object | total_cost:object | refill_count:int64 | insurance_covered:object",
        "mapping": {
            "fact_table": "prescription_fact",
            "dimensions": ["date_dim", "patient_dim", "doctor_dim", "drug_dim"],
            "measures": ["dosage_mg", "qty_dispensed", "unit_cost", "total_cost", "refill_count"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:unit_cost",
            "strip_currency_symbol:total_cost",
            "standardize_date_format:prescription_date",
            "fill_null:refill_count",
            "normalize_text:insurance_covered",
        ],
    },
    {
        "source_name": "patient_appointments",
        "schema_text": "appointment_id:int64 | scheduled_date:object | actual_date:object | patient_id:int64 | doctor_id:int64 | specialty:object | appointment_type:object | duration_minutes:int64 | no_show:object | cancellation_reason:object | fee:object",
        "mapping": {
            "fact_table": "appointment_fact",
            "dimensions": ["date_dim", "patient_dim", "doctor_dim", "specialty_dim", "appointment_type_dim"],
            "measures": ["duration_minutes", "fee"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:fee",
            "standardize_date_format:scheduled_date",
            "standardize_date_format:actual_date",
            "normalize_text:no_show",
            "fill_null:cancellation_reason",
            "fix_timestamp_order:scheduled_date",
        ],
    },
    # ── Procurement / B2B Invoices (3 examples) ───────────────────────────────
    {
        "source_name": "supplier_invoices_b2b",
        "schema_text": "invoice_id:object | invoice_date:object | due_date:object | vendor_id:int64 | vendor_name:object | vendor_country:object | po_number:object | line_item:int64 | description:object | quantity:int64 | unit_price:object | line_total:object | vat_rate:float64 | vat_amount:object | invoice_total:object | currency:object | payment_status:object",
        "mapping": {
            "fact_table": "invoice_fact",
            "dimensions": ["date_dim", "vendor_dim", "purchase_order_dim", "currency_dim"],
            "measures": ["quantity", "unit_price", "line_total", "vat_amount", "invoice_total"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:unit_price",
            "strip_currency_symbol:line_total",
            "strip_currency_symbol:vat_amount",
            "strip_currency_symbol:invoice_total",
            "standardize_date_format:invoice_date",
            "standardize_date_format:due_date",
            "fix_vat_computation:vat_amount",
            "normalize_text:currency",
            "drop_duplicates:invoice_id",
            "fix_timestamp_order:due_date",
        ],
    },
    {
        "source_name": "procurement_purchase_orders",
        "schema_text": "po_id:object | created_date:object | approved_date:object | department:object | buyer_id:int64 | supplier_id:int64 | item_code:object | item_description:object | uom:object | qty_ordered:int64 | qty_received:int64 | unit_cost:object | total_cost:object | delivery_status:object",
        "mapping": {
            "fact_table": "purchase_order_fact",
            "dimensions": ["date_dim", "department_dim", "supplier_dim", "item_dim", "buyer_dim"],
            "measures": ["qty_ordered", "qty_received", "unit_cost", "total_cost"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:unit_cost",
            "strip_currency_symbol:total_cost",
            "standardize_date_format:created_date",
            "standardize_date_format:approved_date",
            "normalize_text:delivery_status",
            "fill_null:approved_date",
            "fix_inconsistency:qty_received",
        ],
    },
    {
        "source_name": "contract_spend_analysis",
        "schema_text": "contract_id:object | start_date:object | end_date:object | supplier_id:int64 | category:object | subcategory:object | committed_value:object | actual_spend:object | savings:object | contract_manager:object | region:object | compliance_status:object",
        "mapping": {
            "fact_table": "contract_spend_fact",
            "dimensions": ["date_dim", "supplier_dim", "category_dim", "contract_manager_dim", "region_dim"],
            "measures": ["committed_value", "actual_spend", "savings"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:committed_value",
            "strip_currency_symbol:actual_spend",
            "strip_currency_symbol:savings",
            "standardize_date_format:start_date",
            "standardize_date_format:end_date",
            "normalize_text:compliance_status",
            "fix_timestamp_order:start_date",
        ],
    },
    # ── E-commerce / Clickstream Events (3 examples) ──────────────────────────
    {
        "source_name": "web_clickstream_events",
        "schema_text": "event_id:object | event_timestamp:object | session_id:object | user_id:int64 | device_type:object | browser:object | page_url:object | event_type:object | referrer:object | product_id:int64 | category:object | time_on_page_seconds:int64 | scroll_depth_pct:float64 | clicked_cta:object",
        "mapping": {
            "fact_table": "clickstream_fact",
            "dimensions": ["date_dim", "user_dim", "device_dim", "page_dim", "product_dim", "event_type_dim"],
            "measures": ["time_on_page_seconds", "scroll_depth_pct"],
        },
        "cleaning_rules": [
            "standardize_date_format:event_timestamp",
            "normalize_text:device_type",
            "normalize_text:event_type",
            "fill_null:product_id",
            "fill_null:referrer",
            "fix_timestamp_order:event_timestamp",
            "drop_duplicates:event_id",
        ],
    },
    {
        "source_name": "marketplace_seller_metrics",
        "schema_text": "seller_id:int64 | report_date:object | category:object | total_listings:int64 | active_listings:int64 | impressions:int64 | clicks:int64 | orders:int64 | revenue:object | avg_rating:float64 | review_count:int64 | return_rate:float64 | fulfillment_type:object",
        "mapping": {
            "fact_table": "seller_performance_fact",
            "dimensions": ["date_dim", "seller_dim", "category_dim", "fulfillment_dim"],
            "measures": ["total_listings", "active_listings", "impressions", "clicks", "orders", "revenue", "avg_rating", "review_count", "return_rate"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:revenue",
            "standardize_date_format:report_date",
            "fill_null:avg_rating",
            "remove_negative:return_rate",
            "normalize_text:fulfillment_type",
        ],
    },
    {
        "source_name": "cart_abandonment_events",
        "schema_text": "session_id:object | user_id:int64 | created_at:object | abandoned_at:object | product_id:int64 | product_name:object | cart_value:object | items_count:int64 | abandon_reason:object | recovery_email_sent:object | recovered:object",
        "mapping": {
            "fact_table": "cart_abandonment_fact",
            "dimensions": ["date_dim", "user_dim", "product_dim", "abandon_reason_dim"],
            "measures": ["cart_value", "items_count"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:cart_value",
            "standardize_date_format:created_at",
            "standardize_date_format:abandoned_at",
            "normalize_text:abandon_reason",
            "fill_null:abandon_reason",
            "normalize_text:recovered",
            "fix_timestamp_order:created_at",
        ],
    },
    # ── HR / Human Resources (2 examples) ────────────────────────────────────
    {
        "source_name": "hr_employee_payroll",
        "schema_text": "employee_id:int64 | hire_date:object | termination_date:object | first_name:object | last_name:object | department:object | job_title:object | employment_type:object | base_salary:object | bonus:object | overtime_hours:float64 | total_compensation:object | manager_id:int64 | office_location:object | performance_score:float64",
        "mapping": {
            "fact_table": "payroll_fact",
            "dimensions": ["date_dim", "employee_dim", "department_dim", "job_title_dim", "manager_dim", "location_dim"],
            "measures": ["base_salary", "bonus", "overtime_hours", "total_compensation", "performance_score"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:base_salary",
            "strip_currency_symbol:bonus",
            "strip_currency_symbol:total_compensation",
            "standardize_date_format:hire_date",
            "standardize_date_format:termination_date",
            "normalize_text:employment_type",
            "fill_null:termination_date",
            "fill_null:performance_score",
            "fix_inconsistency:total_compensation",
        ],
    },
    {
        "source_name": "hr_recruitment_pipeline",
        "schema_text": "application_id:int64 | applied_date:object | job_id:int64 | job_title:object | department:object | candidate_id:int64 | recruiter_id:int64 | stage:object | outcome:object | offer_amount:object | time_to_hire_days:int64 | source_channel:object",
        "mapping": {
            "fact_table": "recruitment_fact",
            "dimensions": ["date_dim", "job_dim", "candidate_dim", "recruiter_dim", "stage_dim", "channel_dim"],
            "measures": ["offer_amount", "time_to_hire_days"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:offer_amount",
            "standardize_date_format:applied_date",
            "normalize_text:stage",
            "normalize_text:outcome",
            "normalize_text:source_channel",
            "fill_null:offer_amount",
        ],
    },
    # ── Finance / Accounting (2 examples) ────────────────────────────────────
    {
        "source_name": "general_ledger_transactions",
        "schema_text": "journal_id:object | posting_date:object | account_code:object | account_name:object | cost_center:object | description:object | debit_amount:object | credit_amount:object | currency:object | exchange_rate:float64 | base_currency_amount:float64 | period:object | fiscal_year:int64 | posted_by:object",
        "mapping": {
            "fact_table": "gl_transaction_fact",
            "dimensions": ["date_dim", "account_dim", "cost_center_dim", "currency_dim", "fiscal_period_dim"],
            "measures": ["debit_amount", "credit_amount", "exchange_rate", "base_currency_amount"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:debit_amount",
            "strip_currency_symbol:credit_amount",
            "standardize_date_format:posting_date",
            "normalize_text:currency",
            "fill_null:exchange_rate",
            "drop_duplicates:journal_id",
            "fix_inconsistency:base_currency_amount",
        ],
    },
    {
        "source_name": "budget_vs_actual",
        "schema_text": "record_id:int64 | period:object | department:object | cost_center:object | account:object | budget_amount:object | actual_amount:object | variance:object | variance_pct:float64 | status:object | notes:object",
        "mapping": {
            "fact_table": "budget_actual_fact",
            "dimensions": ["period_dim", "department_dim", "cost_center_dim", "account_dim"],
            "measures": ["budget_amount", "actual_amount", "variance", "variance_pct"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:budget_amount",
            "strip_currency_symbol:actual_amount",
            "strip_currency_symbol:variance",
            "normalize_text:status",
            "fill_null:notes",
            "fix_inconsistency:variance",
        ],
    },
    # ── Logistics / Supply Chain (2 examples) ─────────────────────────────────
    {
        "source_name": "shipment_tracking",
        "schema_text": "shipment_id:object | ship_date:object | estimated_delivery:object | actual_delivery:object | origin_warehouse:object | destination:object | carrier:object | tracking_number:object | weight_kg:float64 | volume_m3:float64 | freight_cost:object | delivery_status:object | delay_reason:object",
        "mapping": {
            "fact_table": "shipment_fact",
            "dimensions": ["date_dim", "warehouse_dim", "destination_dim", "carrier_dim", "delay_reason_dim"],
            "measures": ["weight_kg", "volume_m3", "freight_cost"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:freight_cost",
            "standardize_date_format:ship_date",
            "standardize_date_format:estimated_delivery",
            "standardize_date_format:actual_delivery",
            "normalize_text:delivery_status",
            "fill_null:delay_reason",
            "fix_timestamp_order:ship_date",
            "remove_negative:weight_kg",
        ],
    },
    {
        "source_name": "inventory_movements",
        "schema_text": "movement_id:int64 | movement_date:object | warehouse_id:int64 | product_id:int64 | sku:object | movement_type:object | qty_in:int64 | qty_out:int64 | qty_balance:int64 | unit_value:object | total_value:object | batch_number:object | expiry_date:object",
        "mapping": {
            "fact_table": "inventory_fact",
            "dimensions": ["date_dim", "warehouse_dim", "product_dim", "movement_type_dim"],
            "measures": ["qty_in", "qty_out", "qty_balance", "unit_value", "total_value"],
        },
        "cleaning_rules": [
            "strip_currency_symbol:unit_value",
            "strip_currency_symbol:total_value",
            "standardize_date_format:movement_date",
            "standardize_date_format:expiry_date",
            "normalize_text:movement_type",
            "remove_negative:qty_balance",
            "fix_inconsistency:total_value",
            "drop_duplicates:movement_id",
        ],
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# RAGSchemaStore
# ─────────────────────────────────────────────────────────────────────────────

class RAGSchemaStore:
    """FAISS-backed adaptive few-shot memory for schema→star-schema mappings.

    Corresponds to Innovation #2 (Adaptive Few-Shot Memory) from the paper.
    Every time a mapping is validated (human-approved or high-confidence auto),
    it is added here.  Subsequent pipeline runs retrieve the k nearest mappings
    as few-shot prompt examples, improving LLM accuracy over time.

    Usage::

        store = RAGSchemaStore("data/rag_store")

        # After a human approves a mapping:
        store.add(source_name="dataset1", schema=schema_ctx,
                  mapping=result.to_dict(), approved_by_human=True)

        # Before building the next prompt:
        examples = store.retrieve(schema=new_ctx, k=3)
        few_shot_text = store.build_few_shot_prompt(examples)
    """

    DIM = 384  # all-MiniLM-L6-v2 output dimension

    def __init__(self, store_path: str = "data/rag_store") -> None:
        import faiss

        self._store_path = Path(store_path)
        self._store_path.mkdir(parents=True, exist_ok=True)
        self._index_file = self._store_path / "schema.index"
        self._meta_file = self._store_path / "schema.meta.json"
        self._metadata: List[dict] = []

        if self._index_file.exists():
            self._index = faiss.read_index(str(self._index_file))
            if self._meta_file.exists():
                self._metadata = json.loads(
                    self._meta_file.read_text(encoding="utf-8")
                )
            logger.info("RAGSchemaStore loaded: %d entries", len(self._metadata))
        else:
            self._index = faiss.IndexFlatL2(self.DIM)
            logger.info("RAGSchemaStore initialised (empty)")

    # ── Core API ──────────────────────────────────────────────────────────────

    def add(
        self,
        source_name: str,
        schema: Any,
        mapping: dict,
        approved_by_human: bool = False,
        cleaning_rules: Optional[List[str]] = None,
    ) -> None:
        """Index a new schema→mapping pair with optional cleaning rules.

        Parameters
        ----------
        source_name:
            Dataset identifier (e.g. ``"dataset1_retail_sales"``).
        schema:
            SchemaContext object, column list, or plain text describing the
            source schema.
        mapping:
            Star-schema mapping dict (keys: ``fact_table``, ``dimensions``,
            ``measures``).
        approved_by_human:
            ``True`` when a human reviewer validated this mapping.  These are
            prioritised during retrieval.
        cleaning_rules:
            List of rule strings in ``"rule_type:column"`` format, e.g.
            ``["standardize_date_format:order_date", "strip_currency_symbol:amount"]``.
            Used by CleaningAgent RAG retrieval.
        """
        schema_text = schema_to_text(schema)
        vec = embed_text(schema_text)
        self._index.add(np.expand_dims(vec, 0))
        self._metadata.append(
            {
                "source_name": source_name,
                "schema_text": schema_text,
                "mapping": mapping,
                "cleaning_rules": cleaning_rules or [],
                "approved_by_human": approved_by_human,
                "timestamp": time.time(),
            }
        )
        self._save()

    def retrieve(self, schema: Any, k: int = 3) -> List[dict]:
        """Retrieve the *k* most similar stored mappings.

        Human-approved mappings are surfaced first in the result list.
        Returns an empty list when the store contains no entries yet.

        Parameters
        ----------
        schema:
            Schema to search against (same types accepted as ``add()``).
        k:
            Number of candidates to retrieve.
        """
        if self._index.ntotal == 0:
            return []

        schema_text = schema_to_text(schema)
        vec = embed_text(schema_text)
        actual_k = min(k, self._index.ntotal)
        distances, indices = self._index.search(np.expand_dims(vec, 0), actual_k)

        results: List[dict] = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx < 0:
                continue
            meta = dict(self._metadata[idx])
            meta["distance"] = float(dist)
            results.append(meta)

        # Human-approved mappings bubble to the front
        results.sort(key=lambda x: (not x["approved_by_human"], x["distance"]))
        return results

    def build_few_shot_prompt(self, similar: List[dict]) -> str:
        """Format retrieved pairs as a few-shot prompt section.

        Example output::

            Here are examples of previously approved mappings:

            Example 1 (human-approved):
              Schema: date:datetime64 | product:object | amount:float64
              Mapping: {"fact_table": "sales_fact", ...}
              Cleaning rules applied: standardize_date_format:date, strip_currency_symbol:amount
        """
        if not similar:
            return ""
        lines = ["Here are examples of previously approved mappings:", ""]
        for i, item in enumerate(similar, 1):
            tag = "human-approved" if item["approved_by_human"] else "auto"
            lines.append(f"Example {i} ({tag}):")
            lines.append(f"  Schema: {item['schema_text']}")
            lines.append(f"  Mapping: {json.dumps(item['mapping'])}")
            rules = item.get("cleaning_rules", [])
            if rules:
                lines.append(f"  Cleaning rules applied: {', '.join(rules[:6])}")
            lines.append("")
        return "\n".join(lines)

    def build_cleaning_prompt(self, similar: List[dict]) -> str:
        """Format retrieved examples as a cleaning few-shot section for CleaningAgent."""
        if not similar:
            return ""
        useful = [ex for ex in similar if ex.get("cleaning_rules")]
        if not useful:
            return ""
        lines = ["\nSIMILAR CLEANING PATTERNS (from knowledge base — use as strong reference):\n"]
        for i, ex in enumerate(useful, 1):
            name = ex.get("source_name", f"example_{i}")
            schema = ex.get("schema_text", "")
            rules = ex.get("cleaning_rules", [])
            lines.append(f"  [{i}] Dataset: {name}")
            lines.append(f"      Schema columns: {schema[:120]}")
            lines.append(f"      Rules applied:  {', '.join(rules)}")
            lines.append("")
        return "\n".join(lines)

    def seed_with_defaults(self, force: bool = False) -> int:
        """Populate the store with curated domain-expert examples if empty.

        Parameters
        ----------
        force:
            If ``True``, reset the store and re-seed even if already populated.

        Returns
        -------
        int
            Number of examples added (0 if store was already populated and
            ``force=False``).
        """
        if self._index.ntotal > 0 and not force:
            logger.info("RAGSchemaStore already has %d entries — skipping seed", self._index.ntotal)
            return 0
        if force:
            self.reset()

        examples = _SEED_EXAMPLES
        for ex in examples:
            schema_text = ex["schema_text"]
            vec = embed_text(schema_text)
            self._index.add(np.expand_dims(vec, 0))
            self._metadata.append({
                "source_name":      ex["source_name"],
                "schema_text":      schema_text,
                "mapping":          ex["mapping"],
                "cleaning_rules":   ex["cleaning_rules"],
                "approved_by_human": True,
                "timestamp":        time.time(),
            })
        self._save()
        logger.info("RAGSchemaStore seeded with %d expert examples", len(examples))
        return len(examples)

    @property
    def size(self) -> int:
        """Number of entries in the store."""
        return self._index.ntotal

    def reset(self) -> None:
        """Remove all stored entries (useful for ablation studies)."""
        import faiss
        self._index = faiss.IndexFlatL2(self.DIM)
        self._metadata = []
        if self._index_file.exists():
            self._index_file.unlink()
        if self._meta_file.exists():
            self._meta_file.unlink()
        logger.info("RAGSchemaStore reset")

    # ── Persistence ───────────────────────────────────────────────────────────

    def _save(self) -> None:
        import faiss
        faiss.write_index(self._index, str(self._index_file))
        self._meta_file.write_text(
            json.dumps(self._metadata, indent=2), encoding="utf-8"
        )
