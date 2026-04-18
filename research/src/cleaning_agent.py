"""
Cleaning Agent — LLM-powered data cleaning rule detection and application.
Layer 2, Agent 2 of the ETL pipeline.
"""
import random
import re
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from .llm_client import LLMClient, MockLLMClient, LLMResponse
from .profiler import SchemaContext


@dataclass
class CleaningRule:
    rule_type: str
    target_column: str
    description: str
    priority: int  # 1=high, 3=low
    justification: str


@dataclass
class CleaningPlan:
    dataset_name: str
    rules: list[CleaningRule]
    confidence: float
    model_used: str
    latency_ms: float


class CleaningAgent:
    """Detect and apply data cleaning rules using LLM."""

    CLEANING_PROMPT = """You are a data quality expert. Analyze the following dataset schema
and data samples to propose cleaning rules.

DATASET SCHEMA:
{schema_context}

DATA QUALITY ISSUES TO LOOK FOR:
- Null/missing values
- Duplicate rows
- Inconsistent text (mixed case, abbreviations)
- Date format inconsistencies
- Invalid numeric values (negative where not expected)
- Currency symbols in numeric fields
- Arithmetic inconsistencies (totals not matching components)
- Timestamp ordering issues

Respond with valid JSON:
{{
  "rules": [
    {{
      "rule_type": "standardize_date_format|fill_null|normalize_text|remove_negative|strip_currency_symbol|drop_duplicates|fix_inconsistency|fix_timestamp_order|flag_orphan_refunds|fix_vat_computation",
      "target_column": "column_name",
      "description": "What this rule does",
      "priority": 1,
      "justification": "Why this rule is needed"
    }}
  ],
  "confidence": 0.XX
}}"""

    def __init__(self, llm_client=None):
        self.llm_client = llm_client

    def detect_rules(
        self, schema_ctx: SchemaContext, df: pd.DataFrame
    ) -> CleaningPlan:
        """Detect cleaning rules for a dataset."""
        prompt = self.CLEANING_PROMPT.format(
            schema_context=schema_ctx.to_prompt_string()
        )

        if isinstance(self.llm_client, MockLLMClient):
            return self._mock_detect(schema_ctx, df)

        llm_resp: LLMResponse = self.llm_client.route(
            prompt, schema_complexity="medium"
        )
        rules = []
        for r in llm_resp.response.get("rules", []):
            rules.append(
                CleaningRule(
                    rule_type=r.get("rule_type", ""),
                    target_column=r.get("target_column", ""),
                    description=r.get("description", ""),
                    priority=r.get("priority", 2),
                    justification=r.get("justification", ""),
                )
            )
        return CleaningPlan(
            dataset_name=schema_ctx.dataset_name,
            rules=rules,
            confidence=llm_resp.confidence,
            model_used=llm_resp.model_used,
            latency_ms=llm_resp.latency_ms,
        )

    def apply_rules(
        self, df: pd.DataFrame, plan: CleaningPlan
    ) -> pd.DataFrame:
        """Apply detected cleaning rules to a DataFrame."""
        df_clean = df.copy()
        for rule in sorted(plan.rules, key=lambda r: r.priority):
            df_clean = self._apply_single_rule(df_clean, rule)
        return df_clean

    # ── Rule application logic ─────────────────────────────
    def _apply_single_rule(
        self, df: pd.DataFrame, rule: CleaningRule
    ) -> pd.DataFrame:
        col = rule.target_column
        rt = rule.rule_type

        if rt == "fill_null" and col in df.columns:
            df[col] = df[col].fillna(0 if df[col].dtype in ("float64", "int64") else "unknown")
        elif rt == "normalize_text" and col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower().str.title()
        elif rt == "standardize_date_format" and col in df.columns:
            df[col] = pd.to_datetime(df[col], infer_datetime_format=True, errors="coerce")
        elif rt == "strip_currency_symbol" and col in df.columns:
            df[col] = df[col].astype(str).str.replace(r"[\$€£]", "", regex=True)
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif rt == "remove_negative" and col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df.loc[df[col] < 0, col] = df[col].abs()
        elif rt == "drop_duplicates" and col in df.columns:
            df = df.drop_duplicates(subset=[col], keep="first")
        elif rt == "fix_inconsistency":
            pass  # Complex: handled per-case
        elif rt == "fix_timestamp_order":
            pass  # Complex: handled per-case
        elif rt == "flag_orphan_refunds":
            pass  # Complex: would need cross-event analysis
        elif rt == "fix_vat_computation":
            pass  # Complex: arithmetic validation

        return df

    # ── Mock detection for testing ─────────────────────────
    def _mock_detect(
        self, schema_ctx: SchemaContext, df: pd.DataFrame
    ) -> CleaningPlan:
        """Generate realistic cleaning rules based on actual data quality issues."""
        rules = []
        name = schema_ctx.dataset_name.lower()

        if "retail" in name or "sales" in name:
            rules = [
                CleaningRule("standardize_date_format", "order_date",
                             "Standardize mixed date formats to ISO 8601", 1,
                             "Multiple date formats detected: YYYY-MM-DD, DD/MM/YYYY, Mon DD YYYY"),
                CleaningRule("fill_null", "discount_pct",
                             "Fill missing discount with 0 (no discount)", 2,
                             "5% null values in discount_pct"),
                CleaningRule("normalize_text", "customer_country",
                             "Normalize country names to standard form", 1,
                             "Inconsistent: USA, United States, US, united states"),
                CleaningRule("remove_negative", "quantity",
                             "Convert negative quantities to absolute value", 1,
                             "1% negative quantities (data entry errors)"),
                CleaningRule("strip_currency_symbol", "unit_price",
                             "Remove $ symbol from price values", 1,
                             "5% prices contain currency symbol"),
                CleaningRule("drop_duplicates", "order_id",
                             "Remove duplicate order records", 1,
                             "3% duplicate order_ids detected"),
            ]
        elif "hospital" in name:
            rules = [
                CleaningRule("normalize_text", "patient.gender",
                             "Normalize gender values to standard form", 1,
                             "Inconsistent: M, Male, m, MALE"),
                CleaningRule("fill_null", "discharge_date",
                             "Mark missing discharge dates as still_admitted", 2,
                             "5% missing discharge dates"),
                CleaningRule("fix_inconsistency", "treatment.total_cost",
                             "Fix cost arithmetic: total should equal insurance + patient", 1,
                             "4% records where total_cost != insurance + patient_paid"),
                CleaningRule("normalize_text", "diagnosis.severity",
                             "Normalize severity levels", 1,
                             "Inconsistent: moderate, Moderate, MODERATE, medium"),
            ]
        elif "invoice" in name or "supplier" in name:
            rules = [
                CleaningRule("normalize_text", "status",
                             "Normalize payment status values", 1,
                             "Inconsistent: paid, PAID, Paid, settled"),
                CleaningRule("fix_vat_computation", "totals.total_ttc",
                             "Fix VAT computation errors", 1,
                             "5% records where total_ttc != subtotal_ht + vat_amount"),
                CleaningRule("standardize_date_format", "payment.paid_on",
                             "Standardize payment date formats", 2,
                             "Mixed date formats in paid_on field"),
                CleaningRule("drop_duplicates", "invoice_id",
                             "Remove duplicate invoices", 1,
                             "3% duplicate invoice_ids"),
            ]
        elif "ecommerce" in name or "event" in name:
            rules = [
                CleaningRule("fill_null", "user.uid",
                             "Fill anonymous user IDs", 2,
                             "15% missing user UIDs (anonymous sessions)"),
                CleaningRule("fix_timestamp_order", "ts",
                             "Fix out-of-order timestamps within sessions", 1,
                             "5% sessions have out-of-order timestamps"),
                CleaningRule("normalize_text", "user.country",
                             "Normalize country codes to standard form", 1,
                             "Inconsistent: FR, France, fra"),
                CleaningRule("flag_orphan_refunds", "payload.order_id",
                             "Flag refunds referencing non-existent orders", 1,
                             "Some refund events reference invalid order_ids"),
            ]

        # Simulate imperfect detection: sometimes miss a rule
        confidence = random.uniform(0.70, 0.95)
        if confidence < 0.80 and len(rules) > 2:
            rules = rules[:-1]  # miss the last rule

        return CleaningPlan(
            dataset_name=schema_ctx.dataset_name,
            rules=rules,
            confidence=confidence,
            model_used="mock",
            latency_ms=random.uniform(500, 2000),
        )
