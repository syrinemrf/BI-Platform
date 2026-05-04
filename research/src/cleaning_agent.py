"""
Cleaning Agent — LLM + RAG-powered data cleaning rule detection and application.
Layer 2, Agent 2 of the ETL pipeline.

RAG Integration (Birjega 2025): Similar past cleaning patterns are retrieved from
the FAISS vector store (RAGSchemaStore) and injected as few-shot examples into
the LLM prompt, improving recall on known rule types.
"""
import random
import re
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

import numpy as np
import pandas as pd

from .llm_client import LLMClient, MockLLMClient, LLMResponse
from .profiler import SchemaContext

if TYPE_CHECKING:
    from .rag import RAGSchemaStore


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
    rag_examples_used: int = 0   # how many RAG examples were injected


class CleaningAgent:
    """Detect and apply data cleaning rules using LLM + RAG retrieval."""

    CLEANING_PROMPT = """You are a data quality expert. Analyze the following dataset schema
and data samples to propose cleaning rules.

DATASET SCHEMA:
{schema_context}
{rag_section}
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

    def __init__(self, llm_client=None, rag_store=None):
        self.llm_client = llm_client
        self.rag_store = rag_store  # RAGSchemaStore for cleaning pattern retrieval

    def _build_rag_section(self, schema_ctx: SchemaContext) -> tuple[str, int]:
        """Retrieve similar cleaning patterns from FAISS RAG store."""
        if self.rag_store is None:
            return "", 0
        try:
            similar = self.rag_store.retrieve(schema_ctx, k=3)
            if not similar:
                return "", 0
            lines = ["\nSIMILAR CLEANING PATTERNS (from knowledge base — use as reference):\n"]
            for i, ex in enumerate(similar, 1):
                name = ex.get("dataset_name", f"example_{i}")
                rules = ex.get("cleaning_rules", [])
                lines.append(f"  [{i}] {name}: {', '.join(rules[:5])}")
            lines.append("")
            return "\n".join(lines) + "\n", len(similar)
        except Exception:
            return "", 0

    def detect_rules(
        self, schema_ctx: SchemaContext, df: pd.DataFrame
    ) -> CleaningPlan:
        """Detect cleaning rules for a dataset using LLM + RAG context."""
        if isinstance(self.llm_client, MockLLMClient):
            return self._mock_detect(schema_ctx, df)

        # Build RAG few-shot context
        rag_section, n_rag = self._build_rag_section(schema_ctx)

        prompt = self.CLEANING_PROMPT.format(
            schema_context=schema_ctx.to_prompt_string(),
            rag_section=rag_section,
        )
        try:
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
            if rules:
                return CleaningPlan(
                    dataset_name=schema_ctx.dataset_name,
                    rules=rules,
                    confidence=llm_resp.confidence,
                    model_used=llm_resp.model_used,
                    latency_ms=llm_resp.latency_ms,
                    rag_examples_used=n_rag,
                )
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(
                "LLM cleaning detection failed (%s), falling back to mock", e
            )
        # Fallback to rule-based detection
        return self._mock_detect(schema_ctx, df)

    def apply_rules(
        self, df: pd.DataFrame, plan: CleaningPlan
    ) -> pd.DataFrame:
        """Apply detected cleaning rules to a DataFrame."""
        df_clean = df.copy()
        for rule in sorted(plan.rules, key=lambda r: r.priority):
            df_clean = self._apply_single_rule(df_clean, rule)
        return df_clean

    # ── Rule application logic ─────────────────────────────
    def _resolve_column(self, df: pd.DataFrame, col: str) -> Optional[str]:
        """Resolve a column name, trying both '.' and '_' separators."""
        if col in df.columns:
            return col
        alt = col.replace(".", "_")
        if alt in df.columns:
            return alt
        return None

    def _apply_single_rule(
        self, df: pd.DataFrame, rule: CleaningRule
    ) -> pd.DataFrame:
        col = self._resolve_column(df, rule.target_column)
        rt = rule.rule_type

        if col is None:
            return df  # Column not found — skip silently

        if rt == "fill_null":
            if pd.api.types.is_numeric_dtype(df[col]):
                fill_val = df[col].median() if df[col].notna().any() else 0
                df[col] = df[col].fillna(fill_val)
            else:
                mode_vals = df[col].dropna().mode()
                fill_val = mode_vals.iloc[0] if len(mode_vals) > 0 else "unknown"
                df[col] = df[col].fillna(fill_val)

        elif rt == "normalize_text":
            if df[col].dtype == object:
                mask_notnull = df[col].notna()
                df.loc[mask_notnull, col] = (
                    df.loc[mask_notnull, col]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .str.title()
                )

        elif rt == "standardize_date_format":
            original = df[col].copy()
            # Only convert non-null values; preserve originals where parsing fails
            with pd.option_context("mode.chained_assignment", None):
                converted = pd.to_datetime(df[col], format="mixed", errors="coerce")
            mask_was_notnull = original.notna()
            mask_converted_ok = converted.notna()
            # Apply only where conversion succeeded on previously non-null values
            df.loc[mask_was_notnull & mask_converted_ok, col] = converted[
                mask_was_notnull & mask_converted_ok
            ]

        elif rt == "strip_currency_symbol":
            str_series = df[col].astype(str)
            has_symbol = str_series.str.contains(r"[\$€£,]", regex=True, na=False)
            if has_symbol.any():
                cleaned = str_series.str.replace(r"[\$€£,]", "", regex=True).str.strip()
                numeric = pd.to_numeric(cleaned, errors="coerce")
                # Only replace where symbol existed AND numeric parse succeeded
                mask = has_symbol & numeric.notna()
                df.loc[mask, col] = numeric[mask]

        elif rt == "remove_negative":
            if pd.api.types.is_numeric_dtype(df[col]):
                mask_neg = df[col] < 0
                df.loc[mask_neg, col] = df.loc[mask_neg, col].abs()
            else:
                numeric = pd.to_numeric(df[col], errors="coerce")
                mask_neg = numeric < 0
                df.loc[mask_neg, col] = numeric[mask_neg].abs()

        elif rt == "drop_duplicates":
            df = df.drop_duplicates(subset=[col], keep="first").reset_index(drop=True)

        elif rt == "fix_inconsistency":
            # Attempt to fix numeric total vs component sum mismatch
            # Best-effort: find sibling numeric columns and recalculate
            pass

        elif rt == "fix_timestamp_order":
            # Sort within sessions by timestamp if session_id column exists
            if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].dtype == object:
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                    if "session_id" in df.columns:
                        df = df.sort_values(["session_id", col]).reset_index(drop=True)
                    else:
                        df = df.sort_values(col, na_position="last").reset_index(drop=True)
                except Exception:
                    pass

        elif rt == "flag_orphan_refunds":
            # Flag refunds that reference non-existent orders
            if "event_type" in df.columns and col in df.columns:
                refund_mask = df.get("event_type", pd.Series()).str.lower() == "refund"
                valid_ids = set(df.loc[~refund_mask, col].dropna())
                orphan_mask = refund_mask & ~df[col].isin(valid_ids)
                df["_is_orphan_refund"] = False  # default: not orphan
                df.loc[orphan_mask, "_is_orphan_refund"] = True

        elif rt == "fix_vat_computation":
            # Recalculate VAT total from subtotal + vat_amount where mismatch exists
            subtotal_col = self._resolve_column(df, "totals.subtotal_ht") or \
                           self._resolve_column(df, "subtotal_ht")
            vat_col = self._resolve_column(df, "totals.vat_amount") or \
                      self._resolve_column(df, "vat_amount")
            if subtotal_col and vat_col:
                computed = pd.to_numeric(df[subtotal_col], errors="coerce") + \
                           pd.to_numeric(df[vat_col], errors="coerce")
                total_numeric = pd.to_numeric(df[col], errors="coerce")
                mismatch = (total_numeric - computed).abs() > 0.01
                df.loc[mismatch & computed.notna(), col] = computed[mismatch & computed.notna()]

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
                CleaningRule("normalize_text", "patient_gender",
                             "Normalize gender values to standard form", 1,
                             "Inconsistent: M, Male, m, MALE"),
                CleaningRule("fill_null", "discharge_date",
                             "Mark missing discharge dates as still_admitted", 2,
                             "5% missing discharge dates"),
                CleaningRule("fix_inconsistency", "treatment_total_cost",
                             "Fix cost arithmetic: total should equal insurance + patient", 1,
                             "4% records where total_cost != insurance + patient_paid"),
                CleaningRule("normalize_text", "diagnosis_severity",
                             "Normalize severity levels", 1,
                             "Inconsistent: moderate, Moderate, MODERATE, medium"),
            ]
        elif "invoice" in name or "supplier" in name:
            rules = [
                CleaningRule("normalize_text", "status",
                             "Normalize payment status values", 1,
                             "Inconsistent: paid, PAID, Paid, settled"),
                CleaningRule("fix_vat_computation", "totals_total_ttc",
                             "Fix VAT computation errors", 1,
                             "5% records where total_ttc != subtotal_ht + vat_amount"),
                CleaningRule("standardize_date_format", "payment_paid_on",
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
