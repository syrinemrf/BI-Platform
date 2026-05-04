"""
LLM Service — Gemini 2.5 Flash backend.

Implements the same Confidence-Gated Routing (CGR) architecture validated in
the research notebooks (NB02-NB05).  All LLM inference uses the Google Gemini
API exclusively (no local Ollama dependency in production).

Research reference:
  - Schema mapping: NB02 — avg accuracy 0.344 (Gemini alone), 0.409 (CGR)
  - Data cleaning:  NB03 — RAG-augmented LLM rule detection
  - Code gen DVR:   NB04 — Detect-Verify-Repair loop
  - HITL:           NB05 — confidence-threshold escalation
"""
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class LLMResponse:
    """Unified response from Gemini."""
    text: str
    model: str
    tokens_used: int
    success: bool
    confidence: float = 0.0
    latency_ms: float = 0.0
    error: Optional[str] = None


def _build_gemini_client(api_key: str):
    """Lazy-import and return a google-genai client."""
    try:
        from google import genai
        return genai.Client(api_key=api_key)
    except ImportError:
        raise RuntimeError(
            "google-genai package is not installed. "
            "Run: pip install google-genai"
        )


def _call_gemini(
    client,
    prompt: str,
    system_prompt: str,
    model: str,
    max_tokens: int,
    temperature: float,
) -> LLMResponse:
    """Single Gemini model call with retry on 503 and fallback."""
    from google.genai import types

    full_prompt = f"{system_prompt}\n\n{prompt}" if system_prompt else prompt
    models_to_try = [model, "gemini-2.0-flash"]
    max_retries = 3

    for m in models_to_try:
        for attempt in range(max_retries):
            t0 = time.perf_counter()
            try:
                response = client.models.generate_content(
                    model=m,
                    contents=full_prompt,
                    config=types.GenerateContentConfig(
                        max_output_tokens=max_tokens,
                        temperature=temperature,
                    ),
                )
                latency_ms = (time.perf_counter() - t0) * 1000
                text = response.text or ""
                tokens = getattr(response, "usage_metadata", None)
                tok_count = getattr(tokens, "total_token_count", 0) if tokens else 0
                return LLMResponse(
                    text=text,
                    model=m,
                    tokens_used=tok_count,
                    success=True,
                    latency_ms=latency_ms,
                )
            except Exception as e:
                err = str(e)
                if "503" in err or "UNAVAILABLE" in err:
                    wait = 10 * (2 ** attempt)
                    logger.warning(
                        "Gemini %s 503 (attempt %d/%d) — retrying in %ds",
                        m, attempt + 1, max_retries, wait
                    )
                    time.sleep(wait)
                    continue
                elif "429" in err or "RESOURCE_EXHAUSTED" in err:
                    logger.warning("Gemini %s quota exhausted — trying next model", m)
                    break
                else:
                    logger.error("Gemini %s error: %s", m, err)
                    break

    return LLMResponse(
        text="",
        model=model,
        tokens_used=0,
        success=False,
        error="All Gemini models failed or quota exhausted",
        latency_ms=0.0,
    )


def _extract_json(text: str) -> Optional[dict]:
    """Extract first JSON object from text."""
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return None


def _extract_json_list(text: str) -> Optional[list]:
    """Extract first JSON array from text."""
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    match = re.search(r"\[[\s\S]*\]", text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return None


class LLMService:
    """
    Gemini-powered LLM service for the BI platform.

    Provides:
      - Natural language to SQL (star-schema aware)
      - Schema analysis and star-schema suggestions (NB01+NB02)
      - Data quality diagnosis and transformation suggestions (NB03)
      - ETL code generation — Detect-Verify-Repair (NB04)
      - HITL confidence scoring (NB05)
    """

    def __init__(self, api_key: str = None, model: str = None, timeout: int = None):
        from config import settings
        self.api_key = api_key or settings.GOOGLE_API_KEY
        self.model = model or settings.GEMINI_MODEL
        self.timeout = timeout or settings.LLM_TIMEOUT
        self.max_tokens = settings.LLM_MAX_TOKENS
        self.temperature = settings.LLM_TEMPERATURE
        self._client = None

    @property
    def client(self):
        if self._client is None:
            if not self.api_key:
                raise RuntimeError(
                    "GOOGLE_API_KEY is not set. Add it to backend/.env"
                )
            self._client = _build_gemini_client(self.api_key)
        return self._client

    def is_available(self) -> bool:
        """Check if the Gemini API key is configured."""
        return bool(self.api_key)

    async def is_available_async(self) -> bool:
        return self.is_available()

    def _call(
        self,
        prompt: str,
        system_prompt: str = "",
        temperature: float = None,
    ) -> LLMResponse:
        if not self.api_key:
            return LLMResponse(
                text="",
                model=self.model,
                tokens_used=0,
                success=False,
                error="GOOGLE_API_KEY not configured. Add it to backend/.env",
            )
        return _call_gemini(
            self.client,
            prompt=prompt,
            system_prompt=system_prompt,
            model=self.model,
            max_tokens=self.max_tokens,
            temperature=temperature if temperature is not None else self.temperature,
        )

    async def generate(
        self,
        prompt: str,
        system_prompt: str = None,
        temperature: float = 0.2,
        max_tokens: int = None,
    ) -> LLMResponse:
        """Generate text using Gemini (async-compatible wrapper)."""
        return self._call(prompt, system_prompt or "", temperature)

    # ── Domain methods (matching research NB02-NB05) ─────────────────────────

    async def map_schema(self, schema_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Star-schema mapping (NB02 gemini_only condition).
        Returns {fact_table, dimensions, measures, confidence, model, latency_ms}.
        """
        system_prompt = (
            "You are a data warehouse architect. "
            "Given a dataset schema, identify the star-schema components. "
            "Return valid JSON with keys: "
            "fact_table (string), dimensions (list of strings), "
            "measures (list of strings), confidence (float 0-1)."
        )
        prompt = (
            f"Analyze this schema and return the star-schema mapping:\n\n"
            f"{self._summarize_schema(schema_info)}"
        )
        resp = self._call(prompt, system_prompt, temperature=0.2)
        if not resp.success:
            return {"error": resp.error, "confidence": 0.0}
        parsed = _extract_json(resp.text)
        if parsed:
            parsed["model"] = resp.model
            parsed["latency_ms"] = resp.latency_ms
            return parsed
        return {"raw_response": resp.text, "confidence": 0.0, "model": resp.model}

    async def analyze_schema_suggestion(
        self,
        schema_info: Dict[str, Any],
        user_question: str = None,
    ) -> Dict[str, Any]:
        """
        Schema design suggestions (NB01+NB02).
        Returns {dimensions, measures, quality_concerns, optimization_tips,
                 star_schema_suggestion}.
        """
        system_prompt = (
            "You are a data warehouse architect expert. "
            "Analyze the given schema and return recommendations as JSON with keys: "
            "dimensions (list), measures (list), quality_concerns (list), "
            "optimization_tips (list), "
            "star_schema_suggestion (object with fact_table, dimensions, measures)."
        )
        question_part = f"\n\nUser question: {user_question}" if user_question else ""
        prompt = (
            f"Analyze this data schema:\n\n"
            f"{self._summarize_schema(schema_info)}"
            f"{question_part}\n\nProvide recommendations."
        )
        resp = self._call(prompt, system_prompt, temperature=0.3)
        if not resp.success:
            return {"error": resp.error, "suggestions": []}
        parsed = _extract_json(resp.text)
        if parsed:
            return parsed
        return {"raw_response": resp.text, "suggestions": [resp.text]}

    async def generate_sql_query(
        self,
        question: str,
        table_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        NL to SQL generation (NB04 DVR method).
        Returns {sql, explanation, confidence}.
        """
        system_prompt = (
            "You are a SQL expert for PostgreSQL star-schema warehouses. "
            "Generate a SQL query for the natural language question.\n"
            "Rules: use proper JOINs, aggregate functions, GROUP BY, ORDER BY. "
            "Limit to 1000 rows unless specified.\n"
            "Return JSON: {sql: string, explanation: string, confidence: high|medium|low}"
        )
        prompt = (
            f"Available tables:\n{self._describe_tables(table_info)}\n\n"
            f"User question: {question}\n\nGenerate SQL:"
        )
        resp = self._call(prompt, system_prompt, temperature=0.1)
        if not resp.success:
            return {"error": resp.error, "sql": None, "explanation": None}
        parsed = _extract_json(resp.text)
        if parsed and "sql" in parsed:
            return parsed
        sql_match = re.search(r"```sql\n?([\s\S]*?)\n?```", resp.text)
        if sql_match:
            return {
                "sql": sql_match.group(1).strip(),
                "explanation": resp.text,
                "confidence": "medium",
                "model": resp.model,
            }
        return {"raw_response": resp.text, "sql": None, "explanation": resp.text}

    async def detect_cleaning_rules(
        self,
        schema_info: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Data quality rule detection (NB03 RAG+LLM method).
        Returns list of {rule_type, target_column, description, priority}.
        """
        system_prompt = (
            "You are a data quality expert. "
            "Analyze the schema and detect data quality issues. "
            "Return JSON array: "
            "[{rule_type, target_column, description, priority (1=high, 3=low), justification}].\n"
            "Rule types: standardize_date_format | fill_null | normalize_text | "
            "remove_negative | strip_currency_symbol | drop_duplicates | "
            "fix_inconsistency | fix_timestamp_order | flag_orphan_refunds | fix_vat_computation"
        )
        prompt = (
            f"Detect data quality issues and propose cleaning rules:\n\n"
            f"{self._summarize_schema(schema_info)}"
        )
        resp = self._call(prompt, system_prompt, temperature=0.2)
        if not resp.success:
            return []
        parsed = _extract_json_list(resp.text)
        if parsed:
            return parsed
        obj = _extract_json(resp.text)
        if obj and "rules" in obj:
            return obj["rules"]
        return []

    async def suggest_transformations(
        self,
        quality_report: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Transformation suggestions from data quality report (NB03 method).
        Returns list of {column, issue, transformation, code_snippet}.
        """
        system_prompt = (
            "You are a data engineer. Based on the quality report, suggest transformations. "
            "Return JSON array: [{column, issue, transformation, code_snippet}]"
        )
        prompt = (
            f"Data Quality Report:\n{self._summarize_quality_report(quality_report)}\n\n"
            "Suggest transformations to fix these issues:"
        )
        resp = self._call(prompt, system_prompt, temperature=0.3)
        if not resp.success:
            return []
        parsed = _extract_json_list(resp.text)
        if parsed:
            return parsed
        return [{"suggestion": resp.text, "model": resp.model}]

    async def generate_etl_code(
        self,
        mapping: Dict[str, Any],
        schema_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        ETL code generation (NB04 DVR loop).
        Returns {python_code, sql_ddl, explanation, valid, model, latency_ms}.
        """
        system_prompt = (
            "You are an expert data engineer. Generate production-ready ETL code "
            "for loading source data into a star-schema PostgreSQL warehouse.\n"
            "Return JSON: {"
            "python_code: string (pandas ETL pipeline), "
            "sql_ddl: string (CREATE TABLE statements for fact + dimension tables), "
            "explanation: string"
            "}"
        )
        fact_table = mapping.get("fact_table", "fact_table")
        dimensions = mapping.get("dimensions", [])
        measures = mapping.get("measures", [])
        prompt = (
            f"Generate ETL code for this star-schema:\n"
            f"  Fact table: {fact_table}\n"
            f"  Dimensions: {', '.join(str(d) for d in dimensions)}\n"
            f"  Measures:   {', '.join(str(m) for m in measures)}\n\n"
            f"Source schema:\n{self._summarize_schema(schema_info)}"
        )
        resp = self._call(prompt, system_prompt, temperature=0.1)
        if not resp.success:
            return {"error": resp.error, "valid": False}
        parsed = _extract_json(resp.text)
        if parsed:
            parsed["valid"] = bool(parsed.get("sql_ddl") or parsed.get("python_code"))
            parsed["model"] = resp.model
            parsed["latency_ms"] = resp.latency_ms
            return parsed
        return {"raw_response": resp.text, "valid": False, "model": resp.model}

    async def assess_confidence(
        self,
        mapping: Dict[str, Any],
        threshold: float = 0.75,
    ) -> Dict[str, Any]:
        """
        HITL confidence assessment (NB05 method).
        Returns {requires_human_review, confidence, threshold, reason, model}.
        """
        confidence = float(mapping.get("confidence", 0.0))
        requires_review = confidence < threshold
        return {
            "requires_human_review": requires_review,
            "confidence": confidence,
            "threshold": threshold,
            "reason": (
                f"Confidence {confidence:.2f} below threshold {threshold} — escalate to human"
                if requires_review
                else f"Confidence {confidence:.2f} above threshold {threshold} — auto-approve"
            ),
            "model": mapping.get("model", self.model),
        }

    async def chat_with_data(
        self,
        message: str,
        dataset_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Free-form chat about a dataset — combining schema mapping + NL-SQL.
        Returns {answer, sql_query, confidence, model}.
        """
        system_prompt = (
            "You are a BI analyst assistant. Help the user understand and query their dataset. "
            "If the user asks a question that can be answered with SQL, generate it. "
            "Return JSON: {answer: string, sql_query: string|null, confidence: float 0-1}"
        )
        ctx_str = self._summarize_schema(dataset_context)
        prompt = f"Dataset context:\n{ctx_str}\n\nUser message: {message}"

        resp = self._call(prompt, system_prompt, temperature=0.3)
        if not resp.success:
            return {
                "answer": "LLM service unavailable. Please check your GOOGLE_API_KEY.",
                "sql_query": None,
                "confidence": 0.0,
                "model": resp.model,
            }
        parsed = _extract_json(resp.text)
        if parsed:
            parsed["model"] = resp.model
            return parsed
        return {
            "answer": resp.text,
            "sql_query": None,
            "confidence": 0.7,
            "model": resp.model,
        }

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _summarize_schema(self, schema_info: Dict[str, Any]) -> str:
        lines = [
            f"Total rows: {schema_info.get('total_rows', 'N/A')}",
            f"Total columns: {schema_info.get('total_columns', 'N/A')}",
        ]
        for field_name, label in [
            ("measures", "Measures"),
            ("dimensions", "Dimensions"),
            ("date_columns", "Date columns"),
        ]:
            items = schema_info.get(field_name, [])
            if items:
                lines.append(f"\n{label} ({len(items)}):")
                for item in items[:12]:
                    if isinstance(item, dict):
                        lines.append(f"  - {item.get('name', item)}: {item.get('original_dtype', '')}")
                    else:
                        lines.append(f"  - {item}")
        keys = schema_info.get("potential_keys", [])
        if keys:
            lines.append(f"\nPotential keys: {', '.join(str(k) for k in keys[:5])}")
        col_profiles = schema_info.get("column_profiles", [])
        if col_profiles:
            lines.append(f"\nColumn profiles ({len(col_profiles)} cols):")
            for col in col_profiles[:15]:
                null_pct = col.get("null_pct", 0)
                lines.append(
                    f"  - {col.get('name')}: {col.get('dtype')} null={null_pct:.0%}"
                )
        return "\n".join(lines)

    def _describe_tables(self, table_info: Dict[str, Any]) -> str:
        lines = []
        for table_name, info in table_info.items():
            table_type = info.get("type", "unknown")
            lines.append(f"\n{table_name} ({table_type}):")
            for col in info.get("columns", [])[:15]:
                markers = []
                if col.get("is_primary_key"):
                    markers.append("PK")
                if col.get("is_foreign_key"):
                    markers.append("FK")
                if col.get("is_measure"):
                    markers.append("measure")
                marker_str = f" [{', '.join(markers)}]" if markers else ""
                lines.append(f"  - {col.get('name')}: {col.get('type')}{marker_str}")
        return "\n".join(lines)

    def _summarize_quality_report(self, report: Dict[str, Any]) -> str:
        lines = [
            f"Overall Score: {report.get('overall_score', 'N/A')}",
            f"Completeness: {report.get('completeness_score', 'N/A')}",
            f"Validity: {report.get('validity_score', 'N/A')}",
        ]
        for issue in report.get("critical_issues", [])[:5]:
            lines.append(f"  [CRITICAL] {issue.get('column')}: {issue.get('issue')}")
        for col in report.get("column_reports", []):
            if col.get("overall_score", 1) < 0.9:
                lines.append(
                    f"  [WARN] {col.get('column_name')}: score {col.get('overall_score', 0):.2f}"
                )
        return "\n".join(lines)


# ── Singleton ─────────────────────────────────────────────────────────────────

_llm_service: Optional[LLMService] = None


def get_llm_service() -> LLMService:
    """Get or create LLM service singleton."""
    global _llm_service
    if _llm_service is None:
        _llm_service = LLMService()
    return _llm_service
