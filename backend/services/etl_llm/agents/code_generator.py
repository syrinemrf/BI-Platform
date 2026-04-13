"""
ETL Code Generation Agent (Layer 2, Agent 3)
=============================================
Generates Python extraction/transformation code and SQL loading statements,
with a Divide-Verify-Refine (DVR) self-correction loop.

Reference: [Zhang et al. 2024] — Divide-Verify-Refine (DVR) framework:
  "Doubles adherence to complex instructions without retraining, using
  tool-based constraint decomposition and iterative response refinement."
"""

from __future__ import annotations

import ast
import json
import logging
from typing import Any

import httpx
from pydantic import BaseModel, Field

from services.etl_llm.agents.cleaning_agent import CleaningPlan
from services.etl_llm.agents.schema_mapper import SchemaMappingResult
from services.etl_llm.profiling.schema_profiler import SchemaContext

logger = logging.getLogger(__name__)

MAX_CORRECTION_ATTEMPTS = 3


# ── Pydantic models ─────────────────────────────────────────────


class GeneratedETLCode(BaseModel):
    """Code artefacts produced by the ETL Code Generator."""

    extraction_code: str = ""
    transformation_code: str = ""
    loading_code: str = ""
    full_pipeline_code: str = ""
    correction_attempts: int = 0
    final_confidence: float = Field(ge=0.0, le=1.0, default=0.0)


# ── Agent ────────────────────────────────────────────────────────


class ETLCodeGeneratorAgent:
    """ETL Code Generator — Agent 3 of the LLM pipeline.

    Generates Python + SQL code and applies DVR self-correction to fix
    syntax errors before returning.

    Reference: [Zhang et al. 2024] — DVR self-correction loop.
    """

    def __init__(
        self,
        ollama_url: str = "http://localhost:11434",
        anthropic_key: str | None = None,
    ) -> None:
        self._ollama_url = ollama_url.rstrip("/")
        self._anthropic_key = anthropic_key

    # ── code generation ──────────────────────────────────────────

    def generate_code(
        self,
        schema: SchemaContext,
        mapping: SchemaMappingResult,
        cleaning_plan: CleaningPlan,
    ) -> GeneratedETLCode:
        """Generate ETL code (Python + SQL) from schema, mapping, and cleaning plan."""
        prompt = self._build_generation_prompt(schema, mapping, cleaning_plan)

        try:
            parsed = self._call_llm(prompt)
        except Exception as e:
            logger.error(f"LLM code generation failed: {e}")
            return GeneratedETLCode()

        return GeneratedETLCode(
            extraction_code=parsed.get("extraction_code", ""),
            transformation_code=parsed.get("transformation_code", ""),
            loading_code=parsed.get("loading_code", ""),
            full_pipeline_code=parsed.get("full_pipeline_code", ""),
            final_confidence=float(parsed.get("confidence", 0.5)),
        )

    def _build_generation_prompt(
        self,
        schema: SchemaContext,
        mapping: SchemaMappingResult,
        plan: CleaningPlan,
    ) -> str:
        cols = ", ".join(c.name for c in schema.columns)
        fact = mapping.fact_table.model_dump()
        dims = [d.model_dump() for d in mapping.dimension_tables]
        rules = [r.model_dump() for r in plan.rules]

        return f"""Generate ETL code for this pipeline:

Source columns: {cols}
Fact table: {json.dumps(fact)}
Dimensions: {json.dumps(dims)}
Cleaning rules: {json.dumps(rules)}

Generate:
1. Python extraction code (reads CSV into DataFrame)
2. Python transformation code (applies mapping + cleaning)
3. SQL INSERT statements for star schema tables

Respond ONLY with JSON:
{{
  "extraction_code": "python code string",
  "transformation_code": "python code string",
  "loading_code": "SQL statements string",
  "full_pipeline_code": "combined python script",
  "confidence": 0.0
}}"""

    def _call_llm(self, prompt: str) -> dict:
        try:
            response = httpx.post(
                f"{self._ollama_url}/api/generate",
                json={"model": "llama3:8b", "prompt": prompt, "format": "json", "stream": False},
                timeout=120,
            )
            response.raise_for_status()
            return json.loads(response.json().get("response", "{}"))
        except Exception:
            if self._anthropic_key:
                import anthropic

                client = anthropic.Anthropic(api_key=self._anthropic_key)
                msg = client.messages.create(
                    model="claude-3-5-sonnet-20241022",
                    max_tokens=4096,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = msg.content[0].text
                start, end = text.find("{"), text.rfind("}") + 1
                if start >= 0 and end > start:
                    text = text[start:end]
                return json.loads(text)
            raise

    # ── validation ───────────────────────────────────────────────

    @staticmethod
    def validate_sql(sql: str) -> tuple[bool, str]:
        """Validate SQL syntax using sqlfluff.

        Returns (is_valid, error_message).
        Only parse errors (PRS codes) are treated as failures.
        """
        if not sql.strip():
            return True, ""
        try:
            import sqlfluff

            result = sqlfluff.lint(sql + "\n", dialect="ansi")
            errors = [v for v in result if v["code"].startswith("PRS")]
            if errors:
                msg = "; ".join(f"{e['code']}: {e['description']}" for e in errors[:3])
                return False, msg
            return True, ""
        except Exception as e:
            return False, str(e)

    @staticmethod
    def validate_python(code: str) -> tuple[bool, str]:
        """Validate Python syntax using ``ast.parse()``.

        Returns (is_valid, error_message).
        """
        if not code.strip():
            return True, ""
        try:
            ast.parse(code)
            return True, ""
        except SyntaxError as e:
            return False, f"SyntaxError at line {e.lineno}: {e.msg}"

    # ── self-correction (DVR) ────────────────────────────────────

    def self_correct(
        self, code: str, error: str, language: str, attempt: int
    ) -> str:
        """Apply DVR self-correction: send code + error back to LLM.

        Reference: [Zhang et al. 2024] — iterative response refinement.
        """
        prompt = (
            f"The following {language} code has this error: {error}\n"
            f"Original code:\n{code}\n\n"
            f"Fix ONLY the error. Return ONLY the corrected code. No explanation."
        )
        try:
            parsed = self._call_llm(prompt)
            return parsed.get("corrected_code", parsed.get("code", code))
        except Exception:
            return code

    def run_with_self_correction(
        self,
        schema: SchemaContext,
        mapping: SchemaMappingResult,
        plan: CleaningPlan,
    ) -> GeneratedETLCode:
        """Full DVR loop: generate → validate → self-correct (max 3 attempts).

        Reference: [Zhang et al. 2024] — DVR: tool-based constraint
        decomposition and iterative response refinement.
        """
        code = self.generate_code(schema, mapping, plan)
        correction_attempts = 0

        # Validate & correct SQL
        for _ in range(MAX_CORRECTION_ATTEMPTS):
            valid, err = self.validate_sql(code.loading_code)
            if valid:
                break
            correction_attempts += 1
            code.loading_code = self.self_correct(
                code.loading_code, err, "SQL", correction_attempts
            )

        # Validate & correct Python
        for _ in range(MAX_CORRECTION_ATTEMPTS):
            valid, err = self.validate_python(code.full_pipeline_code)
            if valid:
                break
            correction_attempts += 1
            code.full_pipeline_code = self.self_correct(
                code.full_pipeline_code, err, "Python", correction_attempts
            )

        code.correction_attempts = correction_attempts
        return code
