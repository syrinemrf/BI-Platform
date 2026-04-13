"""
Schema Mapping Agent (Layer 2, Agent 1)
========================================
Maps source columns to a star schema (fact + dimension tables) using
LLM-powered structured reasoning with few-shot retrieval.

Innovation #1 — Confidence-Gated Routing:
  LLaMA 3 8B handles simple schemas (confidence > 0.75), Claude API
  handles complex ones. This is the first system to benchmark the
  optimal routing threshold for heterogeneous ETL schema mapping.

References:
  [Annam 2025] — LLMs for schema mapping in enterprise ETL.
  [Colombo et al. 2025] — LLM-assisted ETL pipeline, fine-tuning approach.
  [Talaei et al. 2024] — CHESS: column filtering / table selection paradigm.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx
from pydantic import BaseModel, Field

from services.etl_llm.profiling.schema_profiler import SchemaContext
from services.etl_llm.rag.schema_store import SchemaVectorStore

logger = logging.getLogger(__name__)

# Threshold 0.75 determined empirically — see evaluation/
# Results in Section 5 of the paper (Table 3)
CONFIDENCE_THRESHOLD = 0.75


# ── Pydantic output models ──────────────────────────────────────


class FactTableSpec(BaseModel):
    """Specification for the fact table in a star schema."""
    name: str
    measures: list[str]
    foreign_keys: list[str]


class DimensionTableSpec(BaseModel):
    """Specification for a dimension table in a star schema."""
    name: str
    source_columns: list[str]
    surrogate_key: str


class SchemaMappingResult(BaseModel):
    """Complete star-schema mapping produced by the Schema Mapper Agent."""
    fact_table: FactTableSpec
    dimension_tables: list[DimensionTableSpec]
    confidence: float = Field(ge=0.0, le=1.0)
    model_used: str
    reasoning: str
    few_shot_examples_used: int = 0


# ── Agent ────────────────────────────────────────────────────────


class SchemaMappingAgent:
    """Schema Mapper — Agent 1 of the LLM pipeline.

    Uses confidence-gated routing (Innovation #1) to select between
    a local LLaMA 3 8B model and the Claude API based on the predicted
    confidence of the mapping output.

    Reference: [Talaei et al. 2024] — column filtering paradigm; we
    extend this with automatic routing between local and cloud models.
    """

    def __init__(
        self,
        vector_store: SchemaVectorStore,
        ollama_url: str = "http://localhost:11434",
        anthropic_key: str | None = None,
    ) -> None:
        self._vector_store = vector_store
        self._ollama_url = ollama_url.rstrip("/")
        self._anthropic_key = anthropic_key

    # ── prompt construction ──────────────────────────────────────

    def build_prompt(self, schema: SchemaContext, few_shot: str) -> str:
        """Build a structured prompt for star-schema mapping.

        Includes:
        a) System role
        b) Schema context (column profiles)
        c) Few-shot examples from RAG
        d) Target JSON format
        e) Chain-of-thought instruction
        f) JSON-only constraint

        Reference: [Annam 2025] — structured prompt design for ETL.
        """
        columns_text = "\n".join(
            f"  - {c.name} (dtype={c.dtype}, null%={c.null_pct}, "
            f"unique={c.unique_count}, cardinality={c.cardinality_ratio}, "
            f"samples={c.samples[:3]}, "
            f"key={c.is_candidate_key}, measure={c.is_candidate_measure}, "
            f"dimension={c.is_candidate_dimension}, date={c.is_candidate_date})"
            for c in schema.columns
        )

        few_shot_block = f"\n\nPrevious approved mappings for similar schemas:\n{few_shot}" if few_shot else ""

        return f"""You are an expert data warehouse architect specializing in star schema design.

Given the following source schema with {schema.column_count} columns and {schema.row_count} rows:
{columns_text}
{few_shot_block}

Map these columns to a star schema. Think step by step:
1) Identify numeric measures (candidates for the fact table)
2) Identify categorical dimensions (candidates for dimension tables)
3) Identify candidate keys and foreign keys
4) Name the fact table and dimension tables following naming conventions

Respond ONLY with valid JSON matching this exact structure:
{{
  "fact_table": {{
    "name": "string",
    "measures": ["column_name", ...],
    "foreign_keys": ["column_name", ...]
  }},
  "dimension_tables": [
    {{
      "name": "string",
      "source_columns": ["column_name", ...],
      "surrogate_key": "string"
    }}
  ],
  "confidence": 0.0,
  "reasoning": "string explaining your mapping decisions"
}}

STRICT: Respond ONLY with valid JSON. No explanation text outside the JSON."""

    # ── LLM callers ──────────────────────────────────────────────

    def call_llama(self, prompt: str) -> tuple[dict, float]:
        """Call local LLaMA 3 8B via Ollama API.

        Returns (parsed_response_dict, confidence_score).
        Timeout: 120 seconds.
        """
        response = httpx.post(
            f"{self._ollama_url}/api/generate",
            json={"model": "llama3:8b", "prompt": prompt, "format": "json", "stream": False},
            timeout=120,
        )
        response.raise_for_status()
        text = response.json().get("response", "{}")
        parsed = json.loads(text)
        confidence = float(parsed.get("confidence", 0.5))
        return parsed, confidence

    def call_claude(self, prompt: str) -> tuple[dict, float]:
        """Call Claude API as high-confidence fallback.

        Returns (parsed_response_dict, 0.95).
        """
        import anthropic

        client = anthropic.Anthropic(api_key=self._anthropic_key)
        message = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text
        # Extract JSON from response
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            text = text[start:end]
        parsed = json.loads(text)
        return parsed, 0.95

    # ── main mapping method ──────────────────────────────────────

    def map_schema(self, schema: SchemaContext) -> SchemaMappingResult:
        """Map a source schema to a star schema using confidence-gated routing.

        Innovation #1 — Confidence-Gated Routing:
        1. Retrieve similar schemas from FAISS
        2. Build few-shot prompt
        3. Try LLaMA first (fast, local, free)
        4. If confidence < 0.75 OR parsing fails → fallback to Claude
        5. Log which model was used
        6. Return SchemaMappingResult

        Reference: [Annam 2025] — schema mapping with LLM reasoning.
        """
        # Retrieve similar schemas (Innovation #2)
        similar = self._vector_store.retrieve_similar(schema, k=3)
        few_shot = self._vector_store.build_few_shot_prompt(similar)
        prompt = self.build_prompt(schema, few_shot)

        model_used = "llama3:8b"
        try:
            parsed, confidence = self.call_llama(prompt)
            if confidence < CONFIDENCE_THRESHOLD:
                logger.info(
                    f"LLaMA confidence {confidence:.2f} < {CONFIDENCE_THRESHOLD} — "
                    f"escalating to Claude"
                )
                raise ValueError("Low confidence — fallback to Claude")
        except Exception as e:
            logger.warning(f"LLaMA call failed or low confidence: {e}")
            if self._anthropic_key:
                parsed, confidence = self.call_claude(prompt)
                model_used = "claude-3-5-sonnet"
            else:
                raise RuntimeError("LLaMA failed and no Anthropic API key configured") from e

        # Build result
        fact = FactTableSpec(
            name=parsed.get("fact_table", {}).get("name", "fact_table"),
            measures=parsed.get("fact_table", {}).get("measures", []),
            foreign_keys=parsed.get("fact_table", {}).get("foreign_keys", []),
        )
        dims = [
            DimensionTableSpec(
                name=d.get("name", "dim"),
                source_columns=d.get("source_columns", []),
                surrogate_key=d.get("surrogate_key", "sk"),
            )
            for d in parsed.get("dimension_tables", [])
        ]

        return SchemaMappingResult(
            fact_table=fact,
            dimension_tables=dims,
            confidence=confidence,
            model_used=model_used,
            reasoning=parsed.get("reasoning", ""),
            few_shot_examples_used=len(similar),
        )
