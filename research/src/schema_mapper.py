"""
Schema Mapper — uses LLM to map source schema to star schema.
Implements few-shot prompting and FAISS-based adaptive memory (Innovation #2).
"""
import json
import random
from dataclasses import dataclass, field
from typing import Optional

from .llm_client import LLMClient, MockLLMClient, LLMResponse
from .profiler import SchemaContext
from .rag import RAGSchemaStore


@dataclass
class MappingResult:
    dataset_name: str
    fact_table: str
    dimensions: list[str]
    measures: list[str]
    confidence: float
    model_used: str
    latency_ms: float
    condition: str  # "llama_only", "llama_fewshot", "routed"
    fallback_reason: Optional[str] = None
    raw_response: Optional[dict] = None


# ── Few-shot examples for prompt construction ──────────────
FEW_SHOT_EXAMPLES = [
    {
        "schema": "columns: order_id, date, customer, product, amount, qty",
        "mapping": {
            "fact_table": "order_fact",
            "dimensions": ["date_dim", "customer_dim", "product_dim"],
            "measures": ["amount", "qty"],
        },
    },
    {
        "schema": "columns: employee_id, dept, hire_date, salary, bonus, manager",
        "mapping": {
            "fact_table": "payroll_fact",
            "dimensions": ["employee_dim", "department_dim", "date_dim"],
            "measures": ["salary", "bonus"],
        },
    },
    {
        "schema": "columns: flight_id, origin, dest, departure, arrival, passengers, fare",
        "mapping": {
            "fact_table": "flight_fact",
            "dimensions": ["route_dim", "date_dim", "airport_dim"],
            "measures": ["passengers", "fare"],
        },
    },
    {
        "schema": "columns: student_id, course, semester, grade, credits, instructor",
        "mapping": {
            "fact_table": "enrollment_fact",
            "dimensions": ["student_dim", "course_dim", "semester_dim", "instructor_dim"],
            "measures": ["grade", "credits"],
        },
    },
    {
        "schema": "columns: claim_id, policy_id, claim_date, amount, type, adjuster",
        "mapping": {
            "fact_table": "claim_fact",
            "dimensions": ["policy_dim", "date_dim", "claim_type_dim", "adjuster_dim"],
            "measures": ["amount"],
        },
    },
]


class SchemaMapper:
    """Map source schemas to star schema using LLM."""

    PROMPT_TEMPLATE = """You are a data warehouse architect. Given a source dataset schema,
propose a star schema mapping.

INSTRUCTIONS:
1. Identify the fact table name (use snake_case ending in _fact)
2. List dimension tables (use snake_case ending in _dim)
3. List measure columns (numeric columns for aggregation)
4. Think step by step before answering
5. Include a "confidence" field (0.0-1.0) indicating how certain you are

{few_shot_section}

SOURCE SCHEMA:
{schema_context}

Respond ONLY with valid JSON in this exact format:
{{
  "fact_table": "...",
  "dimensions": ["...", "..."],
  "measures": ["...", "..."],
  "confidence": 0.XX,
  "reasoning": "Step-by-step reasoning..."
}}"""

    def __init__(self, llm_client=None, rag_store: Optional[RAGSchemaStore] = None):
        self.llm_client = llm_client
        self.rag_store = rag_store  # RAGSchemaStore for adaptive few-shot retrieval

    def build_prompt(
        self, schema_ctx: SchemaContext, k_shots: int = 0
    ) -> str:
        """Build the mapping prompt with optional few-shot examples.

        Priority order for few-shot examples:
          1. RAG-retrieved approved mappings (if ``rag_store`` set and non-empty)
          2. Static fallback examples from ``FEW_SHOT_EXAMPLES``
        """
        few_shot_section = ""
        if k_shots > 0:
            # Try RAG store first
            if self.rag_store is not None and self.rag_store.size > 0:
                similar = self.rag_store.retrieve(schema_ctx, k=k_shots)
                few_shot_section = self.rag_store.build_few_shot_prompt(similar)
            else:
                # Fallback to static examples
                examples = FEW_SHOT_EXAMPLES[:k_shots]
                lines = ["Here are examples of correct mappings:", ""]
                for i, ex in enumerate(examples, 1):
                    lines.append(f"Example {i}:")
                    lines.append(f"  Schema: {ex['schema']}")
                    lines.append(f"  Mapping: {json.dumps(ex['mapping'])}")
                    lines.append("")
                few_shot_section = "\n".join(lines)

        return self.PROMPT_TEMPLATE.format(
            few_shot_section=few_shot_section,
            schema_context=schema_ctx.to_prompt_string(),
        )

    def store_approved_mapping(
        self,
        schema_ctx: SchemaContext,
        result: "MappingResult",
        approved_by_human: bool = False,
    ) -> None:
        """Persist an approved mapping into the RAG store.

        Call this after HITL validation to grow the adaptive memory.
        Does nothing if no ``rag_store`` was configured.
        """
        if self.rag_store is None:
            return
        self.rag_store.add(
            source_name=result.dataset_name,
            schema=schema_ctx,
            mapping={
                "fact_table": result.fact_table,
                "dimensions": result.dimensions,
                "measures": result.measures,
            },
            approved_by_human=approved_by_human,
        )

    def map_schema(
        self,
        schema_ctx: SchemaContext,
        condition: str = "routed",
        k_shots: int = 3,
        difficulty: str = "medium",
    ) -> MappingResult:
        """
        Run schema mapping under a specific experimental condition.

        Conditions:
          - "llama_only":   LLaMA without few-shot, no fallback
          - "llama_fewshot": LLaMA with few-shot, no fallback
          - "routed":       Confidence-Gated Routing (LLaMA + Claude fallback)
        """
        if condition == "llama_only":
            prompt = self.build_prompt(schema_ctx, k_shots=0)
            resp_dict, conf, lat = self.llm_client.call_llama(prompt)
            return MappingResult(
                dataset_name=schema_ctx.dataset_name,
                fact_table=resp_dict.get("fact_table", ""),
                dimensions=resp_dict.get("dimensions", []),
                measures=resp_dict.get("measures", []),
                confidence=conf,
                model_used="llama3",
                latency_ms=lat,
                condition=condition,
                raw_response=resp_dict,
            )

        elif condition == "llama_fewshot":
            prompt = self.build_prompt(schema_ctx, k_shots=k_shots)
            resp_dict, conf, lat = self.llm_client.call_llama(prompt)
            return MappingResult(
                dataset_name=schema_ctx.dataset_name,
                fact_table=resp_dict.get("fact_table", ""),
                dimensions=resp_dict.get("dimensions", []),
                measures=resp_dict.get("measures", []),
                confidence=conf,
                model_used="llama3",
                latency_ms=lat,
                condition=condition,
                raw_response=resp_dict,
            )

        else:  # "routed"
            prompt = self.build_prompt(schema_ctx, k_shots=k_shots)
            llm_resp: LLMResponse = self.llm_client.route(
                prompt, schema_complexity=difficulty
            )
            return MappingResult(
                dataset_name=schema_ctx.dataset_name,
                fact_table=llm_resp.response.get("fact_table", ""),
                dimensions=llm_resp.response.get("dimensions", []),
                measures=llm_resp.response.get("measures", []),
                confidence=llm_resp.confidence,
                model_used=llm_resp.model_used,
                latency_ms=llm_resp.latency_ms,
                condition=condition,
                fallback_reason=llm_resp.fallback_reason,
                raw_response=llm_resp.response,
            )
