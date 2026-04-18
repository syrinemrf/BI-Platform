"""
ETL Code Generator with DVR (Divide-Verify-Refine) self-correction loop.
Generates Python and SQL code for ETL pipelines.
References: DVR framework (Zhang et al. 2024).
"""
import ast
import random
import re
from dataclasses import dataclass, field
from typing import Optional

from .llm_client import LLMClient, MockLLMClient, LLMResponse
from .schema_mapper import MappingResult
from .profiler import SchemaContext


@dataclass
class GeneratedCode:
    python_code: str
    sql_code: str
    python_valid: bool
    sql_valid: bool
    correction_attempts: int
    errors_fixed: list[str] = field(default_factory=list)
    model_used: str = ""
    latency_ms: float = 0.0


@dataclass
class DVRResult:
    dataset_name: str
    max_attempts: int
    codes: list[GeneratedCode]
    final_python_valid: bool
    final_sql_valid: bool
    total_attempts: int
    error_types_encountered: list[str] = field(default_factory=list)


class ETLCodeGenerator:
    """Generate ETL code with DVR self-correction."""

    CODE_GEN_PROMPT = """You are an ETL code generator. Given a star schema mapping,
generate Python (pandas) and SQL (CREATE TABLE + INSERT) code to transform
the source data into the star schema.

SCHEMA MAPPING:
  Fact table: {fact_table}
  Dimensions: {dimensions}
  Measures: {measures}
  Source columns: {source_columns}

Generate:
1. Python code using pandas to create dimension and fact DataFrames
2. SQL DDL (CREATE TABLE statements) for PostgreSQL
3. SQL DML (INSERT statements) to load the data

Respond with valid JSON:
{{
  "python_code": "...",
  "sql_code": "...",
  "confidence": 0.XX
}}"""

    CORRECTION_PROMPT = """The following {language} code has errors:

CODE:
```
{code}
```

ERRORS:
{errors}

Fix the errors and return the corrected code. Respond with valid JSON:
{{
  "corrected_code": "..."
}}"""

    def __init__(self, llm_client=None):
        self.llm_client = llm_client

    def generate(
        self,
        mapping: MappingResult,
        schema_ctx: SchemaContext,
        max_attempts: int = 2,
    ) -> DVRResult:
        """Generate ETL code with DVR self-correction loop."""
        if isinstance(self.llm_client, MockLLMClient):
            return self._mock_generate(mapping, schema_ctx, max_attempts)

        prompt = self.CODE_GEN_PROMPT.format(
            fact_table=mapping.fact_table,
            dimensions=", ".join(mapping.dimensions),
            measures=", ".join(mapping.measures),
            source_columns=", ".join(c.name for c in schema_ctx.columns),
        )

        llm_resp = self.llm_client.route(prompt)
        python_code = llm_resp.response.get("python_code", "")
        sql_code = llm_resp.response.get("sql_code", "")

        codes = []
        total_attempts = 0
        error_types = []

        for attempt in range(max_attempts + 1):
            py_valid, py_errors = self._validate_python(python_code)
            sql_valid, sql_errors = self._validate_sql(sql_code)

            codes.append(
                GeneratedCode(
                    python_code=python_code,
                    sql_code=sql_code,
                    python_valid=py_valid,
                    sql_valid=sql_valid,
                    correction_attempts=attempt,
                    model_used=llm_resp.model_used,
                    latency_ms=llm_resp.latency_ms,
                )
            )

            if py_valid and sql_valid:
                break

            total_attempts = attempt + 1

            # Attempt correction
            if not py_valid and attempt < max_attempts:
                error_types.extend(self._classify_errors(py_errors))
                correction_resp = self.llm_client.route(
                    self.CORRECTION_PROMPT.format(
                        language="Python",
                        code=python_code,
                        errors="\n".join(py_errors),
                    )
                )
                python_code = correction_resp.response.get(
                    "corrected_code", python_code
                )

            if not sql_valid and attempt < max_attempts:
                error_types.extend(self._classify_errors(sql_errors))
                correction_resp = self.llm_client.route(
                    self.CORRECTION_PROMPT.format(
                        language="SQL",
                        code=sql_code,
                        errors="\n".join(sql_errors),
                    )
                )
                sql_code = correction_resp.response.get("corrected_code", sql_code)

        final = codes[-1]
        return DVRResult(
            dataset_name=mapping.dataset_name,
            max_attempts=max_attempts,
            codes=codes,
            final_python_valid=final.python_valid,
            final_sql_valid=final.sql_valid,
            total_attempts=total_attempts,
            error_types_encountered=error_types,
        )

    # ── Validation ─────────────────────────────────────────
    @staticmethod
    def _validate_python(code: str) -> tuple[bool, list[str]]:
        """Validate Python code using ast.parse."""
        if not code.strip():
            return False, ["empty code"]
        try:
            ast.parse(code)
            return True, []
        except SyntaxError as e:
            return False, [f"SyntaxError: {e.msg} at line {e.lineno}"]

    @staticmethod
    def _validate_sql(code: str) -> tuple[bool, list[str]]:
        """Basic SQL validation (structural checks)."""
        if not code.strip():
            return False, ["empty SQL"]
        errors = []
        # Check for basic SQL structure
        code_upper = code.upper()
        if "CREATE TABLE" not in code_upper and "INSERT" not in code_upper:
            errors.append("missing CREATE TABLE or INSERT statement")
        # Check for balanced parentheses
        if code.count("(") != code.count(")"):
            errors.append("unbalanced parentheses")
        # Check for unterminated strings
        if code.count("'") % 2 != 0:
            errors.append("unterminated string literal")
        return len(errors) == 0, errors

    @staticmethod
    def _classify_errors(errors: list[str]) -> list[str]:
        """Classify SQL/Python errors into categories."""
        types = []
        for err in errors:
            err_lower = err.lower()
            if "syntax" in err_lower:
                types.append("syntax")
            elif "table" in err_lower:
                types.append("wrong_table")
            elif "column" in err_lower:
                types.append("wrong_column")
            elif "join" in err_lower:
                types.append("wrong_join")
            elif "parenthes" in err_lower:
                types.append("syntax")
            else:
                types.append("other")
        return types

    # ── Mock generation ────────────────────────────────────
    def _mock_generate(
        self, mapping: MappingResult, schema_ctx: SchemaContext,
        max_attempts: int,
    ) -> DVRResult:
        """Generate realistic mock ETL code with controllable quality."""
        fact = mapping.fact_table or "fact_table"
        dims = mapping.dimensions or ["dim_1"]
        measures = mapping.measures or ["measure_1"]
        cols = [c.name for c in schema_ctx.columns]

        # Generate Python code
        py_lines = [
            "import pandas as pd",
            "",
            f"# Load source data",
            f"df = pd.read_csv('source_data.csv')",
            "",
        ]
        for dim in dims:
            dim_cols = [c for c in cols if any(kw in c.lower() for kw in dim.replace("_dim", "").split("_"))]
            if dim_cols:
                py_lines.append(f"# Create {dim}")
                py_lines.append(f"{dim} = df[{dim_cols[:3]}].drop_duplicates()")
                py_lines.append(f"{dim}['{dim}_id'] = range(1, len({dim}) + 1)")
                py_lines.append("")

        py_lines.append(f"# Create {fact}")
        py_lines.append(f"{fact} = df[{measures}].copy()")
        python_code = "\n".join(py_lines)

        # Generate SQL code
        sql_lines = []
        for dim in dims:
            sql_lines.append(f"CREATE TABLE {dim} (")
            sql_lines.append(f"    {dim}_id SERIAL PRIMARY KEY,")
            sql_lines.append(f"    name VARCHAR(255)")
            sql_lines.append(f");")
            sql_lines.append("")

        sql_lines.append(f"CREATE TABLE {fact} (")
        sql_lines.append(f"    {fact}_id SERIAL PRIMARY KEY,")
        for dim in dims:
            sql_lines.append(f"    {dim}_id INTEGER REFERENCES {dim}({dim}_id),")
        for m in measures:
            sql_lines.append(f"    {m} DECIMAL(12,2),")
        # Remove trailing comma from last line
        if sql_lines and sql_lines[-1].endswith(","):
            sql_lines[-1] = sql_lines[-1][:-1]
        sql_lines.append(f");")
        sql_code = "\n".join(sql_lines)

        codes = []
        error_types = []

        for attempt in range(max_attempts + 1):
            py_valid, py_errors = self._validate_python(python_code)
            sql_valid, sql_errors = self._validate_sql(sql_code)

            # Simulate fixing errors on each attempt
            if attempt > 0 and not py_valid:
                # Fix common Python issues
                python_code = python_code.replace("df[{", "df[[").replace("}]", "]]")
                py_valid, py_errors = self._validate_python(python_code)

            codes.append(
                GeneratedCode(
                    python_code=python_code,
                    sql_code=sql_code,
                    python_valid=py_valid,
                    sql_valid=sql_valid,
                    correction_attempts=attempt,
                    errors_fixed=[],
                    model_used="mock",
                    latency_ms=random.uniform(500, 2000),
                )
            )

            if py_valid and sql_valid:
                break

            error_types.extend(self._classify_errors(py_errors + sql_errors))

        final = codes[-1]
        return DVRResult(
            dataset_name=mapping.dataset_name,
            max_attempts=max_attempts,
            codes=codes,
            final_python_valid=final.python_valid,
            final_sql_valid=final.sql_valid,
            total_attempts=len(codes) - 1,
            error_types_encountered=error_types,
        )
