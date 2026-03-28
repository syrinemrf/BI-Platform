"""
LLM Service.

Integration with local LLM (Ollama/LLaMA) for intelligent assistance.
"""
import httpx
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import logging
import re

from config import settings

logger = logging.getLogger(__name__)


@dataclass
class LLMResponse:
    """Response from LLM."""
    text: str
    model: str
    tokens_used: int
    success: bool
    error: Optional[str] = None


class LLMService:
    """
    Service for interacting with local LLM via Ollama.
    """

    def __init__(
            self,
            base_url: str = None,
            model: str = None,
            timeout: int = None
    ):
        """
        Initialize LLM service.

        Args:
            base_url: Ollama API base URL
            model: Model name (e.g., "llama3:8b")
            timeout: Request timeout in seconds
        """
        self.base_url = base_url or settings.OLLAMA_BASE_URL
        self.model = model or settings.OLLAMA_MODEL
        self.timeout = timeout or settings.LLM_TIMEOUT

    async def is_available(self) -> bool:
        """Check if LLM service is available."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.base_url}/api/tags")
                return response.status_code == 200
        except Exception:
            return False

    async def generate(
            self,
            prompt: str,
            system_prompt: str = None,
            temperature: float = 0.7,
            max_tokens: int = 2048
    ) -> LLMResponse:
        """
        Generate text using the LLM.

        Args:
            prompt: User prompt
            system_prompt: System/context prompt
            temperature: Sampling temperature
            max_tokens: Maximum tokens to generate

        Returns:
            LLMResponse object
        """
        try:
            messages = []

            if system_prompt:
                messages.append({
                    "role": "system",
                    "content": system_prompt
                })

            messages.append({
                "role": "user",
                "content": prompt
            })

            payload = {
                "model": self.model,
                "messages": messages,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens
                }
            }

            logger.info(f"Calling Ollama at {self.base_url}/api/chat with model {self.model}")
            logger.debug(f"Prompt length: {len(prompt)} chars")

            async with httpx.AsyncClient(timeout=float(self.timeout)) as client:
                response = await client.post(
                    f"{self.base_url}/api/chat",
                    json=payload
                )

                logger.info(f"Ollama response status: {response.status_code}")

                if response.status_code != 200:
                    error_text = response.text
                    logger.error(f"Ollama API error: {response.status_code} - {error_text}")
                    return LLMResponse(
                        text="",
                        model=self.model,
                        tokens_used=0,
                        success=False,
                        error=f"API error: {response.status_code} - {error_text}"
                    )

                data = response.json()
                response_text = data.get("message", {}).get("content", "")
                tokens = data.get("eval_count", 0)
                logger.info(f"Ollama response received: {len(response_text)} chars, {tokens} tokens")

                return LLMResponse(
                    text=response_text,
                    model=self.model,
                    tokens_used=tokens,
                    success=True
                )

        except httpx.TimeoutException as e:
            logger.error(f"LLM timeout after {self.timeout}s: {str(e)}")
            return LLMResponse(
                text="",
                model=self.model,
                tokens_used=0,
                success=False,
                error=f"Request timed out after {self.timeout} seconds. Try a simpler query."
            )
        except httpx.ConnectError as e:
            logger.error(f"LLM connection error: {str(e)}")
            return LLMResponse(
                text="",
                model=self.model,
                tokens_used=0,
                success=False,
                error="Cannot connect to Ollama. Make sure 'ollama serve' is running."
            )
        except Exception as e:
            import traceback
            logger.error(f"LLM generation failed: {str(e)}")
            logger.error(traceback.format_exc())
            return LLMResponse(
                text="",
                model=self.model,
                tokens_used=0,
                success=False,
                error=str(e)
            )

    async def analyze_schema_suggestion(
            self,
            schema_info: Dict[str, Any],
            user_question: str = None
    ) -> Dict[str, Any]:
        """
        Get LLM suggestions for schema design.

        Args:
            schema_info: Schema analysis results
            user_question: Optional specific question

        Returns:
            Dictionary with suggestions
        """
        system_prompt = """You are a data warehouse architect expert.
Analyze the given schema information and provide recommendations for:
1. Dimension table design
2. Fact table measures
3. Data quality concerns
4. Star schema optimization

Be concise and specific. Format your response as JSON with keys:
- dimensions: list of dimension suggestions
- measures: list of measure recommendations
- quality_concerns: list of potential issues
- optimization_tips: list of optimization suggestions"""

        schema_summary = self._summarize_schema(schema_info)

        prompt = f"""Analyze this data schema:

{schema_summary}

{f"User question: {user_question}" if user_question else ""}

Provide your analysis and recommendations."""

        response = await self.generate(prompt, system_prompt, temperature=0.3)

        if not response.success:
            return {
                "error": response.error,
                "suggestions": []
            }

        try:
            # Try to parse JSON response
            json_match = re.search(r'\{[\s\S]*\}', response.text)
            if json_match:
                return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass

        # Return raw text if JSON parsing fails
        return {
            "raw_response": response.text,
            "suggestions": [response.text]
        }

    async def generate_sql_query(
            self,
            question: str,
            table_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate SQL query from natural language question.

        Args:
            question: Natural language question
            table_info: Information about available tables

        Returns:
            Dictionary with SQL query and explanation
        """
        system_prompt = """You are a SQL expert. Generate PostgreSQL queries based on natural language questions.
The database uses a star schema with fact and dimension tables.

Rules:
1. Use proper JOINs between fact and dimension tables
2. Use appropriate aggregation functions (SUM, AVG, COUNT, etc.)
3. Include GROUP BY for aggregated queries
4. Add ORDER BY for better readability
5. Limit results to 1000 rows unless specified

Return your response as JSON with keys:
- sql: the SQL query
- explanation: brief explanation of what the query does
- confidence: confidence level (high/medium/low)"""

        tables_description = self._describe_tables(table_info)

        prompt = f"""Available tables:
{tables_description}

User question: {question}

Generate a SQL query to answer this question."""

        response = await self.generate(prompt, system_prompt, temperature=0.2)

        if not response.success:
            return {
                "error": response.error,
                "sql": None,
                "explanation": None
            }

        try:
            json_match = re.search(r'\{[\s\S]*\}', response.text)
            if json_match:
                result = json.loads(json_match.group())
                return result
        except json.JSONDecodeError:
            pass

        # Try to extract SQL from response
        sql_match = re.search(r'```sql\n?([\s\S]*?)\n?```', response.text)
        if sql_match:
            return {
                "sql": sql_match.group(1).strip(),
                "explanation": response.text,
                "confidence": "medium"
            }

        return {
            "raw_response": response.text,
            "sql": None,
            "explanation": response.text
        }

    async def suggest_transformations(
            self,
            quality_report: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Suggest data transformations based on quality report.

        Args:
            quality_report: Data quality check results

        Returns:
            List of transformation suggestions
        """
        system_prompt = """You are a data engineer expert. Based on the data quality report,
suggest specific transformations to improve data quality.

Return a JSON array of transformations, each with:
- column: affected column
- issue: the problem identified
- transformation: suggested transformation
- code_snippet: example pandas code"""

        quality_summary = self._summarize_quality_report(quality_report)

        prompt = f"""Data Quality Report:
{quality_summary}

Suggest transformations to fix these data quality issues."""

        response = await self.generate(prompt, system_prompt, temperature=0.3)

        if not response.success:
            return []

        try:
            json_match = re.search(r'\[[\s\S]*\]', response.text)
            if json_match:
                return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass

        return [{
            "suggestion": response.text
        }]

    def _summarize_schema(self, schema_info: Dict[str, Any]) -> str:
        """Create a text summary of schema info."""
        lines = []

        lines.append(f"Total rows: {schema_info.get('total_rows', 'N/A')}")
        lines.append(f"Total columns: {schema_info.get('total_columns', 'N/A')}")

        measures = schema_info.get('measures', [])
        if measures:
            lines.append(f"\nMeasures ({len(measures)}):")
            for m in measures[:10]:
                lines.append(f"  - {m.get('name')}: {m.get('original_dtype')}")

        dimensions = schema_info.get('dimensions', [])
        if dimensions:
            lines.append(f"\nDimensions ({len(dimensions)}):")
            for d in dimensions[:10]:
                lines.append(f"  - {d.get('name')}: {d.get('unique_count')} unique values")

        date_cols = schema_info.get('date_columns', [])
        if date_cols:
            lines.append(f"\nDate columns ({len(date_cols)}):")
            for d in date_cols:
                lines.append(f"  - {d.get('name')}")

        potential_keys = schema_info.get('potential_keys', [])
        if potential_keys:
            lines.append(f"\nPotential keys: {', '.join(potential_keys)}")

        return '\n'.join(lines)

    def _describe_tables(self, table_info: Dict[str, Any]) -> str:
        """Create a text description of tables."""
        lines = []

        for table_name, info in table_info.items():
            table_type = info.get('type', 'unknown')
            lines.append(f"\n{table_name} ({table_type}):")

            columns = info.get('columns', [])
            for col in columns[:15]:
                markers = []
                if col.get('is_primary_key'):
                    markers.append('PK')
                if col.get('is_foreign_key'):
                    markers.append('FK')
                if col.get('is_measure'):
                    markers.append('measure')

                marker_str = f" [{', '.join(markers)}]" if markers else ""
                lines.append(f"  - {col.get('name')}: {col.get('type')}{marker_str}")

        return '\n'.join(lines)

    def _summarize_quality_report(self, report: Dict[str, Any]) -> str:
        """Create a text summary of quality report."""
        lines = []

        lines.append(f"Overall Score: {report.get('overall_score', 'N/A')}")
        lines.append(f"Completeness: {report.get('completeness_score', 'N/A')}")
        lines.append(f"Validity: {report.get('validity_score', 'N/A')}")
        lines.append(f"Passed: {report.get('passed', 'N/A')}")

        critical = report.get('critical_issues', [])
        if critical:
            lines.append(f"\nCritical Issues ({len(critical)}):")
            for issue in critical[:5]:
                lines.append(f"  - {issue.get('column')}: {issue.get('issue')}")

        column_reports = report.get('column_reports', [])
        problematic = [c for c in column_reports if c.get('overall_score', 1) < 0.9]
        if problematic:
            lines.append(f"\nProblematic Columns ({len(problematic)}):")
            for col in problematic[:5]:
                lines.append(f"  - {col.get('column_name')}: score {col.get('overall_score')}")

        return '\n'.join(lines)


# Singleton instance
_llm_service: Optional[LLMService] = None


def get_llm_service() -> LLMService:
    """Get or create LLM service singleton."""
    global _llm_service
    if _llm_service is None:
        _llm_service = LLMService()
    return _llm_service
