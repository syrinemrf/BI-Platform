"""
LLM Client with Confidence-Gated Routing (Innovation #1).
Supports Ollama (LLaMA 3 8B) and Anthropic (Claude 3.5 Sonnet).
Falls back to MockLLMClient when services are unavailable.
"""
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Optional, Tuple

import httpx

logger = logging.getLogger(__name__)


@dataclass
class LLMResponse:
    response: dict
    confidence: float
    latency_ms: float
    model_used: str
    fallback_reason: Optional[str] = None


class LLMClient:
    """Production LLM client with confidence-gated routing."""

    def __init__(
        self,
        ollama_url: str = "http://localhost:11434",
        anthropic_key: str = None,
        confidence_threshold: float = 0.75,
    ):
        self.ollama_url = ollama_url.rstrip("/")
        self.anthropic_key = anthropic_key or os.getenv("ANTHROPIC_API_KEY", "")
        self.confidence_threshold = confidence_threshold
        self._routing_log: list[dict] = []

    @property
    def routing_log(self) -> list[dict]:
        return self._routing_log

    def is_ollama_available(self) -> bool:
        try:
            r = httpx.get(f"{self.ollama_url}/api/tags", timeout=3.0)
            return r.status_code == 200
        except Exception:
            return False

    def call_llama(
        self, prompt: str, expect_json: bool = True
    ) -> Tuple[dict, float, float]:
        """Call Ollama LLaMA 3 8B. Returns (response_dict, confidence, latency_ms)."""
        start = time.perf_counter()
        body = {
            "model": "llama3:8b",
            "prompt": prompt,
            "stream": False,
        }
        if expect_json:
            body["format"] = "json"

        try:
            r = httpx.post(
                f"{self.ollama_url}/api/generate",
                json=body,
                timeout=120.0,
            )
            r.raise_for_status()
            latency_ms = (time.perf_counter() - start) * 1000

            raw = r.json().get("response", "{}")
            parsed = json.loads(raw)
            confidence = float(parsed.get("confidence", 0.5))
            confidence = max(0.0, min(1.0, confidence))
            return parsed, confidence, latency_ms

        except (httpx.HTTPError, json.JSONDecodeError, Exception) as e:
            latency_ms = (time.perf_counter() - start) * 1000
            logger.warning("LLaMA call failed: %s", e)
            return {}, 0.0, latency_ms

    def call_claude(self, prompt: str) -> Tuple[dict, float, float]:
        """Call Anthropic Claude 3.5 Sonnet. Returns (response_dict, confidence, latency_ms)."""
        if not self.anthropic_key:
            logger.warning("No ANTHROPIC_API_KEY set, Claude unavailable")
            return {}, 0.0, 0.0

        start = time.perf_counter()
        try:
            import anthropic

            client = anthropic.Anthropic(api_key=self.anthropic_key)
            message = client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            latency_ms = (time.perf_counter() - start) * 1000
            raw = message.content[0].text
            # Try to extract JSON from response
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                # Try to find JSON block in text
                start_idx = raw.find("{")
                end_idx = raw.rfind("}") + 1
                if start_idx >= 0 and end_idx > start_idx:
                    parsed = json.loads(raw[start_idx:end_idx])
                else:
                    parsed = {"raw_response": raw}

            return parsed, 0.95, latency_ms

        except Exception as e:
            latency_ms = (time.perf_counter() - start) * 1000
            logger.warning("Claude call failed: %s", e)
            return {}, 0.0, latency_ms

    def route(
        self, prompt: str, schema_complexity: str = "medium"
    ) -> LLMResponse:
        """
        Confidence-Gated Routing (Innovation #1).

        1. Try LLaMA first
        2. If confidence < threshold OR parse error → fallback to Claude
        3. Log routing decision with reason
        """
        # Step 1: Try LLaMA
        llama_resp, llama_conf, llama_lat = self.call_llama(prompt)

        if llama_resp and llama_conf >= self.confidence_threshold:
            entry = {
                "model_used": "llama3",
                "confidence": llama_conf,
                "latency_ms": llama_lat,
                "fallback": False,
                "reason": "confidence above threshold",
                "schema_complexity": schema_complexity,
            }
            self._routing_log.append(entry)
            return LLMResponse(
                response=llama_resp,
                confidence=llama_conf,
                latency_ms=llama_lat,
                model_used="llama3",
            )

        # Step 2: Fallback to Claude
        reason = "parse_error" if not llama_resp else f"low_confidence ({llama_conf:.2f})"
        claude_resp, claude_conf, claude_lat = self.call_claude(prompt)

        if claude_resp:
            entry = {
                "model_used": "claude",
                "confidence": claude_conf,
                "latency_ms": claude_lat,
                "fallback": True,
                "reason": reason,
                "schema_complexity": schema_complexity,
            }
            self._routing_log.append(entry)
            return LLMResponse(
                response=claude_resp,
                confidence=claude_conf,
                latency_ms=llama_lat + claude_lat,
                model_used="claude",
                fallback_reason=reason,
            )

        # Both failed — return best effort
        entry = {
            "model_used": "llama3_degraded",
            "confidence": llama_conf,
            "latency_ms": llama_lat,
            "fallback": True,
            "reason": "both_failed",
            "schema_complexity": schema_complexity,
        }
        self._routing_log.append(entry)
        return LLMResponse(
            response=llama_resp,
            confidence=llama_conf,
            latency_ms=llama_lat,
            model_used="llama3_degraded",
            fallback_reason="both_failed",
        )


class MockLLMClient:
    """
    Mock LLM client for environments without Ollama/Claude.
    Returns realistic but slightly imperfect results for testing.
    """

    def __init__(self, confidence_threshold: float = 0.75):
        self.confidence_threshold = confidence_threshold
        self._routing_log: list[dict] = []
        random.seed(42)

    @property
    def routing_log(self) -> list[dict]:
        return self._routing_log

    def is_ollama_available(self) -> bool:
        return False

    def call_llama(
        self, prompt: str, expect_json: bool = True
    ) -> Tuple[dict, float, float]:
        """Simulate LLaMA response with random confidence."""
        latency = random.uniform(800, 3000)
        confidence = random.uniform(0.4, 0.95)
        response = self._generate_mock_response(prompt, "llama3", confidence)
        return response, confidence, latency

    def call_claude(self, prompt: str) -> Tuple[dict, float, float]:
        """Simulate Claude response with high confidence."""
        latency = random.uniform(1500, 5000)
        confidence = random.uniform(0.85, 0.98)
        response = self._generate_mock_response(prompt, "claude", confidence)
        return response, confidence, latency

    def route(
        self, prompt: str, schema_complexity: str = "medium"
    ) -> LLMResponse:
        """Mock routing with realistic behavior."""
        # Difficulty affects routing
        complexity_penalty = {
            "easy": 0.15,
            "medium": 0.0,
            "medium_hard": -0.1,
            "hard": -0.2,
        }.get(schema_complexity, 0.0)

        llama_resp, llama_conf, llama_lat = self.call_llama(prompt)
        llama_conf = max(0.1, min(0.99, llama_conf + complexity_penalty))

        if llama_conf >= self.confidence_threshold:
            entry = {
                "model_used": "llama3",
                "confidence": llama_conf,
                "latency_ms": llama_lat,
                "fallback": False,
                "reason": "confidence above threshold",
                "schema_complexity": schema_complexity,
            }
            self._routing_log.append(entry)
            return LLMResponse(
                response=llama_resp,
                confidence=llama_conf,
                latency_ms=llama_lat,
                model_used="llama3",
            )

        # Fallback
        claude_resp, claude_conf, claude_lat = self.call_claude(prompt)
        reason = f"low_confidence ({llama_conf:.2f})"
        entry = {
            "model_used": "claude",
            "confidence": claude_conf,
            "latency_ms": llama_lat + claude_lat,
            "fallback": True,
            "reason": reason,
            "schema_complexity": schema_complexity,
        }
        self._routing_log.append(entry)
        return LLMResponse(
            response=claude_resp,
            confidence=claude_conf,
            latency_ms=llama_lat + claude_lat,
            model_used="claude",
            fallback_reason=reason,
        )

    def _generate_mock_response(
        self, prompt: str, model: str, confidence: float
    ) -> dict:
        """Generate a plausible mock response based on prompt content."""
        prompt_lower = prompt.lower()

        # Detect dataset from prompt
        if "retail" in prompt_lower or "sales" in prompt_lower:
            base = {
                "fact_table": "sales_fact",
                "dimensions": ["date_dim", "customer_dim", "product_dim", "sales_rep_dim"],
                "measures": ["unit_price", "quantity", "discount_pct", "total_amount"],
                "confidence": confidence,
            }
        elif "hospital" in prompt_lower or "patient" in prompt_lower:
            base = {
                "fact_table": "hospital_visit_fact",
                "dimensions": ["patient_dim", "doctor_dim", "department_dim",
                               "diagnosis_dim", "date_dim"],
                "measures": ["total_cost", "insurance_covered", "patient_paid",
                             "length_of_stay_days"],
                "confidence": confidence,
            }
        elif "invoice" in prompt_lower or "supplier" in prompt_lower:
            base = {
                "fact_table": "invoice_fact",
                "dimensions": ["supplier_dim", "buyer_department_dim", "date_dim"],
                "measures": ["subtotal_ht", "vat_amount", "total_ttc",
                             "payment_delay_days"],
                "confidence": confidence,
            }
        elif "ecommerce" in prompt_lower or "event" in prompt_lower:
            base = {
                "fact_table": "purchase_fact",
                "dimensions": ["user_dim", "product_dim", "date_dim", "device_dim"],
                "measures": ["price", "quantity", "total", "refund_amount"],
                "confidence": confidence,
            }
        else:
            base = {
                "fact_table": "generic_fact",
                "dimensions": ["dim_1", "dim_2"],
                "measures": ["measure_1"],
                "confidence": confidence,
            }

        # Introduce errors based on confidence (lower confidence = more errors)
        if confidence < 0.6:
            # Remove a dimension or add a wrong one
            if base["dimensions"] and random.random() < 0.5:
                base["dimensions"] = base["dimensions"][:-1]
            else:
                base["dimensions"].append("wrong_dim")
            if random.random() < 0.3:
                base["fact_table"] = base["fact_table"].replace("_fact", "_table")

        elif confidence < 0.75:
            # Minor error: miss one measure
            if base["measures"] and random.random() < 0.4:
                base["measures"] = base["measures"][:-1]

        return base
