"""
FAISS Semantic Schema Store with Adaptive Few-Shot Memory
==========================================================
Embeds schema profiles using sentence-transformers and stores them in
a FAISS ``IndexFlatL2`` index for fast similarity retrieval.

Innovation #2 — Adaptive Few-Shot Memory:
  Every human-approved mapping is stored alongside its schema embedding.
  On future pipeline runs, similar schemas retrieve these approved
  mappings as few-shot examples, improving LLM accuracy over time.

References:
  [Colombo et al. 2025] — Knowledge graph enrichment with retrieval.
  [Birjega 2025] — Semantic-RAG architecture for schema-aware retrieval.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import numpy as np

from services.etl_llm.profiling.schema_profiler import SchemaContext

logger = logging.getLogger(__name__)


def _get_model():
    """Lazy-load sentence-transformers model to avoid import overhead in tests."""
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer("all-MiniLM-L6-v2")


class SchemaVectorStore:
    """Semantic vector store for schema embeddings and approved mappings.

    Architecture:
    * FAISS ``IndexFlatL2`` stores embedding vectors of schema descriptions.
    * A parallel JSON metadata file stores the mapping, approval status,
      and source name for each indexed schema.

    Reference: [Birjega 2025] — Semantic-RAG retrieves contextually similar
    schema knowledge for downstream LLM prompts.
    """

    def __init__(self, index_path: str = "faiss_schema.index") -> None:
        import faiss

        self._index_path = Path(index_path)
        self._meta_path = self._index_path.with_suffix(".meta.json")
        self._model = None  # lazy loaded
        self._dim = 384  # MiniLM-L6 output dim

        if self._index_path.exists():
            self._index = faiss.read_index(str(self._index_path))
            self._metadata: list[dict] = json.loads(
                self._meta_path.read_text(encoding="utf-8")
            ) if self._meta_path.exists() else []
        else:
            self._index = faiss.IndexFlatL2(self._dim)
            self._metadata = []

    def _ensure_model(self):
        if self._model is None:
            self._model = _get_model()

    def _save(self) -> None:
        import faiss

        faiss.write_index(self._index, str(self._index_path))
        self._meta_path.write_text(json.dumps(self._metadata, indent=2), encoding="utf-8")

    # ── Core API ────────────────────────────────────────────────

    def embed_schema(self, schema: SchemaContext) -> np.ndarray:
        """Encode a schema profile into a dense vector.

        The text representation concatenates ``name:dtype`` for every column,
        mirroring the fingerprint logic used in profiling.
        """
        self._ensure_model()
        text = " | ".join(f"{c.name}:{c.dtype}" for c in schema.columns)
        return self._model.encode([text])[0].astype("float32")

    def add_schema(
        self,
        schema: SchemaContext,
        mapping: dict[str, Any],
        approved_by_human: bool = False,
    ) -> None:
        """Store a schema embedding and its mapping metadata.

        Innovation #2: when *approved_by_human* is ``True`` the mapping
        becomes a high-priority few-shot example for future retrievals.
        """
        vec = self.embed_schema(schema)
        self._index.add(np.expand_dims(vec, 0))
        self._metadata.append(
            {
                "fingerprint": schema.schema_fingerprint,
                "source_name": schema.source_name,
                "schema_text": " | ".join(f"{c.name}:{c.dtype}" for c in schema.columns),
                "mapping": mapping,
                "approved_by_human": approved_by_human,
            }
        )
        self._save()
        logger.info(
            f"Added schema '{schema.source_name}' to vector store "
            f"(approved={approved_by_human})"
        )

    def retrieve_similar(
        self, schema: SchemaContext, k: int = 3
    ) -> list[dict[str, Any]]:
        """Retrieve the *k* most similar schemas from the FAISS index.

        If any human-approved mappings exist among the top results they are
        prioritised; otherwise all results are returned.

        Reference: [Colombo et al. 2025] — retrieval-augmented enrichment.
        """
        if self._index.ntotal == 0:
            return []

        vec = self.embed_schema(schema)
        actual_k = min(k, self._index.ntotal)
        distances, indices = self._index.search(np.expand_dims(vec, 0), actual_k)

        results: list[dict] = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx < 0:
                continue
            meta = self._metadata[idx]
            results.append(
                {
                    "schema_text": meta["schema_text"],
                    "mapping": meta["mapping"],
                    "approved_by_human": meta["approved_by_human"],
                    "similarity_score": float(1 / (1 + dist)),
                }
            )

        # Prefer human-approved examples
        approved = [r for r in results if r["approved_by_human"]]
        return approved if approved else results

    def build_few_shot_prompt(self, similar: list[dict[str, Any]]) -> str:
        """Format retrieved schema/mapping pairs as few-shot prompt examples.

        Example output::

            Example 1 (human-approved):
              Schema: date:datetime64 | product:object | quantity:int64
              Mapping: {"fact_table": "sales_fact", ...}
        """
        if not similar:
            return ""

        lines: list[str] = []
        for i, item in enumerate(similar, 1):
            tag = "human-approved" if item["approved_by_human"] else "auto"
            lines.append(f"Example {i} ({tag}):")
            lines.append(f"  Schema: {item['schema_text']}")
            mapping_str = json.dumps(item["mapping"], indent=2)
            lines.append(f"  Mapping: {mapping_str}")
            lines.append("")
        return "\n".join(lines)
