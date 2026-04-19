"""
RAG Adaptive Few-Shot Memory — FAISS-backed schema store (Innovation #2).

Mirrors the production SchemaVectorStore
(backend/services/etl_llm/rag/schema_store.py) for isolated research evaluation.

Architecture:
  * FAISS IndexFlatL2 stores 384-dim embedding vectors of schema descriptions.
  * Parallel JSON metadata file stores mapping, approval status, source name.
  * sentence-transformers MiniLM-L6-v2 for schema text encoding.
  * Falls back to a hash-based TF-IDF embedding when sentence-transformers
    is unavailable (so the module works without GPU / heavy ML deps).

References:
  [Colombo et al. 2025] — Knowledge graph enrichment with retrieval-augmented
    prompting for LLM-driven ETL pipelines.
  [Birjega 2025] — Semantic-RAG architecture for schema-aware retrieval.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, List, Optional

import numpy as np

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Embedding helpers (sentence-transformers or TF-IDF fallback)
# ─────────────────────────────────────────────────────────────────────────────

_EMBED_MODEL = None
_USE_TRANSFORMERS: Optional[bool] = None  # None = not yet probed


def _probe_transformers() -> bool:
    global _USE_TRANSFORMERS
    if _USE_TRANSFORMERS is None:
        try:
            from sentence_transformers import SentenceTransformer  # noqa: F401
            _USE_TRANSFORMERS = True
        except ImportError:
            _USE_TRANSFORMERS = False
            logger.warning(
                "sentence-transformers not installed — RAGSchemaStore will use "
                "a hash-based TF-IDF fallback.  Install with: "
                "pip install sentence-transformers"
            )
    return _USE_TRANSFORMERS


def _get_embed_model():
    global _EMBED_MODEL
    if _probe_transformers() and _EMBED_MODEL is None:
        from sentence_transformers import SentenceTransformer
        _EMBED_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
    return _EMBED_MODEL


def _tfidf_embed(text: str, dim: int = 384) -> np.ndarray:
    """Hash-based bag-of-words embedding (no ML deps required)."""
    import hashlib
    tokens = text.lower().split()
    vec = np.zeros(dim, dtype="float32")
    for tok in tokens:
        idx = int(hashlib.md5(tok.encode()).hexdigest(), 16) % dim
        vec[idx] += 1.0
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec /= norm
    return vec


def embed_text(text: str) -> np.ndarray:
    """Embed a schema text to a 384-dim float32 vector."""
    model = _get_embed_model()
    if model is not None:
        return model.encode([text])[0].astype("float32")
    return _tfidf_embed(text)


# ─────────────────────────────────────────────────────────────────────────────
# Schema → text helper
# ─────────────────────────────────────────────────────────────────────────────

def schema_to_text(schema: Any) -> str:
    """Convert a schema description to a plain text string for embedding.

    Accepts:
      - a SchemaContext object (has ``.columns`` with ``.name`` / ``.dtype``)
      - a list of dicts with ``name`` / ``dtype`` keys
      - a plain ``{column: dtype}`` dict
      - a plain str (passed through unchanged)
    """
    if isinstance(schema, str):
        return schema
    if hasattr(schema, "columns"):          # SchemaContext
        return " | ".join(f"{c.name}:{c.dtype}" for c in schema.columns)
    if isinstance(schema, list):
        if schema and isinstance(schema[0], dict):
            return " | ".join(f"{c['name']}:{c.get('dtype', '?')}" for c in schema)
        return " | ".join(str(c) for c in schema)
    if isinstance(schema, dict):
        return " | ".join(f"{k}:{v}" for k, v in schema.items())
    return str(schema)


# ─────────────────────────────────────────────────────────────────────────────
# RAGSchemaStore
# ─────────────────────────────────────────────────────────────────────────────

class RAGSchemaStore:
    """FAISS-backed adaptive few-shot memory for schema→star-schema mappings.

    Corresponds to Innovation #2 (Adaptive Few-Shot Memory) from the paper.
    Every time a mapping is validated (human-approved or high-confidence auto),
    it is added here.  Subsequent pipeline runs retrieve the k nearest mappings
    as few-shot prompt examples, improving LLM accuracy over time.

    Usage::

        store = RAGSchemaStore("data/rag_store")

        # After a human approves a mapping:
        store.add(source_name="dataset1", schema=schema_ctx,
                  mapping=result.to_dict(), approved_by_human=True)

        # Before building the next prompt:
        examples = store.retrieve(schema=new_ctx, k=3)
        few_shot_text = store.build_few_shot_prompt(examples)
    """

    DIM = 384  # all-MiniLM-L6-v2 output dimension

    def __init__(self, store_path: str = "data/rag_store") -> None:
        import faiss

        self._store_path = Path(store_path)
        self._store_path.mkdir(parents=True, exist_ok=True)
        self._index_file = self._store_path / "schema.index"
        self._meta_file = self._store_path / "schema.meta.json"
        self._metadata: List[dict] = []

        if self._index_file.exists():
            self._index = faiss.read_index(str(self._index_file))
            if self._meta_file.exists():
                self._metadata = json.loads(
                    self._meta_file.read_text(encoding="utf-8")
                )
            logger.info("RAGSchemaStore loaded: %d entries", len(self._metadata))
        else:
            self._index = faiss.IndexFlatL2(self.DIM)
            logger.info("RAGSchemaStore initialised (empty)")

    # ── Core API ──────────────────────────────────────────────────────────────

    def add(
        self,
        source_name: str,
        schema: Any,
        mapping: dict,
        approved_by_human: bool = False,
    ) -> None:
        """Index a new schema→mapping pair.

        Parameters
        ----------
        source_name:
            Dataset identifier (e.g. ``"dataset1_retail_sales"``).
        schema:
            SchemaContext object, column list, or plain text describing the
            source schema.
        mapping:
            Star-schema mapping dict (keys: ``fact_table``, ``dimensions``,
            ``measures``).
        approved_by_human:
            ``True`` when a human reviewer validated this mapping.  These are
            prioritised during retrieval.
        """
        schema_text = schema_to_text(schema)
        vec = embed_text(schema_text)
        self._index.add(np.expand_dims(vec, 0))
        self._metadata.append(
            {
                "source_name": source_name,
                "schema_text": schema_text,
                "mapping": mapping,
                "approved_by_human": approved_by_human,
                "timestamp": time.time(),
            }
        )
        self._save()

    def retrieve(self, schema: Any, k: int = 3) -> List[dict]:
        """Retrieve the *k* most similar stored mappings.

        Human-approved mappings are surfaced first in the result list.
        Returns an empty list when the store contains no entries yet.

        Parameters
        ----------
        schema:
            Schema to search against (same types accepted as ``add()``).
        k:
            Number of candidates to retrieve.
        """
        if self._index.ntotal == 0:
            return []

        schema_text = schema_to_text(schema)
        vec = embed_text(schema_text)
        actual_k = min(k, self._index.ntotal)
        distances, indices = self._index.search(np.expand_dims(vec, 0), actual_k)

        results: List[dict] = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx < 0:
                continue
            meta = dict(self._metadata[idx])
            meta["distance"] = float(dist)
            results.append(meta)

        # Human-approved mappings bubble to the front
        results.sort(key=lambda x: (not x["approved_by_human"], x["distance"]))
        return results

    def build_few_shot_prompt(self, similar: List[dict]) -> str:
        """Format retrieved pairs as a few-shot prompt section.

        Example output::

            Here are examples of previously approved mappings:

            Example 1 (human-approved):
              Schema: date:datetime64 | product:object | amount:float64
              Mapping: {"fact_table": "sales_fact", ...}
        """
        if not similar:
            return ""
        lines = ["Here are examples of previously approved mappings:", ""]
        for i, item in enumerate(similar, 1):
            tag = "human-approved" if item["approved_by_human"] else "auto"
            lines.append(f"Example {i} ({tag}):")
            lines.append(f"  Schema: {item['schema_text']}")
            lines.append(f"  Mapping: {json.dumps(item['mapping'])}")
            lines.append("")
        return "\n".join(lines)

    @property
    def size(self) -> int:
        """Number of entries in the store."""
        return self._index.ntotal

    def reset(self) -> None:
        """Remove all stored entries (useful for ablation studies)."""
        import faiss
        self._index = faiss.IndexFlatL2(self.DIM)
        self._metadata = []
        if self._index_file.exists():
            self._index_file.unlink()
        if self._meta_file.exists():
            self._meta_file.unlink()
        logger.info("RAGSchemaStore reset")

    # ── Persistence ───────────────────────────────────────────────────────────

    def _save(self) -> None:
        import faiss
        faiss.write_index(self._index, str(self._index_file))
        self._meta_file.write_text(
            json.dumps(self._metadata, indent=2), encoding="utf-8"
        )
