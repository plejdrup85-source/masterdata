"""Unified learning store for product matching.

All learning examples — from UI overrides, approvals, and historical anbud imports —
are stored in one structure and used by the matching engine to boost candidates.

Storage: data/learning/examples.json (single file, append-optimized)
Batches: data/learning/batches.json (import batch metadata)
"""

import json
import logging
import math
import os
import re
import time
import uuid
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

LEARNING_DIR = Path(os.environ.get("LEARNING_DIR", "data/learning"))
LEARNING_DIR.mkdir(parents=True, exist_ok=True)

EXAMPLES_FILE = LEARNING_DIR / "examples.json"
BATCHES_FILE = LEARNING_DIR / "batches.json"

# Admin password for learning module
LEARNING_ADMIN_PASSWORD_ENV = "LEARNING_MODULE_ADMIN_PASSWORD"


def verify_learning_admin(password: str) -> bool:
    """Verify admin password for learning module access."""
    admin_pw = os.environ.get(LEARNING_ADMIN_PASSWORD_ENV)
    if not admin_pw:
        return False
    return password == admin_pw


# ── Example Storage ──


def _load_examples() -> list[dict]:
    if not EXAMPLES_FILE.exists():
        return []
    try:
        return json.loads(EXAMPLES_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def _save_examples(examples: list[dict]) -> None:
    tmp = EXAMPLES_FILE.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(examples, ensure_ascii=False, default=str), encoding="utf-8")
        tmp.replace(EXAMPLES_FILE)
    except OSError as e:
        logger.error(f"Failed to save learning examples: {e}")
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def add_example(
    input_text: str,
    input_spec: str,
    matched_article: str,
    matched_product_name: str = "",
    source: str = "ui_override",
    source_job_id: str = "",
    import_batch_id: str = "",
    confidence: float = 1.0,
) -> dict:
    """Add a single learning example.

    Sources: 'ui_override', 'ui_approval', 'historical_import'
    """
    example = {
        "id": str(uuid.uuid4())[:12],
        "input_text": input_text.strip(),
        "input_spec": input_spec.strip(),
        "input_tokens": _tokenize(f"{input_text} {input_spec}"),
        "matched_article": matched_article.strip(),
        "matched_product_name": matched_product_name.strip(),
        "source": source,
        "source_job_id": source_job_id,
        "import_batch_id": import_batch_id,
        "created_at": time.time(),
        "confidence": confidence,
    }

    examples = _load_examples()
    examples.append(example)
    _save_examples(examples)
    return example


def add_examples_batch(
    items: list[dict],
    batch_id: str,
    source_job_id: str = "",
) -> int:
    """Add multiple learning examples from an import batch.

    Each item: {input_text, input_spec, matched_article, matched_product_name}
    Returns count of examples added.
    """
    examples = _load_examples()
    count = 0
    for item in items:
        input_text = item.get("input_text", "").strip()
        matched_article = item.get("matched_article", "").strip()
        if not input_text or not matched_article:
            continue

        example = {
            "id": str(uuid.uuid4())[:12],
            "input_text": input_text,
            "input_spec": item.get("input_spec", "").strip(),
            "input_tokens": _tokenize(f"{input_text} {item.get('input_spec', '')}"),
            "matched_article": matched_article,
            "matched_product_name": item.get("matched_product_name", "").strip(),
            "source": "historical_import",
            "source_job_id": source_job_id,
            "import_batch_id": batch_id,
            "created_at": time.time(),
            "confidence": 1.0,
        }
        examples.append(example)
        count += 1

    _save_examples(examples)
    return count


def get_all_examples() -> list[dict]:
    """Get all learning examples."""
    return _load_examples()


def get_examples_by_batch(batch_id: str) -> list[dict]:
    """Get examples for a specific import batch."""
    return [e for e in _load_examples() if e.get("import_batch_id") == batch_id]


def delete_batch(batch_id: str) -> int:
    """Delete all examples from a batch. Returns count removed."""
    examples = _load_examples()
    before = len(examples)
    examples = [e for e in examples if e.get("import_batch_id") != batch_id]
    _save_examples(examples)
    removed = before - len(examples)

    # Also remove batch metadata
    batches = _load_batches()
    batches = [b for b in batches if b.get("batch_id") != batch_id]
    _save_batches(batches)

    return removed


def get_stats() -> dict:
    """Get learning store statistics."""
    examples = _load_examples()
    sources = {}
    for e in examples:
        src = e.get("source", "unknown")
        sources[src] = sources.get(src, 0) + 1

    unique_articles = len(set(e.get("matched_article", "") for e in examples))
    return {
        "total_examples": len(examples),
        "by_source": sources,
        "unique_articles": unique_articles,
        "batches": len(_load_batches()),
    }


# ── Batch Metadata ──


def _load_batches() -> list[dict]:
    if not BATCHES_FILE.exists():
        return []
    try:
        return json.loads(BATCHES_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def _save_batches(batches: list[dict]) -> None:
    tmp = BATCHES_FILE.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(batches, ensure_ascii=False, default=str), encoding="utf-8")
        tmp.replace(BATCHES_FILE)
    except OSError as e:
        logger.error(f"Failed to save batch metadata: {e}")
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def register_batch(
    batch_id: str,
    name: str,
    source_filename: str,
    total_rows: int,
    valid_examples: int,
    rejected_rows: int,
    mapped_fields: dict,
    imported_by: str = "",
) -> dict:
    """Register an import batch for tracking."""
    batch = {
        "batch_id": batch_id,
        "name": name,
        "source_filename": source_filename,
        "total_rows": total_rows,
        "valid_examples": valid_examples,
        "rejected_rows": rejected_rows,
        "mapped_fields": mapped_fields,
        "imported_by": imported_by,
        "created_at": time.time(),
    }
    batches = _load_batches()
    batches.append(batch)
    _save_batches(batches)
    return batch


def list_batches() -> list[dict]:
    """List all import batches."""
    batches = _load_batches()
    batches.sort(key=lambda b: b.get("created_at", 0), reverse=True)
    return batches


# ── Similarity Search (used by matching engine) ──


_STOP_WORDS = frozenset([
    "og", "i", "for", "med", "til", "av", "en", "et", "den", "det", "de",
    "som", "er", "var", "på", "fra", "ved", "om", "eller", "the", "and",
    "for", "with", "of", "in", "to", "a", "an", "is", "are", "str",
    "stk", "pk", "per", "pr", "ca", "mm", "cm", "ml", "mg", "kg", "g",
])


def _tokenize(text: str) -> list[str]:
    """Tokenize text for similarity matching."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\sæøåÆØÅ]", " ", text)
    tokens = text.split()
    return [t for t in tokens if len(t) > 1 and t not in _STOP_WORDS]


def _jaccard(tokens_a: list[str], tokens_b: list[str]) -> float:
    """Jaccard similarity between two token lists."""
    if not tokens_a or not tokens_b:
        return 0.0
    set_a = set(tokens_a)
    set_b = set(tokens_b)
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union if union > 0 else 0.0


def find_matching_examples(
    input_text: str,
    input_spec: str = "",
    min_similarity: float = 0.3,
    max_results: int = 10,
) -> list[dict]:
    """Find learning examples similar to input text.

    Returns examples sorted by similarity, with similarity score added.
    Used by the matching engine to boost candidates.
    """
    examples = _load_examples()
    if not examples:
        return []

    query_tokens = _tokenize(f"{input_text} {input_spec}")
    if not query_tokens:
        return []

    scored = []
    for ex in examples:
        ex_tokens = ex.get("input_tokens") or _tokenize(f"{ex.get('input_text', '')} {ex.get('input_spec', '')}")
        sim = _jaccard(query_tokens, ex_tokens)
        if sim >= min_similarity:
            scored.append({
                **ex,
                "similarity": round(sim, 3),
            })

    scored.sort(key=lambda x: x["similarity"], reverse=True)
    return scored[:max_results]


def get_learned_articles(
    input_text: str,
    input_spec: str = "",
    min_similarity: float = 0.3,
) -> dict[str, dict]:
    """Get article numbers that have been learned for similar inputs.

    Returns: {article_number: {"similarity": float, "source": str, "count": int, "best_example": dict}}
    Used directly by the matching engine for candidate boosting.
    """
    matches = find_matching_examples(input_text, input_spec, min_similarity)
    if not matches:
        return {}

    articles: dict[str, dict] = {}
    for m in matches:
        artnr = m["matched_article"]
        if artnr not in articles:
            articles[artnr] = {
                "similarity": m["similarity"],
                "source": m["source"],
                "count": 1,
                "best_example": m,
            }
        else:
            articles[artnr]["count"] += 1
            if m["similarity"] > articles[artnr]["similarity"]:
                articles[artnr]["similarity"] = m["similarity"]
                articles[artnr]["best_example"] = m

    return articles


def reindex() -> int:
    """Reindex all examples (regenerate tokens). Returns count."""
    examples = _load_examples()
    for ex in examples:
        ex["input_tokens"] = _tokenize(f"{ex.get('input_text', '')} {ex.get('input_spec', '')}")
    _save_examples(examples)
    return len(examples)
