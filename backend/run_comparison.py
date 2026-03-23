"""Run comparison — detect what changed between two analysis runs.

Compares the current analysis results against a previous run's snapshot
to show what improved, what regressed, and what's new.

Usage:
    from backend.run_comparison import compare_runs, load_snapshot, save_snapshot
    save_snapshot(job_id, results)  # After analysis completes
    prev = load_snapshot(prev_job_id)
    delta = compare_runs(current_results, prev)
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ProductDelta:
    """Change summary for a single product between two runs."""
    article_number: str
    product_name: str = ""
    is_new: bool = False            # First time analyzed
    is_removed: bool = False        # Was in previous, not in current

    # Score changes
    score_before: Optional[float] = None
    score_after: Optional[float] = None
    score_change: float = 0.0       # Positive = improved

    # Webshop readiness changes
    webshop_before: Optional[str] = None
    webshop_after: Optional[str] = None
    webshop_changed: bool = False

    # Priority changes
    priority_before: Optional[str] = None
    priority_after: Optional[str] = None
    priority_changed: bool = False

    # Field-level changes
    fields_improved: list[str] = field(default_factory=list)
    fields_regressed: list[str] = field(default_factory=list)
    fields_unchanged: int = 0

    # Suggestion changes
    new_suggestions: int = 0
    resolved_suggestions: int = 0   # Were suggested before, now OK

    summary: str = ""               # One-line Norwegian summary


@dataclass
class RunComparison:
    """Comparison between two analysis runs."""
    current_job_id: str
    previous_job_id: str
    current_timestamp: str = ""
    previous_timestamp: str = ""

    # Aggregate stats
    total_current: int = 0
    total_previous: int = 0
    new_products: int = 0
    removed_products: int = 0
    improved_count: int = 0         # Score went up
    regressed_count: int = 0        # Score went down
    unchanged_count: int = 0

    # Score movement
    avg_score_before: float = 0.0
    avg_score_after: float = 0.0
    avg_score_change: float = 0.0

    # Webshop readiness movement
    webshop_ready_before: int = 0
    webshop_ready_after: int = 0

    # Per-product deltas (only those with changes)
    deltas: list[ProductDelta] = field(default_factory=list)

    summary: str = ""


# ─── Field status ordering for better/worse comparison ───

_STATUS_RANK = {
    "Sterk": 6,
    "OK": 5,
    "Forbedring klar": 4,
    "Svak": 3,
    "Avvik fra kilde": 3,
    "Bør forbedres": 2,
    "Sannsynlig feil": 1,
    "Mangler": 0,
    "Ingen sikker kilde": 0,
    "Manuell vurdering": 1,
    "Krever produsent": 1,
}


# ═══════════════════════════════════════════════════════════
# SNAPSHOT STORAGE
# ═══════════════════════════════════════════════════════════


def _get_snapshot_dir() -> Path:
    """Get the directory for storing run snapshots."""
    import os
    history_dir = Path(os.environ.get("HISTORY_DIR", "data/history"))
    snap_dir = history_dir / "snapshots"
    snap_dir.mkdir(parents=True, exist_ok=True)
    return snap_dir


def save_snapshot(job_id: str, results: list, timestamp: str = "") -> Path:
    """Save a lightweight snapshot of analysis results for future comparison.

    Stores only the fields needed for comparison (not the full ProductAnalysis).
    """
    snapshot = {
        "job_id": job_id,
        "timestamp": timestamp or datetime.now().isoformat(),
        "product_count": len(results),
        "products": {},
    }

    for r in results:
        artno = r.article_number
        field_statuses = {}
        for fa in r.field_analyses:
            field_statuses[fa.field_name] = {
                "status": fa.status.value if hasattr(fa.status, "value") else str(fa.status),
                "has_suggestion": bool(fa.suggested_value),
            }

        snapshot["products"][artno] = {
            "product_name": r.product_data.product_name or "",
            "total_score": r.total_score,
            "overall_status": r.overall_status.value if hasattr(r.overall_status, "value") else str(r.overall_status),
            "webshop_status": r.webshop_status,
            "priority_label": r.priority_label,
            "priority_score": r.priority_score,
            "field_statuses": field_statuses,
            "suggestion_count": len(r.enrichment_suggestions),
            "auto_fix_possible": r.auto_fix_possible,
            "manual_review_needed": r.manual_review_needed,
        }

    snap_path = _get_snapshot_dir() / f"snapshot_{job_id}.json"
    try:
        snap_path.write_text(
            json.dumps(snapshot, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info(f"Snapshot lagret: {snap_path} ({len(results)} produkter)")
    except Exception as e:
        logger.warning(f"Kunne ikke lagre snapshot: {e}")

    return snap_path


def load_snapshot(job_id: str) -> Optional[dict]:
    """Load a previously saved snapshot."""
    snap_path = _get_snapshot_dir() / f"snapshot_{job_id}.json"
    if not snap_path.exists():
        return None
    try:
        return json.loads(snap_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"Kunne ikke laste snapshot {job_id}: {e}")
        return None


def list_snapshots() -> list[dict]:
    """List available snapshots with metadata."""
    snap_dir = _get_snapshot_dir()
    snapshots = []
    for p in sorted(snap_dir.glob("snapshot_*.json"), key=lambda x: x.stat().st_mtime, reverse=True):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            snapshots.append({
                "job_id": data.get("job_id", ""),
                "timestamp": data.get("timestamp", ""),
                "product_count": data.get("product_count", 0),
            })
        except Exception:
            continue
    return snapshots


def delete_snapshot(job_id: str) -> bool:
    """Delete a snapshot file."""
    snap_path = _get_snapshot_dir() / f"snapshot_{job_id}.json"
    if snap_path.exists():
        snap_path.unlink()
        return True
    return False


# ═══════════════════════════════════════════════════════════
# COMPARISON LOGIC
# ═══════════════════════════════════════════════════════════


def compare_runs(
    current_results: list,
    previous_snapshot: dict,
    current_job_id: str = "",
) -> RunComparison:
    """Compare current analysis results against a previous snapshot.

    Returns a RunComparison with per-product deltas.
    """
    prev_products = previous_snapshot.get("products", {})
    comparison = RunComparison(
        current_job_id=current_job_id,
        previous_job_id=previous_snapshot.get("job_id", ""),
        current_timestamp=datetime.now().isoformat(),
        previous_timestamp=previous_snapshot.get("timestamp", ""),
        total_current=len(current_results),
        total_previous=len(prev_products),
    )

    current_artnos = set()
    score_changes = []

    for r in current_results:
        artno = r.article_number
        current_artnos.add(artno)

        prev = prev_products.get(artno)
        if prev is None:
            comparison.new_products += 1
            comparison.deltas.append(ProductDelta(
                article_number=artno,
                product_name=r.product_data.product_name or "",
                is_new=True,
                score_after=r.total_score,
                webshop_after=r.webshop_status,
                priority_after=r.priority_label,
                summary="Nytt produkt i denne kjøringen",
            ))
            continue

        delta = _compare_product(r, prev)
        if delta.score_change > 0:
            comparison.improved_count += 1
        elif delta.score_change < 0:
            comparison.regressed_count += 1
        else:
            comparison.unchanged_count += 1
        score_changes.append(delta.score_change)

        # Only include products with actual changes
        has_changes = (
            delta.score_change != 0
            or delta.webshop_changed
            or delta.priority_changed
            or delta.fields_improved
            or delta.fields_regressed
            or delta.new_suggestions > 0
            or delta.resolved_suggestions > 0
        )
        if has_changes:
            comparison.deltas.append(delta)

    # Products in previous but not in current
    for artno in prev_products:
        if artno not in current_artnos:
            comparison.removed_products += 1
            prev = prev_products[artno]
            comparison.deltas.append(ProductDelta(
                article_number=artno,
                product_name=prev.get("product_name", ""),
                is_removed=True,
                score_before=prev.get("total_score"),
                webshop_before=prev.get("webshop_status"),
                summary="Ikke med i denne kjøringen",
            ))

    # Aggregate scores
    current_scores = [r.total_score for r in current_results]
    prev_scores = [p.get("total_score", 0) for p in prev_products.values()]
    comparison.avg_score_after = round(sum(current_scores) / max(len(current_scores), 1), 1)
    comparison.avg_score_before = round(sum(prev_scores) / max(len(prev_scores), 1), 1)
    comparison.avg_score_change = round(comparison.avg_score_after - comparison.avg_score_before, 1)

    # Webshop readiness counts
    comparison.webshop_ready_before = sum(
        1 for p in prev_products.values() if p.get("webshop_status") == "Klar"
    )
    comparison.webshop_ready_after = sum(
        1 for r in current_results if r.webshop_status == "Klar"
    )

    # Build summary
    parts = []
    if comparison.avg_score_change > 0:
        parts.append(f"Snittscoren økte med {comparison.avg_score_change}")
    elif comparison.avg_score_change < 0:
        parts.append(f"Snittscoren sank med {abs(comparison.avg_score_change)}")
    if comparison.improved_count:
        parts.append(f"{comparison.improved_count} produkter forbedret")
    if comparison.regressed_count:
        parts.append(f"{comparison.regressed_count} produkter ble dårligere")
    ws_change = comparison.webshop_ready_after - comparison.webshop_ready_before
    if ws_change > 0:
        parts.append(f"{ws_change} nye nettbutikkklare")
    elif ws_change < 0:
        parts.append(f"{abs(ws_change)} færre nettbutikkklare")
    if comparison.new_products:
        parts.append(f"{comparison.new_products} nye produkter")
    comparison.summary = ". ".join(parts) if parts else "Ingen endringer"

    # Sort deltas: biggest regressions first, then improvements
    comparison.deltas.sort(key=lambda d: d.score_change)

    return comparison


def _compare_product(current, prev: dict) -> ProductDelta:
    """Compare a single product between current result and previous snapshot."""
    artno = current.article_number
    delta = ProductDelta(
        article_number=artno,
        product_name=current.product_data.product_name or "",
    )

    # Score
    delta.score_before = prev.get("total_score", 0)
    delta.score_after = current.total_score
    delta.score_change = round(delta.score_after - (delta.score_before or 0), 1)

    # Webshop status
    delta.webshop_before = prev.get("webshop_status")
    delta.webshop_after = current.webshop_status
    delta.webshop_changed = delta.webshop_before != delta.webshop_after

    # Priority
    delta.priority_before = prev.get("priority_label")
    delta.priority_after = current.priority_label
    delta.priority_changed = delta.priority_before != delta.priority_after

    # Field status changes
    prev_fields = prev.get("field_statuses", {})
    for fa in current.field_analyses:
        fname = fa.field_name
        curr_status = fa.status.value if hasattr(fa.status, "value") else str(fa.status)
        prev_fa = prev_fields.get(fname)
        if prev_fa is None:
            continue
        prev_status = prev_fa.get("status", "")
        curr_rank = _STATUS_RANK.get(curr_status, 0)
        prev_rank = _STATUS_RANK.get(prev_status, 0)
        if curr_rank > prev_rank:
            delta.fields_improved.append(fname)
        elif curr_rank < prev_rank:
            delta.fields_regressed.append(fname)
        else:
            delta.fields_unchanged += 1

    # Suggestion changes
    prev_suggestion_count = prev.get("suggestion_count", 0)
    curr_suggestion_count = len(current.enrichment_suggestions)
    if curr_suggestion_count > prev_suggestion_count:
        delta.new_suggestions = curr_suggestion_count - prev_suggestion_count
    # Resolved: had suggestions before, fewer now (fields improved)
    for fname in delta.fields_improved:
        prev_fa = prev_fields.get(fname, {})
        if prev_fa.get("has_suggestion"):
            delta.resolved_suggestions += 1

    # Build summary
    parts = []
    if delta.score_change > 0:
        parts.append(f"Score +{delta.score_change}")
    elif delta.score_change < 0:
        parts.append(f"Score {delta.score_change}")
    if delta.webshop_changed:
        parts.append(f"Nettbutikk: {delta.webshop_before} → {delta.webshop_after}")
    if delta.fields_improved:
        parts.append(f"{len(delta.fields_improved)} felt forbedret")
    if delta.fields_regressed:
        parts.append(f"{len(delta.fields_regressed)} felt ble dårligere")
    if delta.resolved_suggestions:
        parts.append(f"{delta.resolved_suggestions} forslag løst")
    delta.summary = "; ".join(parts) if parts else "Ingen endring"

    return delta
