"""Batch filtering for product analysis results.

Provides composable filters that can be combined to select subsets of
products for export or review. Each filter is a simple predicate function.

Usage:
    from backend.batch_filters import apply_filters, AVAILABLE_FILTERS
    filtered = apply_filters(results, {"webshop_status": "Ikke klar", "priority": "Høy"})
"""

import logging
from typing import Optional

from backend.models import ProductAnalysis, QualityStatus

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# FILTER FUNCTIONS — each takes a result and returns bool
# ═══════════════════════════════════════════════════════════


def _filter_webshop_status(result: ProductAnalysis, value: str) -> bool:
    """Filter by webshop readiness status."""
    return (result.webshop_status or "") == value


def _filter_priority(result: ProductAnalysis, value: str) -> bool:
    """Filter by priority label."""
    return (result.priority_label or "") == value


def _filter_min_priority_score(result: ProductAnalysis, value: int) -> bool:
    """Filter by minimum priority score."""
    return (result.priority_score or 0) >= value


def _filter_manufacturer(result: ProductAnalysis, value: str) -> bool:
    """Filter by manufacturer name (case-insensitive partial match)."""
    mfr = (result.product_data.manufacturer or "").lower()
    jeeves_supplier = ""
    if result.jeeves_data and result.jeeves_data.supplier:
        jeeves_supplier = result.jeeves_data.supplier.lower()
    needle = value.lower()
    return needle in mfr or needle in jeeves_supplier


def _filter_field_status(result: ProductAnalysis, value: str) -> bool:
    """Filter products where a specific field has problems.

    value format: "field_name" or "field_name:status"
    Examples: "Beskrivelse", "Produktnavn:Mangler", "Spesifikasjon:Bør forbedres"
    """
    parts = value.split(":", 1)
    field_name = parts[0].strip()
    target_status = parts[1].strip() if len(parts) > 1 else None

    for fa in result.field_analyses:
        if fa.field_name == field_name:
            if target_status:
                return fa.status.value == target_status
            else:
                # Any non-OK status
                return fa.status not in (QualityStatus.OK, QualityStatus.STRONG)
    return False


def _filter_image_problem(result: ProductAnalysis, _value: str = "") -> bool:
    """Filter products with image problems (missing, fail, or warn)."""
    iq = result.image_quality or {}
    status = iq.get("image_quality_status", "MISSING")
    count = iq.get("image_count_found", 0)
    return status in ("MISSING", "FAIL", "WARN") or count == 0


def _filter_missing_mfr_artno(result: ProductAnalysis, _value: str = "") -> bool:
    """Filter products missing manufacturer article number."""
    artno = (result.product_data.manufacturer_article_number or "").strip()
    # Only flag if manufacturer is known
    mfr = (result.product_data.manufacturer or "").strip()
    jeeves_supplier = ""
    if result.jeeves_data and result.jeeves_data.supplier:
        jeeves_supplier = result.jeeves_data.supplier.strip()
    has_manufacturer = bool(mfr or jeeves_supplier)
    return has_manufacturer and not artno


def _filter_quick_wins(result: ProductAnalysis, _value: str = "") -> bool:
    """Filter products that have at least one quick win."""
    from backend.quick_wins import classify_suggestions
    if not result.enrichment_suggestions:
        return False
    quick_wins, _ = classify_suggestions(
        result.enrichment_suggestions,
        result.field_analyses or [],
    )
    return len(quick_wins) > 0


def _filter_has_suggestions(result: ProductAnalysis, _value: str = "") -> bool:
    """Filter products with enrichment suggestions."""
    return bool(result.enrichment_suggestions)


def _filter_high_confidence(result: ProductAnalysis, _value: str = "") -> bool:
    """Filter products with at least one high-confidence (≥0.75) suggestion."""
    return any(
        es.confidence >= 0.75 and es.suggested_value
        for es in (result.enrichment_suggestions or [])
    )


def _filter_needs_manufacturer_contact(result: ProductAnalysis, _value: str = "") -> bool:
    """Filter products that need manufacturer contact."""
    return result.requires_manufacturer_contact


def _filter_auto_fixable(result: ProductAnalysis, _value: str = "") -> bool:
    """Filter products where auto-fix is possible."""
    return result.auto_fix_possible


def _filter_manual_review(result: ProductAnalysis, _value: str = "") -> bool:
    """Filter products needing manual review."""
    return result.manual_review_needed


# ═══════════════════════════════════════════════════════════
# FILTER REGISTRY
# ═══════════════════════════════════════════════════════════

AVAILABLE_FILTERS = {
    "webshop_status": {
        "label": "Nettbutikk-status",
        "fn": _filter_webshop_status,
        "options": ["Klar", "Delvis klar", "Ikke klar"],
        "takes_value": True,
    },
    "priority": {
        "label": "Prioritet",
        "fn": _filter_priority,
        "options": ["Høy", "Middels", "Lav"],
        "takes_value": True,
    },
    "min_priority_score": {
        "label": "Min. prioritetsscore",
        "fn": _filter_min_priority_score,
        "takes_value": True,
    },
    "manufacturer": {
        "label": "Produsent",
        "fn": _filter_manufacturer,
        "takes_value": True,
    },
    "field_problem": {
        "label": "Felt med problem",
        "fn": _filter_field_status,
        "options": [
            "Produktnavn", "Beskrivelse", "Spesifikasjon",
            "Produsent", "Produsentens varenummer", "Kategori",
            "Pakningsinformasjon",
        ],
        "takes_value": True,
    },
    "image_problem": {
        "label": "Bildeproblem",
        "fn": _filter_image_problem,
        "takes_value": False,
    },
    "missing_mfr_artno": {
        "label": "Mangler prod.varenr",
        "fn": _filter_missing_mfr_artno,
        "takes_value": False,
    },
    "quick_wins": {
        "label": "Quick wins",
        "fn": _filter_quick_wins,
        "takes_value": False,
    },
    "has_suggestions": {
        "label": "Har forbedringsforslag",
        "fn": _filter_has_suggestions,
        "takes_value": False,
    },
    "high_confidence": {
        "label": "Høy confidence-forslag",
        "fn": _filter_high_confidence,
        "takes_value": False,
    },
    "needs_manufacturer": {
        "label": "Krever produsentkontakt",
        "fn": _filter_needs_manufacturer_contact,
        "takes_value": False,
    },
    "auto_fixable": {
        "label": "Auto-fix mulig",
        "fn": _filter_auto_fixable,
        "takes_value": False,
    },
    "manual_review": {
        "label": "Manuell vurdering",
        "fn": _filter_manual_review,
        "takes_value": False,
    },
}


# ═══════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════


def apply_filters(
    results: list[ProductAnalysis],
    filters: dict[str, str],
) -> list[ProductAnalysis]:
    """Apply multiple filters to a list of results.

    Args:
        results: List of ProductAnalysis objects
        filters: Dict of {filter_name: value} pairs. For boolean filters,
                 value is ignored (pass "").

    Returns:
        Filtered list (AND logic — all filters must match).
    """
    if not filters:
        return results

    filtered = results
    for filter_name, value in filters.items():
        spec = AVAILABLE_FILTERS.get(filter_name)
        if not spec:
            logger.warning(f"Ukjent filter: {filter_name}")
            continue

        fn = spec["fn"]
        if spec.get("takes_value"):
            # Convert numeric values
            if filter_name == "min_priority_score":
                try:
                    value = int(value)
                except (ValueError, TypeError):
                    continue
            filtered = [r for r in filtered if fn(r, value)]
        else:
            filtered = [r for r in filtered if fn(r)]

    logger.info(
        f"Batch-filter: {len(results)} → {len(filtered)} produkter "
        f"(filtre: {', '.join(f'{k}={v}' for k, v in filters.items())})"
    )
    return filtered


def get_filter_counts(
    results: list[ProductAnalysis],
) -> dict[str, dict]:
    """Calculate counts for each filter option given current results.

    Returns a dict suitable for the frontend to build filter UI with counts.
    """
    counts = {}

    # Webshop status distribution
    ws_counts = {}
    for r in results:
        ws = r.webshop_status or "Ikke vurdert"
        ws_counts[ws] = ws_counts.get(ws, 0) + 1
    counts["webshop_status"] = ws_counts

    # Priority distribution
    prio_counts = {}
    for r in results:
        p = r.priority_label or "Ikke vurdert"
        prio_counts[p] = prio_counts.get(p, 0) + 1
    counts["priority"] = prio_counts

    # Boolean filter counts
    bool_filters = {
        "image_problem": _filter_image_problem,
        "missing_mfr_artno": _filter_missing_mfr_artno,
        "quick_wins": _filter_quick_wins,
        "has_suggestions": _filter_has_suggestions,
        "high_confidence": _filter_high_confidence,
        "needs_manufacturer": _filter_needs_manufacturer_contact,
        "auto_fixable": _filter_auto_fixable,
        "manual_review": _filter_manual_review,
    }
    for key, fn in bool_filters.items():
        counts[key] = sum(1 for r in results if fn(r))

    # Field problem counts
    field_counts = {}
    for field_name in ["Produktnavn", "Beskrivelse", "Spesifikasjon",
                       "Produsent", "Produsentens varenummer", "Kategori",
                       "Pakningsinformasjon"]:
        field_counts[field_name] = sum(
            1 for r in results
            if _filter_field_status(r, field_name)
        )
    counts["field_problems"] = field_counts

    # Manufacturer list with counts
    mfr_counts = {}
    for r in results:
        mfr = (r.product_data.manufacturer or "").strip()
        if not mfr and r.jeeves_data and r.jeeves_data.supplier:
            mfr = r.jeeves_data.supplier.strip()
        if mfr:
            mfr_counts[mfr] = mfr_counts.get(mfr, 0) + 1
    counts["manufacturers"] = mfr_counts

    return counts
