"""Targeted re-check — re-analyze only the products that need attention.

Instead of re-running the full catalog, select specific products based on
criteria from their previous analysis results. This is much faster and
lets users focus improvement efforts.

Usage:
    from backend.recheck import get_recheck_candidates, RECHECK_PRESETS
    candidates = get_recheck_candidates(previous_results, preset="low_confidence")
    # → list of article numbers to re-analyze
"""

import logging
from typing import Optional

from backend.batch_filters import apply_filters, AVAILABLE_FILTERS
from backend.models import ProductAnalysis, QualityStatus

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# PRESETS — common re-check scenarios
# ═══════════════════════════════════════════════════════════

RECHECK_PRESETS = {
    "not_webshop_ready": {
        "label": "Ikke nettbutikk-klare",
        "description": "Produkter som ikke er klare for nettbutikk",
        "filters": {"webshop_status": "Ikke klar"},
    },
    "high_priority": {
        "label": "Høy prioritet",
        "description": "Produkter med høy prioritet som bør forbedres først",
        "filters": {"priority": "Høy"},
    },
    "image_problems": {
        "label": "Bildeproblemer",
        "description": "Produkter med manglende eller dårlige bilder",
        "filters": {"image_problem": ""},
    },
    "missing_mfr_artno": {
        "label": "Mangler prod.varenr",
        "description": "Produkter som mangler produsentens varenummer",
        "filters": {"missing_mfr_artno": ""},
    },
    "manual_review": {
        "label": "Manuell vurdering",
        "description": "Produkter som krever manuell gjennomgang",
        "filters": {"manual_review": ""},
    },
    "quick_wins": {
        "label": "Quick wins",
        "description": "Produkter med trygge, raske forbedringer",
        "filters": {"quick_wins": ""},
    },
    "needs_manufacturer": {
        "label": "Produsentkontakt",
        "description": "Produkter som krever kontakt med produsent",
        "filters": {"needs_manufacturer": ""},
    },
    "high_confidence_suggestions": {
        "label": "Høy confidence-forslag",
        "description": "Produkter med høy-confidence forbedringsforslag",
        "filters": {"high_confidence": ""},
    },
}


def get_recheck_candidates(
    results: list[ProductAnalysis],
    filters: Optional[dict[str, str]] = None,
    preset: Optional[str] = None,
    max_products: int = 500,
) -> list[str]:
    """Extract article numbers for re-check based on filter criteria.

    Args:
        results: Previous analysis results to filter
        filters: Dict of filter key-value pairs (same as batch_filters)
        preset: Name of a preset filter set (e.g. "low_confidence")
        max_products: Maximum number of articles to return

    Returns:
        List of article numbers matching the criteria.
    """
    # Resolve preset to filters
    if preset and preset in RECHECK_PRESETS:
        resolved_filters = RECHECK_PRESETS[preset]["filters"].copy()
        # Merge with any additional filters
        if filters:
            resolved_filters.update(filters)
        filters = resolved_filters
    elif not filters:
        filters = {}

    if not filters:
        logger.warning("Ingen filtre angitt for re-check — returnerer ingenting")
        return []

    # Apply filters
    filtered = apply_filters(results, filters)

    # Extract article numbers
    candidates = [r.article_number for r in filtered[:max_products]]

    filter_desc = ", ".join(f"{k}={v}" for k, v in filters.items())
    logger.info(
        f"Re-check kandidater: {len(candidates)} av {len(results)} "
        f"(filtre: {filter_desc})"
    )

    return candidates


def filter_products_for_recheck(
    results: list[ProductAnalysis],
    field_name: Optional[str] = None,
    manufacturer: Optional[str] = None,
    min_priority_score: Optional[int] = None,
    webshop_status: Optional[str] = None,
    only_with_suggestions: bool = False,
    only_image_problems: bool = False,
    only_missing_mfr_artno: bool = False,
) -> list[str]:
    """Convenience function with named parameters for common re-check scenarios.

    Returns list of article numbers. All specified criteria use AND logic.
    """
    filters = {}
    if webshop_status:
        filters["webshop_status"] = webshop_status
    if manufacturer:
        filters["manufacturer"] = manufacturer
    if min_priority_score is not None:
        filters["min_priority_score"] = str(min_priority_score)
    if field_name:
        filters["field_problem"] = field_name
    if only_with_suggestions:
        filters["has_suggestions"] = ""
    if only_image_problems:
        filters["image_problem"] = ""
    if only_missing_mfr_artno:
        filters["missing_mfr_artno"] = ""

    return get_recheck_candidates(results, filters=filters)


def get_recheck_summary(
    results: list[ProductAnalysis],
) -> dict:
    """Show how many products match each preset — helps user pick the right re-check.

    Returns a dict of {preset_name: {label, description, count}}.
    """
    summary = {}
    for name, preset in RECHECK_PRESETS.items():
        candidates = get_recheck_candidates(
            results, filters=preset["filters"],
        )
        summary[name] = {
            "label": preset["label"],
            "description": preset["description"],
            "count": len(candidates),
        }
    return summary
