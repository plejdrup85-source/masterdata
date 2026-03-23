"""Quick Wins classification for masterdata enrichment.

Identifies low-risk, high-value improvements that can be applied
with minimal manual review. Quick wins are "easy money" — confident,
single-source, non-medical changes that clearly improve the data.

Quick win criteria (ALL must be true):
  1. Confidence ≥ 0.75 (high confidence from a trusted source)
  2. NOT medically sensitive (steril, latex, PVC, sizing etc.)
  3. NOT a source conflict (sources agree, or single source)
  4. Field status is clearly improvable (MISSING, WEAK, SHOULD_IMPROVE, IMPROVEMENT_READY)
  5. NOT from AI/inferred source (only PDF, catalog, website, manufacturer)
  6. NOT a complex family/variant case (single-product scope)
  7. Value passes content validation (no noise, no contact info)

Usage:
    from backend.quick_wins import is_quick_win, classify_suggestions
    for suggestion in suggestions:
        if is_quick_win(suggestion, field_analysis):
            # Safe to auto-apply or export
"""

import logging
from typing import Optional

from backend.models import EnrichmentSuggestion, FieldAnalysis, QualityStatus

logger = logging.getLogger(__name__)

# ── Thresholds ──
QUICK_WIN_MIN_CONFIDENCE = 0.75
QUICK_WIN_MAX_RISK_FIELDS = {"Spesifikasjon"}  # Specs need extra scrutiny


def is_quick_win(
    suggestion: EnrichmentSuggestion,
    field_analysis: Optional[FieldAnalysis] = None,
) -> bool:
    """Determine if a suggestion qualifies as a quick win.

    Returns True only if ALL criteria are met. Conservative by design —
    when in doubt, it's NOT a quick win.
    """
    if not suggestion.suggested_value or not suggestion.suggested_value.strip():
        return False

    # Rule 1: High confidence (threshold adjusts from feedback learning)
    from backend.feedback_learning import get_auto_approval_threshold
    threshold = get_auto_approval_threshold(suggestion.field_name)
    if suggestion.confidence < threshold:
        return False

    # Rule 2: Not medically sensitive
    from backend.medical_safety import is_medically_sensitive
    if is_medically_sensitive(suggestion.suggested_value):
        return False

    # Rule 3: Not a source conflict
    ev = suggestion.evidence_structured or {}
    if ev.get("Kildekonflikter"):
        return False
    # Check field status for SOURCE_CONFLICT
    if field_analysis and field_analysis.status == QualityStatus.SOURCE_CONFLICT:
        return False

    # Rule 4: Field is clearly improvable
    _improvable = {
        QualityStatus.MISSING,
        QualityStatus.WEAK,
        QualityStatus.SHOULD_IMPROVE,
        QualityStatus.IMPROVEMENT_READY,
    }
    if field_analysis and field_analysis.status not in _improvable:
        # STRONG/OK fields shouldn't have suggestions anyway, but guard
        if field_analysis.status in (QualityStatus.STRONG, QualityStatus.OK):
            return False

    # Rule 5: Not AI/inferred source
    source = (suggestion.source or "").lower()
    if any(k in source for k in ("ai", "utledet", "inferred", "heuristic", "spec_structuring")):
        return False

    # Rule 6: Not flagged for review due to complexity
    if suggestion.review_required and suggestion.confidence < 0.85:
        # review_required + moderate confidence = not a quick win
        return False

    # Rule 7: Structured evidence confirms quality
    if ev.get("Variant sikkert identifisert") == "nei":
        return False

    # Extra: Specifications need even higher bar (complex, may contain medical data)
    if suggestion.field_name in QUICK_WIN_MAX_RISK_FIELDS:
        if suggestion.confidence < 0.80:
            return False

    return True


def classify_suggestions(
    suggestions: list[EnrichmentSuggestion],
    field_analyses: list[FieldAnalysis],
) -> tuple[list[EnrichmentSuggestion], list[EnrichmentSuggestion]]:
    """Split suggestions into quick wins and non-quick-wins.

    Returns (quick_wins, other_suggestions).
    """
    fa_map = {fa.field_name: fa for fa in field_analyses}
    quick_wins = []
    others = []

    for s in suggestions:
        fa = fa_map.get(s.field_name)
        if is_quick_win(s, fa):
            quick_wins.append(s)
        else:
            others.append(s)

    return quick_wins, others


def get_quick_win_summary(
    suggestions: list[EnrichmentSuggestion],
    field_analyses: list[FieldAnalysis],
) -> dict:
    """Get a summary of quick win statistics for a product.

    Returns a dict suitable for inclusion in analysis results.
    """
    quick_wins, others = classify_suggestions(suggestions, field_analyses)
    return {
        "quick_win_count": len(quick_wins),
        "other_count": len(others),
        "quick_win_fields": [s.field_name for s in quick_wins],
        "quick_win_total_confidence": (
            round(sum(s.confidence for s in quick_wins) / len(quick_wins), 2)
            if quick_wins else 0
        ),
    }


def filter_quick_wins_from_results(
    results: list,
) -> list:
    """Filter analysis results to only products that have quick wins.

    Returns a filtered copy of the results list where each product
    has at least one quick win suggestion. Used for "export quick wins only".
    """
    filtered = []
    for result in results:
        if not hasattr(result, "enrichment_suggestions") or not hasattr(result, "field_analyses"):
            continue
        quick_wins, _ = classify_suggestions(
            result.enrichment_suggestions or [],
            result.field_analyses or [],
        )
        if quick_wins:
            filtered.append(result)
    return filtered
