"""Priority scoring — which products should be fixed first?

Answers: "Given limited time, which products give the most value if improved?"

The score is 0–100 where higher = should be fixed sooner. It combines:

  **Impact signals** (how bad is the current state?):
    - Webshop readiness gap: Ikke klar=30, Delvis=15, Klar=0
    - Field problem count: up to 20 pts (scaled by severity)
    - Image problems: up to 10 pts
    - Low total quality score: up to 10 pts

  **Effort signals** (how easy is it to fix?):
    - Quick wins available: +15 pts (easy improvements exist)
    - High-confidence suggestions: +10 pts
    - Auto-fix possible: +5 pts
    - Manufacturer contact needed: −10 pts (slow, external dependency)

Each signal produces a sub-score that's summed and clamped to 0–100.
The reasons list explains the top contributors in plain Norwegian.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ── Weight constants ──

# Impact weights
W_WEBSHOP_NOT_READY = 30
W_WEBSHOP_PARTIAL = 15
W_FIELD_PROBLEMS_MAX = 20
W_IMAGE_PROBLEMS_MAX = 10
W_LOW_QUALITY_MAX = 10

# Effort weights (positive = easier to fix = higher priority)
W_QUICK_WINS = 15
W_HIGH_CONF_SUGGESTIONS = 10
W_AUTO_FIX = 5
W_MANUFACTURER_CONTACT = -10  # negative = harder


@dataclass
class PriorityResult:
    """Priority evaluation for a single product."""
    score: int = 0                    # 0-100 composite priority
    label: str = ""                   # "Høy" / "Middels" / "Lav"
    reasons: list[str] = field(default_factory=list)  # Norwegian explanations
    impact_score: int = 0             # Impact sub-total
    effort_score: int = 0             # Effort sub-total
    summary: str = ""                 # One-line for Excel


# ── Severity weights for field statuses ──

_STATUS_SEVERITY = {
    "Mangler": 3.0,
    "Sannsynlig feil": 2.5,
    "Krever produsent": 2.0,
    "Ingen sikker kilde": 2.0,
    "Manuell vurdering": 1.5,
    "Bør forbedres": 1.5,
    "Avvik fra kilde": 1.0,
    "Svak": 1.0,
    "Forbedring klar": 0.5,
    "OK": 0.0,
    "Sterk": 0.0,
}


def calculate_priority_score(analysis: "ProductAnalysis") -> PriorityResult:
    """Calculate a composite priority score for a product.

    Higher score = should be addressed sooner (more impact, less effort).
    """
    from backend.quick_wins import is_quick_win

    result = PriorityResult()
    reasons = []
    impact = 0
    effort = 0

    # ── Impact: Webshop readiness ──
    ws = analysis.webshop_status or ""
    if ws == "Ikke klar":
        impact += W_WEBSHOP_NOT_READY
        reasons.append(f"Ikke nettbutikk-klar (+{W_WEBSHOP_NOT_READY})")
    elif ws == "Delvis klar":
        impact += W_WEBSHOP_PARTIAL
        reasons.append(f"Delvis nettbutikk-klar (+{W_WEBSHOP_PARTIAL})")

    # ── Impact: Field problems (weighted by severity) ──
    severity_sum = 0.0
    problem_fields = []
    for fa in analysis.field_analyses:
        sev = _STATUS_SEVERITY.get(fa.status.value if hasattr(fa.status, 'value') else str(fa.status), 0.0)
        if sev > 0:
            severity_sum += sev
            if sev >= 1.5:
                problem_fields.append(fa.field_name)

    # Scale: 0–10 severity points → 0–20 priority points
    field_pts = min(W_FIELD_PROBLEMS_MAX, round(severity_sum * 2))
    if field_pts > 0:
        impact += field_pts
        if problem_fields:
            reasons.append(
                f"{len(problem_fields)} felt med problemer: "
                + ", ".join(problem_fields[:3])
                + (f" +{len(problem_fields)-3}" if len(problem_fields) > 3 else "")
                + f" (+{field_pts})"
            )

    # ── Impact: Image problems ──
    iq = analysis.image_quality or {}
    img_status = iq.get("image_quality_status", "MISSING")
    img_count = iq.get("image_count_found", 0)
    if img_status == "MISSING" or img_count == 0:
        impact += W_IMAGE_PROBLEMS_MAX
        reasons.append(f"Mangler bilder (+{W_IMAGE_PROBLEMS_MAX})")
    elif img_status == "FAIL":
        impact += round(W_IMAGE_PROBLEMS_MAX * 0.7)
        reasons.append(f"Dårlig bildekvalitet (+{round(W_IMAGE_PROBLEMS_MAX * 0.7)})")
    elif img_status == "WARN":
        impact += round(W_IMAGE_PROBLEMS_MAX * 0.3)

    # ── Impact: Low overall quality score ──
    total_score = analysis.total_score or 0
    if total_score < 30:
        impact += W_LOW_QUALITY_MAX
        reasons.append(f"Svært lav kvalitetsscore ({total_score}%) (+{W_LOW_QUALITY_MAX})")
    elif total_score < 50:
        pts = round(W_LOW_QUALITY_MAX * 0.6)
        impact += pts
        reasons.append(f"Lav kvalitetsscore ({total_score}%) (+{pts})")
    elif total_score < 70:
        pts = round(W_LOW_QUALITY_MAX * 0.3)
        impact += pts

    # ── Effort: Quick wins available ──
    fa_map = {fa.field_name: fa for fa in analysis.field_analyses}
    qw_count = 0
    for es in (analysis.enrichment_suggestions or []):
        if es.suggested_value:
            fa = fa_map.get(es.field_name)
            if is_quick_win(es, fa):
                qw_count += 1
    if qw_count >= 3:
        effort += W_QUICK_WINS
        reasons.append(f"{qw_count} quick wins tilgjengelig (+{W_QUICK_WINS})")
    elif qw_count >= 1:
        pts = round(W_QUICK_WINS * 0.6)
        effort += pts
        reasons.append(f"{qw_count} quick win(s) tilgjengelig (+{pts})")

    # ── Effort: High-confidence suggestions ──
    high_conf = sum(
        1 for es in (analysis.enrichment_suggestions or [])
        if es.confidence >= 0.75 and es.suggested_value
    )
    if high_conf >= 2:
        effort += W_HIGH_CONF_SUGGESTIONS
        reasons.append(f"{high_conf} forslag med høy confidence (+{W_HIGH_CONF_SUGGESTIONS})")
    elif high_conf == 1:
        pts = round(W_HIGH_CONF_SUGGESTIONS * 0.5)
        effort += pts

    # ── Effort: Auto-fix possible ──
    if analysis.auto_fix_possible:
        effort += W_AUTO_FIX
        reasons.append(f"Auto-fix mulig (+{W_AUTO_FIX})")

    # ── Effort: Manufacturer contact penalty ──
    if analysis.requires_manufacturer_contact:
        effort += W_MANUFACTURER_CONTACT  # negative
        reasons.append(f"Krever produsentkontakt ({W_MANUFACTURER_CONTACT})")

    # ── Combine ──
    result.impact_score = impact
    result.effort_score = effort
    result.score = max(0, min(100, impact + effort))
    result.reasons = reasons

    # ── Label ──
    if result.score >= 60:
        result.label = "Høy"
    elif result.score >= 30:
        result.label = "Middels"
    else:
        result.label = "Lav"

    # ── Summary ──
    if reasons:
        top_reasons = [r.split(" (+")[0].split(" (−")[0] for r in reasons[:3]]
        result.summary = f"Prioritet {result.score} ({result.label}): {'; '.join(top_reasons)}"
    else:
        result.summary = f"Prioritet {result.score} ({result.label})"

    return result


def get_priority_reasons(analysis: "ProductAnalysis") -> list[str]:
    """Return the list of reasons contributing to the priority score."""
    return calculate_priority_score(analysis).reasons


def sort_products_by_priority(
    analyses: list["ProductAnalysis"],
) -> list[tuple["ProductAnalysis", PriorityResult]]:
    """Sort products by priority score (highest first).

    Returns a list of (analysis, priority_result) tuples.
    """
    scored = [(a, calculate_priority_score(a)) for a in analyses]
    scored.sort(key=lambda x: x[1].score, reverse=True)
    return scored
