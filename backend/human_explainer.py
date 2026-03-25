"""Human-readable product explanations — explain like I'm a human.

Generates plain-language summaries of product analysis results,
translating technical statuses and scores into actionable insights
that non-technical users can understand.

Each explanation answers five questions:
  1. Hva er bra? — What's working well
  2. Hva er problemet? — What needs attention
  3. Hva foreslår vi? — What improvements are available
  4. Hva må vurderes manuelt? — What needs human judgment
  5. Hva bør vi gjøre nå? — Recommended next step
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

from backend.models import (
    ApprovalStatus,
    ProductAnalysis,
    QualityStatus,
)

logger = logging.getLogger(__name__)

# ── Status groups for plain-language mapping ──
_GOOD_STATUSES = {QualityStatus.STRONG, QualityStatus.OK}
_ACTIONABLE_STATUSES = {
    QualityStatus.IMPROVEMENT_READY, QualityStatus.WEAK,
    QualityStatus.SOURCE_CONFLICT, QualityStatus.SHOULD_IMPROVE,
}
_PROBLEM_STATUSES = {
    QualityStatus.PROBABLE_ERROR, QualityStatus.MISSING,
}
_BLOCKED_STATUSES = {
    QualityStatus.NO_RELIABLE_SOURCE, QualityStatus.MANUAL_REVIEW,
    QualityStatus.REQUIRES_MANUFACTURER,
}

# ── Norwegian field names → plain language ──
_FIELD_PLAIN = {
    "Produktnavn": "produktnavnet",
    "Beskrivelse": "beskrivelsen",
    "Spesifikasjon": "spesifikasjonen",
    "Kategori": "kategorien",
    "Pakningsinformasjon": "pakningsinformasjonen",
    "Produsent": "produsentinformasjonen",
    "Produsentens varenummer": "produsentens varenummer",
    "Merkevare": "merkevaren",
    "Bildekvalitet": "bildekvaliteten",
}

# ── Confidence → plain trust level ──
_TRUST_LEVELS = [
    (0.85, "Høy tillit — vi er ganske sikre på dette forslaget"),
    (0.70, "Middels tillit — forslaget ser riktig ut, men bør sjekkes"),
    (0.50, "Lav tillit — dette er et utkast som trenger manuell vurdering"),
    (0.0, "Veldig lav tillit — usikkert forslag, bør verifiseres grundig"),
]


@dataclass
class HumanExplanation:
    """Plain-language explanation of a product's status."""
    article_number: str
    product_name: str
    whats_good: list[str] = field(default_factory=list)
    whats_wrong: list[str] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)
    needs_manual_review: list[str] = field(default_factory=list)
    next_steps: list[str] = field(default_factory=list)
    overall_verdict: str = ""
    confidence_note: str = ""


# ═══════════════════════════════════════════════════════════
# MAIN FUNCTIONS
# ═══════════════════════════════════════════════════════════


def explain_product_like_a_human(result: ProductAnalysis) -> HumanExplanation:
    """Generate a complete human-readable explanation for a product.

    Translates all technical analysis into plain Norwegian that
    a non-technical user can understand and act on.
    """
    pd = result.product_data
    explanation = HumanExplanation(
        article_number=result.article_number,
        product_name=pd.product_name or "(uten navn)",
    )

    # ── 1. What's good ──
    explanation.whats_good = _summarize_whats_good(result)

    # ── 2. What's wrong ──
    explanation.whats_wrong = _summarize_main_issues(result)

    # ── 3. What we suggest ──
    explanation.suggestions = _summarize_suggestions(result)

    # ── 4. What needs manual review ──
    explanation.needs_manual_review = _summarize_manual_review(result)

    # ── 5. Next steps ──
    explanation.next_steps = _summarize_recommended_actions(result)

    # ── Overall verdict ──
    explanation.overall_verdict = _build_verdict(result, explanation)

    # ── Confidence note ──
    explanation.confidence_note = _build_confidence_note(result)

    return explanation


def summarize_main_issues(result: ProductAnalysis) -> list[str]:
    """Get the main issues for a product in plain language."""
    return _summarize_main_issues(result)


def summarize_recommended_actions(result: ProductAnalysis) -> list[str]:
    """Get recommended next actions in plain language."""
    return _summarize_recommended_actions(result)


def build_human_readable_summary(result: ProductAnalysis) -> str:
    """Build a single-paragraph summary suitable for a table cell or tooltip."""
    explanation = explain_product_like_a_human(result)
    parts = []

    if explanation.overall_verdict:
        parts.append(explanation.overall_verdict)

    if explanation.whats_wrong:
        parts.append("Problemer: " + "; ".join(explanation.whats_wrong[:3]))

    if explanation.suggestions:
        parts.append("Forslag: " + "; ".join(explanation.suggestions[:2]))

    if explanation.next_steps:
        parts.append("Neste steg: " + explanation.next_steps[0])

    return " | ".join(parts) if parts else "Ingen vesentlige funn."


# ═══════════════════════════════════════════════════════════
# INTERNAL BUILDERS
# ═══════════════════════════════════════════════════════════


def _summarize_whats_good(result: ProductAnalysis) -> list[str]:
    """Identify what's working well for this product."""
    good = []
    pd = result.product_data

    # Check each field analysis
    good_fields = []
    for fa in result.field_analyses:
        if fa.status in _GOOD_STATUSES:
            plain = _FIELD_PLAIN.get(fa.field_name, fa.field_name.lower())
            good_fields.append(plain)

    if good_fields:
        if len(good_fields) <= 3:
            good.append(f"{_join_list(good_fields)} ser bra ut")
        else:
            good.append(f"{len(good_fields)} av {len(result.field_analyses)} felt har god kvalitet")

    # Website presence
    if pd.found_on_onemed:
        good.append("Produktet finnes på nettbutikken")

    # Images
    iq = result.image_quality or {}
    count = iq.get("image_count_found", 0)
    if count and count >= 2:
        good.append(f"Har {count} bilder")

    # Category
    if result.category_status == "OK":
        good.append("Kategorien er godt strukturert")

    # Webshop readiness
    if result.webshop_status == "Klar":
        good.append("Produktet er klart for nettbutikk")

    return good


def _summarize_main_issues(result: ProductAnalysis) -> list[str]:
    """Identify problems in plain language."""
    issues = []
    pd = result.product_data

    # Missing fields
    missing_fields = []
    weak_fields = []
    conflict_fields = []
    error_fields = []

    for fa in result.field_analyses:
        plain = _FIELD_PLAIN.get(fa.field_name, fa.field_name.lower())
        if fa.status == QualityStatus.MISSING:
            missing_fields.append(plain)
        elif fa.status in (QualityStatus.WEAK, QualityStatus.SHOULD_IMPROVE):
            weak_fields.append(plain)
        elif fa.status == QualityStatus.SOURCE_CONFLICT:
            conflict_fields.append(plain)
        elif fa.status == QualityStatus.PROBABLE_ERROR:
            error_fields.append(plain)

    if missing_fields:
        issues.append(f"Mangler {_join_list(missing_fields)}")

    if error_fields:
        issues.append(f"Sannsynlig feil i {_join_list(error_fields)}")

    if weak_fields:
        issues.append(f"{_join_list(weak_fields)} har for dårlig kvalitet")

    if conflict_fields:
        issues.append(f"Ulike kilder er uenige om {_join_list(conflict_fields)}")

    # Not on website
    if not pd.found_on_onemed:
        issues.append("Produktet finnes ikke på nettbutikken")

    # Image issues
    iq = result.image_quality or {}
    if iq.get("image_count_found", 0) == 0:
        issues.append("Ingen produktbilder funnet")
    elif iq.get("image_quality_status") in ("Dårlig", "Ikke tilstrekkelig"):
        issues.append("Bildekvaliteten er for lav")

    # Category issues
    if result.category_status == "SHOULD_SIMPLIFY":
        issues.append("Kategorien er for detaljert og bør forenkles")
    elif result.category_status == "ATTRIBUTE_AS_CATEGORY":
        issues.append("Noe som er kategorisert burde heller vært et filter (f.eks. materiale eller størrelse)")
    elif result.category_status == "WRONG_CATEGORY":
        issues.append("Produktet ser ut til å ligge i feil kategori")
    elif result.category_status == "MISSING":
        issues.append("Produktet mangler kategori")

    # Webshop readiness
    if result.webshop_status == "Ikke klar" and result.webshop_missing:
        missing = result.webshop_missing
        issues.append(f"Ikke klart for nettbutikk — mangler: {missing}")

    return issues


def _summarize_suggestions(result: ProductAnalysis) -> list[str]:
    """Summarize available improvement suggestions in plain language."""
    suggestions = []

    for es in result.enrichment_suggestions:
        if not es.suggested_value:
            continue

        plain_field = _FIELD_PLAIN.get(es.field_name, es.field_name.lower())
        trust = _trust_label(es.confidence)

        if es.current_value:
            suggestions.append(
                f"Forbedre {plain_field}: «{_truncate(es.suggested_value, 60)}» ({trust})"
            )
        else:
            suggestions.append(
                f"Legg til {plain_field}: «{_truncate(es.suggested_value, 60)}» ({trust})"
            )

    # Category suggestion
    if result.category_suggestion:
        suggestions.append(
            f"Forenkle kategorien til: {result.category_suggestion}"
        )

    return suggestions


def _summarize_manual_review(result: ProductAnalysis) -> list[str]:
    """Identify items that need human judgment."""
    review = []

    # Fields requiring manual review
    for fa in result.field_analyses:
        if fa.status in _BLOCKED_STATUSES:
            plain = _FIELD_PLAIN.get(fa.field_name, fa.field_name.lower())
            if fa.status == QualityStatus.REQUIRES_MANUFACTURER:
                review.append(f"{plain.capitalize()} — må avklares med produsent")
            elif fa.status == QualityStatus.NO_RELIABLE_SOURCE:
                review.append(f"{plain.capitalize()} — ingen pålitelig kilde tilgjengelig")
            elif fa.status == QualityStatus.MANUAL_REVIEW:
                review.append(f"{plain.capitalize()} — uklart, trenger manuell vurdering")

    # Suggestions needing review
    review_suggestions = [
        es for es in result.enrichment_suggestions
        if es.review_required and es.suggested_value
        and es.approval_status in (ApprovalStatus.NOT_REVIEWED, ApprovalStatus.NEEDS_REVIEW)
    ]
    if review_suggestions:
        count = len(review_suggestions)
        review.append(
            f"{count} forbedringsforslag venter på godkjenning"
        )

    # Source conflicts
    conflict_count = sum(
        1 for er in result.enrichment_results
        if er.match_status == "FOUND_IN_BOTH_CONFLICT"
    )
    if conflict_count:
        review.append(
            f"{conflict_count} felt har motstridende informasjon fra ulike kilder"
        )

    return review


def _summarize_recommended_actions(result: ProductAnalysis) -> list[str]:
    """Determine the most important next steps."""
    actions = []

    # Count auto-approved (can be applied directly)
    auto_approved = [
        es for es in result.enrichment_suggestions
        if es.approval_status == ApprovalStatus.AUTO_APPROVED
    ]
    if auto_approved:
        actions.append(
            f"Bruk {len(auto_approved)} automatisk godkjente forbedringer"
        )

    # Pending review suggestions
    pending = [
        es for es in result.enrichment_suggestions
        if es.approval_status in (ApprovalStatus.NOT_REVIEWED, ApprovalStatus.NEEDS_REVIEW)
        and es.suggested_value
    ]
    if pending:
        actions.append(
            f"Vurder {len(pending)} forslag som venter på gjennomgang"
        )

    # Manufacturer contact
    if result.requires_manufacturer_contact:
        actions.append("Kontakt produsent for manglende informasjon")

    # Missing critical fields
    missing_critical = [
        fa for fa in result.field_analyses
        if fa.status == QualityStatus.MISSING
        and fa.field_name in ("Produktnavn", "Beskrivelse", "Produsent")
    ]
    if missing_critical:
        fields = [_FIELD_PLAIN.get(fa.field_name, fa.field_name) for fa in missing_critical]
        actions.append(f"Fyll inn {_join_list(fields)} manuelt — dette er kritisk")

    # Category action
    if result.category_status in ("SHOULD_SIMPLIFY", "ATTRIBUTE_AS_CATEGORY"):
        actions.append("Vurder å forenkle kategorien")

    if not actions:
        if result.webshop_status == "Klar":
            actions.append("Ingen tiltak nødvendig — produktet er klart")
        else:
            actions.append("Gjør en manuell gjennomgang av produktdata")

    return actions


def _build_verdict(result: ProductAnalysis, explanation: HumanExplanation) -> str:
    """Build a one-sentence overall verdict."""
    n_issues = len(explanation.whats_wrong)
    n_suggestions = len(explanation.suggestions)

    if n_issues == 0 and result.webshop_status == "Klar":
        return "Alt ser bra ut — produktet er klart for nettbutikk."

    if n_issues == 0:
        return "Produktdataene ser bra ut, men noen småting kan forbedres."

    if result.priority_label == "Høy":
        return f"Produktet har {n_issues} problemer som bør fikses snart."

    if n_suggestions > 0:
        return (
            f"Vi fant {n_issues} ting å forbedre og har "
            f"{n_suggestions} forslag klare."
        )

    return f"Produktet har {n_issues} ting som trenger oppmerksomhet."


def _build_confidence_note(result: ProductAnalysis) -> str:
    """Build a note about how trustworthy the suggestions are."""
    suggestions = [es for es in result.enrichment_suggestions if es.suggested_value]
    if not suggestions:
        return ""

    avg_conf = sum(es.confidence for es in suggestions) / len(suggestions)
    for threshold, label in _TRUST_LEVELS:
        if avg_conf >= threshold:
            return label
    return ""


# ═══════════════════════════════════════════════════════════
# TEXT HELPERS
# ═══════════════════════════════════════════════════════════


def _trust_label(confidence: float) -> str:
    """Convert confidence score to a short trust label."""
    if confidence >= 0.85:
        return "høy tillit"
    elif confidence >= 0.70:
        return "middels tillit"
    elif confidence >= 0.50:
        return "lav tillit"
    return "veldig lav tillit"


def _truncate(text: str, max_len: int = 80) -> str:
    """Truncate text with ellipsis."""
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return text[:max_len - 1] + "…"


def _join_list(items: list[str]) -> str:
    """Join items with comma and 'og' before last item."""
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    return ", ".join(items[:-1]) + " og " + items[-1]
