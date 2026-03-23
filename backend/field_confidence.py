"""Per-field confidence scoring for masterdata quality analysis.

Computes a 0–100 confidence score for each product field based on
multiple weighted components. The score answers: "How confident are
we that this field's current value is correct and complete?"

Components (each 0–100, weighted by field type):
  - source_quality:    How many/trustworthy sources confirm this value
  - completeness:      How rich/detailed the value is for its field type
  - source_agreement:  Do sources (website, Jeeves, PDF) agree?
  - language_quality:  Is the value in proper Norwegian?
  - data_cleanliness:  Is the value free of noise, contact info, PDF artifacts?

Usage:
    from backend.field_confidence import calculate_field_confidence
    score, details = calculate_field_confidence(field_analysis)
    field_analysis.confidence = score
    field_analysis.confidence_details = details
"""

import re
from typing import Optional

from backend.models import FieldAnalysis, QualityStatus


# ── Component weights per field type ──
# Each field type has different priorities for what makes confidence high.
_FIELD_WEIGHTS = {
    "Produktnavn": {
        "source_quality": 0.30,
        "completeness": 0.25,
        "source_agreement": 0.25,
        "language_quality": 0.10,
        "data_cleanliness": 0.10,
    },
    "Beskrivelse": {
        "source_quality": 0.20,
        "completeness": 0.30,
        "source_agreement": 0.15,
        "language_quality": 0.20,
        "data_cleanliness": 0.15,
    },
    "Spesifikasjon": {
        "source_quality": 0.25,
        "completeness": 0.35,
        "source_agreement": 0.15,
        "language_quality": 0.10,
        "data_cleanliness": 0.15,
    },
    "Kategori": {
        "source_quality": 0.35,
        "completeness": 0.30,
        "source_agreement": 0.20,
        "language_quality": 0.05,
        "data_cleanliness": 0.10,
    },
    "Bildekvalitet": {
        "source_quality": 0.40,
        "completeness": 0.40,
        "source_agreement": 0.05,
        "language_quality": 0.00,
        "data_cleanliness": 0.15,
    },
    "Produsent": {
        "source_quality": 0.35,
        "completeness": 0.20,
        "source_agreement": 0.30,
        "language_quality": 0.05,
        "data_cleanliness": 0.10,
    },
    "Produsentens varenummer": {
        "source_quality": 0.40,
        "completeness": 0.20,
        "source_agreement": 0.30,
        "language_quality": 0.00,
        "data_cleanliness": 0.10,
    },
}

# Default weights for fields not in the map
_DEFAULT_WEIGHTS = {
    "source_quality": 0.30,
    "completeness": 0.25,
    "source_agreement": 0.20,
    "language_quality": 0.10,
    "data_cleanliness": 0.15,
}


# ═══════════════════════════════════════════════════════════
# Component scorers (each returns 0–100)
# ═══════════════════════════════════════════════════════════

def score_source_quality(fa: FieldAnalysis) -> int:
    """Score based on source trustworthiness using the golden source hierarchy.

    Uses the field's golden source priority to determine how good the
    winning source is for THIS specific field. A catalog source scores
    highest for Produsent but lower for Beskrivelse (where website is king).

    100 = value comes from the #1 golden source for this field
    85  = value comes from the #2 golden source
    70  = value from a mid-tier source
    40  = value from a weak/inferred source
    Bonus: +15 if two independent sources both have values
    """
    if not fa.current_value:
        return 0

    from backend.golden_source import (
        get_source_priority, get_tier_for_origin,
        TIER_CONFIDENCE, TIER_NONE,
    )

    has_website = bool(fa.website_value and fa.website_value.strip())
    has_jeeves = bool(fa.jeeves_value and fa.jeeves_value.strip())
    origin = fa.value_origin or ""

    # Determine which tier the winning value came from
    tier = get_tier_for_origin(origin)
    priority = get_source_priority(fa.field_name)

    # Score based on position in this field's golden source list
    if tier != TIER_NONE and tier in priority:
        position = priority.index(tier)  # 0 = best
        # Position 0 → 90, position 1 → 78, position 2 → 66, etc.
        base_score = max(40, 90 - position * 12)
    elif tier != TIER_NONE:
        base_score = int(TIER_CONFIDENCE.get(tier, 0.3) * 70)
    else:
        base_score = 30 if fa.current_value else 0

    # Bonus for multi-source confirmation
    if has_website and has_jeeves:
        base_score = min(100, base_score + 15)

    return min(100, base_score)


def score_completeness(fa: FieldAnalysis) -> int:
    """Score based on how rich/complete the value is for its field type.

    Uses field-specific heuristics: description needs length + sentences,
    specification needs structured attributes, name needs proper length, etc.
    """
    val = fa.current_value
    if not val or not val.strip():
        return 0

    val = val.strip()
    field = fa.field_name

    if field == "Produktnavn":
        length = len(val)
        if length >= 15:
            return 95
        if length >= 10:
            return 80
        if length >= 5:
            return 60
        return 30

    if field == "Beskrivelse":
        length = len(val)
        has_sentences = bool(re.search(r"[.!?]\s", val))
        has_multiple_sentences = len(re.findall(r"[.!?]\s", val)) >= 2
        if length >= 100 and has_multiple_sentences:
            return 95
        if length >= 80 and has_sentences:
            return 80
        if length >= 40:
            return 60
        if length >= 20:
            return 40
        return 20

    if field == "Spesifikasjon":
        # Count key-value pairs (e.g., "Materiale: Nitril")
        kv_count = len(re.findall(r"\w+\s*:\s*\w+", val))
        has_units = bool(re.search(r"\d+\s*(?:mm|cm|ml|g|kg|stk|pk|%)", val, re.I))
        if kv_count >= 4 and has_units:
            return 95
        if kv_count >= 3:
            return 80
        if kv_count >= 1 or has_units:
            return 60
        if len(val) >= 20:
            return 40
        return 20

    if field == "Kategori":
        # Deeper hierarchy = more complete
        if " > " in val:
            depth = val.count(" > ") + 1
            if depth >= 3:
                return 95
            if depth >= 2:
                return 80
            return 65
        if len(val) >= 5:
            return 50
        return 30

    if field == "Bildekvalitet":
        # Parse score from value like "Score: 80/100, 3 bilde(r) funnet"
        m = re.search(r"Score:\s*(\d+)", val)
        if m:
            return min(100, int(m.group(1)))
        if "tilgjengelig" in val.lower():
            return 50
        return 0

    if field in ("Produsent", "Produsentens varenummer"):
        if len(val) >= 3:
            return 85
        if len(val) >= 1:
            return 50
        return 0

    # Default: based on length
    if len(val) >= 20:
        return 70
    if len(val) >= 5:
        return 50
    return 30


def score_source_agreement(fa: FieldAnalysis) -> int:
    """Score based on whether sources agree on the value.

    100 = sources agree (or only one source exists)
    50  = sources partially agree (one is subset of other)
    0   = sources clearly disagree
    """
    web = (fa.website_value or "").strip()
    jeeves = (fa.jeeves_value or "").strip()

    if not web or not jeeves:
        return 80  # Only one source — no conflict, but can't fully confirm

    web_lower = web.lower().rstrip(".,;:")
    jeeves_lower = jeeves.lower().rstrip(".,;:")

    if web_lower == jeeves_lower:
        return 100  # Exact match

    # One contains the other
    if web_lower in jeeves_lower or jeeves_lower in web_lower:
        return 75  # Partial agreement (more detail in one)

    # Check word overlap
    web_words = set(web_lower.split())
    jeeves_words = set(jeeves_lower.split())
    if web_words and jeeves_words:
        overlap = web_words & jeeves_words
        overlap_ratio = len(overlap) / max(len(web_words), len(jeeves_words))
        if overlap_ratio >= 0.5:
            return 60  # Significant overlap
        if overlap_ratio >= 0.2:
            return 40  # Some overlap

    return 15  # Clearly different


def score_language_quality(fa: FieldAnalysis) -> int:
    """Score based on whether the value is in proper Norwegian.

    Uses lightweight heuristics (not full language detection).
    """
    val = fa.current_value
    if not val or not val.strip():
        return 0

    val = val.strip()

    # Very short values are often codes/names — language doesn't apply
    if len(val) < 15:
        return 80

    val_lower = val.lower()

    # Norwegian indicator words
    no_words = {"og", "for", "med", "som", "til", "av", "er", "ikke", "dette",
                "brukes", "egnet", "hanske", "steril", "pudderfri", "lateksfri"}
    # English indicator words
    en_words = {"the", "and", "for", "with", "this", "that", "is", "are",
                "designed", "intended", "suitable", "glove", "sterile"}
    # Swedish indicator words
    sv_words = {"och", "för", "inte", "användas", "handske", "storlek"}

    words = set(val_lower.split())
    no_count = len(words & no_words)
    en_count = len(words & en_words)
    sv_count = len(words & sv_words)

    total_signals = no_count + en_count + sv_count
    if total_signals == 0:
        return 70  # Can't determine — neutral

    if no_count >= en_count and no_count >= sv_count:
        return min(100, 70 + no_count * 10)

    if en_count > no_count:
        return max(10, 50 - en_count * 10)

    if sv_count > no_count:
        return max(20, 50 - sv_count * 8)

    return 60


def score_data_cleanliness(fa: FieldAnalysis) -> int:
    """Score based on how clean the value is (free of noise, artifacts).

    Checks for contact info, PDF artifacts, table headers, etc.
    """
    val = fa.current_value
    if not val or not val.strip():
        return 0

    val = val.strip()
    deductions = 0

    # Phone numbers
    if re.search(r"(?i)(?:tel|telefon|tlf|fax|phone)\s*[.:]?\s*[\+\d\(\)\s\-]{7,}", val):
        deductions += 30

    # Email addresses
    if re.search(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", val):
        deductions += 25

    # PDF page markers
    if re.search(r"(?i)\bside\s+\d+\b|\bpage\s+\d+\b", val):
        deductions += 15

    # PDF metadata
    if re.search(r"(?i)\bproduktdatablad\b|\bcopyright\b|©", val):
        deductions += 10

    # URLs in non-URL fields
    if fa.field_name not in ("Bildekvalitet",) and re.search(r"https?://", val):
        deductions += 10

    # Excessive special characters
    special_ratio = len(re.findall(r"[^\w\s.,;:\-/()]", val)) / max(len(val), 1)
    if special_ratio > 0.1:
        deductions += 15

    # Very long lines without structure (raw dump)
    if len(val) > 500 and "\n" not in val and ". " not in val:
        deductions += 15

    return max(0, 100 - deductions)


# ═══════════════════════════════════════════════════════════
# Main scoring function
# ═══════════════════════════════════════════════════════════

def calculate_field_confidence(fa: FieldAnalysis) -> tuple[int, str]:
    """Calculate a composite confidence score (0–100) for a field analysis.

    Returns (score, details_string) where details_string is a human-readable
    breakdown of the component scores.

    The score reflects: "How confident are we that this field's value is
    correct and complete enough for production use?"

    Components:
      - Kildekvalitet (source_quality): Number and trustworthiness of sources
      - Fullstendighet (completeness): How rich/detailed the value is
      - Kildesamsvar (source_agreement): Do sources agree?
      - Språkkvalitet (language_quality): Is it in proper Norwegian?
      - Datarenhet (data_cleanliness): Is it free of noise/artifacts?
    """
    # Status-based floor/ceiling: certain statuses cap the confidence
    if fa.status == QualityStatus.MISSING:
        return 0, "Verdi mangler"
    if fa.status == QualityStatus.PROBABLE_ERROR:
        return 10, "Sannsynlig feil — confidence satt til minimum"
    if fa.status == QualityStatus.NO_RELIABLE_SOURCE:
        return 5, "Ingen sikker kilde — kan ikke vurdere"

    # Compute each component
    src = score_source_quality(fa)
    comp = score_completeness(fa)
    agree = score_source_agreement(fa)
    lang = score_language_quality(fa)
    clean = score_data_cleanliness(fa)

    # Get weights for this field type
    weights = _FIELD_WEIGHTS.get(fa.field_name, _DEFAULT_WEIGHTS)

    # Weighted composite
    composite = (
        src * weights["source_quality"]
        + comp * weights["completeness"]
        + agree * weights["source_agreement"]
        + lang * weights["language_quality"]
        + clean * weights["data_cleanliness"]
    )
    score = round(composite)
    score = max(0, min(100, score))

    # Build details string
    detail_parts = [
        f"Kildekvalitet: {src}",
        f"Fullstendighet: {comp}",
        f"Kildesamsvar: {agree}",
    ]
    if weights.get("language_quality", 0) > 0:
        detail_parts.append(f"Språk: {lang}")
    detail_parts.append(f"Renhet: {clean}")

    details = " | ".join(detail_parts)

    return score, details


def calculate_all_field_confidences(field_analyses: list[FieldAnalysis]) -> None:
    """Calculate and set confidence for all field analyses in a list.

    Modifies the FieldAnalysis objects in place.
    """
    for fa in field_analyses:
        score, details = calculate_field_confidence(fa)
        fa.confidence = score
        fa.confidence_details = details
