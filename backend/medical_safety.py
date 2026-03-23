"""Medical safety gate for masterdata enrichment.

Enforces a "do not guess" policy for medically critical attributes.
When the system cannot confirm a value with high confidence from
a trustworthy source, it blocks the suggestion rather than risking
incorrect medical data reaching production.

This is a PATIENT SAFETY module. Incorrect sterility, material,
or sizing data on medical products can cause real harm.

Usage:
    from backend.medical_safety import (
        screen_suggestion,
        screen_spec_attributes,
    )
    result = screen_suggestion(suggestion)
    if result.blocked:
        # Don't export this suggestion
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# Registry of medically sensitive attributes
# ═══════════════════════════════════════════════════════════

# Each entry: (pattern to match attribute key, human-readable label, reason)
# Patterns are case-insensitive and matched against specification keys,
# field names, and values.

@dataclass(frozen=True)
class SensitiveAttribute:
    """A medically sensitive attribute that requires high confidence."""
    key_pattern: str          # Regex matching the spec key name
    label: str                # Human-readable Norwegian label
    category: str             # Grouping: "sterility", "material", "dimension", etc.
    min_confidence: float     # Minimum confidence to allow suggestion (0.0-1.0)
    reason: str               # Why this attribute is sensitive


# The registry. Order doesn't matter — all are checked.
SENSITIVE_ATTRIBUTES: list[SensitiveAttribute] = [
    # ── Sterility ──
    SensitiveAttribute(
        key_pattern=r"(?i)\b(?:steril|sterilt|usteril|sterilitet|sterilisering|sterilisert)\b",
        label="Sterilitet",
        category="sterility",
        min_confidence=0.90,
        reason="Feil sterilitetsinformasjon kan føre til infeksjoner",
    ),

    # ── Latex / allergens ──
    SensitiveAttribute(
        key_pattern=r"(?i)\b(?:latex|lateksfri|latex.?free|lateks)\b",
        label="Latex/lateksfri",
        category="allergen",
        min_confidence=0.90,
        reason="Feil lateksinformasjon kan utløse allergiske reaksjoner",
    ),
    SensitiveAttribute(
        key_pattern=r"(?i)\b(?:pudder|pudderfri|powder.?free|puderfri)\b",
        label="Pudderfri",
        category="allergen",
        min_confidence=0.85,
        reason="Pudderfri-status er viktig for allergikere",
    ),

    # ── Material safety ──
    SensitiveAttribute(
        key_pattern=r"(?i)\b(?:pvc|pvc.?fri|pvc.?free|ftalat|dehp|di.?etylheksyl)\b",
        label="PVC/PVC-fri/ftalater",
        category="material_safety",
        min_confidence=0.90,
        reason="PVC/ftalat-status er kritisk for pasientsikkerhet",
    ),
    SensitiveAttribute(
        key_pattern=r"(?i)\b(?:nitril|vinyl|silikon|polyuretan|neopren|gummi)\b",
        label="Materiale (medisinsk)",
        category="material",
        min_confidence=0.80,
        reason="Feil materialangivelse kan utløse allergier eller kontaminering",
    ),

    # ── Dimensions / sizing (wrong size = wrong treatment) ──
    SensitiveAttribute(
        key_pattern=r"(?i)\b(?:størrelse|size|str\b|french|gauge|charrière|ch\b|fr\b|ga\b)\b",
        label="Størrelse/gauge",
        category="dimension",
        min_confidence=0.85,
        reason="Feil størrelse på medisinsk utstyr kan skade pasienten",
    ),
    SensitiveAttribute(
        key_pattern=r"(?i)\b(?:diameter|lengde|bredde|tykkelse|dybde)\b.*\d",
        label="Mål (med tallverdi)",
        category="dimension",
        min_confidence=0.85,
        reason="Feil mål på medisinsk utstyr kan gi feil behandling",
    ),
    SensitiveAttribute(
        key_pattern=r"(?i)\b(?:volum|kapasitet|ml\b|liter|cc\b)\b",
        label="Volum/kapasitet",
        category="dimension",
        min_confidence=0.85,
        reason="Feil volum kan føre til feil dosering eller behandling",
    ),

    # ── Connection / compatibility ──
    SensitiveAttribute(
        key_pattern=r"(?i)\b(?:kobling|tilkobling|connector|luer.?lock|luer.?slip|"
                    r"enfit|kompatibel|kompatibilitet|compatible|adapter)\b",
        label="Koblingstype/kompatibilitet",
        category="compatibility",
        min_confidence=0.90,
        reason="Feil koblingstype kan føre til lekkasjer eller feilkobling",
    ),

    # ── Indication / usage domain ──
    SensitiveAttribute(
        key_pattern=r"(?i)\b(?:indikasjon|kontraindikasjon|bruksområde|"
                    r"indication|contraindication|intended.?use)\b",
        label="Indikasjon/bruksområde",
        category="indication",
        min_confidence=0.85,
        reason="Feil indikasjon kan føre til bruk på feil pasientgruppe",
    ),

    # ── Classification / CE marking ──
    SensitiveAttribute(
        key_pattern=r"(?i)\b(?:ce.?merke|ce.?mark|mdr|mdd|klasse\s*(?:i|ii|iii)|"
                    r"class\s*(?:i|ii|iii)|risikoklasse|medisinsk.?utstyr.?klasse)\b",
        label="CE-klassifisering",
        category="classification",
        min_confidence=0.90,
        reason="Feil klassifisering kan ha regulatoriske konsekvenser",
    ),

    # ── Biocompatibility ──
    SensitiveAttribute(
        key_pattern=r"(?i)\b(?:biokompatibel|biocompatible|cytotoksisk|cytotoxic|"
                    r"pyrogenf|pyrogen.?free|endotoksin)\b",
        label="Biokompatibilitet",
        category="biocompatibility",
        min_confidence=0.90,
        reason="Feil biokompatibilitetsdata kan skade pasienten",
    ),

    # ── Storage / expiry (affects product safety) ──
    SensitiveAttribute(
        key_pattern=r"(?i)\b(?:holdbarhet|utløpsdato|expiry|shelf.?life|"
                    r"oppbevar(?:ing|es)|lagr(?:ing|es).*(?:°C|grad|temperatur))\b",
        label="Holdbarhet/oppbevaring",
        category="storage",
        min_confidence=0.80,
        reason="Feil oppbevaringsinfo kan gi degradert produkt",
    ),
]


# ═══════════════════════════════════════════════════════════
# Core detection and blocking functions
# ═══════════════════════════════════════════════════════════

def is_medically_sensitive(text: str) -> list[SensitiveAttribute]:
    """Check if text contains any medically sensitive attributes.

    Returns list of matched SensitiveAttribute entries (empty if none).
    Checks against the full registry of sensitive patterns.
    """
    if not text:
        return []

    matches = []
    for attr in SENSITIVE_ATTRIBUTES:
        if re.search(attr.key_pattern, text):
            matches.append(attr)
    return matches


def is_medically_sensitive_field(field_name: str, value: str = "") -> bool:
    """Quick check: does this field or value contain medical-critical content?"""
    # Check the field name itself
    if is_medically_sensitive(field_name):
        return True
    # Check the value content
    if value and is_medically_sensitive(value):
        return True
    return False


def requires_high_confidence(field_name: str, value: str = "") -> tuple[bool, float, str]:
    """Check if a field/value requires elevated confidence.

    Returns (requires_high, min_confidence, reason).
    If requires_high is True, the suggestion must meet min_confidence
    or be blocked.
    """
    matches = is_medically_sensitive(f"{field_name} {value}")
    if not matches:
        return False, 0.0, ""

    # Use the strictest (highest) confidence requirement among all matches
    strictest = max(matches, key=lambda a: a.min_confidence)
    all_labels = sorted(set(m.label for m in matches))
    reason = (
        f"Medisinsk kritisk: {', '.join(all_labels)}. "
        f"Krever confidence ≥ {strictest.min_confidence:.0%}. "
        f"{strictest.reason}."
    )
    return True, strictest.min_confidence, reason


# ═══════════════════════════════════════════════════════════
# Screening results
# ═══════════════════════════════════════════════════════════

@dataclass
class MedicalScreenResult:
    """Result of medical safety screening for a suggestion."""
    blocked: bool = False
    reason: str = ""
    matched_attributes: list = field(default_factory=list)
    required_confidence: float = 0.0
    actual_confidence: float = 0.0
    field_name: str = ""
    category: str = ""  # Most severe category found


# ═══════════════════════════════════════════════════════════
# Suggestion-level screening
# ═══════════════════════════════════════════════════════════

def screen_suggestion(
    field_name: str,
    suggested_value: str,
    confidence: float,
    source: str = "",
) -> MedicalScreenResult:
    """Screen a single enrichment suggestion against the medical safety gate.

    Returns a MedicalScreenResult. If blocked=True, the suggestion must
    NOT be applied or exported.

    Rules:
    1. If the suggested value contains medically sensitive content AND
       the confidence is below the required threshold → BLOCK.
    2. If source is AI/inferred (not PDF or manufacturer) AND the content
       is medically sensitive → BLOCK regardless of confidence.
    3. If the value doesn't contain sensitive content → PASS.
    """
    result = MedicalScreenResult(
        field_name=field_name,
        actual_confidence=confidence,
    )

    # Check both field name and value for sensitive content
    text_to_check = f"{field_name} {suggested_value}"
    matches = is_medically_sensitive(text_to_check)

    if not matches:
        return result  # Not sensitive — pass through

    result.matched_attributes = [m.label for m in matches]
    strictest = max(matches, key=lambda a: a.min_confidence)
    result.required_confidence = strictest.min_confidence
    result.category = strictest.category

    # Rule 2: AI/inferred sources are NEVER trusted for medical attributes
    source_lower = (source or "").lower()
    is_ai_source = any(k in source_lower for k in ("ai", "utledet", "inferred", "heuristic", "spec_structuring"))
    if is_ai_source:
        result.blocked = True
        result.reason = (
            f"BLOKKERT: Medisinsk kritisk attributt ({', '.join(result.matched_attributes)}) "
            f"kan ikke foreslås fra AI/utledet kilde. "
            f"Kun datablad, katalog eller produsentside er godkjent."
        )
        logger.warning(
            f"[medical-safety] Blocked {field_name}: AI source for "
            f"sensitive content ({', '.join(result.matched_attributes)})"
        )
        return result

    # Rule 1: Confidence must meet the elevated threshold
    if confidence < strictest.min_confidence:
        result.blocked = True
        result.reason = (
            f"BLOKKERT: {', '.join(result.matched_attributes)} krever confidence ≥ "
            f"{strictest.min_confidence:.0%}, men forslaget har {confidence:.0%}. "
            f"{strictest.reason}. Krever manuell vurdering med kildedokument."
        )
        logger.warning(
            f"[medical-safety] Blocked {field_name}: confidence {confidence:.2f} "
            f"< {strictest.min_confidence:.2f} for {', '.join(result.matched_attributes)}"
        )
        return result

    # Passed — high enough confidence from a trusted source
    logger.debug(
        f"[medical-safety] Passed {field_name}: confidence {confidence:.2f} "
        f"≥ {strictest.min_confidence:.2f} for {', '.join(result.matched_attributes)}"
    )
    return result


def screen_spec_attributes(
    spec_text: str,
    confidence: float,
    source: str = "",
) -> tuple[str, list[MedicalScreenResult]]:
    """Screen a specification text and remove unsafe individual attributes.

    Unlike screen_suggestion (which blocks the entire suggestion),
    this function can selectively remove individual key-value pairs
    from a specification string while keeping the safe ones.

    Returns (cleaned_spec_text, list_of_block_results).
    """
    if not spec_text:
        return spec_text, []

    blocks = []

    # Parse semicolon-separated key-value pairs
    if ";" in spec_text and ":" in spec_text:
        pairs = spec_text.split(";")
        safe_pairs = []
        for pair in pairs:
            pair_stripped = pair.strip()
            if not pair_stripped:
                continue

            # Check this individual pair
            result = screen_suggestion("Spesifikasjon", pair_stripped, confidence, source)
            if result.blocked:
                blocks.append(result)
                logger.info(
                    f"[medical-safety] Removed spec pair: '{pair_stripped[:60]}' — "
                    f"{result.reason[:80]}"
                )
            else:
                safe_pairs.append(pair_stripped)

        cleaned = "; ".join(safe_pairs)
        return cleaned, blocks

    # Not parseable as key-value — screen the whole text
    result = screen_suggestion("Spesifikasjon", spec_text, confidence, source)
    if result.blocked:
        return "", [result]

    return spec_text, []
