"""Information scope detection — determines whether content applies to a
specific SKU, a variant within a family, the whole product family/series,
or a packaging level.

The core problem: PDF datasheets, manufacturer pages, and product catalogs
often contain information at different levels:

  - **SKU-level**: applies to exactly one sellable article
    Example: "Størrelse M, Art.nr 12345, 100 stk/eske"
  - **Variant-level**: applies to a specific variant but references others
    Example: a table row for "M" in a size table covering S/M/L/XL
  - **Family-level**: applies to the entire product series
    Example: "SELEFA Kompresser er laget av 100% bomull" (no size/variant)
  - **Packaging-level**: describes packaging hierarchy, not the product itself
    Example: "Transportkartong: 20 esker á 100 stk"

When family-level content is used as if it were SKU-specific, it leads to:
  - Wrong specifications (series spec applied to one variant)
  - Misleading descriptions (generic family text as product description)
  - Noisy suggestions (table data treated as field values)

This module provides detection functions used by the enricher and
content_validator to block or downgrade content that doesn't match
the expected scope for a given SKU.
"""

import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


# ── Scope levels ──


class InformationScope(str, Enum):
    """The level at which a piece of content applies."""
    SKU = "sku"              # Specific to one sellable article
    VARIANT = "variant"      # Specific to a variant, but in context of siblings
    FAMILY = "family"        # Applies to the whole product series
    PACKAGING = "packaging"  # Describes packaging hierarchy only
    UNKNOWN = "unknown"      # Cannot determine


@dataclass
class ScopeResult:
    """Result of scope detection for a piece of text."""
    scope: InformationScope
    confidence: float           # 0.0–1.0
    reason: str                 # Human-readable explanation
    sku_mentioned: bool = False # True if current SKU found in text
    variant_count: int = 0     # Number of distinct variants detected
    blocking: bool = False     # True if this content should be blocked for SKU use


# ── Detection patterns ──

# Patterns that indicate a table of multiple variants/sizes
_MULTI_VARIANT_TABLE = re.compile(
    r"(?i)(?:"
    r"\b(?:størrelser?|size|str)\s*[:/]?\s*(?:[SMLXsmlx]{1,3}\s*[,/]\s*){2,}"   # S, M, L, XL
    r"|\b(?:gauge|ga)\s*[:/]?\s*(?:\d+G?\s*[,/]\s*){2,}"                       # 18G, 21G, 23G
    r"|\b(?:dimensjon|dim|mål)\s*[:/]?\s*(?:\d+\s*x\s*\d+\s*(?:cm|mm)?\s*[,/]\s*){2,}"  # 5x5, 10x10
    r"|\b(?:volum|vol)\s*[:/]?\s*(?:\d+\s*ml\s*[,/]\s*){2,}"                   # 2ml, 5ml, 10ml
    r"|\b(?:CH|Fr)\s*[:/]?\s*(?:\d+\s*[,/]\s*){2,}"                            # CH 6, 8, 10
    r"|\b(?:lengde|length)\s*[:/]?\s*(?:\d+\s*(?:mm|cm)\s*[,/]\s*){2,}"        # 25mm, 40mm
    r")"
)

# Patterns that indicate a table structure with multiple article numbers
_MULTI_ARTNR_PATTERN = re.compile(
    r"\b(?:[A-Z]{0,3}\d{5,8})\b"
)

# Family/series-level language patterns
_FAMILY_LANGUAGE = re.compile(
    r"(?i)(?:"
    r"\b(?:serie[nr]?|family|range|sortiment|produktserie|produktfamilie)\b"
    r"|\b(?:finnes\s+i|leveres\s+i|tilgjengelig\s+i)\s+(?:flere|ulike|forskjellige)\b"
    r"|\b(?:fås\s+i|comes?\s+in|available\s+in)\s+(?:several|various|multiple|different)\b"
    r"|\b(?:alle\s+(?:størrelser|varianter|modeller))\b"
    r"|\b(?:produktene\s+i\s+(?:denne\s+)?serien)\b"
    r"|\b(?:hele\s+(?:serien|sortimentet|produktlinjen))\b"
    r")"
)

# Packaging-level language patterns
_PACKAGING_LANGUAGE = re.compile(
    r"(?i)(?:"
    r"\b(?:transportkartong|pall|masterpakk|ytteremballasje|grossistpakk)\b"
    r"|\b(?:colli|pallet|shipper|outer\s*box|master\s*(?:pack|carton))\b"
    r"|\b(?:esker?\s+(?:pr|per|á|a)\s+(?:kartong|pall))\b"
    r"|\b(?:pakningshierarki|emballasjeoversikt)\b"
    r"|\b(?:enhet|unit)\s*[:\s]+(?:EA|BX|CS|PAL|PK|ST|CTN)\b"
    r")"
)

# Single-product indicators (strengthens SKU scope)
_SKU_SPECIFIC_LANGUAGE = re.compile(
    r"(?i)(?:"
    r"\b(?:dette\s+produktet|this\s+product)\b"
    r"|\b(?:artikkelen|artikkelnummer|art\.?\s*nr\.?)\s*[:\s]+\d"
    r"|\b(?:produsentens?\s+(?:art|vare)\.?\s*nr\.?)\s*[:\s]+"
    r"|\b(?:EAN|GTIN)\s*[:\s]+\d"
    r")"
)

# Variant dimension value patterns — suggest the text targets one specific variant
_SINGLE_VARIANT_INDICATORS = re.compile(
    r"(?i)(?:"
    r"\bstørrelse\s*[:\s]+[A-Z]{1,3}\b"
    r"|\bsize\s*[:\s]+[A-Z]{1,3}\b"
    r"|\bgauge\s*[:\s]+\d+\s*G?\b"
    r"|\b\d+\s*x\s*\d+\s*(?:cm|mm)\b"
    r"|\b(?:farge|color|colour)\s*[:\s]+\w+"
    r"|\b(?:CH|Fr)\s+\d+\b"
    r")"
)

# Table structure markers (rows with multiple columns of data)
_TABLE_ROW_PATTERN = re.compile(
    r"(?:"
    r"(?:\t|  {2,}|\|).*(?:\t|  {2,}|\|)"  # Tab/space/pipe separated columns
    r")"
)


# ── Core detection functions ──


def detect_information_scope(
    text: str,
    current_sku: str = "",
    product_name: str = "",
    known_variant_dims: Optional[list[str]] = None,
) -> ScopeResult:
    """Determine the information scope of a piece of text.

    Args:
        text: The text to classify
        current_sku: Article number of the target product
        product_name: Product name (helps disambiguate)
        known_variant_dims: Known variant dimension values for this product
            (e.g., ["M", "10x10cm"])

    Returns:
        ScopeResult with scope classification, confidence, and reason.
    """
    if not text or not text.strip():
        return ScopeResult(
            scope=InformationScope.UNKNOWN,
            confidence=0.0,
            reason="Tom tekst",
        )

    text = text.strip()

    # ── Signal collection ──
    sku_found = bool(current_sku and current_sku in text)
    multi_variant = bool(_MULTI_VARIANT_TABLE.search(text))
    family_lang = bool(_FAMILY_LANGUAGE.search(text))
    packaging_lang = bool(_PACKAGING_LANGUAGE.search(text))
    sku_specific_lang = bool(_SKU_SPECIFIC_LANGUAGE.search(text))
    single_variant = bool(_SINGLE_VARIANT_INDICATORS.search(text))

    # Count distinct article numbers in text
    artnrs = set(_MULTI_ARTNR_PATTERN.findall(text))
    artnr_count = len(artnrs)
    if current_sku:
        artnrs.discard(current_sku)

    # Count table-like rows
    table_rows = len(_TABLE_ROW_PATTERN.findall(text))

    # Count lines overall
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    line_count = len(lines)

    # ── Packaging scope (check first — most specific) ──
    if packaging_lang and not sku_specific_lang:
        return ScopeResult(
            scope=InformationScope.PACKAGING,
            confidence=0.85,
            reason="Teksten beskriver pakningshierarki/emballasje",
            sku_mentioned=sku_found,
            blocking=True,
        )

    # ── Family scope ──
    family_score = 0.0
    family_reasons = []

    if family_lang:
        family_score += 0.40
        family_reasons.append("Inneholder familie-/seriereferanser")

    # Count how many distinct variant dimension types are listed
    multi_variant_matches = _MULTI_VARIANT_TABLE.findall(text)
    if multi_variant:
        family_score += 0.30
        family_reasons.append("Lister flere varianter/størrelser")
        # Multiple distinct dimension types (e.g. size AND gauge) is strong family signal
        if len(multi_variant_matches) >= 2:
            family_score += 0.15
            family_reasons.append("Flere ulike variasjonsdimensjoner")

    if artnr_count >= 3 and len(artnrs) >= 2:
        family_score += 0.35
        family_reasons.append(f"Refererer til {artnr_count} artikkelnumre")

    if table_rows >= 3 and multi_variant:
        family_score += 0.15
        family_reasons.append(f"Tabellstruktur med {table_rows} rader")

    # Multi-variant + multiple artnrs is a strong family signal
    if multi_variant and artnr_count >= 2:
        family_score += 0.15
        family_reasons.append("Variant-tabell med flere artikler")

    # Reduce family score if current SKU is specifically referenced
    if sku_found and sku_specific_lang:
        family_score -= 0.20

    if family_score >= 0.40:
        return ScopeResult(
            scope=InformationScope.FAMILY,
            confidence=min(0.95, family_score),
            reason="; ".join(family_reasons),
            sku_mentioned=sku_found,
            variant_count=artnr_count,
            blocking=not sku_found,  # Block if our SKU isn't even mentioned
        )

    # ── Variant scope (text is about one variant in a family context) ──
    # Only classify as VARIANT if family signals are weak (few artnrs).
    if single_variant and (multi_variant or artnr_count >= 2) and artnr_count < 3:
        # Text references a specific variant but in context of a variant table
        variant_match = known_variant_dims and any(
            dim.lower() in text.lower() for dim in known_variant_dims
        )
        return ScopeResult(
            scope=InformationScope.VARIANT,
            confidence=0.70 if variant_match else 0.55,
            reason="Teksten refererer til en spesifikk variant i en familie-kontekst",
            sku_mentioned=sku_found,
            variant_count=artnr_count,
            blocking=False,  # Variant content is usable but with care
        )

    # ── SKU scope (default for focused text) ──
    sku_score = 0.0
    sku_reasons = []

    if sku_found:
        sku_score += 0.35
        sku_reasons.append("Artikkelnummer funnet i tekst")

    if sku_specific_lang:
        sku_score += 0.30
        sku_reasons.append("SKU-spesifikt språk")

    if not multi_variant and not family_lang and artnr_count <= 1:
        sku_score += 0.30
        sku_reasons.append("Ingen variant-/familiereferanser")

    if single_variant and not multi_variant:
        sku_score += 0.15
        sku_reasons.append("Én variant nevnt, ingen tabell")

    if sku_score >= 0.30:
        return ScopeResult(
            scope=InformationScope.SKU,
            confidence=min(0.95, sku_score),
            reason="; ".join(sku_reasons) if sku_reasons else "Standard SKU-nivå",
            sku_mentioned=sku_found,
        )

    # ── Unknown / ambiguous ──
    return ScopeResult(
        scope=InformationScope.UNKNOWN,
        confidence=0.30,
        reason="Kan ikke fastslå nivå med sikkerhet",
        sku_mentioned=sku_found,
        variant_count=artnr_count,
    )


def is_family_level_content(
    text: str,
    current_sku: str = "",
    product_name: str = "",
) -> bool:
    """Quick check: is this text family/series-level rather than SKU-specific?

    Use this as a fast boolean gate before using text as SKU-specific content.
    """
    result = detect_information_scope(text, current_sku, product_name)
    return result.scope == InformationScope.FAMILY


def is_variant_specific_content(
    text: str,
    current_sku: str = "",
    variant_dims: Optional[list[str]] = None,
) -> bool:
    """Check if text is about a specific variant (not the whole family).

    Returns True if the text targets a specific variant — meaning it can
    potentially be used for a SKU if the variant matches.
    """
    result = detect_information_scope(
        text, current_sku, known_variant_dims=variant_dims
    )
    return result.scope in (InformationScope.SKU, InformationScope.VARIANT)


def is_packaging_level_content(text: str) -> bool:
    """Check if text is about packaging hierarchy rather than the product."""
    result = detect_information_scope(text)
    return result.scope == InformationScope.PACKAGING


def block_family_content_for_sku(
    text: str,
    current_sku: str,
    field_name: str = "",
    product_name: str = "",
    variant_dims: Optional[list[str]] = None,
) -> tuple[bool, str, ScopeResult]:
    """Decide whether to block a piece of content from being used as SKU-specific data.

    Returns:
        (should_block, reason, scope_result)

    Blocking rules:
    - FAMILY scope: block for description/spec if SKU not mentioned
    - FAMILY scope: downgrade confidence (don't block) if SKU mentioned
    - PACKAGING scope: block for all fields except Pakningsinformasjon
    - VARIANT scope: allow, but flag for review
    - SKU scope: allow
    """
    result = detect_information_scope(
        text, current_sku, product_name, variant_dims
    )

    # SKU-level is always fine
    if result.scope == InformationScope.SKU:
        return False, "", result

    # Packaging-level content: only allow for packaging field
    if result.scope == InformationScope.PACKAGING:
        if field_name == "Pakningsinformasjon":
            return False, "", result
        return (
            True,
            f"Innhold handler om pakningshierarki, ikke produktdata ({result.reason})",
            result,
        )

    # Family-level content — only block for content-heavy fields
    _content_fields = ("Beskrivelse", "Spesifikasjon", "Produktnavn")
    if result.scope == InformationScope.FAMILY:
        # If detected as family and SKU is not mentioned, block content fields
        if result.confidence >= 0.40 and not result.sku_mentioned and field_name in _content_fields:
            return (
                True,
                f"Familie-/serienivå-innhold brukt som SKU-spesifikk data ({result.reason})",
                result,
            )
        # If SKU is mentioned in family-level text, allow but note it
        if result.sku_mentioned:
            return (
                False,
                f"Familie-nivå innhold, men SKU {current_sku} er nevnt — tillatt med forbehold",
                result,
            )
        # Low confidence family detection — allow but flag
        return (
            False,
            f"Mulig familie-nivå ({result.confidence:.0%}) — vurder manuelt",
            result,
        )

    # Variant-level: allow but note
    if result.scope == InformationScope.VARIANT:
        return (
            False,
            f"Variant-nivå innhold — relevant men fra variant-kontekst ({result.reason})",
            result,
        )

    # Unknown: don't block, but flag
    return (
        False,
        f"Scope ukjent ({result.reason})",
        result,
    )


def adjust_confidence_for_scope(
    base_confidence: float,
    scope_result: ScopeResult,
) -> float:
    """Adjust a suggestion's confidence based on information scope.

    Family-level content gets a confidence penalty since it may not apply
    to the specific SKU. Packaging-level content gets a stronger penalty
    when used outside packaging fields.
    """
    if scope_result.scope == InformationScope.SKU:
        return base_confidence  # No adjustment

    if scope_result.scope == InformationScope.VARIANT:
        # Slight penalty — variant content is usually relevant but less certain
        return base_confidence * 0.85

    if scope_result.scope == InformationScope.FAMILY:
        # Significant penalty — family content might not apply to this SKU
        if scope_result.sku_mentioned:
            return base_confidence * 0.65
        return base_confidence * 0.45

    if scope_result.scope == InformationScope.PACKAGING:
        # Heavy penalty outside packaging context
        return base_confidence * 0.30

    # Unknown
    return base_confidence * 0.70
