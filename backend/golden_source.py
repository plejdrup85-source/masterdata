"""Golden source hierarchy for masterdata fields.

Defines the authoritative source priority per field. Every part of the app
that needs to pick "which value wins?" should consult this module instead
of implementing its own logic.

The hierarchy is field-specific because different fields have different
trust characteristics:
  - Manufacturer: catalog (Jeeves supplier) is ground truth
  - Description: website prose is usually richest
  - Specification: PDF datasheets have the most structured data
  - Image: website is the canonical display source

Usage:
    from backend.golden_source import resolve_field_value, FIELD_SOURCE_PRIORITY
    winner = resolve_field_value("Beskrivelse", candidates)
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# Source tier definitions
# ═══════════════════════════════════════════════════════════

# Source tier IDs — lower number = higher authority.
# Used in the priority registry and in confidence scoring.
TIER_CATALOG = 1       # Jeeves ERP / product catalog
TIER_WEBSITE = 2       # onemed.no product page
TIER_PDF = 3           # Product datasheet (internal PDF)
TIER_MANUFACTURER = 4  # Manufacturer's own website
TIER_INFERRED = 5      # URL inference, AI, heuristic
TIER_NONE = 99         # No source

TIER_LABELS = {
    TIER_CATALOG: "katalog",
    TIER_WEBSITE: "nettside",
    TIER_PDF: "datablad",
    TIER_MANUFACTURER: "produsentside",
    TIER_INFERRED: "utledet",
    TIER_NONE: "ingen kilde",
}

# Confidence multiplier per tier — used by field_confidence scoring.
# Higher tier = lower multiplier on source_quality component.
TIER_CONFIDENCE = {
    TIER_CATALOG: 1.0,
    TIER_WEBSITE: 0.9,
    TIER_PDF: 0.85,
    TIER_MANUFACTURER: 0.8,
    TIER_INFERRED: 0.5,
    TIER_NONE: 0.0,
}


# ═══════════════════════════════════════════════════════════
# Per-field source priority registry
# ═══════════════════════════════════════════════════════════

# Each field maps to an ordered list of source tiers, best first.
# The resolver tries each tier in order and picks the first non-empty value.
FIELD_SOURCE_PRIORITY: dict[str, list[int]] = {
    "Produsent": [
        TIER_CATALOG,        # supplier from Jeeves (ground truth)
        TIER_WEBSITE,        # manufacturer from onemed.no
        TIER_PDF,            # manufacturer extracted from PDF
        TIER_MANUFACTURER,   # inferred from manufacturer lookup URL
    ],
    "Produsentens varenummer": [
        TIER_CATALOG,        # supplier_item_no from Jeeves
        TIER_PDF,            # extracted from datasheet
        TIER_WEBSITE,        # from product page
        TIER_MANUFACTURER,   # from manufacturer site
    ],
    "Produktnavn": [
        TIER_WEBSITE,        # product name from onemed.no (display name)
        TIER_CATALOG,        # item_description / web_title from Jeeves
        TIER_PDF,            # name from datasheet
        TIER_MANUFACTURER,   # name from manufacturer page
    ],
    "Beskrivelse": [
        TIER_WEBSITE,        # richest prose description
        TIER_MANUFACTURER,   # manufacturer's product text
        TIER_PDF,            # description from datasheet
        TIER_INFERRED,       # AI-normalized or spec-derived
    ],
    "Spesifikasjon": [
        TIER_PDF,            # datasheets have the best structured specs
        TIER_MANUFACTURER,   # manufacturer product specs
        TIER_WEBSITE,        # website specs (sometimes incomplete)
        TIER_CATALOG,        # Jeeves specification field
    ],
    "Kategori": [
        TIER_WEBSITE,        # breadcrumb / category from onemed.no
        TIER_CATALOG,        # catalog mapping if defined
        TIER_INFERRED,       # URL inference, AI categorization
    ],
    "Bildekvalitet": [
        TIER_WEBSITE,        # canonical product image
        TIER_MANUFACTURER,   # manufacturer product image
        TIER_INFERRED,       # secondary source (norengros etc.)
    ],
    "Pakningsinformasjon": [
        TIER_PDF,            # datasheets have packaging details
        TIER_WEBSITE,        # packaging from product page
        TIER_CATALOG,        # Jeeves packaging data
    ],
}

# Default for fields not listed above
_DEFAULT_PRIORITY = [TIER_WEBSITE, TIER_CATALOG, TIER_PDF, TIER_MANUFACTURER, TIER_INFERRED]


def get_source_priority(field_name: str) -> list[int]:
    """Return the source priority list for a field (best-first)."""
    return FIELD_SOURCE_PRIORITY.get(field_name, _DEFAULT_PRIORITY)


def get_tier_for_origin(origin: Optional[str]) -> int:
    """Map an origin label string to a source tier.

    Handles various origin labels used throughout the codebase.
    """
    if not origin:
        return TIER_NONE

    origin_lower = origin.lower().strip()

    # Catalog / Jeeves
    if any(k in origin_lower for k in ("katalog", "jeeves", "erp", "supplier")):
        return TIER_CATALOG

    # Website
    if any(k in origin_lower for k in ("nettside", "onemed", "website", "web")):
        return TIER_WEBSITE

    # PDF / datasheet
    if any(k in origin_lower for k in ("pdf", "datablad", "datasheet", "product_sheet")):
        return TIER_PDF

    # Manufacturer
    if any(k in origin_lower for k in ("produsent", "manufacturer", "mfr")):
        return TIER_MANUFACTURER

    # Inferred / AI
    if any(k in origin_lower for k in ("utledet", "inferred", "ai", "url", "heuristic")):
        return TIER_INFERRED

    return TIER_NONE


def get_tier_label(tier: int) -> str:
    """Return the Norwegian label for a source tier."""
    return TIER_LABELS.get(tier, "ukjent")


# ═══════════════════════════════════════════════════════════
# Candidate value container
# ═══════════════════════════════════════════════════════════

@dataclass
class SourceCandidate:
    """A candidate value from a specific source for a specific field."""
    value: Optional[str] = None
    tier: int = TIER_NONE
    origin_label: str = ""       # e.g. "nettside", "Jeeves", "PDF"
    source_url: Optional[str] = None
    confidence: float = 0.0      # 0.0-1.0 from the source
    evidence: Optional[str] = None


@dataclass
class ResolvedValue:
    """The result of golden source resolution for one field."""
    value: Optional[str] = None
    winning_tier: int = TIER_NONE
    winning_label: str = ""
    source_url: Optional[str] = None
    confidence: float = 0.0
    evidence: str = ""
    candidates_considered: int = 0
    all_candidates: list = field(default_factory=list)  # For conflict detection


# ═══════════════════════════════════════════════════════════
# Core resolution function
# ═══════════════════════════════════════════════════════════

def _is_valid_value(val: Optional[str]) -> bool:
    """Check if a value is non-empty and not a placeholder."""
    if not val:
        return False
    stripped = val.strip()
    if not stripped:
        return False
    placeholders = {"ukjent", "unknown", "n/a", "-", ".", "na", "ingen", ""}
    return stripped.lower() not in placeholders


def resolve_field_value(
    field_name: str,
    candidates: list[SourceCandidate],
) -> ResolvedValue:
    """Resolve the best value for a field using the golden source hierarchy.

    Picks the highest-priority (lowest tier number) candidate that has
    a valid, non-empty value. Never lets a weaker source overwrite a
    stronger one.

    Args:
        field_name: The field being resolved (e.g., "Beskrivelse")
        candidates: List of SourceCandidate from different sources

    Returns:
        ResolvedValue with the winning value and full provenance
    """
    priority = get_source_priority(field_name)

    # Filter to valid candidates and sort by tier priority
    valid = [c for c in candidates if _is_valid_value(c.value)]

    result = ResolvedValue(
        candidates_considered=len(valid),
        all_candidates=valid,
    )

    if not valid:
        result.evidence = f"Ingen gyldige kandidater for {field_name}"
        return result

    # Sort candidates by their position in the priority list
    def tier_rank(c: SourceCandidate) -> int:
        try:
            return priority.index(c.tier)
        except ValueError:
            return len(priority) + c.tier  # Unknown tier → lowest priority

    valid.sort(key=tier_rank)

    winner = valid[0]
    result.value = winner.value.strip() if winner.value else None
    result.winning_tier = winner.tier
    result.winning_label = winner.origin_label or get_tier_label(winner.tier)
    result.source_url = winner.source_url
    result.confidence = winner.confidence
    result.evidence = (
        f"Kilde: {result.winning_label} "
        f"(prioritet {priority.index(winner.tier) + 1} av {len(priority)} for {field_name})"
    )

    # Note if there are conflicts (other candidates with different values)
    if len(valid) > 1:
        others = [c for c in valid[1:] if c.value and c.value.strip().lower() != (winner.value or "").strip().lower()]
        if others:
            other_labels = [c.origin_label or get_tier_label(c.tier) for c in others[:3]]
            result.evidence += f" | Avvik fra: {', '.join(other_labels)}"

    return result


# ═══════════════════════════════════════════════════════════
# Convenience builders — construct candidates from app data
# ═══════════════════════════════════════════════════════════

def build_candidates_for_field(
    field_name: str,
    website_value: Optional[str] = None,
    jeeves_value: Optional[str] = None,
    pdf_value: Optional[str] = None,
    manufacturer_value: Optional[str] = None,
    inferred_value: Optional[str] = None,
    pdf_confidence: float = 0.7,
    manufacturer_confidence: float = 0.6,
) -> list[SourceCandidate]:
    """Build a list of SourceCandidates from the typical data sources.

    This is the standard way to feed data into resolve_field_value().
    """
    candidates = []

    if website_value:
        candidates.append(SourceCandidate(
            value=website_value,
            tier=TIER_WEBSITE,
            origin_label="nettside",
            confidence=0.85,
        ))

    if jeeves_value:
        candidates.append(SourceCandidate(
            value=jeeves_value,
            tier=TIER_CATALOG,
            origin_label="katalog (Jeeves)",
            confidence=0.95,
        ))

    if pdf_value:
        candidates.append(SourceCandidate(
            value=pdf_value,
            tier=TIER_PDF,
            origin_label="datablad (PDF)",
            confidence=pdf_confidence,
        ))

    if manufacturer_value:
        candidates.append(SourceCandidate(
            value=manufacturer_value,
            tier=TIER_MANUFACTURER,
            origin_label="produsentside",
            confidence=manufacturer_confidence,
        ))

    if inferred_value:
        candidates.append(SourceCandidate(
            value=inferred_value,
            tier=TIER_INFERRED,
            origin_label="utledet",
            confidence=0.4,
        ))

    return candidates


def resolve_product_name(
    website_name: Optional[str] = None,
    jeeves_name: Optional[str] = None,
    pdf_name: Optional[str] = None,
    manufacturer_name: Optional[str] = None,
) -> ResolvedValue:
    """Resolve the best product name from all sources."""
    candidates = build_candidates_for_field(
        "Produktnavn",
        website_value=website_name,
        jeeves_value=jeeves_name,
        pdf_value=pdf_name,
        manufacturer_value=manufacturer_name,
    )
    return resolve_field_value("Produktnavn", candidates)


def resolve_description(
    website_desc: Optional[str] = None,
    manufacturer_desc: Optional[str] = None,
    pdf_desc: Optional[str] = None,
    inferred_desc: Optional[str] = None,
) -> ResolvedValue:
    """Resolve the best description from all sources."""
    candidates = build_candidates_for_field(
        "Beskrivelse",
        website_value=website_desc,
        manufacturer_value=manufacturer_desc,
        pdf_value=pdf_desc,
        inferred_value=inferred_desc,
    )
    return resolve_field_value("Beskrivelse", candidates)


def resolve_specification(
    pdf_spec: Optional[str] = None,
    manufacturer_spec: Optional[str] = None,
    website_spec: Optional[str] = None,
    jeeves_spec: Optional[str] = None,
) -> ResolvedValue:
    """Resolve the best specification from all sources."""
    candidates = build_candidates_for_field(
        "Spesifikasjon",
        website_value=website_spec,
        jeeves_value=jeeves_spec,
        pdf_value=pdf_spec,
        manufacturer_value=manufacturer_spec,
    )
    return resolve_field_value("Spesifikasjon", candidates)


def resolve_category(
    website_category: Optional[str] = None,
    catalog_category: Optional[str] = None,
    inferred_category: Optional[str] = None,
) -> ResolvedValue:
    """Resolve the best category from all sources."""
    candidates = build_candidates_for_field(
        "Kategori",
        website_value=website_category,
        jeeves_value=catalog_category,
        inferred_value=inferred_category,
    )
    return resolve_field_value("Kategori", candidates)


def resolve_image(
    website_image: Optional[str] = None,
    manufacturer_image: Optional[str] = None,
    other_image: Optional[str] = None,
) -> ResolvedValue:
    """Resolve the best image URL from all sources."""
    candidates = build_candidates_for_field(
        "Bildekvalitet",
        website_value=website_image,
        manufacturer_value=manufacturer_image,
        inferred_value=other_image,
    )
    return resolve_field_value("Bildekvalitet", candidates)


def source_tier_for_field(field_name: str, origin_label: Optional[str]) -> int:
    """Get the tier number for a given origin within a field's hierarchy.

    Used by field_confidence.py to adjust source_quality scoring.
    """
    return get_tier_for_origin(origin_label)
