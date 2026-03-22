"""Product family / variant relationship detection module.

Analyzes a catalog of medical products and identifies product families
(Mother/Child variant structures) suitable for Inriver PIM and webshop use.

Design principles:
- Conservative grouping: false positive families are worse than missing one
- Evidence-based: grouping must be supported by multiple signals
- Review-flagged: uncertain groupings require manual review
- PIM-practical: output maps directly to Inriver Mother/Child model

Mother/Child model for Inriver:
  - Mother: abstract family entity, NOT a sellable SKU
    - Contains: family-level title, shared description, common attributes
    - Does NOT contain: size/gauge/dimension values that vary by child
  - Child: sellable article/SKU
    - Contains: article number, exact variant attributes, exact specifications
    - Inherits: family description, shared images, common attributes from Mother
  - Standalone: product that does not belong to any variant family

Variant dimensions (what differs between children):
  - størrelse / size (S, M, L, XL, etc.)
  - gauge (18G, 21G, 23G, etc.)
  - lengde / length (25mm, 40mm, etc.)
  - bredde / width
  - dimensjon / dimensions (5x5cm, 10x10cm, etc.)
  - volum / volume (2ml, 5ml, 10ml, etc.)
  - farge / color
  - CH / French size (catheter sizing)
  - tråd / thread size (suture sizing)
  - nåltype / needle type
  - sidevalg / laterality (left/right)
"""

import hashlib
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ── Data structures ──


@dataclass
class VariantDimension:
    """A single axis of variation between siblings (e.g. size, gauge)."""
    dimension_name: str       # e.g. "Størrelse", "Gauge", "Lengde"
    value: str                # e.g. "M", "21G", "40mm"
    source: str = ""          # where detected: "name", "spec", "technical_details"


@dataclass
class FamilyMember:
    """A product's role within a product family."""
    article_number: str
    role: str                         # "mother", "child", "standalone"
    family_id: Optional[str] = None
    family_name: Optional[str] = None
    mother_article_number: Optional[str] = None
    shared_base_title: Optional[str] = None
    child_specific_title: Optional[str] = None
    variant_dimensions: list[VariantDimension] = field(default_factory=list)
    candidate_siblings: list[str] = field(default_factory=list)
    family_size: int = 1
    confidence: float = 0.0
    review_required: bool = True
    grouping_reason: str = ""
    notes: str = ""
    source_signals: list[str] = field(default_factory=list)

    # Raw product data for reference
    product_name: str = ""
    brand: str = ""
    specification: str = ""


@dataclass
class ProductFamily:
    """A group of related products forming a variant family."""
    family_id: str
    family_name: str
    base_title: str                           # shared title stem
    members: list[FamilyMember] = field(default_factory=list)
    variant_dimension_names: list[str] = field(default_factory=list)
    mother_article: Optional[str] = None      # chosen/created mother
    confidence: float = 0.0
    review_required: bool = True
    grouping_reason: str = ""


# ── Variant patterns ──
# Each pattern: (regex, dimension_name, value_group_index)
# These are applied to product names to extract variant suffixes.

# Size labels: S, M, L, XL, XXL, etc.
_SIZE_PATTERN = re.compile(
    r"\b((?:X{0,3}S)|(?:X{0,3}L)|M)\b(?!\w)",
    re.IGNORECASE,
)

# Numbered sizes: str 4, str. 6, størrelse 8, size 10
_NUMBERED_SIZE_PATTERN = re.compile(
    r"\b(?:str\.?|størrelse|size)\s*(\d+(?:[.,]\d+)?)\b",
    re.IGNORECASE,
)

# Gauge: 18G, 21G, 23G etc.
_GAUGE_PATTERN = re.compile(
    r"\b(\d{1,2})\s*[Gg]\b",
)

# Length with unit: 25mm, 40 mm, 1.2m, 15cm
_LENGTH_PATTERN = re.compile(
    r"\b(\d+(?:[.,]\d+)?)\s*(mm|cm|m)\b",
    re.IGNORECASE,
)

# Volume: 2ml, 5 ml, 10ml, 1L
_VOLUME_PATTERN = re.compile(
    r"\b(\d+(?:[.,]\d+)?)\s*(ml|l|µl)\b",
    re.IGNORECASE,
)

# Dimensions: 5x5cm, 10x10, 7.5x10cm
_DIMENSION_PATTERN = re.compile(
    r"\b(\d+(?:[.,]\d+)?\s*x\s*\d+(?:[.,]\d+)?(?:\s*x\s*\d+(?:[.,]\d+)?)?)\s*(cm|mm|m)?\b",
    re.IGNORECASE,
)

# CH / French size: CH 12, CH12, Fr 14
_CH_PATTERN = re.compile(
    r"\b(?:CH|Fr\.?)\s*(\d+)\b",
    re.IGNORECASE,
)

# Color words (Norwegian + English)
_COLOR_WORDS = {
    "hvit", "white", "svart", "black", "blå", "blue", "rød", "red",
    "grønn", "green", "gul", "yellow", "rosa", "pink", "lilla", "purple",
    "oransje", "orange", "brun", "brown", "grå", "grey", "gray",
    "transparent", "klar", "clear",
}

# Thread/suture sizes: 2-0, 3/0, 4-0, USP 2-0
_SUTURE_SIZE_PATTERN = re.compile(
    r"\b(?:USP\s*)?(\d+[-/]0)\b",
    re.IGNORECASE,
)

# Left/right
_LATERALITY_PATTERN = re.compile(
    r"\b(venstre|høyre|left|right|sin|dex)\b",
    re.IGNORECASE,
)

# Sterile/non-sterile (only as variant if siblings differ)
_STERILITY_PATTERN = re.compile(
    r"\b(steril|usteril|non-steril|sterile|non-sterile|unsterile)\b",
    re.IGNORECASE,
)


# ── Name normalization ──

# Words/tokens to strip for base-name comparison (not meaningful for grouping)
_STRIP_TOKENS = {
    "stk", "pk", "stykk", "engangs", "disposable", "flergangs", "reusable",
}

# Product type keywords that help confirm grouping
_PRODUCT_TYPE_KEYWORDS = {
    "hanske", "hansker", "glove", "gloves",
    "bandasje", "bandage", "dressing",
    "kompress", "compress", "swab",
    "sprøyte", "syringe",
    "kanyle", "needle", "nål",
    "kateter", "catheter",
    "sutur", "suture",
    "plaster", "tape",
    "frakk", "gown",
    "munnbind", "mask",
    "slange", "tube", "tubing",
    "sonde", "probe",
    "skalpell", "scalpel", "blade",
}


def _normalize_for_grouping(text: str) -> str:
    """Normalize a product name/title for base-name comparison.

    Removes variant-specific tokens (sizes, dimensions, colors) and
    normalizes whitespace/punctuation so that siblings with different
    variant values produce the same base key.
    """
    if not text:
        return ""
    t = text.lower().strip()
    # Remove variant-specific patterns (sizes, dimensions, gauge, etc.)
    t = _SIZE_PATTERN.sub("", t)
    t = _NUMBERED_SIZE_PATTERN.sub("", t)
    t = _GAUGE_PATTERN.sub("", t)
    t = _DIMENSION_PATTERN.sub("", t)
    t = _VOLUME_PATTERN.sub("", t)
    t = _CH_PATTERN.sub("", t)
    t = _SUTURE_SIZE_PATTERN.sub("", t)
    t = _LATERALITY_PATTERN.sub("", t)
    # Remove color words
    for color in _COLOR_WORDS:
        t = re.sub(rf"\b{re.escape(color)}\b", "", t, flags=re.IGNORECASE)
    # Remove strip tokens
    for tok in _STRIP_TOKENS:
        t = re.sub(rf"\b{re.escape(tok)}\b", "", t, flags=re.IGNORECASE)
    # Remove length patterns (after gauge to avoid double-removal)
    t = _LENGTH_PATTERN.sub("", t)
    # Normalize punctuation and whitespace (keep hyphens within words like KD-Ject)
    t = re.sub(r"(?<!\w)[,;/\-–—]+(?!\w)", " ", t)  # Remove standalone punctuation
    t = re.sub(r"(?<=\w)-(?=\w)", "", t)  # Collapse intra-word hyphens (KD-Ject → KDJect)
    t = re.sub(r"\s+", " ", t).strip()
    # Remove trailing numbers that might be leftover from size removal
    t = re.sub(r"\s+\d+$", "", t)
    return t


def _extract_brand(
    product_name: str,
    brand: str = "",
    supplier: str = "",
) -> str:
    """Extract effective brand for grouping. Prefer explicit brand field."""
    if brand and brand.strip() and brand.strip().lower() not in ("", "none", "ukjent", "unknown"):
        return brand.strip().lower()
    if supplier and supplier.strip() and supplier.strip().lower() not in ("", "none", "ukjent"):
        return supplier.strip().lower()
    return ""


def _extract_variant_dimensions(
    product_name: str,
    specification: str = "",
    technical_details: Optional[dict] = None,
) -> list[VariantDimension]:
    """Extract all variant dimensions from a product's data.

    Checks product name, specification text, and structured technical details.
    In real Jeeves data, variant info (size, gauge, color, dimensions) often
    lives in the Specification field rather than the Item Description.
    All three sources are parsed with the same regex patterns.

    Returns list of detected variant dimensions.
    """
    dims = []
    name = product_name or ""
    spec = specification or ""
    td = technical_details or {}

    def _already_has(dim_name: str) -> bool:
        return any(d.dimension_name == dim_name for d in dims)

    # ── Helper: apply patterns to a text field ──
    def _extract_from_text(text: str, source_label: str) -> None:
        """Apply all variant regex patterns to a text field."""
        if not text:
            return

        # Size labels (S/M/L/XL) — only at word boundary
        if not _already_has("Størrelse"):
            m = _SIZE_PATTERN.search(text)
            if m:
                dims.append(VariantDimension("Størrelse", m.group(0).upper(), source_label))

        # Numbered sizes (str 4, størrelse 6)
        if not _already_has("Størrelse"):
            m = _NUMBERED_SIZE_PATTERN.search(text)
            if m:
                dims.append(VariantDimension("Størrelse", m.group(1), source_label))

        # Gauge (18G, 21G)
        if not _already_has("Gauge"):
            m = _GAUGE_PATTERN.search(text)
            if m:
                dims.append(VariantDimension("Gauge", f"{m.group(1)}G", source_label))

        # Dimensions (5x5cm, 10x20cm)
        if not _already_has("Dimensjon"):
            m = _DIMENSION_PATTERN.search(text)
            if m:
                unit = m.group(2) or ""
                dims.append(VariantDimension("Dimensjon", f"{m.group(1)}{unit}", source_label))

        # Volume (30ml, 2ml)
        if not _already_has("Volum"):
            m = _VOLUME_PATTERN.search(text)
            if m:
                dims.append(VariantDimension("Volum", f"{m.group(1)}{m.group(2)}", source_label))

        # CH/French size (CH12)
        if not _already_has("CH"):
            m = _CH_PATTERN.search(text)
            if m:
                dims.append(VariantDimension("CH", f"CH{m.group(1)}", source_label))

        # Suture thread size (3-0, 4/0)
        if not _already_has("Trådstørrelse"):
            m = _SUTURE_SIZE_PATTERN.search(text)
            if m:
                dims.append(VariantDimension("Trådstørrelse", m.group(1), source_label))

        # Length (25mm, 40cm) — only if no dimension already captured
        if not _already_has("Dimensjon") and not _already_has("Lengde"):
            m = _LENGTH_PATTERN.search(text)
            if m:
                dims.append(VariantDimension("Lengde", f"{m.group(1)}{m.group(2)}", source_label))

        # Color
        if not _already_has("Farge"):
            text_lower = text.lower()
            for color in _COLOR_WORDS:
                if re.search(rf"\b{re.escape(color)}\b", text_lower):
                    dims.append(VariantDimension("Farge", color.capitalize(), source_label))
                    break

        # Laterality (venstre/høyre)
        if not _already_has("Sidevalg"):
            m = _LATERALITY_PATTERN.search(text)
            if m:
                dims.append(VariantDimension("Sidevalg", m.group(1).capitalize(), source_label))

    # ── Apply patterns: name first (highest priority), then specification ──
    _extract_from_text(name, "name")
    _extract_from_text(spec, "spec")

    # ── From technical_details (structured key-value) ──
    td_lower = {k.lower().strip(): v for k, v in td.items()}

    for key_pattern, dim_name in [
        (r"størrelse|size", "Størrelse"),
        (r"gauge", "Gauge"),
        (r"lengde|length", "Lengde"),
        (r"bredde|width", "Bredde"),
        (r"farge|color|colour", "Farge"),
        (r"volum|volume", "Volum"),
        (r"ch\b|french", "CH"),
        (r"diameter", "Diameter"),
    ]:
        for k, v in td_lower.items():
            if re.search(key_pattern, k, re.IGNORECASE) and v.strip():
                if not _already_has(dim_name):
                    dims.append(VariantDimension(dim_name, v.strip(), "technical_details"))
                break

    return dims


# ── Core grouping logic ──


@dataclass
class _ProductRecord:
    """Internal record for grouping analysis."""
    article_number: str
    product_name: str
    brand: str
    specification: str
    technical_details: dict
    category: str
    base_name: str  # normalized for grouping
    variant_dims: list[VariantDimension]


def _build_records(
    products: list[dict],
) -> list[_ProductRecord]:
    """Build internal records from raw product data dicts.

    Each dict should have: article_number, product_name, brand, supplier,
    specification, technical_details, category.
    """
    records = []
    for p in products:
        name = p.get("product_name") or p.get("item_description") or p.get("web_title") or ""
        brand = _extract_brand(
            name,
            brand=p.get("brand") or p.get("product_brand") or "",
            supplier=p.get("supplier") or "",
        )
        spec = p.get("specification") or ""
        td = p.get("technical_details") or {}
        cat = p.get("category") or ""

        base = _normalize_for_grouping(name)
        dims = _extract_variant_dimensions(name, spec, td)

        records.append(_ProductRecord(
            article_number=p.get("article_number", ""),
            product_name=name,
            brand=brand,
            specification=spec,
            technical_details=td,
            category=cat,
            base_name=base,
            variant_dims=dims,
        ))
    return records


def _group_candidates(records: list[_ProductRecord]) -> dict[str, list[_ProductRecord]]:
    """Group products into candidate families by normalized base name + brand.

    Products must share:
    1. Same normalized base name (after variant tokens stripped)
    2. Same brand (if brand is known for either product)

    Returns dict of group_key → list of records.
    """
    groups: dict[str, list[_ProductRecord]] = defaultdict(list)

    for rec in records:
        if not rec.base_name:
            continue

        # Group key = brand + base_name
        # If brand is unknown, still group by base_name alone (but lower confidence)
        key = f"{rec.brand}||{rec.base_name}" if rec.brand else f"_unknown_||{rec.base_name}"
        groups[key].append(rec)

    return dict(groups)


def _score_family(members: list[_ProductRecord]) -> tuple[float, str, list[str]]:
    """Score a candidate family's grouping quality.

    Returns (confidence, reason, signals).

    Key constraints:
    - P0-2: families with zero detected variant dimensions are capped at 0.40
    - P0-3: unknown-brand families are penalized and require stronger evidence
    - P1-2: families >15 members get a size warning
    """
    if len(members) < 2:
        return 0.0, "Kun ett produkt — ikke en familie", []

    signals = []
    score = 0.0
    has_brand = False

    # ── Signal 1: Multiple products share exact base name ──
    base_names = {m.base_name for m in members}
    if len(base_names) == 1:
        score += 0.30
        signals.append("Alle deler samme basenavn")
    else:
        score += 0.10
        signals.append("Basenavn varierer noe")

    # ── Signal 2: Brand consistency ──
    brands = {m.brand for m in members if m.brand}
    if len(brands) == 1:
        score += 0.20
        has_brand = True
        signals.append(f"Felles merkevare: {brands.pop()}")
    elif len(brands) == 0:
        # P0-3: Unknown brand — penalize to require stronger evidence
        score -= 0.10
        signals.append("ADVARSEL: Ingen merkevare — krever sterkere bevis for gruppering")
    else:
        score -= 0.20
        signals.append(f"Ulike merkevarer: {brands} — mulig feilgruppering")

    # ── Signal 3: Variant dimensions detected ──
    all_dim_names = set()
    members_with_dims = 0
    for m in members:
        if m.variant_dims:
            members_with_dims += 1
            for d in m.variant_dims:
                all_dim_names.add(d.dimension_name)

    has_variant_dims = members_with_dims >= 2 and len(all_dim_names) > 0
    if has_variant_dims:
        score += 0.25
        signals.append(f"Variantdimensjoner: {', '.join(sorted(all_dim_names))}")
    elif members_with_dims == 1:
        score += 0.05
        signals.append("Bare én variant har dimensjonsdata")
    else:
        signals.append("ADVARSEL: Ingen variantdimensjoner oppdaget")

    # ── Signal 4: Shared product type keyword ──
    shared_types = set()
    for kw in _PRODUCT_TYPE_KEYWORDS:
        if all(kw in m.product_name.lower() for m in members):
            shared_types.add(kw)
    if shared_types:
        score += 0.15
        signals.append(f"Felles produkttype: {', '.join(sorted(shared_types))}")

    # ── Signal 5: Shared category ──
    categories = {m.category for m in members if m.category}
    if len(categories) == 1 and categories != {""}:
        score += 0.10
        signals.append(f"Felles kategori: {categories.pop()}")
    elif len(categories) > 1:
        score -= 0.05
        signals.append(f"Ulike kategorier: {categories}")

    # ── Signal 6: Family size ──
    if len(members) > 15:
        # P1-2: Large family warning
        score -= 0.05
        signals.append(
            f"ADVARSEL: Stor familie ({len(members)} produkter) — "
            f"kan trenge oppdeling i underfamilier"
        )
    elif len(members) >= 3:
        score += 0.05
        signals.append(f"Rimelig familiestørrelse ({len(members)})")

    # ── Signal 7: Variant dimension consistency ──
    dim_counter: dict[str, int] = defaultdict(int)
    for m in members:
        for d in m.variant_dims:
            dim_counter[d.dimension_name] += 1
    consistent_dims = [
        name for name, count in dim_counter.items()
        if count >= len(members) * 0.6
    ]
    if consistent_dims:
        score += 0.10
        signals.append(f"Konsistente dimensjoner: {', '.join(consistent_dims)}")

    # ── P0-2 ENFORCEMENT: Cap confidence when no variant dimensions detected ──
    # Without variant dimensions, the family structure is not actionable for
    # Inriver/webshop — even if the name match is perfect.
    score = max(0.0, min(1.0, score))
    if not has_variant_dims:
        score = min(score, 0.40)
        signals.append("Konfidenstak: maks 0.40 uten variantdimensjoner")

    # ── P0-3 ENFORCEMENT: Extra penalty for unknown-brand families ──
    # Without brand, require both product type keyword AND variant dims to score well
    if not has_brand and not has_variant_dims:
        score = min(score, 0.30)
        signals.append("Konfidenstak: maks 0.30 uten merkevare og uten varianter")

    # Build reason
    if score >= 0.7:
        reason = "Sterk familiegruppe — høy likhet og konsistente varianter"
    elif score >= 0.5:
        reason = "Sannsynlig familie — moderat likhet"
    elif score >= 0.3:
        reason = "Mulig familie — krever manuell gjennomgang"
    else:
        reason = "Svak kandidat — bør sannsynligvis være frittstående"

    return round(score, 2), reason, signals


def _determine_variant_dimensions_for_family(
    members: list[_ProductRecord],
) -> list[str]:
    """Determine which dimensions define variance in this family.

    A dimension is a variant axis if different members have different
    values for it.
    """
    dim_values: dict[str, set] = defaultdict(set)

    for m in members:
        for d in m.variant_dims:
            dim_values[d.dimension_name].add(d.value)

    # A dimension is variant-defining if it has 2+ distinct values
    variant_dims = [
        name for name, values in sorted(dim_values.items())
        if len(values) >= 2
    ]
    return variant_dims


def _choose_mother(
    members: list[_ProductRecord],
) -> Optional[str]:
    """Choose which article should be the Mother (or None for abstract mother).

    For Inriver, the Mother is typically an abstract product that doesn't map
    to a sellable SKU. We return None to indicate an abstract mother should
    be created, and use the base_name as the mother title.

    However, if there's a clear "base" product (no variant dims), use it.
    """
    # Look for a member with NO variant dimensions — it might be the base product
    base_members = [m for m in members if not m.variant_dims]
    if len(base_members) == 1:
        return base_members[0].article_number

    # Otherwise: abstract mother (no specific article)
    return None


def _build_family_name(members: list[_ProductRecord]) -> str:
    """Build a human-readable family name from the original product names.

    Uses the original product name (not the normalized base name) for proper
    capitalization and readability. Picks the most common original name,
    or if all are identical, uses that directly.
    """
    if not members:
        return ""

    # Count original product names to find the most common one
    name_counts: dict[str, int] = defaultdict(int)
    for m in members:
        if m.product_name:
            name_counts[m.product_name.strip()] += 1

    if name_counts:
        # Use the most frequent original name (preserves original capitalization)
        best_name = max(name_counts, key=name_counts.get)
        return best_name

    # Fallback to base name with title case
    base_names = [m.base_name for m in members if m.base_name]
    if base_names:
        return max(base_names, key=len).title()

    return ""


def _stable_family_id(group_key: str) -> str:
    """Generate a stable, deterministic family ID from the group key.

    Uses a content-based hash of the group key (brand||base_name) so the same
    logical family gets the same ID across independent re-runs, regardless of
    processing order.  Format: FAM-<8-char hex hash>.
    """
    digest = hashlib.sha256(group_key.encode("utf-8")).hexdigest()[:8]
    return f"FAM-{digest}"


# ── Main API ──


def detect_families(
    products: list[dict],
    min_family_size: int = 2,
    min_confidence: float = 0.30,
) -> tuple[list[ProductFamily], list[FamilyMember]]:
    """Detect product families and variant relationships.

    Args:
        products: list of product dicts with keys:
            article_number, product_name, brand, supplier,
            specification, technical_details, category
        min_family_size: minimum number of members to form a family
        min_confidence: minimum confidence to report a family

    Returns:
        (families, all_members) where:
        - families: list of ProductFamily objects
        - all_members: list of FamilyMember objects (one per product)
    """
    logger.info(f"Family detection: analyzing {len(products)} products")

    # Step 1: Build internal records
    records = _build_records(products)
    logger.info(f"Built {len(records)} records with base names")

    # Step 2: Group by base name + brand
    candidate_groups = _group_candidates(records)
    multi_groups = {k: v for k, v in candidate_groups.items() if len(v) >= min_family_size}
    logger.info(
        f"Found {len(multi_groups)} candidate families "
        f"(from {len(candidate_groups)} groups, "
        f"filtered to size >= {min_family_size})"
    )

    # Step 3: Score and build families
    families: list[ProductFamily] = []
    member_lookup: dict[str, FamilyMember] = {}  # article_number → member

    for group_key, group_records in sorted(multi_groups.items(), key=lambda x: -len(x[1])):
        confidence, reason, signals = _score_family(group_records)

        if confidence < min_confidence:
            # Below threshold — mark as standalone
            for rec in group_records:
                member_lookup[rec.article_number] = FamilyMember(
                    article_number=rec.article_number,
                    role="standalone",
                    confidence=confidence,
                    review_required=True,
                    grouping_reason=f"Kandidatfamilie under terskel ({confidence:.2f} < {min_confidence})",
                    notes=reason,
                    product_name=rec.product_name,
                    brand=rec.brand,
                    specification=rec.specification,
                    source_signals=signals,
                )
            continue

        # Stable family ID: content-based hash from group key (brand+base_name)
        # This ensures the same logical family gets the same ID across re-runs
        family_id = _stable_family_id(group_key)
        family_name = _build_family_name(group_records)
        variant_dim_names = _determine_variant_dimensions_for_family(group_records)
        mother_article = _choose_mother(group_records)

        # P0-2: Enforce confidence cap and review when no variant dimensions detected.
        # _score_family may see "some members have dims" but if those dims don't have
        # 2+ distinct values, variant_dim_names is empty and the family isn't actionable.
        has_dims = len(variant_dim_names) > 0
        if not has_dims:
            confidence = min(confidence, 0.40)
        review_required = confidence < 0.65 or not has_dims

        family = ProductFamily(
            family_id=family_id,
            family_name=family_name,
            base_title=group_records[0].base_name if group_records else "",
            variant_dimension_names=variant_dim_names,
            mother_article=mother_article,
            confidence=confidence,
            review_required=review_required,
            grouping_reason=reason,
        )

        sibling_numbers = [r.article_number for r in group_records]

        for rec in group_records:
            is_mother = (mother_article == rec.article_number)
            role = "mother" if is_mother else "child"

            # Build child-specific title (what differentiates this child)
            child_specifics = []
            for d in rec.variant_dims:
                if d.dimension_name in variant_dim_names:
                    child_specifics.append(f"{d.dimension_name}: {d.value}")
            child_title = ", ".join(child_specifics) if child_specifics else ""

            member = FamilyMember(
                article_number=rec.article_number,
                role=role,
                family_id=family_id,
                family_name=family_name,
                mother_article_number=mother_article,
                shared_base_title=family_name,
                child_specific_title=child_title,
                variant_dimensions=rec.variant_dims,
                candidate_siblings=[s for s in sibling_numbers if s != rec.article_number],
                family_size=len(group_records),
                confidence=confidence,
                review_required=review_required,
                grouping_reason=reason,
                source_signals=signals,
                product_name=rec.product_name,
                brand=rec.brand,
                specification=rec.specification,
            )
            member_lookup[rec.article_number] = member
            family.members.append(member)

        families.append(family)

    # Step 4: Mark remaining products as standalone
    for rec in records:
        if rec.article_number not in member_lookup:
            member_lookup[rec.article_number] = FamilyMember(
                article_number=rec.article_number,
                role="standalone",
                confidence=1.0,
                review_required=False,
                grouping_reason="Ingen matchende familiemedlemmer funnet",
                product_name=rec.product_name,
                brand=rec.brand,
                specification=rec.specification,
            )

    all_members = list(member_lookup.values())

    # Step 5: Summary logging and diagnostics
    total_in_families = sum(len(f.members) for f in families)
    standalone_count = sum(1 for m in all_members if m.role == "standalone")
    strong_families = sum(1 for f in families if f.confidence >= 0.65)
    weak_families = sum(1 for f in families if f.confidence < 0.65)
    families_with_dims = sum(1 for f in families if f.variant_dimension_names)
    families_without_dims = len(families) - families_with_dims
    large_families = sum(1 for f in families if len(f.members) > 15)

    logger.info(
        f"Family detection complete: "
        f"{len(families)} families ({strong_families} strong, {weak_families} weak), "
        f"{total_in_families} products in families, "
        f"{standalone_count} standalone"
    )
    logger.info(
        f"Variant dimensions: {families_with_dims} families with dimensions, "
        f"{families_without_dims} without"
    )

    # P1-4: Summary-level warning when no dimensions are detected
    if families and families_without_dims == len(families):
        logger.warning(
            "ADVARSEL: Ingen familier har variantdimensjoner! "
            "Spesifikasjonsdata kan mangle eller ikke bli gjenkjent. "
            "Familiestrukturen er ikke handlingsbar for Inriver/webshop uten varianter."
        )
    elif families_without_dims > 0:
        logger.warning(
            f"{families_without_dims} av {len(families)} familier mangler "
            f"variantdimensjoner — disse krever manuell gjennomgang"
        )

    if large_families:
        logger.warning(
            f"{large_families} familier har mer enn 15 medlemmer — "
            f"vurder oppdeling i underfamilier"
        )

    return families, all_members


# ── Convenience: build product dicts from available data ──


def products_from_jeeves_index(jeeves_index) -> list[dict]:
    """Convert a JeevesIndex into product dicts for detect_families().

    Uses Jeeves ERP data as the primary source for grouping.
    """
    products = []
    for artnr in jeeves_index.all_article_numbers():
        j = jeeves_index.get(artnr)
        if not j:
            continue
        products.append({
            "article_number": j.article_number,
            "product_name": j.item_description or j.web_title or "",
            "brand": j.product_brand or "",
            "supplier": j.supplier or "",
            "specification": j.specification or "",
            "technical_details": {},
            "category": "",
        })
    return products


def products_from_analyses(analyses: list) -> list[dict]:
    """Convert ProductAnalysis results into product dicts for detect_families().

    Uses both website and Jeeves data for richer grouping signals.
    """
    products = []
    for a in analyses:
        pd = a.product_data
        j = a.jeeves_data

        # Prefer website data, fall back to Jeeves
        name = pd.product_name or (j.item_description if j else "") or (j.web_title if j else "")
        brand = (j.product_brand if j else "") or pd.manufacturer or ""
        supplier = (j.supplier if j else "") or ""
        spec = pd.specification or (j.specification if j else "") or ""
        td = pd.technical_details or {}
        cat = pd.category or ""

        products.append({
            "article_number": a.article_number,
            "product_name": name,
            "brand": brand,
            "supplier": supplier,
            "specification": spec,
            "technical_details": td,
            "category": cat,
        })
    return products
