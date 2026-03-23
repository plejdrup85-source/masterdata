"""Category intelligence — e-commerce-optimized category analysis.

Analyzes whether product categories are well-structured for online store
navigation and suggests improvements following e-commerce best practices.

E-commerce category principles:
  1. 2-4 levels deep (not too shallow, not too deep)
  2. Each level adds navigation value for the customer
  3. Attributes like size, material, sterility → should be filters, not categories
  4. Leaf categories should contain enough products to justify their existence
  5. Category names should be customer-facing (not internal jargon)

Status outputs per product:
  - OK: category is well-structured for e-commerce
  - SHOULD_SIMPLIFY: too many levels or too specific
  - ATTRIBUTE_AS_CATEGORY: leaf level describes an attribute, not a product type
  - WRONG_CATEGORY: product doesn't seem to match its category
  - MISSING: no category at all
  - NEEDS_REVIEW: ambiguous, can't determine automatically
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# CONFIGURATION — e-commerce category rules
# ═══════════════════════════════════════════════════════════

# Ideal breadcrumb depth for e-commerce
IDEAL_MIN_DEPTH = 2
IDEAL_MAX_DEPTH = 4
TOO_DEEP_THRESHOLD = 5  # 5+ levels is almost always too deep

# Root categories to strip (not customer-facing)
ROOT_CATEGORIES_TO_STRIP = {"sortiment", "produkter", "products", "alle produkter", "katalog"}

# Patterns that indicate a category level is really an attribute/filter
ATTRIBUTE_PATTERNS = [
    # Size patterns
    (r"\b(?:str|størrelse)\s*[.:=]?\s*(?:xs|s|m|l|xl|xxl|\d+)\b", "størrelse"),
    (r"\b\d+\s*(?:x\s*\d+)?\s*(?:cm|mm|ml|l|cl|g|kg)\b", "størrelse"),
    (r"\b\d+\s*(?:stk|pk|per|pr)\b", "pakningsstørrelse"),
    # Material patterns (no trailing \b — materials often prefix product names)
    (r"\b(?:nitril|latex|vinyl|silikon|polyester|bomull|plast|papir|metall)", "materiale"),
    (r"\b(?:polyuretan|polypropylen|polyetylen|nylon|gummi)", "materiale"),
    # Sterility patterns
    (r"\b(?:steril|usteril|ikke.?steril|autoklaverbar)\b", "sterilisering"),
    # Color patterns
    (r"\b(?:blå|hvit|grønn|sort|svart|rød|gul|rosa|transparent|klar)\b", "farge"),
    # Variant patterns
    (r"\b(?:med|uten)\s+(?:pudd|mansjett|finger|hette|lokk|håndtak)\b", "variant"),
    (r"\b(?:pudder.?fri|lateks.?fri|pvc.?fri)\b", "variant"),
    # Packaging patterns
    (r"\b(?:enkeltpakk|bulk|dispenser|boks|eske|pose|rull)\b", "pakningstype"),
]

# Compiled patterns
_ATTRIBUTE_RES = [(re.compile(pat, re.I), attr) for pat, attr in ATTRIBUTE_PATTERNS]

# Patterns that indicate a real product type (NOT an attribute)
PRODUCT_TYPE_KEYWORDS = {
    "hansker", "hanske", "bandasje", "kompress", "plaster", "sprøyte",
    "kanyle", "kateter", "sutur", "munnbind", "frakk", "sko", "laken",
    "tape", "saks", "pinsett", "skalpell", "slange", "pose", "maske",
    "brille", "hette", "forkle", "fikseringsstrips", "tupfer", "gasbind",
    "steriliseringspose", "indikatortape", "desinfeksjon", "sårvask",
    "sårforband", "elastisk", "fikseringsbind", "gips", "skinne",
    "trachealkanyle", "ernæringssonde", "urinpose", "stomipose",
}


@dataclass
class CategoryEvaluation:
    """Result of evaluating a product's category for e-commerce fitness."""
    status: str = "OK"                           # OK, SHOULD_SIMPLIFY, ATTRIBUTE_AS_CATEGORY, etc.
    original_breadcrumb: list[str] = field(default_factory=list)
    suggested_breadcrumb: Optional[list[str]] = None
    suggested_category: Optional[str] = None      # Formatted "A > B > C"
    depth: int = 0
    effective_depth: int = 0                      # After stripping root/attribute levels
    issues: list[str] = field(default_factory=list)
    attribute_levels: list[dict] = field(default_factory=list)  # Levels that should be filters
    is_too_deep: bool = False
    is_too_shallow: bool = False
    has_attribute_as_category: bool = False
    product_type_match: bool = True               # Does the product seem to fit the category?
    summary: str = ""                             # Norwegian summary for Excel/UI


@dataclass
class CategoryRecommendation:
    """Aggregate recommendation for a category path across many products."""
    original_path: str                            # "A > B > C > D > E"
    suggested_path: str                           # "A > B > C"
    product_count: int = 0
    issue: str = ""                               # What's wrong
    action: str = ""                              # What to do
    attribute_candidates: list[str] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════
# CORE FUNCTIONS
# ═══════════════════════════════════════════════════════════


def evaluate_category_fit(
    breadcrumb: Optional[list[str]],
    product_name: str = "",
    description: str = "",
    specification: str = "",
) -> CategoryEvaluation:
    """Evaluate whether a product's category is well-structured for e-commerce.

    Checks:
      1. Depth (2-4 ideal, 5+ too deep)
      2. Attribute levels (size, material, sterility in category → should be filter)
      3. Product-category match (does the product belong here?)
      4. Root stripping (non-customer-facing roots)
    """
    result = CategoryEvaluation()

    if not breadcrumb:
        result.status = "MISSING"
        result.summary = "Kategori mangler"
        return result

    result.original_breadcrumb = list(breadcrumb)
    result.depth = len(breadcrumb)

    # Strip non-customer-facing root levels
    effective = [
        level for level in breadcrumb
        if level.lower().strip() not in ROOT_CATEGORIES_TO_STRIP
    ]
    result.effective_depth = len(effective)

    # ── Check depth ──
    if result.effective_depth > IDEAL_MAX_DEPTH:
        result.is_too_deep = True
        result.issues.append(
            f"For dypt hierarki ({result.effective_depth} nivåer, ideelt {IDEAL_MIN_DEPTH}-{IDEAL_MAX_DEPTH})"
        )
    elif result.effective_depth < IDEAL_MIN_DEPTH:
        result.is_too_shallow = True
        result.issues.append(
            f"For grunt hierarki ({result.effective_depth} nivåer, ideelt {IDEAL_MIN_DEPTH}-{IDEAL_MAX_DEPTH})"
        )

    # ── Detect attribute-like levels ──
    for i, level in enumerate(breadcrumb):
        attrs = _detect_attributes_in_text(level)
        if attrs and i >= 2:  # Only flag levels below top 2 (root + main category)
            # Check if this level is purely an attribute, not a product type
            is_product_type = _is_product_type(level)
            if not is_product_type:
                result.attribute_levels.append({
                    "level_index": i,
                    "level_name": level,
                    "attributes_detected": attrs,
                    "suggestion": f"Bruk som filter/attributt i stedet for underkategori",
                })
                result.has_attribute_as_category = True

    # ── Check product-category match ──
    if product_name and effective:
        result.product_type_match = _check_product_category_match(
            product_name, description, effective
        )
        if not result.product_type_match:
            result.issues.append("Produktet ser ikke ut til å passe i denne kategorien")

    # ── Build suggested breadcrumb ──
    if result.is_too_deep or result.has_attribute_as_category:
        result.suggested_breadcrumb = _build_simplified_breadcrumb(breadcrumb, result.attribute_levels)
        result.suggested_category = " > ".join(result.suggested_breadcrumb)

    # ── Determine status ──
    if result.has_attribute_as_category and result.is_too_deep:
        result.status = "SHOULD_SIMPLIFY"
    elif result.has_attribute_as_category:
        result.status = "ATTRIBUTE_AS_CATEGORY"
    elif result.is_too_deep:
        result.status = "SHOULD_SIMPLIFY"
    elif not result.product_type_match:
        result.status = "WRONG_CATEGORY"
    elif result.is_too_shallow:
        result.status = "NEEDS_REVIEW"
    else:
        result.status = "OK"

    # ── Build summary ──
    result.summary = _build_summary(result)

    return result


def detect_overly_specific_category(breadcrumb: Optional[list[str]]) -> Optional[str]:
    """Check if a category is overly specific and return a reason if so.

    Returns None if the category depth is appropriate.
    """
    if not breadcrumb:
        return None
    effective = [l for l in breadcrumb if l.lower().strip() not in ROOT_CATEGORIES_TO_STRIP]
    if len(effective) <= IDEAL_MAX_DEPTH:
        return None
    return (
        f"Kategorien har {len(effective)} nivåer (anbefalt maks {IDEAL_MAX_DEPTH}). "
        f"De siste nivåene ({', '.join(effective[IDEAL_MAX_DEPTH:])}) "
        f"bør vurderes som filter/attributt."
    )


def should_be_attribute_instead_of_category(category_name: str) -> Optional[str]:
    """Check if a category name contains attribute-like content.

    Returns the attribute type (e.g., "størrelse", "materiale") or None.
    A name can be both a product type AND contain attributes (e.g., "Nitrilhansker"
    contains the material "nitril" — the material should be a filter).
    """
    attrs = _detect_attributes_in_text(category_name)
    if attrs:
        return ", ".join(sorted(set(a for _, a in attrs)))
    return None


def suggest_better_category(
    breadcrumb: Optional[list[str]],
    product_name: str = "",
) -> Optional[str]:
    """Suggest an improved category path.

    Returns a simplified breadcrumb string or None if current is fine.
    """
    if not breadcrumb:
        return None
    evaluation = evaluate_category_fit(breadcrumb, product_name)
    return evaluation.suggested_category


def build_ecommerce_category_recommendations(
    results: list,
) -> list[CategoryRecommendation]:
    """Analyze all products to find systemic category structure issues.

    Groups products by category path and identifies:
    - Paths that should be simplified
    - Leaf categories that should become filters
    - Categories with very few products (fragmentation)
    """
    # Count products per category path
    path_counts: dict[str, int] = {}
    path_evaluations: dict[str, CategoryEvaluation] = {}

    for r in results:
        bc = r.product_data.category_breadcrumb
        if not bc:
            continue
        path = " > ".join(bc)
        path_counts[path] = path_counts.get(path, 0) + 1

        if path not in path_evaluations:
            path_evaluations[path] = evaluate_category_fit(
                bc, r.product_data.product_name or ""
            )

    recommendations = []

    for path, count in sorted(path_counts.items(), key=lambda x: -x[1]):
        ev = path_evaluations[path]

        if ev.status == "OK" and count >= 3:
            continue  # Fine as is

        rec = CategoryRecommendation(
            original_path=path,
            suggested_path=ev.suggested_category or path,
            product_count=count,
        )

        if ev.is_too_deep:
            rec.issue = f"For dypt ({ev.effective_depth} nivåer)"
            rec.action = "Forenkle til maks 4 nivåer"
        elif ev.has_attribute_as_category:
            attr_names = [a["attributes_detected"][0][1] for a in ev.attribute_levels if a["attributes_detected"]]
            rec.attribute_candidates = attr_names
            rec.issue = f"Attributt som kategori ({', '.join(attr_names)})"
            rec.action = "Flytt til filter/attributt"
        elif count == 1:
            rec.issue = "Kun 1 produkt i kategorien"
            rec.action = "Vurder sammenslåing med overordnet kategori"
        elif ev.is_too_shallow:
            rec.issue = "For grunt hierarki"
            rec.action = "Legg til underkategori for bedre navigering"
        else:
            continue  # No actionable recommendation

        recommendations.append(rec)

    # Sort: most impactful first (most products affected)
    recommendations.sort(key=lambda r: -r.product_count)
    return recommendations


# ═══════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════


def _detect_attributes_in_text(text: str) -> list[tuple[str, str]]:
    """Detect attribute-like content in text.

    Returns list of (matched_text, attribute_type) tuples.
    """
    matches = []
    for pattern, attr_type in _ATTRIBUTE_RES:
        m = pattern.search(text)
        if m:
            matches.append((m.group(), attr_type))
    return matches


def _is_product_type(text: str) -> bool:
    """Check if text describes a product type (not just an attribute)."""
    lower = text.lower()
    for keyword in PRODUCT_TYPE_KEYWORDS:
        if keyword in lower:
            return True
    return False


def _check_product_category_match(
    product_name: str,
    description: str,
    category_levels: list[str],
) -> bool:
    """Check if a product seems to belong in its category.

    Uses stem-like prefix matching between product and category words
    to handle Norwegian word forms (e.g., "hanske" ↔ "hansker").
    """
    name_lower = product_name.lower()
    desc_lower = (description or "").lower()
    combined = f"{name_lower} {desc_lower}"

    # Extract words from category levels
    cat_words = set()
    for level in category_levels:
        for word in re.findall(r"\w{4,}", level.lower()):
            cat_words.add(word)

    if not cat_words or len(cat_words) < 2:
        return True  # Too few words to evaluate

    product_words = set(re.findall(r"\w{4,}", combined))

    # Check for overlap using prefix matching (Norwegian stems)
    # "hansker" matches "hanske", "nitrilhanske" contains "hansk"
    for cat_word in cat_words:
        stem = cat_word[:min(len(cat_word), 5)]  # Use 5-char prefix as stem
        if any(stem in pw for pw in product_words):
            return True
        if any(pw[:min(len(pw), 5)] in cat_word for pw in product_words):
            return True

    return False


def _build_simplified_breadcrumb(
    original: list[str],
    attribute_levels: list[dict],
) -> list[str]:
    """Build a simplified breadcrumb by removing attribute and root levels."""
    attribute_indices = {a["level_index"] for a in attribute_levels}
    simplified = []
    for i, level in enumerate(original):
        if level.lower().strip() in ROOT_CATEGORIES_TO_STRIP:
            continue
        if i in attribute_indices:
            continue
        simplified.append(level)

    # Ensure at least 2 levels
    if len(simplified) < 2 and len(original) >= 2:
        # Keep the first non-root and the last product-type level
        simplified = [l for l in original if l.lower().strip() not in ROOT_CATEGORIES_TO_STRIP]
        if len(simplified) > IDEAL_MAX_DEPTH:
            simplified = simplified[:IDEAL_MAX_DEPTH]

    return simplified or original[:IDEAL_MAX_DEPTH]


def _build_summary(ev: CategoryEvaluation) -> str:
    """Build a Norwegian summary string for the evaluation."""
    if ev.status == "OK":
        return f"Kategori OK ({ev.effective_depth} nivåer)"
    elif ev.status == "MISSING":
        return "Kategori mangler"
    elif ev.status == "SHOULD_SIMPLIFY":
        parts = []
        if ev.is_too_deep:
            parts.append(f"for dypt ({ev.effective_depth} nivåer)")
        if ev.has_attribute_as_category:
            attrs = [a["level_name"] for a in ev.attribute_levels]
            parts.append(f"attributt som kategori: {', '.join(attrs[:2])}")
        simplified = ev.suggested_category or "?"
        return f"Bør forenkles: {'; '.join(parts)}. Forslag: {simplified}"
    elif ev.status == "ATTRIBUTE_AS_CATEGORY":
        attrs = [a["level_name"] for a in ev.attribute_levels]
        return f"Underkategori bør være filter: {', '.join(attrs[:2])}"
    elif ev.status == "WRONG_CATEGORY":
        return "Produktet ser ikke ut til å passe i kategorien"
    elif ev.status == "NEEDS_REVIEW":
        return "; ".join(ev.issues) if ev.issues else "Krever manuell vurdering"
    return "Ukjent status"
