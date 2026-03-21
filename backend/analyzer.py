"""Masterdata quality analysis engine.

Validates product data quality and scores each field.
Handles cases where data exists but in unexpected formats.
"""

import logging
import re

from backend.models import (
    FieldAnalysis,
    ProductAnalysis,
    ProductData,
    QualityStatus,
)

logger = logging.getLogger(__name__)

# Minimum acceptable lengths for fields
MIN_NAME_LENGTH = 5
MIN_DESCRIPTION_LENGTH = 20

# Field weights for scoring - critical fields weigh more
FIELD_WEIGHTS = {
    "Produktnavn": 2.0,
    "Beskrivelse": 1.5,
    "Spesifikasjon": 2.0,
    "Produsent": 2.0,
    "Produsentens varenummer": 1.5,
    "Kategori": 1.0,
    "Pakningsinformasjon": 1.0,
    "Bildekvalitet": 1.5,
    "Konsistens mellom felter": 0.5,
}

# Technical/measurable keywords that indicate specification content
SPEC_KEYWORDS = {
    "mm", "cm", "m", "ml", "l", "g", "kg", "stk", "pk", "µm",
    "størrelse", "size", "materiale", "material", "farge", "color",
    "vekt", "weight", "lengde", "length", "bredde", "width",
    "høyde", "height", "tykkelse", "thickness", "diameter",
    "latex", "nitril", "vinyl", "polyester", "bomull", "cotton",
    "steril", "sterile", "usteril", "non-sterile",
    "engangs", "disposable", "flergangs", "reusable",
    "ce-merket", "ce-marked", "iso", "en-", "astm",
}


def _analyze_product_name(product: ProductData) -> FieldAnalysis:
    """Analyze product name quality."""
    name = product.product_name
    analysis = FieldAnalysis(
        field_name="Produktnavn",
        current_value=name,
    )

    if not name:
        analysis.status = QualityStatus.MISSING
        analysis.comment = "Produktnavn mangler helt"
        return analysis

    issues = []

    if len(name) < MIN_NAME_LENGTH:
        issues.append("Navn er for kort (under 5 tegn)")

    generic_names = {"produkt", "vare", "artikkel", "item", "product", "test"}
    if name.lower().strip() in generic_names:
        issues.append("Navn er for generisk")

    if re.match(r"^[\d\s\-/]+$", name):
        issues.append("Navn inneholder kun tall/tegn, mangler beskrivende tekst")

    english_indicators = ["gloves", "bandage", "tape", "pack", "box", "piece"]
    name_lower = name.lower()
    english_count = sum(1 for word in english_indicators if word in name_lower)
    if english_count >= 2:
        issues.append("Navn ser ut til \u00e5 v\u00e6re p\u00e5 engelsk, b\u00f8r vurderes for norsk oversettelse")

    if name == name.upper() and len(name) > 3:
        issues.append("Navn er i STORE BOKSTAVER, b\u00f8r ha normal casing")

    if not issues:
        analysis.status = QualityStatus.OK
        analysis.comment = "Produktnavn ser bra ut"
    elif any("mangler" in i.lower() or "kun tall" in i.lower() for i in issues):
        analysis.status = QualityStatus.PROBABLE_ERROR
        analysis.comment = "; ".join(issues)
    else:
        analysis.status = QualityStatus.SHOULD_IMPROVE
        analysis.comment = "; ".join(issues)

    return analysis


def _analyze_description(product: ProductData) -> FieldAnalysis:
    """Analyze product description quality."""
    desc = product.description
    analysis = FieldAnalysis(
        field_name="Beskrivelse",
        current_value=desc,
    )

    if not desc:
        analysis.status = QualityStatus.MISSING
        analysis.comment = "Beskrivelse mangler helt"
        return analysis

    issues = []

    if len(desc) < MIN_DESCRIPTION_LENGTH:
        issues.append(f"Beskrivelse er for kort ({len(desc)} tegn, minimum {MIN_DESCRIPTION_LENGTH})")

    if product.product_name and desc.strip().lower() == product.product_name.strip().lower():
        issues.append("Beskrivelse er identisk med produktnavn")

    if product.article_number in desc and len(desc) < 50:
        issues.append("Beskrivelse inneholder hovedsakelig artikkelnummer")

    if not issues:
        analysis.status = QualityStatus.OK
        analysis.comment = "Beskrivelse OK"
    else:
        analysis.status = QualityStatus.SHOULD_IMPROVE
        analysis.comment = "; ".join(issues)

    return analysis


def _has_measurable_content(text: str) -> bool:
    """Check if text contains measurable/technical information."""
    if not text:
        return False
    text_lower = text.lower()
    # Check for technical keywords
    keyword_count = sum(1 for kw in SPEC_KEYWORDS if kw in text_lower)
    if keyword_count >= 2:
        return True
    # Check for numeric patterns with units (e.g. "100 ml", "25cm", "3.5 kg")
    unit_pattern = r'\d+[\.,]?\d*\s*(?:mm|cm|m|ml|l|g|kg|stk|pk|µm|%)'
    if len(re.findall(unit_pattern, text_lower)) >= 1:
        return True
    # Check for key:value patterns (e.g. "Materiale: Nitril")
    kv_pattern = r'[\w]+\s*[:=]\s*[\w]+'
    if len(re.findall(kv_pattern, text)) >= 2:
        return True
    return False


def _count_structured_attributes(details: dict, spec: str) -> int:
    """Count the number of meaningful structured attributes."""
    count = 0
    if details:
        count += len(details)
    if spec and not details:
        # Count semicolon-separated or line-separated attributes
        separators = spec.count(";") + spec.count("\n")
        if separators > 0:
            count += separators + 1
    return count


def _analyze_specification(product: ProductData) -> FieldAnalysis:
    """Analyze product specification quality.

    Valid if:
    - At least 2 structured attributes exist in technical_details
    - OR specification text contains measurable/technical info
    - OR description contains substantial technical details
    """
    spec = product.specification
    details = product.technical_details

    # Build current_value from all available sources
    display_value = spec
    if details and not display_value:
        display_value = "; ".join(f"{k}: {v}" for k, v in details.items())

    analysis = FieldAnalysis(
        field_name="Spesifikasjon",
        current_value=display_value,
    )

    # Count structured attributes from technical_details
    attr_count = _count_structured_attributes(details, spec)

    # Check for technical content in spec text
    has_tech_content = _has_measurable_content(spec)

    # Also check description for embedded technical info
    has_tech_in_desc = _has_measurable_content(product.description)

    # Determine status
    if not spec and not details:
        if has_tech_in_desc:
            analysis.status = QualityStatus.SHOULD_IMPROVE
            analysis.comment = (
                "Spesifikasjoner mangler som eget felt, men beskrivelsen inneholder "
                "teknisk informasjon. Bør struktureres som egne spesifikasjonsfelter."
            )
        else:
            analysis.status = QualityStatus.MISSING
            analysis.comment = "Spesifikasjoner mangler helt. Bør innhentes fra produsent."
        return analysis

    issues = []

    if attr_count < 2 and not has_tech_content:
        issues.append(f"Få spesifikasjonsfelter (kun {attr_count})")

    # Check if spec just repeats the product name or description
    if spec and product.product_name:
        if spec.strip().lower() == product.product_name.strip().lower():
            issues.append("Spesifikasjon er identisk med produktnavn")
    if spec and product.description:
        if spec.strip().lower() == product.description.strip().lower():
            issues.append("Spesifikasjon er identisk med beskrivelse")

    expected_fields = {
        "størrelse", "size", "materiale", "material", "farge", "color",
        "vekt", "weight", "lengde", "length", "bredde", "width",
    }
    if details:
        detail_keys_lower = {k.lower() for k in details.keys()}
        matching = expected_fields & detail_keys_lower
        if not matching and attr_count < 3:
            issues.append("Mangler vanlige spesifikasjoner (størrelse, materiale, farge, vekt)")

    if not issues:
        analysis.status = QualityStatus.OK
        if attr_count >= 2:
            analysis.comment = f"Spesifikasjoner OK ({attr_count} attributter)"
        else:
            analysis.comment = "Spesifikasjoner OK (teknisk innhold funnet)"
    else:
        analysis.status = QualityStatus.SHOULD_IMPROVE
        analysis.comment = "; ".join(issues)

    return analysis


def _analyze_manufacturer(product: ProductData) -> FieldAnalysis:
    """Analyze manufacturer information."""
    mfr = product.manufacturer
    analysis = FieldAnalysis(
        field_name="Produsent",
        current_value=mfr,
    )

    if not mfr:
        analysis.status = QualityStatus.MISSING
        analysis.comment = "Produsentinformasjon mangler"
        return analysis

    issues = []

    if len(mfr) < 2:
        issues.append("Produsentnavn er for kort")

    placeholders = {"ukjent", "unknown", "n/a", "-", ".", "na", "ingen"}
    if mfr.lower().strip() in placeholders:
        issues.append("Produsentnavn er en placeholder-verdi")
        analysis.status = QualityStatus.PROBABLE_ERROR
        analysis.comment = "; ".join(issues)
        return analysis

    if not issues:
        analysis.status = QualityStatus.OK
        analysis.comment = "Produsentinfo OK"
    else:
        analysis.status = QualityStatus.SHOULD_IMPROVE
        analysis.comment = "; ".join(issues)

    return analysis


def _analyze_manufacturer_article_number(product: ProductData) -> FieldAnalysis:
    """Analyze manufacturer article number."""
    mfr_num = product.manufacturer_article_number
    analysis = FieldAnalysis(
        field_name="Produsentens varenummer",
        current_value=mfr_num,
    )

    if not mfr_num:
        analysis.status = QualityStatus.MISSING
        analysis.comment = "Produsentens varenummer mangler. B\u00f8r innhentes."
        return analysis

    analysis.status = QualityStatus.OK
    analysis.comment = "Produsentens varenummer finnes"
    return analysis


def _analyze_category(product: ProductData) -> FieldAnalysis:
    """Analyze product categorization.

    Valid if:
    - Breadcrumb hierarchy exists (any depth)
    - OR category field exists with meaningful content
    - OR product URL contains category path segments
    """
    cat = product.category
    breadcrumbs = product.category_breadcrumb

    # Build display value from all available sources
    display_value = None
    if breadcrumbs and len(breadcrumbs) > 0:
        display_value = " > ".join(breadcrumbs)
    elif cat:
        display_value = cat

    # Try to extract category from product URL if nothing else exists
    url_category = None
    if not cat and not breadcrumbs and product.product_url:
        # URLs like /nb-no/products/i0016351/category-slug
        url_parts = [
            p for p in product.product_url.split("/")
            if p and p not in ("nb-no", "products", "https:", "http:", "www.onemed.no")
            and not p.startswith("i00")  # internal ID
        ]
        if url_parts:
            url_category = url_parts[-1].replace("-", " ").title()

    analysis = FieldAnalysis(
        field_name="Kategori",
        current_value=display_value or url_category,
    )

    # Determine if we have any category information at all
    has_category = bool(cat) or bool(breadcrumbs and len(breadcrumbs) > 0)

    if not has_category:
        if url_category:
            analysis.status = QualityStatus.SHOULD_IMPROVE
            analysis.comment = f"Kategori mangler, men URL antyder: {url_category}"
        else:
            analysis.status = QualityStatus.MISSING
            analysis.comment = "Kategori mangler helt"
        return analysis

    issues = []

    if breadcrumbs and len(breadcrumbs) < 2:
        issues.append("Kategorihierarki er grunt (kun 1 nivå)")

    if cat and len(cat.strip()) < 3:
        issues.append("Kategorinavn er for kort/generisk")

    # Check for placeholder categories
    placeholders = {"ukjent", "annet", "diverse", "other", "uncategorized"}
    if cat and cat.lower().strip() in placeholders:
        issues.append("Kategori er en placeholder-verdi")

    if not issues:
        analysis.status = QualityStatus.OK
        if breadcrumbs and len(breadcrumbs) >= 2:
            analysis.comment = f"Kategorisering OK ({len(breadcrumbs)} nivåer)"
        else:
            analysis.comment = "Kategorisering OK"
    else:
        analysis.status = QualityStatus.SHOULD_IMPROVE
        analysis.comment = "; ".join(issues)

    return analysis


def _analyze_packaging(product: ProductData) -> FieldAnalysis:
    """Analyze packaging information."""
    pkg = product.packaging_info or product.packaging_unit
    analysis = FieldAnalysis(
        field_name="Pakningsinformasjon",
        current_value=pkg,
    )

    if not pkg:
        analysis.status = QualityStatus.MISSING
        analysis.comment = "Pakningsinformasjon mangler"
        return analysis

    analysis.status = QualityStatus.OK
    analysis.comment = "Pakningsinformasjon finnes"
    return analysis


def _analyze_image(product: ProductData, image_quality: dict = None) -> FieldAnalysis:
    """Analyze image quality using CV analysis results.

    If image_quality dict is provided (from image_analyzer), uses real CV scores.
    Otherwise falls back to basic availability check.
    """
    analysis = FieldAnalysis(
        field_name="Bildekvalitet",
    )

    if image_quality:
        status = image_quality.get("image_quality_status", "MISSING")
        main_score = image_quality.get("main_image_score", 0)
        avg_score = image_quality.get("avg_image_score", 0)
        count = image_quality.get("image_count_found", 0)
        main_exists = image_quality.get("main_image_exists", False)
        issues = image_quality.get("image_issue_summary", "")

        analysis.current_value = f"Score: {avg_score:.0f}/100, {count} bilde(r) funnet"

        if status == "MISSING" or not main_exists:
            analysis.status = QualityStatus.MISSING
            analysis.comment = "Hovedbilde mangler"
        elif status == "FAIL":
            analysis.status = QualityStatus.PROBABLE_ERROR
            analysis.comment = f"Lav bildekvalitet (score {avg_score:.0f}). Problemer: {issues}"
        elif status in ("REVIEW", "PASS_WITH_NOTES"):
            analysis.status = QualityStatus.SHOULD_IMPROVE
            analysis.comment = f"Bildekvalitet kan forbedres (score {avg_score:.0f}). {issues}"
        else:
            analysis.status = QualityStatus.OK
            analysis.comment = f"Bildekvalitet OK (score {avg_score:.0f}, {count} bilde(r))"

        return analysis

    # Fallback: basic availability check
    analysis.current_value = product.image_url
    if not product.image_url:
        analysis.status = QualityStatus.MISSING
        analysis.comment = "Produktbilde mangler"
    elif product.image_quality_ok is False:
        analysis.status = QualityStatus.SHOULD_IMPROVE
        analysis.comment = "Bilde er ikke tilgjengelig eller har sv\u00e6rt liten filst\u00f8rrelse"
    else:
        analysis.status = QualityStatus.OK
        analysis.comment = "Produktbilde er tilgjengelig (ikke kvalitetsvurdert)"

    return analysis


def _check_field_consistency(product: ProductData) -> FieldAnalysis:
    """Check for inconsistencies between fields."""
    analysis = FieldAnalysis(
        field_name="Konsistens mellom felter",
        current_value="Se kommentar",
    )

    issues = []

    if product.product_name and product.description:
        name_words = set(product.product_name.lower().split())
        desc_words = set(product.description.lower().split())
        common_words = {"og", "i", "for", "med", "til", "av", "en", "et", "den", "det", "de", "er"}
        meaningful_name = name_words - common_words
        meaningful_desc = desc_words - common_words
        if meaningful_name and meaningful_desc:
            overlap = meaningful_name & meaningful_desc
            if not overlap and len(meaningful_name) > 2:
                issues.append("Produktnavn og beskrivelse deler ingen n\u00f8kkelord - mulig inkonsistens")

    if product.category and product.product_name:
        cat_lower = product.category.lower()
        name_lower = product.product_name.lower()
        cat_words = set(cat_lower.split()) - {"og", "i", "for", "med"}
        name_words = set(name_lower.split()) - {"og", "i", "for", "med"}
        if cat_words and name_words and not (cat_words & name_words):
            if len(cat_words) > 1 and len(name_words) > 1:
                issues.append(f"Kategorinavn '{product.category}' og produktnavn deler ingen ord")

    if not issues:
        analysis.status = QualityStatus.OK
        analysis.comment = "Ingen \u00e5penbare inkonsistenser"
    else:
        analysis.status = QualityStatus.SHOULD_IMPROVE
        analysis.comment = "; ".join(issues)

    return analysis


def analyze_product(product: ProductData, image_quality: dict = None) -> ProductAnalysis:
    """Run full quality analysis on a product.

    Args:
        product: Scraped product data
        image_quality: Optional dict from ProductImageSummary.to_dict()
    """
    analysis = ProductAnalysis(
        article_number=product.article_number,
        product_data=product,
    )

    if not product.found_on_onemed:
        analysis.overall_status = QualityStatus.MISSING
        analysis.overall_comment = product.error or "Produkt ikke funnet p\u00e5 onemed.no"
        analysis.manual_review_needed = True
        analysis.field_analyses = [
            FieldAnalysis(
                field_name="Oppslag",
                status=QualityStatus.MISSING,
                comment=product.error or "Ikke funnet"
            )
        ]
        analysis.total_score = 0.0
        return analysis

    # Run all field analyses
    field_analyses = [
        _analyze_product_name(product),
        _analyze_description(product),
        _analyze_specification(product),
        _analyze_manufacturer(product),
        _analyze_manufacturer_article_number(product),
        _analyze_category(product),
        _analyze_packaging(product),
        _analyze_image(product, image_quality),
        _check_field_consistency(product),
    ]

    analysis.field_analyses = field_analyses

    # Calculate weighted score
    score_map = {
        QualityStatus.OK: 1.0,
        QualityStatus.SHOULD_IMPROVE: 0.5,
        QualityStatus.MISSING: 0.0,
        QualityStatus.PROBABLE_ERROR: 0.0,
        QualityStatus.REQUIRES_MANUFACTURER: 0.25,
    }

    weighted_sum = 0.0
    total_weight = 0.0
    for fa in field_analyses:
        weight = FIELD_WEIGHTS.get(fa.field_name, 1.0)
        weighted_sum += score_map.get(fa.status, 0) * weight
        total_weight += weight

    analysis.total_score = round(weighted_sum / total_weight * 100, 1) if total_weight > 0 else 0

    # Determine overall status
    statuses = [fa.status for fa in field_analyses]
    if QualityStatus.PROBABLE_ERROR in statuses:
        analysis.overall_status = QualityStatus.PROBABLE_ERROR
    elif QualityStatus.MISSING in statuses:
        missing_count = statuses.count(QualityStatus.MISSING)
        if missing_count >= 3:
            analysis.overall_status = QualityStatus.MISSING
        else:
            analysis.overall_status = QualityStatus.SHOULD_IMPROVE
    elif QualityStatus.SHOULD_IMPROVE in statuses:
        analysis.overall_status = QualityStatus.SHOULD_IMPROVE
    else:
        analysis.overall_status = QualityStatus.OK

    # Determine follow-up actions
    missing_fields = [fa for fa in field_analyses if fa.status == QualityStatus.MISSING]
    requires_mfr = any(
        fa.field_name in ("Produsent", "Produsentens varenummer", "Spesifikasjon")
        and fa.status == QualityStatus.MISSING
        for fa in field_analyses
    )

    analysis.requires_manufacturer_contact = requires_mfr
    analysis.manual_review_needed = (
        analysis.overall_status in (QualityStatus.PROBABLE_ERROR, QualityStatus.MISSING)
        or product.multiple_hits
    )
    analysis.auto_fix_possible = (
        analysis.overall_status == QualityStatus.SHOULD_IMPROVE
        and not requires_mfr
    )

    # Generate overall comment
    ok_count = statuses.count(QualityStatus.OK)
    total = len(statuses)
    comments = []
    comments.append(f"{ok_count}/{total} felter OK")
    if missing_fields:
        comments.append(f"{len(missing_fields)} felt mangler")
    if requires_mfr:
        comments.append("Krever kontakt med produsent")
    analysis.overall_comment = ". ".join(comments)

    # Generate suggested manufacturer message if needed
    if requires_mfr:
        missing_names = [fa.field_name for fa in missing_fields]
        missing_list = "\n".join(f"- {name}" for name in missing_names)

        if product.manufacturer:
            product_label = product.product_name or product.article_number
            analysis.suggested_manufacturer_message = (
                f"Hei,\n\n"
                f"Vi mangler f\u00f8lgende informasjon for produkt '{product_label}':\n"
                f"{missing_list}\n\n"
                f"Kan dere sende oss oppdatert produktinformasjon?\n\n"
                f"Med vennlig hilsen"
            )
        else:
            analysis.suggested_manufacturer_message = (
                f"Produsent ukjent. Manglende felt: {', '.join(missing_names)}. "
                f"Artikkelnummer: {product.article_number}"
            )

    return analysis
