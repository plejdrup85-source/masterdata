"""Masterdata quality analysis engine.

Validates product data quality and scores each field.
Handles cases where data exists but in unexpected formats.
"""

import logging
import re

from backend.models import (
    FieldAnalysis,
    JeevesData,
    ProductAnalysis,
    ProductData,
    QualityStatus,
    VerificationStatus,
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
    "Merkevare": 0.5,
    "Konsistens mellom felter": 0.5,
}

# Technical/measurable keywords that indicate specification content
def _source_label(website_val, jeeves_val, website_label="nettside", jeeves_label="Jeeves"):
    """Return a human-readable source label for two-source comparison."""
    has_web = bool(website_val)
    has_jeeves = bool(jeeves_val)
    if has_web and has_jeeves:
        return f"{website_label} + {jeeves_label}"
    elif has_web:
        return f"{website_label} kun"
    elif has_jeeves:
        return f"{jeeves_label} kun"
    return None


def _no_page_reason(product: ProductData) -> str:
    """Return an explanation when website value is missing due to no product page."""
    if product.verification_status == VerificationStatus.CDN_ONLY:
        return "Ingen produktside funnet på nettstedet (kun bilde bekreftet via CDN)"
    elif not product.found_on_onemed:
        return "Produktet ble ikke funnet på nettstedet"
    elif product.verification_status == VerificationStatus.MISMATCH:
        return "Produktsiden tilhører et annet produkt (SKU-avvik)"
    return "Nettstedet mangler denne verdien"


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


def _analyze_product_name(product: ProductData, jeeves: JeevesData = None) -> FieldAnalysis:
    """Analyze product name quality using website + Jeeves sources."""
    name = product.product_name
    jeeves_name = (jeeves.item_description or jeeves.web_title) if jeeves else None

    # Use website name, fall back to Jeeves
    effective_name = name or jeeves_name
    source_info = _source_label(name, jeeves_name, "nettside", "Jeeves")
    origin = "nettside" if name else ("Jeeves" if jeeves_name else None)

    analysis = FieldAnalysis(
        field_name="Produktnavn",
        current_value=effective_name,
        source=source_info,
        website_value=name,
        jeeves_value=jeeves_name,
        value_origin=origin,
    )

    if not effective_name:
        analysis.status = QualityStatus.MISSING
        analysis.comment = "Produktnavn mangler i både Jeeves og nettside"
        analysis.status_reason = "Ingen kilde har produktnavn"
        return analysis

    if not name and jeeves_name:
        analysis.status = QualityStatus.OK
        analysis.comment = f"Produktnavn fra Jeeves: {jeeves_name}"
        analysis.status_reason = f"Produktnavn fra Jeeves. {_no_page_reason(product)}"
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
        issues.append("Navn ser ut til å være på engelsk, bør vurderes for norsk oversettelse")

    if name == name.upper() and len(name) > 3:
        issues.append("Navn er i STORE BOKSTAVER, bør ha normal casing")

    if not issues:
        # P1 FIX: Distinguish STRONG from OK based on name length and structure
        if len(name) >= 10 and not name.isupper():
            analysis.status = QualityStatus.STRONG
            analysis.comment = "Produktnavn er godt"
            analysis.status_reason = "Godt produktnavn med tilstrekkelig lengde og struktur"
        else:
            analysis.status = QualityStatus.OK
            analysis.comment = "Produktnavn ser bra ut"
            analysis.status_reason = "Akseptabelt produktnavn"
    elif any("mangler" in i.lower() or "kun tall" in i.lower() for i in issues):
        analysis.status = QualityStatus.PROBABLE_ERROR
        analysis.comment = "; ".join(issues)
        analysis.status_reason = "; ".join(issues)
    else:
        analysis.status = QualityStatus.SHOULD_IMPROVE
        analysis.comment = "; ".join(issues)
        analysis.status_reason = "; ".join(issues)

    return analysis


def _analyze_description(product: ProductData, jeeves: JeevesData = None) -> FieldAnalysis:
    """Analyze product description quality using website + Jeeves sources."""
    desc = product.description
    jeeves_desc = jeeves.web_text if jeeves else None
    effective_desc = desc or jeeves_desc
    source_info = _source_label(desc, jeeves_desc, "nettside", "Jeeves")
    origin = "nettside" if desc else ("Jeeves" if jeeves_desc else None)

    analysis = FieldAnalysis(
        field_name="Beskrivelse",
        current_value=effective_desc,
        source=source_info,
        website_value=desc,
        jeeves_value=jeeves_desc,
        value_origin=origin,
    )

    if not effective_desc:
        analysis.status = QualityStatus.MISSING
        analysis.comment = "Beskrivelse mangler i både Jeeves og nettside"
        analysis.status_reason = "Ingen kilde har beskrivelse"
        return analysis

    if not desc and jeeves_desc:
        analysis.status = QualityStatus.OK
        analysis.comment = f"Beskrivelse fra Jeeves ({len(jeeves_desc)} tegn)"
        analysis.status_reason = f"Beskrivelse fra Jeeves. {_no_page_reason(product)}"
        return analysis

    issues = []

    if len(desc) < MIN_DESCRIPTION_LENGTH:
        issues.append(f"Beskrivelse er for kort ({len(desc)} tegn, minimum {MIN_DESCRIPTION_LENGTH})")

    if product.product_name and desc.strip().lower() == product.product_name.strip().lower():
        issues.append("Beskrivelse er identisk med produktnavn")

    if product.article_number in desc and len(desc) < 50:
        issues.append("Beskrivelse inneholder hovedsakelig artikkelnummer")

    if not issues:
        # P1 FIX: Distinguish STRONG from OK based on content richness
        has_sentences = len(re.findall(r'[.!?]\s', desc)) >= 1
        if len(desc) >= 80 and has_sentences:
            analysis.status = QualityStatus.STRONG
            analysis.comment = f"Beskrivelse er god ({len(desc)} tegn, strukturert)"
            analysis.status_reason = f"Beskrivelse har {len(desc)} tegn med fullstendige setninger"
        else:
            analysis.status = QualityStatus.OK
            analysis.comment = "Beskrivelse OK"
            analysis.status_reason = f"Beskrivelse akseptabel ({len(desc)} tegn)"
    else:
        # P1 FIX: Distinguish WEAK (present but thin) from SHOULD_IMPROVE (real problems)
        is_only_short = (len(issues) == 1 and "for kort" in issues[0] and len(desc) >= 10)
        if is_only_short:
            analysis.status = QualityStatus.WEAK
            analysis.comment = "; ".join(issues)
            analysis.status_reason = f"Beskrivelse finnes men er kort ({len(desc)} tegn)"
        else:
            analysis.status = QualityStatus.SHOULD_IMPROVE
            analysis.comment = "; ".join(issues)
            analysis.status_reason = "; ".join(issues)

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


def _analyze_specification(product: ProductData, jeeves: JeevesData = None) -> FieldAnalysis:
    """Analyze product specification quality using website + Jeeves sources.

    Valid if:
    - At least 2 structured attributes exist in technical_details
    - OR specification text contains measurable/technical info
    - OR Jeeves has specification data
    - OR description contains substantial technical details
    """
    spec = product.specification
    details = product.technical_details
    jeeves_spec = jeeves.specification if jeeves else None

    # Build current_value from all available sources
    web_spec = spec or ("; ".join(f"{k}: {v}" for k, v in details.items()) if details else None)
    display_value = web_spec
    if not display_value and jeeves_spec:
        display_value = jeeves_spec

    source_info = _source_label(web_spec, jeeves_spec, "nettside", "Jeeves")
    origin = "nettside" if web_spec else ("Jeeves" if jeeves_spec else None)

    analysis = FieldAnalysis(
        field_name="Spesifikasjon",
        current_value=display_value,
        source=source_info,
        website_value=web_spec,
        jeeves_value=jeeves_spec,
        value_origin=origin,
    )

    # Count structured attributes from technical_details
    attr_count = _count_structured_attributes(details, spec)

    # Check for technical content in spec text
    has_tech_content = _has_measurable_content(spec)

    # Also check description for embedded technical info
    has_tech_in_desc = _has_measurable_content(product.description)

    # Determine status
    if not spec and not details:
        if jeeves_spec:
            analysis.status = QualityStatus.OK
            analysis.comment = f"Spesifikasjon fra Jeeves: {jeeves_spec}"
            analysis.status_reason = f"Spesifikasjon fra Jeeves. {_no_page_reason(product)}"
            return analysis
        if has_tech_in_desc:
            analysis.status = QualityStatus.WEAK
            analysis.comment = (
                "Spesifikasjoner mangler som eget felt, men beskrivelsen inneholder "
                "teknisk informasjon. Bør struktureres som egne spesifikasjonsfelter."
            )
            analysis.status_reason = "Teknisk info finnes i beskrivelsen men ikke som eget felt"
        else:
            analysis.status = QualityStatus.MISSING
            analysis.comment = "Spesifikasjoner mangler i både Jeeves og nettside"
            analysis.status_reason = "Ingen kilde har spesifikasjoner"
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
        # P1 FIX: STRONG if rich structured specs exist
        if attr_count >= 4 or (attr_count >= 2 and has_tech_content):
            analysis.status = QualityStatus.STRONG
            analysis.comment = f"Spesifikasjoner er gode ({attr_count} attributter)"
            analysis.status_reason = f"{attr_count} strukturerte attributter med teknisk innhold"
        else:
            analysis.status = QualityStatus.OK
            if attr_count >= 2:
                analysis.comment = f"Spesifikasjoner OK ({attr_count} attributter)"
            else:
                analysis.comment = "Spesifikasjoner OK (teknisk innhold funnet)"
            analysis.status_reason = f"Akseptable spesifikasjoner ({attr_count} attributter)"
    else:
        # P1 FIX: WEAK if only minor issues, SHOULD_IMPROVE if serious
        serious = any("identisk" in i.lower() for i in issues)
        if serious:
            analysis.status = QualityStatus.SHOULD_IMPROVE
        elif attr_count >= 1:
            analysis.status = QualityStatus.WEAK
        else:
            analysis.status = QualityStatus.SHOULD_IMPROVE
        analysis.comment = "; ".join(issues)
        analysis.status_reason = "; ".join(issues)

    return analysis


def _analyze_manufacturer(product: ProductData, jeeves: JeevesData = None) -> FieldAnalysis:
    """Analyze manufacturer information using website + Jeeves sources.

    Supplier/brand from Jeeves is the authoritative source for manufacturer.
    Do NOT mark as missing just because the website doesn't show it.
    """
    mfr = product.manufacturer
    jeeves_supplier = jeeves.supplier if jeeves else None
    effective_mfr = mfr or jeeves_supplier
    source_info = _source_label(mfr, jeeves_supplier, "nettside", "Jeeves")
    origin = "nettside" if mfr else ("Jeeves" if jeeves_supplier else None)

    analysis = FieldAnalysis(
        field_name="Produsent",
        current_value=effective_mfr,
        source=source_info,
        website_value=mfr,
        jeeves_value=jeeves_supplier,
        value_origin=origin,
    )

    if not effective_mfr:
        analysis.status = QualityStatus.MISSING
        analysis.comment = "Produsentinformasjon mangler i både Jeeves og nettside"
        analysis.status_reason = "Ingen kilde har produsentinfo"
        return analysis

    if not mfr and jeeves_supplier:
        analysis.status = QualityStatus.OK
        analysis.comment = f"Produsent fra Jeeves: {jeeves_supplier}"
        analysis.status_reason = f"Produsent fra Jeeves. {_no_page_reason(product)}"
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


def _analyze_manufacturer_article_number(product: ProductData, jeeves: JeevesData = None) -> FieldAnalysis:
    """Analyze manufacturer article number using website + Jeeves sources.

    Do NOT mark as missing just because the website doesn't show it.
    Jeeves 'Supplier Item.no' is the authoritative source.
    """
    mfr_num = product.manufacturer_article_number
    jeeves_num = jeeves.supplier_item_no if jeeves else None
    effective_num = mfr_num or jeeves_num
    source_info = _source_label(mfr_num, jeeves_num, "nettside", "Jeeves")
    origin = "nettside" if mfr_num else ("Jeeves" if jeeves_num else None)

    analysis = FieldAnalysis(
        field_name="Produsentens varenummer",
        current_value=effective_num,
        source=source_info,
        website_value=mfr_num,
        jeeves_value=jeeves_num,
        value_origin=origin,
    )

    if not effective_num:
        analysis.status = QualityStatus.MISSING
        analysis.comment = "Produsentens varenummer mangler i både Jeeves og nettside"
        analysis.status_reason = "Ingen kilde har produsentens varenummer"
        return analysis

    if not mfr_num and jeeves_num:
        analysis.status = QualityStatus.OK
        analysis.comment = f"Produsentens varenummer fra Jeeves: {jeeves_num}"
        analysis.status_reason = f"Varenummer fra Jeeves. {_no_page_reason(product)}"
        return analysis

    analysis.status = QualityStatus.OK
    analysis.comment = "Produsentens varenummer finnes"
    analysis.status_reason = "Varenummer finnes i minst én kilde"
    return analysis


def _analyze_brand(product: ProductData, jeeves: JeevesData = None) -> FieldAnalysis:
    """Analyze product brand using Jeeves Product Brand field."""
    jeeves_brand = jeeves.product_brand if jeeves else None

    analysis = FieldAnalysis(
        field_name="Merkevare",
        current_value=jeeves_brand,
        source="Jeeves" if jeeves_brand else None,
    )

    if not jeeves_brand:
        # Brand is not always available — don't treat as critical missing
        analysis.status = QualityStatus.SHOULD_IMPROVE
        analysis.comment = "Merkevare ikke oppgitt i Jeeves"
        return analysis

    analysis.status = QualityStatus.OK
    analysis.comment = f"Merkevare fra Jeeves: {jeeves_brand}"
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


def analyze_product(
    product: ProductData,
    image_quality: dict = None,
    jeeves: JeevesData = None,
) -> ProductAnalysis:
    """Run full quality analysis on a product.

    Args:
        product: Scraped product data from website
        image_quality: Optional dict from ProductImageSummary.to_dict()
        jeeves: Optional Jeeves ERP data for two-source comparison
    """
    analysis = ProductAnalysis(
        article_number=product.article_number,
        product_data=product,
        jeeves_data=jeeves,
    )

    if not product.found_on_onemed and not jeeves:
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

    # If product not on website but exists in Jeeves, still analyze with Jeeves data
    if not product.found_on_onemed and jeeves:
        logger.info(
            f"[analyze:{product.article_number}] Not on website, "
            f"but found in Jeeves — analyzing with Jeeves data only"
        )

    tag = f"[analyze:{product.article_number}]"

    # Run all field analyses with both sources
    field_analyses = [
        _analyze_product_name(product, jeeves),
        _analyze_description(product, jeeves),
        _analyze_specification(product, jeeves),
        _analyze_manufacturer(product, jeeves),
        _analyze_manufacturer_article_number(product, jeeves),
        _analyze_brand(product, jeeves),
        _analyze_category(product),
        _analyze_packaging(product),
        _analyze_image(product, image_quality),
        _check_field_consistency(product),
    ]

    analysis.field_analyses = field_analyses

    # Log per-field status with reasons for missing/weak
    for fa in field_analyses:
        if fa.status in (QualityStatus.MISSING, QualityStatus.PROBABLE_ERROR):
            logger.debug(f"{tag} {fa.field_name}: {fa.status.value} — {fa.comment}")
        elif fa.status == QualityStatus.SHOULD_IMPROVE:
            logger.debug(f"{tag} {fa.field_name}: {fa.status.value} — {fa.comment}")

    # Calculate weighted score
    score_map = {
        QualityStatus.STRONG: 1.0,
        QualityStatus.OK: 1.0,
        QualityStatus.WEAK: 0.65,
        QualityStatus.SHOULD_IMPROVE: 0.5,
        QualityStatus.MISSING: 0.0,
        QualityStatus.PROBABLE_ERROR: 0.0,
        QualityStatus.REQUIRES_MANUFACTURER: 0.25,
        QualityStatus.MANUAL_REVIEW: 0.4,
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
    elif QualityStatus.WEAK in statuses:
        analysis.overall_status = QualityStatus.WEAK
    elif all(s in (QualityStatus.STRONG, QualityStatus.OK) for s in statuses):
        # All fields are strong or OK
        strong_count = statuses.count(QualityStatus.STRONG)
        if strong_count >= len(statuses) // 2:
            analysis.overall_status = QualityStatus.STRONG
        else:
            analysis.overall_status = QualityStatus.OK
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

    # Trigger manual review for quality issues, ambiguous identity, or weak verification
    from backend.models import VerificationStatus
    weak_verification = product.verification_status in (
        VerificationStatus.CDN_ONLY,
        VerificationStatus.UNVERIFIED,
        VerificationStatus.MISMATCH,
        VerificationStatus.AMBIGUOUS,
    )
    analysis.manual_review_needed = (
        analysis.overall_status in (QualityStatus.PROBABLE_ERROR, QualityStatus.MISSING)
        or product.multiple_hits
        or weak_verification
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

    logger.info(
        f"{tag} DONE: score={analysis.total_score}% "
        f"status={analysis.overall_status.value} "
        f"mfr_contact={analysis.requires_manufacturer_contact} "
        f"manual_review={analysis.manual_review_needed} "
        f"auto_fix={analysis.auto_fix_possible}"
    )

    return analysis
