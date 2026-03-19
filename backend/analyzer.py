"""Masterdata quality analysis engine."""

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

    # Too short
    if len(name) < MIN_NAME_LENGTH:
        issues.append("Navn er for kort (under 5 tegn)")

    # Too generic (single word or very common terms)
    generic_names = {"produkt", "vare", "artikkel", "item", "product", "test"}
    if name.lower().strip() in generic_names:
        issues.append("Navn er for generisk")

    # Only numbers
    if re.match(r"^[\d\s\-/]+$", name):
        issues.append("Navn inneholder kun tall/tegn, mangler beskrivende tekst")

    # Wrong language (contains common English words that should be Norwegian)
    english_indicators = ["gloves", "bandage", "tape", "pack", "box", "piece"]
    name_lower = name.lower()
    english_count = sum(1 for word in english_indicators if word in name_lower)
    if english_count >= 2:
        issues.append("Navn ser ut til å være på engelsk, bør vurderes for norsk oversettelse")

    # Check for ALL CAPS
    if name == name.upper() and len(name) > 3:
        issues.append("Navn er i STORE BOKSTAVER, bør ha normal casing")

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

    # Check if description is same as name
    if product.product_name and desc.strip().lower() == product.product_name.strip().lower():
        issues.append("Beskrivelse er identisk med produktnavn")

    # Check for useful content (not just article number repeated)
    if product.article_number in desc and len(desc) < 50:
        issues.append("Beskrivelse inneholder hovedsakelig artikkelnummer")

    if not issues:
        analysis.status = QualityStatus.OK
        analysis.comment = "Beskrivelse OK"
    elif len(issues) == 1 and "kort" in issues[0]:
        analysis.status = QualityStatus.SHOULD_IMPROVE
        analysis.comment = "; ".join(issues)
    else:
        analysis.status = QualityStatus.SHOULD_IMPROVE
        analysis.comment = "; ".join(issues)

    return analysis


def _analyze_specification(product: ProductData) -> FieldAnalysis:
    """Analyze product specification quality."""
    spec = product.specification
    details = product.technical_details

    analysis = FieldAnalysis(
        field_name="Spesifikasjon",
        current_value=spec,
    )

    if not spec and not details:
        analysis.status = QualityStatus.MISSING
        analysis.comment = "Spesifikasjoner mangler helt. Bør innhentes fra produsent."
        return analysis

    issues = []

    if details and len(details) < 2:
        issues.append("Få spesifikasjonsfelter (kun {})".format(len(details)))

    # Check for key specification fields that should exist
    expected_fields = {"størrelse", "size", "materiale", "material", "farge", "color", "vekt", "weight"}
    if details:
        detail_keys_lower = {k.lower() for k in details.keys()}
        matching = expected_fields & detail_keys_lower
        if not matching:
            issues.append("Mangler vanlige spesifikasjoner (størrelse, materiale, farge, vekt)")

    if not issues:
        analysis.status = QualityStatus.OK
        analysis.comment = "Spesifikasjoner OK"
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

    # Check for placeholder values
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
        analysis.comment = "Produsentens varenummer mangler. Bør innhentes."
        return analysis

    analysis.status = QualityStatus.OK
    analysis.comment = "Produsentens varenummer finnes"
    return analysis


def _analyze_category(product: ProductData) -> FieldAnalysis:
    """Analyze product categorization."""
    cat = product.category
    breadcrumbs = product.category_breadcrumb

    analysis = FieldAnalysis(
        field_name="Kategori",
        current_value=cat if cat else (
            " > ".join(breadcrumbs) if breadcrumbs else None
        ),
    )

    if not cat and not breadcrumbs:
        analysis.status = QualityStatus.MISSING
        analysis.comment = "Kategori mangler helt"
        return analysis

    issues = []

    if breadcrumbs and len(breadcrumbs) < 2:
        issues.append("Kategorihierarki er grunt (kun 1 nivå)")

    if cat and len(cat) < 3:
        issues.append("Kategorinavn er for kort/generisk")

    if not issues:
        analysis.status = QualityStatus.OK
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


def _analyze_image(product: ProductData) -> FieldAnalysis:
    """Analyze image quality."""
    analysis = FieldAnalysis(
        field_name="Bilde",
        current_value=product.image_url,
    )

    if not product.image_url:
        analysis.status = QualityStatus.MISSING
        analysis.comment = "Produktbilde mangler"
        return analysis

    if product.image_quality_ok is False:
        analysis.status = QualityStatus.SHOULD_IMPROVE
        analysis.comment = "Bildekvalitet er lav eller bilde er ikke tilgjengelig"
        return analysis

    analysis.status = QualityStatus.OK
    analysis.comment = "Produktbilde finnes"
    return analysis


def _check_field_consistency(product: ProductData) -> FieldAnalysis:
    """Check for inconsistencies between fields."""
    analysis = FieldAnalysis(
        field_name="Konsistens mellom felter",
        current_value="Se kommentar",
    )

    issues = []

    # Check if name and description contradict
    if product.product_name and product.description:
        name_words = set(product.product_name.lower().split())
        desc_words = set(product.description.lower().split())
        # If name and description share no words (besides common ones)
        common_words = {"og", "i", "for", "med", "til", "av", "en", "et", "den", "det", "de", "er"}
        meaningful_name = name_words - common_words
        meaningful_desc = desc_words - common_words
        if meaningful_name and meaningful_desc:
            overlap = meaningful_name & meaningful_desc
            if not overlap and len(meaningful_name) > 2:
                issues.append("Produktnavn og beskrivelse deler ingen nøkkelord - mulig inkonsistens")

    # Check category vs name consistency
    if product.category and product.product_name:
        cat_lower = product.category.lower()
        name_lower = product.product_name.lower()
        # Very basic check - at least one word overlap
        cat_words = set(cat_lower.split()) - {"og", "i", "for", "med"}
        name_words = set(name_lower.split()) - {"og", "i", "for", "med"}
        if cat_words and name_words and not (cat_words & name_words):
            if len(cat_words) > 1 and len(name_words) > 1:
                issues.append(f"Kategorinavn '{product.category}' og produktnavn deler ingen ord")

    if not issues:
        analysis.status = QualityStatus.OK
        analysis.comment = "Ingen åpenbare inkonsistenser"
    else:
        analysis.status = QualityStatus.SHOULD_IMPROVE
        analysis.comment = "; ".join(issues)

    return analysis


def analyze_product(product: ProductData) -> ProductAnalysis:
    """Run full quality analysis on a product."""
    analysis = ProductAnalysis(
        article_number=product.article_number,
        product_data=product,
    )

    if not product.found_on_onemed:
        analysis.overall_status = QualityStatus.MISSING
        analysis.overall_comment = product.error or "Produkt ikke funnet på onemed.no"
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
        _analyze_image(product),
        _check_field_consistency(product),
    ]

    analysis.field_analyses = field_analyses

    # Calculate total score
    score_map = {
        QualityStatus.OK: 1.0,
        QualityStatus.SHOULD_IMPROVE: 0.5,
        QualityStatus.MISSING: 0.0,
        QualityStatus.PROBABLE_ERROR: 0.0,
        QualityStatus.REQUIRES_MANUFACTURER: 0.25,
    }

    scores = [score_map.get(fa.status, 0) for fa in field_analyses]
    analysis.total_score = round(sum(scores) / len(scores) * 100, 1) if scores else 0

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
    if requires_mfr and product.manufacturer:
        missing_names = [fa.field_name for fa in missing_fields]
        analysis.suggested_manufacturer_message = (
            f"Hei,\n\nVi mangler følgende informasjon for produkt "
            f"'{product.product_name or product.article_number}':\n"
            f"- {chr(10).join('- ' + n for n in missing_names[1:]) if len(missing_names) > 1 else missing_names[0] if missing_names else ''}\n\n"
            f"Kan dere sende oss oppdatert produktinformasjon?\n\n"
            f"Med vennlig hilsen"
        )
    elif requires_mfr:
        missing_names = [fa.field_name for fa in missing_fields]
        analysis.suggested_manufacturer_message = (
            f"Produsent ukjent. Manglende felt: {', '.join(missing_names)}. "
            f"Artikkelnummer: {product.article_number}"
        )

    return analysis
