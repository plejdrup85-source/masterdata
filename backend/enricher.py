"""Source-priority field enrichment engine.

Produces actionable enrichment suggestions for each product field by
consolidating data from all available sources in strict priority order:

  1. OneMed product page (already in ProductData — baseline)
  2. Internal product datasheet / PDF
  3. Product catalog / internal source file
  4. Manufacturer website
  5. AI structuring / translation (only from retrieved source data)

Hard rules:
  - Never invent medical facts
  - Only propose values grounded in retrieved source content
  - If a field is already good enough, skip enrichment
  - English source text may be translated to Norwegian if meaning is preserved
"""

import logging
import re
from typing import Optional

from backend.models import (
    EnrichmentMatchStatus,
    EnrichmentResult,
    EnrichmentSuggestion,
    FieldAnalysis,
    ManufacturerLookup,
    ProductAnalysis,
    ProductData,
    QualityStatus,
)

logger = logging.getLogger(__name__)

# ── Source labels (for traceability) ──
SOURCE_ONEMED = "OneMed produktside"
SOURCE_PDF = "Produktdatablad (PDF)"
SOURCE_MANUFACTURER = "Produsent"
SOURCE_SPEC_STRUCTURING = "Strukturering av kildedata"
SOURCE_TRANSLATION = "Oversettelse fra kildedata"

# ── Confidence thresholds ──
MIN_CONFIDENCE_AUTO = 0.75  # Can be applied without manual review
MIN_CONFIDENCE_SUGGEST = 0.50  # Show as suggestion, but flag for review

# ── Norwegian medical product terms for description quality ──
NORWEGIAN_PRODUCT_KEYWORDS = {
    "hanske", "bandasje", "kompress", "plaster", "sprøyte", "kanyle",
    "kateter", "sutur", "frakk", "munnbind", "stetoskop", "termometer",
    "skalpell", "pinsett", "saks", "fikseringstape", "forbinding",
    "engangs", "steril", "usteril", "pudderfri", "lateksfri",
    "nitril", "vinyl", "polyester",
}


def enrich_product(
    analysis: ProductAnalysis,
    enrichment_results: list[EnrichmentResult],
    manufacturer_data: Optional[ManufacturerLookup] = None,
) -> list[EnrichmentSuggestion]:
    """Run source-priority enrichment for all target fields.

    Returns a list of EnrichmentSuggestion objects — one per field that
    has a viable enrichment. Fields that are already good enough are skipped.
    """
    product = analysis.product_data
    tag = f"[enrich:{product.article_number}]"
    suggestions: list[EnrichmentSuggestion] = []

    # Index enrichment results by field name for fast lookup
    er_by_field = _index_enrichment_results(enrichment_results)
    logger.debug(
        f"{tag} sources available: "
        f"enrichment_results={len(enrichment_results)} "
        f"(by_field={list(er_by_field.keys())}), "
        f"manufacturer={'yes' if manufacturer_data and manufacturer_data.found else 'no'}"
    )

    # Run each field enricher
    field_fns = [
        ("Produktnavn", _enrich_product_name),
        ("Beskrivelse", _enrich_description),
        ("Spesifikasjon", _enrich_specification),
        ("Kategori", _enrich_category),
        ("Pakningsinformasjon", _enrich_packaging),
        ("Produsent", _enrich_manufacturer),
        ("Produsentens varenummer", _enrich_manufacturer_article_number),
    ]
    for field_label, fn in field_fns:
        suggestion = fn(product, analysis, er_by_field, manufacturer_data)
        if suggestion:
            suggestions.append(suggestion)
            logger.debug(
                f"{tag} {field_label}: ENRICHED → "
                f"source={suggestion.source}, "
                f"conf={suggestion.confidence:.2f}, "
                f"review={suggestion.review_required}, "
                f"value={repr(suggestion.suggested_value[:60]) if suggestion.suggested_value else None}"
            )
        else:
            fa = _get_field_analysis(analysis, field_label)
            status = fa.status.value if fa else "?"
            logger.debug(f"{tag} {field_label}: skipped (status={status})")

    logger.info(
        f"{tag} DONE: {len(suggestions)} enrichment suggestion(s) "
        f"from {len(enrichment_results)} source results"
    )
    return suggestions


def apply_enrichment_suggestions(
    analysis: ProductAnalysis,
    suggestions: list[EnrichmentSuggestion],
) -> None:
    """Apply enrichment suggestions to field analyses on the ProductAnalysis.

    Updates suggested_value, source, confidence on matching FieldAnalysis entries.
    Only applies where the field is currently missing or weak.
    """
    for suggestion in suggestions:
        for fa in analysis.field_analyses:
            if fa.field_name == suggestion.field_name:
                # Only enrich if field is currently not OK
                if fa.status not in (
                    QualityStatus.MISSING,
                    QualityStatus.SHOULD_IMPROVE,
                    QualityStatus.PROBABLE_ERROR,
                    QualityStatus.REQUIRES_MANUFACTURER,
                ):
                    break

                # Don't overwrite a higher-confidence suggestion
                if (
                    fa.suggested_value
                    and fa.confidence
                    and fa.confidence >= suggestion.confidence
                ):
                    break

                fa.suggested_value = suggestion.suggested_value
                fa.source = suggestion.source
                fa.confidence = suggestion.confidence
                break


# ── Field enrichment functions ──


def _enrich_product_name(
    product: ProductData,
    analysis: ProductAnalysis,
    er_by_field: dict,
    mfr: Optional[ManufacturerLookup],
) -> Optional[EnrichmentSuggestion]:
    """Enrich product name if missing or weak."""
    fa = _get_field_analysis(analysis, "Produktnavn")
    if fa and fa.status == QualityStatus.OK:
        return None

    current = product.product_name

    # Source 1: PDF
    pdf_val, pdf_evidence, pdf_conf = _get_enrichment_value(er_by_field, "product_name")
    if pdf_val and pdf_val != current:
        return EnrichmentSuggestion(
            field_name="Produktnavn",
            current_value=current,
            suggested_value=pdf_val,
            source=SOURCE_PDF,
            source_url=_get_enrichment_url(er_by_field, "product_name"),
            evidence=pdf_evidence,
            confidence=pdf_conf,
            review_required=pdf_conf < MIN_CONFIDENCE_AUTO,
        )

    # Source 2: Manufacturer
    if mfr and mfr.found and mfr.product_name and mfr.product_name != current:
        conf = mfr.confidence * 0.9
        return EnrichmentSuggestion(
            field_name="Produktnavn",
            current_value=current,
            suggested_value=mfr.product_name,
            source=SOURCE_MANUFACTURER,
            source_url=mfr.source_url,
            evidence=f"Produsentens produktnavn: {mfr.product_name}",
            confidence=conf,
            review_required=conf < MIN_CONFIDENCE_AUTO,
        )

    return None


def _enrich_description(
    product: ProductData,
    analysis: ProductAnalysis,
    er_by_field: dict,
    mfr: Optional[ManufacturerLookup],
) -> Optional[EnrichmentSuggestion]:
    """Enrich description: improve/rewrite to concise factual Norwegian."""
    fa = _get_field_analysis(analysis, "Beskrivelse")
    if fa and fa.status == QualityStatus.OK:
        # Even if OK, check if we can improve a short description
        if product.description and len(product.description) >= 80:
            return None

    current = product.description

    # Source 1: PDF description
    pdf_val, pdf_evidence, pdf_conf = _get_enrichment_value(er_by_field, "description")

    # Source 2: Manufacturer description
    mfr_desc = mfr.description if mfr and mfr.found else None

    # Pick best source
    best_val = None
    best_source = None
    best_url = None
    best_evidence = None
    best_conf = 0.0

    if pdf_val and (not current or len(pdf_val) > len(current)):
        best_val = pdf_val
        best_source = SOURCE_PDF
        best_url = _get_enrichment_url(er_by_field, "description")
        best_evidence = pdf_evidence
        best_conf = pdf_conf

    if mfr_desc and (not best_val or len(mfr_desc) > len(best_val)):
        mfr_conf = (mfr.confidence if mfr else 0) * 0.85
        if mfr_conf > best_conf or not best_val:
            best_val = mfr_desc
            best_source = SOURCE_MANUFACTURER
            best_url = mfr.source_url if mfr else None
            best_evidence = f"Produsentens beskrivelse: {mfr_desc[:120]}..."
            best_conf = mfr_conf

    if not best_val or best_val == current:
        # Try to build a concise description from specs + name if description
        # is completely missing and we have enough source material
        if not current and product.product_name and product.technical_details:
            structured = _build_description_from_specs(product)
            if structured:
                return EnrichmentSuggestion(
                    field_name="Beskrivelse",
                    current_value=current,
                    suggested_value=structured,
                    source=SOURCE_SPEC_STRUCTURING,
                    evidence="Generert fra produktnavn og tekniske spesifikasjoner",
                    confidence=0.55,
                    review_required=True,
                )
        return None

    # If source is English, note translation needed
    review = best_conf < MIN_CONFIDENCE_AUTO
    if best_val and _looks_english(best_val):
        best_source = f"{best_source} (oversettelse påkrevet)"
        review = True
        best_conf = min(best_conf, 0.65)

    return EnrichmentSuggestion(
        field_name="Beskrivelse",
        current_value=current,
        suggested_value=best_val,
        source=best_source,
        source_url=best_url,
        evidence=best_evidence,
        confidence=best_conf,
        review_required=review,
    )


def _enrich_specification(
    product: ProductData,
    analysis: ProductAnalysis,
    er_by_field: dict,
    mfr: Optional[ManufacturerLookup],
) -> Optional[EnrichmentSuggestion]:
    """Enrich specification by structuring source data into key-value pairs."""
    fa = _get_field_analysis(analysis, "Spesifikasjon")
    if fa and fa.status == QualityStatus.OK:
        return None

    current_specs = product.technical_details or {}
    new_specs = dict(current_specs)  # Start with what we have
    sources_used = []

    # Source 1: PDF spec fields
    for field_name, er in er_by_field.items():
        if field_name.startswith("spec:") and er.suggested_value:
            key = field_name[5:]
            if key not in new_specs:
                new_specs[key] = er.suggested_value
                sources_used.append(SOURCE_PDF)

    # Source 2: Manufacturer specs
    if mfr and mfr.found and mfr.specifications:
        for key, val in mfr.specifications.items():
            if key not in new_specs:
                new_specs[key] = val
                sources_used.append(SOURCE_MANUFACTURER)

    # Only suggest if we found new specs beyond what exists
    added_keys = set(new_specs.keys()) - set(current_specs.keys())
    if not added_keys:
        return None

    # Build structured spec text
    spec_text = "; ".join(f"{k}: {v}" for k, v in new_specs.items())
    source = ", ".join(sorted(set(sources_used))) if sources_used else SOURCE_SPEC_STRUCTURING
    added_str = ", ".join(sorted(added_keys))

    # Confidence based on source count
    conf = 0.70 if SOURCE_PDF in sources_used else 0.60

    return EnrichmentSuggestion(
        field_name="Spesifikasjon",
        current_value=product.specification,
        suggested_value=spec_text,
        source=source,
        source_url=_get_enrichment_url(er_by_field, next(
            (f for f in er_by_field if f.startswith("spec:")), ""
        )),
        evidence=f"Nye spesifikasjoner funnet: {added_str}",
        confidence=conf,
        review_required=conf < MIN_CONFIDENCE_AUTO,
    )


def _enrich_category(
    product: ProductData,
    analysis: ProductAnalysis,
    er_by_field: dict,
    mfr: Optional[ManufacturerLookup],
) -> Optional[EnrichmentSuggestion]:
    """Suggest category only if source evidence supports it."""
    fa = _get_field_analysis(analysis, "Kategori")
    if fa and fa.status == QualityStatus.OK:
        return None

    current = product.category
    breadcrumbs = product.category_breadcrumb

    # If we already have a breadcrumb hierarchy, don't try to enrich
    if breadcrumbs and len(breadcrumbs) >= 2:
        return None

    # Try to infer category from product name + specs
    suggested = _infer_category_from_sources(product)
    if not suggested or suggested == current:
        return None

    return EnrichmentSuggestion(
        field_name="Kategori",
        current_value=current or (
            " > ".join(breadcrumbs) if breadcrumbs else None
        ),
        suggested_value=suggested,
        source=SOURCE_SPEC_STRUCTURING,
        evidence="Basert på produktnavn og spesifikasjoner",
        confidence=0.50,
        review_required=True,  # Category inference always needs review
    )


def _enrich_packaging(
    product: ProductData,
    analysis: ProductAnalysis,
    er_by_field: dict,
    mfr: Optional[ManufacturerLookup],
) -> Optional[EnrichmentSuggestion]:
    """Parse actual pack/carton/pallet/unit info from sources."""
    fa = _get_field_analysis(analysis, "Pakningsinformasjon")
    if fa and fa.status == QualityStatus.OK:
        return None

    current = product.packaging_info or product.packaging_unit

    # Source 1: PDF packaging
    pdf_val, pdf_evidence, pdf_conf = _get_enrichment_value(er_by_field, "packaging_info")
    if pdf_val and pdf_val != current:
        return EnrichmentSuggestion(
            field_name="Pakningsinformasjon",
            current_value=current,
            suggested_value=pdf_val,
            source=SOURCE_PDF,
            source_url=_get_enrichment_url(er_by_field, "packaging_info"),
            evidence=pdf_evidence,
            confidence=pdf_conf,
            review_required=pdf_conf < MIN_CONFIDENCE_AUTO,
        )

    return None


def _enrich_manufacturer(
    product: ProductData,
    analysis: ProductAnalysis,
    er_by_field: dict,
    mfr: Optional[ManufacturerLookup],
) -> Optional[EnrichmentSuggestion]:
    """Enrich manufacturer from PDF, manufacturer lookup, or brand in specs."""
    fa = _get_field_analysis(analysis, "Produsent")
    if fa and fa.status == QualityStatus.OK:
        return None

    current = product.manufacturer

    # Source 1: PDF
    pdf_val, pdf_evidence, pdf_conf = _get_enrichment_value(er_by_field, "manufacturer")
    if pdf_val and pdf_val != current:
        return EnrichmentSuggestion(
            field_name="Produsent",
            current_value=current,
            suggested_value=pdf_val,
            source=SOURCE_PDF,
            source_url=_get_enrichment_url(er_by_field, "manufacturer"),
            evidence=pdf_evidence,
            confidence=pdf_conf,
            review_required=pdf_conf < MIN_CONFIDENCE_AUTO,
        )

    # Source 2: Manufacturer lookup
    # If manufacturer lookup was done but manufacturer name itself is what we're
    # looking for — check if we can infer from source_url domain
    if mfr and mfr.found and mfr.source_url:
        mfr_name = _infer_manufacturer_from_url(mfr.source_url)
        if mfr_name and mfr_name != current:
            conf = mfr.confidence * 0.85
            return EnrichmentSuggestion(
                field_name="Produsent",
                current_value=current,
                suggested_value=mfr_name,
                source=SOURCE_MANUFACTURER,
                source_url=mfr.source_url,
                evidence=f"Utledet fra produsentens nettside: {mfr.source_url}",
                confidence=conf,
                review_required=True,
            )

    # Source 3: Brand/Merkevare from technical_details
    if product.technical_details:
        for key in ("Merkevare", "Brand", "Merke"):
            brand = product.technical_details.get(key)
            if brand and brand != current:
                # Brand is not always the manufacturer, but it's a lead
                return EnrichmentSuggestion(
                    field_name="Produsent",
                    current_value=current,
                    suggested_value=f"{brand} (merkevare — verifiser produsent)",
                    source=SOURCE_ONEMED,
                    evidence=f"Merkevare '{brand}' funnet i spesifikasjoner",
                    confidence=0.45,
                    review_required=True,
                )

    return None


def _enrich_manufacturer_article_number(
    product: ProductData,
    analysis: ProductAnalysis,
    er_by_field: dict,
    mfr: Optional[ManufacturerLookup],
) -> Optional[EnrichmentSuggestion]:
    """Enrich manufacturer article number from PDF or manufacturer source."""
    fa = _get_field_analysis(analysis, "Produsentens varenummer")
    if fa and fa.status == QualityStatus.OK:
        return None

    current = product.manufacturer_article_number

    # Source 1: PDF
    pdf_val, pdf_evidence, pdf_conf = _get_enrichment_value(
        er_by_field, "manufacturer_article_number"
    )
    if pdf_val and pdf_val != current:
        return EnrichmentSuggestion(
            field_name="Produsentens varenummer",
            current_value=current,
            suggested_value=pdf_val,
            source=SOURCE_PDF,
            source_url=_get_enrichment_url(er_by_field, "manufacturer_article_number"),
            evidence=pdf_evidence,
            confidence=pdf_conf,
            review_required=pdf_conf < MIN_CONFIDENCE_AUTO,
        )

    return None


# ── Helper functions ──


def _index_enrichment_results(
    results: list[EnrichmentResult],
) -> dict[str, EnrichmentResult]:
    """Index enrichment results by field_name.

    For duplicate field names, prefer the one with higher confidence.
    """
    index: dict[str, EnrichmentResult] = {}
    for r in results:
        existing = index.get(r.field_name)
        if not existing or (r.confidence > existing.confidence):
            index[r.field_name] = r
    return index


def _get_field_analysis(
    analysis: ProductAnalysis, field_name: str
) -> Optional[FieldAnalysis]:
    """Find a FieldAnalysis by Norwegian field name."""
    for fa in analysis.field_analyses:
        if fa.field_name == field_name:
            return fa
    return None


def _get_enrichment_value(
    er_by_field: dict[str, EnrichmentResult],
    field_name: str,
) -> tuple[Optional[str], Optional[str], float]:
    """Get (value, evidence, confidence) from enrichment results for a field."""
    er = er_by_field.get(field_name)
    if not er or not er.suggested_value:
        return None, None, 0.0
    if er.match_status == EnrichmentMatchStatus.NOT_FOUND.value:
        return None, None, 0.0
    if er.review_status == "conflict":
        # Still return value but with reduced confidence
        return er.suggested_value, er.evidence_snippet, er.confidence * 0.6
    return er.suggested_value, er.evidence_snippet, er.confidence


def _get_enrichment_url(
    er_by_field: dict[str, EnrichmentResult],
    field_name: str,
) -> Optional[str]:
    """Get source URL from enrichment result."""
    er = er_by_field.get(field_name)
    return er.source_url if er else None


def _looks_english(text: str) -> bool:
    """Quick check if text appears to be in English rather than Norwegian."""
    if not text or len(text) < 20:
        return False
    english_words = {
        "the", "and", "for", "with", "this", "that", "from", "are",
        "is", "was", "has", "have", "will", "can", "use", "used",
        "glove", "gloves", "bandage", "sterile", "non-sterile",
        "disposable", "latex-free", "powder-free",
    }
    words = text.lower().split()
    english_count = sum(1 for w in words if w in english_words)
    return english_count >= 3 and english_count / max(len(words), 1) > 0.15


def _build_description_from_specs(product: ProductData) -> Optional[str]:
    """Build a minimal description from product name and specs.

    Only used when description is completely missing but we have
    structured data to work with. NOT creative writing — just factual
    assembly of existing data.
    """
    if not product.product_name or not product.technical_details:
        return None

    parts = [product.product_name.rstrip(".") + "."]
    specs = product.technical_details

    # Add material if available
    for key in ("Materiale", "Material"):
        if key in specs:
            parts.append(f"Materiale: {specs[key]}.")
            break

    # Add key physical properties
    for key in ("Lengde", "Størrelse", "Size"):
        if key in specs:
            parts.append(f"{key}: {specs[key]}.")
            break

    # Add packaging
    for key in ("Antall i pakningen", "Antall per pakning", "Pack size"):
        if key in specs:
            parts.append(f"Pakning: {specs[key]}.")
            break

    if len(parts) < 2:
        return None  # Not enough info to build a useful description

    return " ".join(parts)


def _infer_category_from_sources(product: ProductData) -> Optional[str]:
    """Try to infer a product category from product name and specs.

    Only returns a suggestion when there's clear evidence from the
    product type. Does NOT guess for ambiguous products.
    """
    name = (product.product_name or "").lower()
    specs = product.technical_details or {}
    spec_text = " ".join(f"{k} {v}" for k, v in specs.items()).lower()
    combined = f"{name} {spec_text}"

    # Map of keyword patterns to category suggestions
    # Only includes clear-cut medical product categories
    category_map = [
        (r"\bhansk", "Hansker og beskyttelsesprodukter"),
        (r"\bbandasj", "Bandasjer og sårpleie"),
        (r"\bkompress", "Kompresser og sårpleie"),
        (r"\bplaster\b", "Plaster og sårforband"),
        (r"\bsprøyte", "Sprøyter og kanyler"),
        (r"\bkanyl", "Sprøyter og kanyler"),
        (r"\bkateter", "Kateter og urologi"),
        (r"\bsutur", "Suturer og sårlukning"),
        (r"\bmunnbind", "Åndedrettsvern og munnbind"),
        (r"\bfrakk", "Beskyttelsesklær"),
        (r"\bsterili", "Steriliseringsprodukter"),
    ]

    for pattern, category in category_map:
        if re.search(pattern, combined):
            return category

    return None


def _infer_manufacturer_from_url(url: str) -> Optional[str]:
    """Try to extract manufacturer name from a URL domain.

    Only returns for known manufacturer domains.
    """
    known_manufacturers = {
        "molnlycke": "Mölnlycke Health Care",
        "coloplast": "Coloplast",
        "bbraun": "B. Braun",
        "b-braun": "B. Braun",
        "smith-nephew": "Smith & Nephew",
        "smithnephew": "Smith & Nephew",
        "medline": "Medline",
        "hartmann": "Paul Hartmann",
        "essity": "Essity",
        "sca": "Essity (SCA)",
        "abena": "Abena",
        "dansac": "Dansac (Coloplast)",
        "convatec": "ConvaTec",
        "cardinal": "Cardinal Health",
        "ansell": "Ansell",
        "sempermed": "Sempermed",
        "medasense": "Medasense",
        "3m": "3M",
    }
    if not url:
        return None
    url_lower = url.lower()
    for domain_part, name in known_manufacturers.items():
        if domain_part in url_lower:
            return name
    return None
