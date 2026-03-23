"""Source-priority field enrichment engine.

Produces actionable enrichment suggestions for each product field by
consolidating data from all available sources in strict priority order:

  1. OneMed product page (already in ProductData — baseline)
  2. Internal product datasheet / PDF
  3. Product catalog / internal source file
  4. Manufacturer website
  5. Norengros (secondary market reference — conservative use only)
  6. AI structuring / translation (only from retrieved source data)

Hard rules:
  - Never invent medical facts
  - Only propose values grounded in retrieved source content
  - If a field is already good enough, skip enrichment
  - English source text may be translated to Norwegian if meaning is preserved
  - Packaging field must only contain packaging-related data
  - Fragment/truncated text must be rejected or flagged for review
"""

import logging
import re
from typing import Optional

from backend.content_validator import (
    clean_all_noise,
    classify_text_as_description_candidate,
    classify_text_as_spec_candidate,
    detect_language,
    normalize_for_webshop_description,
    normalize_for_webshop_specification,
    translate_to_norwegian_if_needed,
    validate_no_contact_info,
    validate_single_product_scope,
    validate_suggestion_output,
)
from backend.description_cleaner import clean_description_source, validate_webshop_description
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
SOURCE_NORENGROS = "Norengros (sekundærkilde)"
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


def _validate_suggestion_value(
    value: str, field_name: str, current_sku: str = ""
) -> tuple[bool, str]:
    """Validate that a suggestion value is suitable for its target field.

    Returns (is_valid, reason). If not valid, the suggestion should be
    rejected or flagged for manual review.
    """
    if not value or not value.strip():
        return False, "Tom verdi"

    value = value.strip()

    # ── Universal checks via content_validator ──

    # Contact info check (phone, email, address)
    ok, reason = validate_no_contact_info(value)
    if not ok:
        return False, reason

    # Multi-SKU check (only references to current product allowed)
    if current_sku:
        ok, reason = validate_single_product_scope(value, current_sku)
        if not ok:
            return False, reason

    # Full output validation (PDF noise, variant tables, etc.)
    if current_sku:
        ok, reason = validate_suggestion_output(value, field_name, current_sku)
        if not ok:
            return False, reason

    # Fragment detection: looks genuinely truncated mid-sentence
    if len(value) > 40 and value[-1] not in ".!?)\"':;,–—0123456789%":
        has_sentence_structure = bool(re.search(r"\b(?:og|for|som|med|til|av|er|i|and|for|with|the|is)\b", value))
        if has_sentence_structure:
            return False, "Teksten ser ut til å være avkortet midt i en setning"

    # Very short for substantive fields
    if field_name in ("Beskrivelse",) and len(value) < 15:
        return False, "Beskrivelsen er for kort til å være nyttig"

    # ── Packaging-specific validation ──
    if field_name == "Pakningsinformasjon":
        packaging_indicators = [
            r"(?i)\d+\s*(?:stk|pk|stykk|per|pr|i\s+pakning)",
            r"(?i)(?:eske|kartong|pall|pose|boks|pakke|forpakning)\b",
            r"(?i)\d+\s*(?:x\s*\d+)",
            r"(?i)(?:inner|outer|master|transport)\s*(?:pak|box|cart)",
        ]
        has_packaging = any(re.search(p, value) for p in packaging_indicators)

        non_packaging = [
            r"(?i)(?:oppbevar|lagr)\w*\s+(?:tørt|kjølig|mørkt)",
            r"(?i)(?:brukes?\s+til|designed\s+for|intended\s+for|suitable\s+for)",
            r"(?i)(?:fordeler|benefits|advantages|features)\b",
            r"(?i)(?:instruksjon|instruction|bruksanvisning)\b",
        ]
        has_non_packaging = any(re.search(p, value) for p in non_packaging)

        if has_non_packaging and not has_packaging:
            return False, "Verdien inneholder ikke pakningsinformasjon (lagring/bruk/markedsføring)"
        if not has_packaging and len(value) > 50:
            return False, "Lang tekst uten gjenkjennelig pakningsdata"

    # ── Produktnavn: should be a clean title, not a paragraph ──
    if field_name == "Produktnavn":
        if len(value) > 150:
            return False, "Produktnavnet er for langt — ser ut som en beskrivelse"
        if value.count(".") > 2:
            return False, "Produktnavnet inneholder flere setninger"

    # ── Description: check if it's actually spec content ──
    if field_name == "Beskrivelse":
        spec_score = classify_text_as_spec_candidate(value)
        desc_score = classify_text_as_description_candidate(value)
        if spec_score > desc_score + 0.3:
            return False, "Innholdet er teknisk/strukturert og passer bedre som spesifikasjon enn beskrivelse"

    # ── Specification: check if it's actually description content ──
    if field_name == "Spesifikasjon":
        spec_score = classify_text_as_spec_candidate(value)
        desc_score = classify_text_as_description_candidate(value)
        if desc_score > spec_score + 0.3:
            return False, "Innholdet er prosa/narrativt og passer bedre som beskrivelse enn spesifikasjon"

    return True, ""


def _add_rationale(suggestion: EnrichmentSuggestion, reason: str) -> EnrichmentSuggestion:
    """Add a rationale comment to a suggestion's evidence field."""
    if suggestion.evidence:
        suggestion.evidence = f"{suggestion.evidence} | {reason}"
    else:
        suggestion.evidence = reason
    return suggestion


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
                # P1 FIX: Only enrich if field needs improvement.
                # STRONG and OK fields must never be overwritten.
                if fa.status in (QualityStatus.STRONG, QualityStatus.OK):
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
    # P1 FIX: STRONG and OK fields should never get suggestions
    if fa and fa.status in (QualityStatus.STRONG, QualityStatus.OK):
        return None

    current = product.product_name

    sku = product.article_number

    # Source 1: PDF
    from backend.evidence import build_evidence
    pdf_val, pdf_evidence, pdf_conf = _get_enrichment_value(er_by_field, "product_name")
    if pdf_val and pdf_val != current:
        pdf_val = clean_all_noise(pdf_val, sku)
        is_valid, reject_reason = _validate_suggestion_value(pdf_val, "Produktnavn", sku)
        if not is_valid:
            logger.info(f"[enrich] Product name rejected: {reject_reason}")
        else:
            pdf_val, lang, translate_msg = _translate_if_needed(pdf_val)
            needs_manual = lang == "en"
            conf = pdf_conf if not needs_manual else min(pdf_conf, 0.55)
            pdf_url = _get_enrichment_url(er_by_field, "product_name")
            noise = ["kontaktinfo"] if pdf_val != (pdf_evidence or "") else []
            ev_text, ev_dict = build_evidence(
                "Produktnavn",
                source_label="datablad (PDF)", source_url=pdf_url, source_tier=3,
                lang=lang, translated=(lang in ("sv", "da")), translate_note=translate_msg,
                variant_matched=True, variant_note=f"SKU {sku} bekreftet i dokument",
                noise_filtered=noise if noise else None,
                confidence=conf, auto_inriver=conf >= MIN_CONFIDENCE_AUTO and not needs_manual,
            )
            return EnrichmentSuggestion(
                field_name="Produktnavn", current_value=current,
                suggested_value=pdf_val, source=SOURCE_PDF, source_url=pdf_url,
                evidence=ev_text, evidence_structured=ev_dict,
                confidence=conf, review_required=conf < MIN_CONFIDENCE_AUTO or needs_manual,
            )

    # Source 2: Manufacturer
    if mfr and mfr.found and mfr.product_name and mfr.product_name != current:
        mfr_name = clean_all_noise(mfr.product_name, sku)
        is_valid, reject_reason = _validate_suggestion_value(mfr_name, "Produktnavn", sku)
        if is_valid:
            conf = mfr.confidence * 0.9
            mfr_name, lang, translate_msg = _translate_if_needed(mfr_name)
            needs_manual = lang == "en"
            if needs_manual:
                conf = min(conf, 0.55)
            ev_text, ev_dict = build_evidence(
                "Produktnavn",
                source_label="produsentside", source_url=mfr.source_url, source_tier=4,
                lang=lang, translated=(lang in ("sv", "da")), translate_note=translate_msg,
                confidence=conf, auto_inriver=conf >= MIN_CONFIDENCE_AUTO and not needs_manual,
            )
            return EnrichmentSuggestion(
                field_name="Produktnavn", current_value=current,
                suggested_value=mfr_name, source=SOURCE_MANUFACTURER, source_url=mfr.source_url,
                evidence=ev_text, evidence_structured=ev_dict,
                confidence=conf, review_required=conf < MIN_CONFIDENCE_AUTO or needs_manual,
            )

    return None


def _description_quality_score(text: str) -> float:
    """Score a description's quality for webshop use (0.0-1.0).

    P1-1: Replaces simple length comparison. A shorter well-structured
    description scores higher than a longer noisy one.

    Factors:
    - Sentence structure (complete sentences with punctuation)
    - Technical keyword density (medical product terms)
    - Passes webshop validation gate
    - Not too short, not excessively long
    """
    if not text or not text.strip():
        return 0.0

    text = text.strip()
    score = 0.0

    # Factor 1: Length adequacy (sweet spot 50-500 chars)
    length = len(text)
    if length < 15:
        return 0.05  # Too short to be useful
    elif length < 50:
        score += 0.10
    elif length <= 500:
        score += 0.25
    elif length <= 1000:
        score += 0.20
    else:
        score += 0.10  # Excessively long — likely unfiltered

    # Factor 2: Sentence structure (complete sentences with punctuation)
    sentences = [s.strip() for s in re.split(r'[.!?]', text) if len(s.strip()) > 10]
    if len(sentences) >= 2:
        score += 0.25
    elif len(sentences) == 1:
        score += 0.15

    # Factor 3: Technical keyword density
    tech_words = re.findall(
        r'\b(?:mm|cm|ml|g|kg|stk|steril|nitril|vinyl|latex|'
        r'engangs|flergangs|pudderfri|materiale|størrelse|'
        r'diameter|lengde|bredde|volum)\b',
        text.lower()
    )
    if len(tech_words) >= 3:
        score += 0.25
    elif len(tech_words) >= 1:
        score += 0.15

    # Factor 4: Webshop validation gate
    gate_ok, _ = validate_webshop_description(text)
    if gate_ok:
        score += 0.25

    return min(1.0, score)


def _enrich_description(
    product: ProductData,
    analysis: ProductAnalysis,
    er_by_field: dict,
    mfr: Optional[ManufacturerLookup],
) -> Optional[EnrichmentSuggestion]:
    """Enrich description: improve/rewrite to concise factual Norwegian."""
    fa = _get_field_analysis(analysis, "Beskrivelse")
    # P1 FIX: STRONG and OK fields should never get suggestions.
    # The old code overrode OK status for short descriptions, generating
    # unnecessary suggestions for descriptions the analyzer deemed acceptable.
    if fa and fa.status in (QualityStatus.STRONG, QualityStatus.OK):
        return None

    current = product.description
    sku = product.article_number

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

    # Clean PDF text before using it — remove tables, metadata, variant lists, contact info
    if pdf_val:
        # First apply content_validator cleaning (contact info, other SKUs, PDF noise)
        pdf_val = clean_all_noise(pdf_val, sku)
        # Then apply description-specific cleaning (table headers, variant blocks)
        cleaned_pdf = clean_description_source(pdf_val)
        if cleaned_pdf:
            # Normalize for webshop
            cleaned_pdf = normalize_for_webshop_description(cleaned_pdf)
            logger.debug(
                f"[enrich] PDF description cleaned: {len(pdf_val)} → {len(cleaned_pdf)} chars"
            )
            pdf_val = cleaned_pdf
        else:
            logger.info(
                f"[enrich] PDF description rejected after cleaning "
                f"(original {len(pdf_val)} chars had no usable content)"
            )
            pdf_val = None

    # Quality-based comparison instead of length-based.
    if pdf_val and (not current or _description_quality_score(pdf_val) > _description_quality_score(current)):
        best_val = pdf_val
        best_source = SOURCE_PDF
        best_url = _get_enrichment_url(er_by_field, "description")
        best_evidence = pdf_evidence
        best_conf = pdf_conf

    # Clean manufacturer description too — apply full noise removal
    if mfr_desc:
        mfr_desc = clean_all_noise(mfr_desc, sku)
        cleaned_mfr = clean_description_source(mfr_desc)
        if cleaned_mfr:
            mfr_desc = normalize_for_webshop_description(cleaned_mfr)
        else:
            mfr_desc = None

    if mfr_desc and (not best_val or _description_quality_score(mfr_desc) > _description_quality_score(best_val)):
        mfr_conf = (mfr.confidence if mfr else 0) * 0.85
        # Golden source: manufacturer (tier 4) only beats PDF (tier 3)
        # if it has genuinely better quality, not just marginally.
        # This prevents weaker sources from overriding stronger ones.
        from backend.golden_source import TIER_PDF, TIER_MANUFACTURER
        pdf_is_higher_tier = best_source == SOURCE_PDF  # PDF is tier 3, manufacturer is tier 4
        quality_margin = 0.15 if pdf_is_higher_tier else 0.0
        mfr_quality = _description_quality_score(mfr_desc)
        best_quality = _description_quality_score(best_val) if best_val else 0

        if (mfr_conf > best_conf or not best_val) and mfr_quality > best_quality + quality_margin:
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
                    evidence="Generert fra produktnavn og tekniske spesifikasjoner. Bør gjennomgås manuelt.",
                    confidence=0.55,
                    review_required=True,
                )
        return None

    # Validate basic suggestion quality (incl. contact info, multi-SKU, etc.)
    is_valid, reject_reason = _validate_suggestion_value(best_val, "Beskrivelse", sku)
    if not is_valid:
        logger.info(
            f"[enrich] Description suggestion rejected: {reject_reason} "
            f"(value: {best_val[:80]})"
        )
        return None

    # Quality gate: validate the cleaned text is webshop-ready
    from backend.evidence import build_evidence
    from backend.content_validator import classify_text_as_description_candidate

    gate_ok, gate_reason = validate_webshop_description(best_val)
    if not gate_ok:
        logger.info(
            f"[enrich] Description failed webshop quality gate: {gate_reason} "
            f"(value: {best_val[:80]})"
        )
        gate_conf = min(best_conf, 0.45)
        ev_text, ev_dict = build_evidence(
            "Beskrivelse",
            source_label=best_source or "ukjent", source_url=best_url,
            confidence=gate_conf, auto_inriver=False,
            notes=[f"Kvalitetssjekk feilet: {gate_reason}", "Krever manuell vurdering"],
        )
        return EnrichmentSuggestion(
            field_name="Beskrivelse", current_value=current,
            suggested_value=best_val, source=best_source, source_url=best_url,
            evidence=ev_text, evidence_structured=ev_dict,
            confidence=gate_conf, review_required=True,
        )

    # Translate if needed
    review = best_conf < MIN_CONFIDENCE_AUTO
    best_val, lang, translate_msg = _translate_if_needed(best_val)
    needs_manual_translation = lang == "en"
    if needs_manual_translation:
        review = True
        best_conf = min(best_conf, 0.55)

    # Build noise list
    noise_list = []
    if best_val != (best_evidence or ""):
        noise_list.append("kontaktinfo/PDF-støy")

    # Classification score
    desc_score = classify_text_as_description_candidate(best_val)
    source_tier = 3 if best_source == SOURCE_PDF else (4 if best_source == SOURCE_MANUFACTURER else 5)

    ev_text, ev_dict = build_evidence(
        "Beskrivelse",
        source_label=best_source or "ukjent", source_url=best_url, source_tier=source_tier,
        lang=lang, translated=(lang in ("sv", "da")), translate_note=translate_msg,
        variant_matched=True, variant_note=f"SKU {sku}",
        noise_filtered=noise_list if noise_list else None,
        classification="beskrivelse", classification_score=desc_score,
        confidence=best_conf, auto_inriver=best_conf >= MIN_CONFIDENCE_AUTO and not needs_manual_translation,
    )

    return EnrichmentSuggestion(
        field_name="Beskrivelse", current_value=current,
        suggested_value=best_val, source=best_source, source_url=best_url,
        evidence=ev_text, evidence_structured=ev_dict,
        confidence=best_conf, review_required=review,
    )


def _enrich_specification(
    product: ProductData,
    analysis: ProductAnalysis,
    er_by_field: dict,
    mfr: Optional[ManufacturerLookup],
) -> Optional[EnrichmentSuggestion]:
    """Enrich specification by structuring source data into key-value pairs."""
    fa = _get_field_analysis(analysis, "Spesifikasjon")
    # P1 FIX: STRONG and OK fields should never get suggestions
    if fa and fa.status in (QualityStatus.STRONG, QualityStatus.OK):
        return None

    current_specs = product.technical_details or {}
    new_specs = dict(current_specs)  # Start with what we have
    sources_used = []
    sku = product.article_number

    # Source 1: PDF spec fields
    from backend.medical_safety import screen_suggestion as _medical_screen
    for field_name_key, er in er_by_field.items():
        if field_name_key.startswith("spec:") and er.suggested_value:
            key = field_name_key[5:]
            val = er.suggested_value.strip()
            # Clean the value — no contact info, no other SKUs
            val = clean_all_noise(val, sku)
            # Validate individual spec values
            ok, _ = validate_no_contact_info(val)
            if not ok:
                logger.debug(f"[enrich] Spec value rejected (contact info): {key}={val[:40]}")
                continue
            # Medical safety gate: block sensitive attributes with low confidence
            med_result = _medical_screen(f"spec:{key}", val, er.confidence, SOURCE_PDF)
            if med_result.blocked:
                logger.info(f"[enrich] Spec '{key}' blocked by medical safety: {med_result.reason[:80]}")
                continue
            if key not in new_specs and val:
                new_specs[key] = val
                sources_used.append(SOURCE_PDF)

    # Source 2: Manufacturer specs
    if mfr and mfr.found and mfr.specifications:
        for key, val in mfr.specifications.items():
            val = clean_all_noise(val, sku) if val else ""
            ok, _ = validate_no_contact_info(val)
            if not ok:
                continue
            mfr_conf = mfr.confidence * 0.7 if mfr.confidence else 0.5
            med_result = _medical_screen(f"spec:{key}", val, mfr_conf, SOURCE_MANUFACTURER)
            if med_result.blocked:
                logger.info(f"[enrich] Mfr spec '{key}' blocked by medical safety: {med_result.reason[:80]}")
                continue
            if key not in new_specs and val:
                new_specs[key] = val
                sources_used.append(SOURCE_MANUFACTURER)

    # Only suggest if we found new specs beyond what exists
    added_keys = set(new_specs.keys()) - set(current_specs.keys())
    if not added_keys:
        return None

    # Build structured spec text and normalize for webshop
    spec_text = "; ".join(f"{k}: {v}" for k, v in new_specs.items())
    spec_text = normalize_for_webshop_specification(spec_text)

    # Final validation
    is_valid, reject_reason = _validate_suggestion_value(spec_text, "Spesifikasjon", sku)
    if not is_valid:
        logger.info(f"[enrich] Specification rejected: {reject_reason}")
        return None

    from backend.evidence import build_evidence

    source = ", ".join(sorted(set(sources_used))) if sources_used else SOURCE_SPEC_STRUCTURING
    added_str = ", ".join(sorted(added_keys))
    conf = 0.70 if SOURCE_PDF in sources_used else 0.60

    spec_text, lang, translate_msg = _translate_if_needed(spec_text)
    needs_manual = lang == "en"
    if needs_manual:
        conf = min(conf, 0.55)

    source_tier = 3 if SOURCE_PDF in sources_used else (4 if SOURCE_MANUFACTURER in sources_used else 5)
    spec_url = _get_enrichment_url(er_by_field, next(
        (f for f in er_by_field if f.startswith("spec:")), ""
    ))

    ev_text, ev_dict = build_evidence(
        "Spesifikasjon",
        source_label=source, source_url=spec_url, source_tier=source_tier,
        lang=lang, translated=(lang in ("sv", "da")), translate_note=translate_msg,
        classification="spesifikasjon",
        confidence=conf, auto_inriver=conf >= MIN_CONFIDENCE_AUTO and not needs_manual,
        notes=[f"Nye attributter: {added_str}"],
    )

    return EnrichmentSuggestion(
        field_name="Spesifikasjon",
        current_value=product.specification,
        suggested_value=spec_text,
        source=source, source_url=spec_url,
        evidence=ev_text, evidence_structured=ev_dict,
        confidence=conf,
        review_required=conf < MIN_CONFIDENCE_AUTO or needs_manual,
    )


def _enrich_category(
    product: ProductData,
    analysis: ProductAnalysis,
    er_by_field: dict,
    mfr: Optional[ManufacturerLookup],
) -> Optional[EnrichmentSuggestion]:
    """Suggest category only if source evidence supports it."""
    fa = _get_field_analysis(analysis, "Kategori")
    if fa and fa.status in (QualityStatus.STRONG, QualityStatus.OK):
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
        evidence=(
            f"Kategori utledet fra produktnavn '{product.product_name or ''}' "
            f"og tekniske spesifikasjoner. Automatisk klassifisering — krever manuell verifisering."
        ),
        confidence=0.50,
        review_required=True,  # Category inference always needs review
    )


def _enrich_packaging(
    product: ProductData,
    analysis: ProductAnalysis,
    er_by_field: dict,
    mfr: Optional[ManufacturerLookup],
) -> Optional[EnrichmentSuggestion]:
    """Parse actual pack/carton/pallet/unit info from sources.

    Strict validation: only accepts values containing actual packaging data
    (quantities, unit types, pack sizes). Rejects storage instructions,
    usage descriptions, marketing text, and random PDF fragments.
    """
    fa = _get_field_analysis(analysis, "Pakningsinformasjon")
    if fa and fa.status in (QualityStatus.STRONG, QualityStatus.OK):
        return None

    current = product.packaging_info or product.packaging_unit

    sku = product.article_number

    # Source 1: PDF packaging
    pdf_val, pdf_evidence, pdf_conf = _get_enrichment_value(er_by_field, "packaging_info")
    if pdf_val and pdf_val != current:
        # Clean noise before validation
        pdf_val = clean_all_noise(pdf_val, sku)
        # Validate that this is actual packaging content
        is_valid, reject_reason = _validate_suggestion_value(pdf_val, "Pakningsinformasjon", sku)
        if not is_valid:
            logger.info(
                f"[enrich] Packaging suggestion rejected: {reject_reason} "
                f"(value: {pdf_val[:80]})"
            )
            return None
        from backend.evidence import build_evidence
        source_url = _get_enrichment_url(er_by_field, "packaging_info")
        ev_text, ev_dict = build_evidence(
            "Pakningsinformasjon",
            source_label="datablad (PDF)", source_url=source_url, source_tier=3,
            confidence=pdf_conf, auto_inriver=pdf_conf >= MIN_CONFIDENCE_AUTO,
            notes=["Validert som reell pakningsinformasjon"],
        )
        return EnrichmentSuggestion(
            field_name="Pakningsinformasjon", current_value=current,
            suggested_value=pdf_val, source=SOURCE_PDF, source_url=source_url,
            evidence=ev_text, evidence_structured=ev_dict,
            confidence=pdf_conf, review_required=pdf_conf < MIN_CONFIDENCE_AUTO,
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
    if fa and fa.status in (QualityStatus.STRONG, QualityStatus.OK):
        return None

    current = product.manufacturer
    sku = product.article_number

    # Source 1: PDF
    pdf_val, pdf_evidence, pdf_conf = _get_enrichment_value(er_by_field, "manufacturer")
    if pdf_val and pdf_val != current:
        # Clean — manufacturer name should not contain phone/email
        pdf_val = clean_all_noise(pdf_val, sku).strip()
        ok, reason = validate_no_contact_info(pdf_val)
        if not ok:
            logger.info(f"[enrich] Manufacturer name rejected (contact info): {pdf_val[:60]}")
        elif pdf_val:
            from backend.evidence import build_evidence
            source_url = _get_enrichment_url(er_by_field, "manufacturer")
            ev_text, ev_dict = build_evidence(
                "Produsent",
                source_label="datablad (PDF)", source_url=source_url, source_tier=3,
                confidence=pdf_conf, auto_inriver=pdf_conf >= MIN_CONFIDENCE_AUTO,
            )
            return EnrichmentSuggestion(
                field_name="Produsent", current_value=current,
                suggested_value=pdf_val, source=SOURCE_PDF, source_url=source_url,
                evidence=ev_text, evidence_structured=ev_dict,
                confidence=pdf_conf, review_required=pdf_conf < MIN_CONFIDENCE_AUTO,
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
    if fa and fa.status in (QualityStatus.STRONG, QualityStatus.OK):
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
    lang = detect_language(text)
    return lang == "en"


def _check_language(text: str) -> tuple[bool, str, str]:
    """Check if text is in Norwegian. Uses content_validator.

    Returns (is_norwegian, language_code, message).
    """
    from backend.content_validator import validate_language_is_norwegian
    return validate_language_is_norwegian(text)


def _translate_if_needed(text: str) -> tuple[str, str, str]:
    """Translate text to Norwegian if needed. Returns (text, lang, message).

    For Swedish/Danish: performs rule-based translation and returns translated text.
    For English: returns original text with flag (too different for rule-based).
    For Norwegian/unknown: returns original text unchanged.
    """
    translated, lang, was_translated = translate_to_norwegian_if_needed(text)
    if was_translated:
        lang_names = {"sv": "svensk", "da": "dansk"}
        lang_name = lang_names.get(lang, lang)
        return translated, lang, f"Automatisk oversatt fra {lang_name} til norsk"
    if lang == "en":
        return text, lang, "Teksten er på engelsk — manuell oversettelse til norsk påkrevet"
    return text, lang, ""


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

    # Join with newlines to preserve paragraph structure in assembled descriptions
    return "\n".join(parts)


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


# ── Final Quality Gate ──


def _normalize_for_comparison(text: str) -> str:
    """Normalize text for comparison purposes.

    P1 FIX: Strips formatting differences so that equivalent content
    with different whitespace, bullets, or line breaks is recognized as equal.
    """
    if not text:
        return ""
    # Normalize bullet markers to a common form
    result = re.sub(r"^\s*[•\-\*►▸‣⁃]\s*", "- ", text, flags=re.MULTILINE)
    # Collapse all whitespace (spaces, tabs, newlines) to single space
    result = re.sub(r"\s+", " ", result)
    # Remove zero-width chars
    result = result.replace("\u200b", "").replace("\ufeff", "").replace("\xa0", " ")
    # Lowercase
    result = result.lower().strip()
    return result


def _text_similarity(a: str, b: str) -> float:
    """Compute simple token-overlap similarity between two texts (0.0-1.0).

    Uses Jaccard similarity on lowercased word tokens.  Fast and sufficient
    for catching near-paraphrases at the quality-gate level.
    """
    if not a or not b:
        return 0.0
    tokens_a = set(a.lower().split())
    tokens_b = set(b.lower().split())
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union) if union else 0.0


def apply_ai_review_to_suggestions(
    suggestions: list[EnrichmentSuggestion],
    ai_reviews: list[dict],
) -> list[EnrichmentSuggestion]:
    """Apply strict AI quality-gate results to enrichment suggestions.

    The AI quality gate uses a verdict-based protocol:
    - "NO_MEANINGFUL_IMPROVEMENT": suggestion is a paraphrase, not an improvement → drop
    - "REJECTED_CONTENT_DEGRADATION": suggestion removes safety/compliance info → drop
    - "APPROVED": genuine improvement → apply reviewed_value
    - Legacy "rejected": true format is also supported for backwards compatibility

    For every suggestion, stores the original_suggested_value before any AI
    modification, creating a diff trail for medical safety review.

    Returns filtered list with only approved, genuinely improved suggestions.
    """
    if not ai_reviews:
        return suggestions

    # Index reviews by field_name
    reviews_by_field = {}
    for r in ai_reviews:
        fname = r.get("field_name")
        if fname:
            reviews_by_field[fname] = r

    result = []
    rejected_count = 0
    no_improvement_count = 0

    for suggestion in suggestions:
        review = reviews_by_field.get(suggestion.field_name)

        if not review:
            # No review for this field — keep as-is
            result.append(suggestion)
            continue

        verdict = review.get("verdict", "").upper()

        # Handle verdict-based rejection
        if verdict == "NO_MEANINGFUL_IMPROVEMENT":
            no_improvement_count += 1
            logger.info(
                f"[quality-gate] {suggestion.field_name} NO_MEANINGFUL_IMPROVEMENT — "
                f"suggestion is not a true improvement over current value"
            )
            continue

        if verdict == "REJECTED_CONTENT_DEGRADATION":
            rejected_count += 1
            reason = review.get("reject_reason", "Innholdstap: sikkerhet/samsvar/materiale")
            logger.info(
                f"[quality-gate] {suggestion.field_name} REJECTED_CONTENT_DEGRADATION: {reason}"
            )
            continue

        # Legacy compatibility: "rejected": true
        if review.get("rejected") and verdict != "APPROVED":
            rejected_count += 1
            reason = review.get("reject_reason", "Avvist av kvalitetskontroll")
            logger.info(
                f"[quality-gate] {suggestion.field_name} REJECTED: {reason}"
            )
            continue

        # Store original value for diff trail (P0-2: medical safety)
        suggestion.original_suggested_value = suggestion.suggested_value

        # Apply reviewed value
        reviewed_value = review.get("reviewed_value")
        if reviewed_value and reviewed_value.strip():
            new_val = reviewed_value.strip()

            # Mechanical similarity check: if AI "improved" to >80% identical text,
            # treat as no meaningful improvement and keep original instead
            if suggestion.suggested_value:
                similarity = _text_similarity(suggestion.suggested_value, new_val)
                if similarity > 0.80 and new_val != suggestion.suggested_value:
                    logger.info(
                        f"[quality-gate] {suggestion.field_name}: AI review is {similarity:.0%} "
                        f"similar to original — keeping source-grounded value"
                    )
                    # Keep original suggested_value, don't apply AI rewrite
                else:
                    suggestion.suggested_value = new_val
                    suggestion.ai_modified = True
            else:
                suggestion.suggested_value = new_val
                suggestion.ai_modified = True

        # Apply confidence adjustment
        conf_adj = review.get("confidence_adjustment", 0)
        if isinstance(conf_adj, (int, float)):
            suggestion.confidence = max(0.0, min(1.0, suggestion.confidence + conf_adj))

        # Apply review_required
        if review.get("review_required") is not None:
            suggestion.review_required = bool(review["review_required"])

        # Add rationale
        rationale = review.get("rationale")
        if rationale:
            suggestion = _add_rationale(suggestion, f"AI: {rationale}")

        result.append(suggestion)

    logger.info(
        f"[quality-gate] {len(suggestions)} suggestions → "
        f"{len(result)} after AI review "
        f"({rejected_count} rejected, "
        f"{no_improvement_count} no meaningful improvement)"
    )
    return result


def final_quality_gate(suggestions: list[EnrichmentSuggestion]) -> list[EnrichmentSuggestion]:
    """Rule-based final quality gate — catches issues AI might miss.

    Runs AFTER AI review. Removes any remaining suggestions that:
    - Have empty/whitespace-only values
    - Are too short to be useful for their field
    - Contain obvious extraction artifacts
    - Have very low confidence
    - Are near-paraphrases of current value (>80% token overlap)
    - Contain medically sensitive attributes below required confidence
    """
    from backend.medical_safety import screen_suggestion as _medical_screen

    result = []
    for s in suggestions:
        if not s.suggested_value or not s.suggested_value.strip():
            continue

        val = s.suggested_value.strip()

        # Medical safety gate: block sensitive content with insufficient confidence
        med_result = _medical_screen(
            s.field_name, val, s.confidence, s.source or ""
        )
        if med_result.blocked:
            logger.info(
                f"[quality-gate] {s.field_name} BLOCKED by medical safety: "
                f"{med_result.reason[:100]}"
            )
            continue

        # Reject suggestions with very low confidence
        if s.confidence < MIN_CONFIDENCE_SUGGEST:
            logger.debug(
                f"[quality-gate] {s.field_name} dropped: confidence {s.confidence:.2f} "
                f"< {MIN_CONFIDENCE_SUGGEST}"
            )
            continue

        # P1 FIX: Reject if value is identical to current after normalization.
        # This catches cases where content is the same but formatting differs
        # (different whitespace, bullet styles, line breaks, etc.)
        if s.current_value:
            norm_current = _normalize_for_comparison(s.current_value)
            norm_suggested = _normalize_for_comparison(val)
            if norm_current and norm_suggested and norm_current == norm_suggested:
                logger.info(
                    f"[quality-gate] {s.field_name} dropped: normalized content "
                    f"is identical to current value"
                )
                continue

        # Reject near-paraphrases: if suggested is >80% similar to current,
        # it's not a meaningful improvement (addresses paraphrase padding)
        if s.current_value and len(s.current_value) > 20:
            similarity = _text_similarity(s.current_value, val)
            if similarity > 0.80:
                logger.info(
                    f"[quality-gate] {s.field_name} dropped: {similarity:.0%} similar "
                    f"to current value — not a meaningful improvement"
                )
                continue

        # Containment check: if suggested text contains most of current text's
        # tokens, the "improvement" is just existing text + noise (variant data,
        # metadata, etc.) — not a real improvement.
        if s.current_value and len(s.current_value) > 30:
            current_tokens = set(s.current_value.lower().split())
            suggested_tokens = set(val.lower().split())
            if current_tokens:
                containment = len(current_tokens & suggested_tokens) / len(current_tokens)
                if containment > 0.85:
                    new_tokens = suggested_tokens - current_tokens
                    new_ratio = len(new_tokens) / len(suggested_tokens) if suggested_tokens else 0
                    if new_ratio < 0.20:
                        logger.info(
                            f"[quality-gate] {s.field_name} dropped: suggestion contains "
                            f"{containment:.0%} of existing content with only "
                            f"{len(new_tokens)} new tokens ({new_ratio:.0%}) — "
                            f"not a meaningful addition"
                        )
                        continue

        # Run field validation one more time
        is_valid, reason = _validate_suggestion_value(val, s.field_name)
        if not is_valid:
            logger.info(
                f"[quality-gate] {s.field_name} dropped at final gate: {reason}"
            )
            continue

        result.append(s)

    return result
