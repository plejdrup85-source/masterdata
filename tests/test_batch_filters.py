"""Tests for batch filtering."""

import pytest

from backend.models import (
    EnrichmentSuggestion,
    FieldAnalysis,
    JeevesData,
    ProductAnalysis,
    ProductData,
    QualityStatus,
)
from backend.batch_filters import (
    apply_filters,
    get_filter_counts,
)


def _make(
    artno="12345",
    webshop="Klar",
    priority="Lav",
    priority_score=20,
    manufacturer="Ansell",
    mfr_artno="REF-1",
    img_status="PASS",
    img_count=2,
    field_statuses=None,
    suggestions=None,
    auto_fix=False,
    mfr_contact=False,
    manual_review=False,
    jeeves_supplier=None,
) -> ProductAnalysis:
    pd = ProductData(
        article_number=artno,
        product_name="Test produkt",
        manufacturer=manufacturer,
        manufacturer_article_number=mfr_artno,
        found_on_onemed=True,
    )
    fas = []
    if field_statuses:
        for fname, status in field_statuses.items():
            fas.append(FieldAnalysis(field_name=fname, status=status))
    else:
        fas = [FieldAnalysis(field_name="Produktnavn", status=QualityStatus.OK)]

    jeeves = JeevesData(article_number=artno, supplier=jeeves_supplier) if jeeves_supplier else None

    return ProductAnalysis(
        article_number=artno,
        product_data=pd,
        jeeves_data=jeeves,
        field_analyses=fas,
        image_quality={"image_quality_status": img_status, "image_count_found": img_count},
        webshop_status=webshop,
        priority_label=priority,
        priority_score=priority_score,
        auto_fix_possible=auto_fix,
        requires_manufacturer_contact=mfr_contact,
        manual_review_needed=manual_review,
        enrichment_suggestions=suggestions or [],
    )


class TestFilterWebshopStatus:
    def test_filter_ikke_klar(self):
        results = [_make(webshop="Klar"), _make(webshop="Ikke klar"), _make(webshop="Delvis klar")]
        filtered = apply_filters(results, {"webshop_status": "Ikke klar"})
        assert len(filtered) == 1
        assert filtered[0].webshop_status == "Ikke klar"

    def test_filter_klar(self):
        results = [_make(webshop="Klar"), _make(webshop="Ikke klar")]
        filtered = apply_filters(results, {"webshop_status": "Klar"})
        assert len(filtered) == 1


class TestFilterPriority:
    def test_filter_hoy(self):
        results = [_make(priority="Høy"), _make(priority="Lav"), _make(priority="Middels")]
        filtered = apply_filters(results, {"priority": "Høy"})
        assert len(filtered) == 1

    def test_filter_min_score(self):
        results = [_make(priority_score=80), _make(priority_score=20), _make(priority_score=50)]
        filtered = apply_filters(results, {"min_priority_score": "50"})
        assert len(filtered) == 2


class TestFilterManufacturer:
    def test_exact_match(self):
        results = [_make(manufacturer="Ansell"), _make(manufacturer="Mölnlycke")]
        filtered = apply_filters(results, {"manufacturer": "Ansell"})
        assert len(filtered) == 1

    def test_partial_match(self):
        results = [_make(manufacturer="Ansell Healthcare")]
        filtered = apply_filters(results, {"manufacturer": "ansell"})
        assert len(filtered) == 1

    def test_jeeves_supplier_fallback(self):
        results = [_make(manufacturer="", jeeves_supplier="Ansell")]
        filtered = apply_filters(results, {"manufacturer": "Ansell"})
        assert len(filtered) == 1


class TestFilterFieldProblem:
    def test_filter_missing_field(self):
        results = [
            _make(field_statuses={"Beskrivelse": QualityStatus.MISSING}),
            _make(field_statuses={"Beskrivelse": QualityStatus.OK}),
        ]
        filtered = apply_filters(results, {"field_problem": "Beskrivelse"})
        assert len(filtered) == 1

    def test_filter_specific_status(self):
        results = [
            _make(field_statuses={"Beskrivelse": QualityStatus.MISSING}),
            _make(field_statuses={"Beskrivelse": QualityStatus.SHOULD_IMPROVE}),
        ]
        filtered = apply_filters(results, {"field_problem": "Beskrivelse:Mangler"})
        assert len(filtered) == 1


class TestFilterImageProblem:
    def test_missing_images(self):
        results = [_make(img_status="MISSING", img_count=0), _make(img_status="PASS")]
        filtered = apply_filters(results, {"image_problem": ""})
        assert len(filtered) == 1

    def test_failed_images(self):
        results = [_make(img_status="FAIL"), _make(img_status="PASS")]
        filtered = apply_filters(results, {"image_problem": ""})
        assert len(filtered) == 1


class TestFilterMissingMfrArtno:
    def test_missing_artno_with_manufacturer(self):
        results = [
            _make(manufacturer="Ansell", mfr_artno=""),
            _make(manufacturer="Ansell", mfr_artno="REF-1"),
            _make(manufacturer="", mfr_artno=""),  # No manufacturer = not flagged
        ]
        filtered = apply_filters(results, {"missing_mfr_artno": ""})
        assert len(filtered) == 1


class TestFilterHighConfidence:
    def test_has_high_conf(self):
        hi = EnrichmentSuggestion(
            field_name="Beskrivelse", suggested_value="Ny", confidence=0.9
        )
        lo = EnrichmentSuggestion(
            field_name="Beskrivelse", suggested_value="Ny", confidence=0.3
        )
        results = [_make(suggestions=[hi]), _make(suggestions=[lo]), _make()]
        filtered = apply_filters(results, {"high_confidence": ""})
        assert len(filtered) == 1


class TestFilterBooleans:
    def test_auto_fixable(self):
        results = [_make(auto_fix=True), _make(auto_fix=False)]
        filtered = apply_filters(results, {"auto_fixable": ""})
        assert len(filtered) == 1

    def test_needs_manufacturer(self):
        results = [_make(mfr_contact=True), _make(mfr_contact=False)]
        filtered = apply_filters(results, {"needs_manufacturer": ""})
        assert len(filtered) == 1

    def test_manual_review(self):
        results = [_make(manual_review=True), _make(manual_review=False)]
        filtered = apply_filters(results, {"manual_review": ""})
        assert len(filtered) == 1


class TestCombinedFilters:
    def test_and_logic(self):
        """Multiple filters use AND logic."""
        results = [
            _make(artno="1", webshop="Ikke klar", priority="Høy"),
            _make(artno="2", webshop="Ikke klar", priority="Lav"),
            _make(artno="3", webshop="Klar", priority="Høy"),
        ]
        filtered = apply_filters(results, {
            "webshop_status": "Ikke klar",
            "priority": "Høy",
        })
        assert len(filtered) == 1
        assert filtered[0].article_number == "1"

    def test_no_filters_returns_all(self):
        results = [_make(), _make(), _make()]
        filtered = apply_filters(results, {})
        assert len(filtered) == 3

    def test_unknown_filter_ignored(self):
        results = [_make()]
        filtered = apply_filters(results, {"nonexistent": "value"})
        assert len(filtered) == 1


class TestGetFilterCounts:
    def test_webshop_counts(self):
        results = [_make(webshop="Klar"), _make(webshop="Klar"), _make(webshop="Ikke klar")]
        counts = get_filter_counts(results)
        assert counts["webshop_status"]["Klar"] == 2
        assert counts["webshop_status"]["Ikke klar"] == 1

    def test_priority_counts(self):
        results = [_make(priority="Høy"), _make(priority="Lav")]
        counts = get_filter_counts(results)
        assert counts["priority"]["Høy"] == 1
        assert counts["priority"]["Lav"] == 1

    def test_boolean_counts(self):
        results = [
            _make(img_status="MISSING", img_count=0),
            _make(img_status="PASS"),
        ]
        counts = get_filter_counts(results)
        assert counts["image_problem"] == 1

    def test_manufacturer_counts(self):
        results = [_make(manufacturer="Ansell"), _make(manufacturer="Ansell"), _make(manufacturer="3M")]
        counts = get_filter_counts(results)
        assert counts["manufacturers"]["Ansell"] == 2
        assert counts["manufacturers"]["3M"] == 1

    def test_field_problem_counts(self):
        results = [
            _make(field_statuses={"Beskrivelse": QualityStatus.MISSING}),
            _make(field_statuses={"Beskrivelse": QualityStatus.OK}),
        ]
        counts = get_filter_counts(results)
        assert counts["field_problems"]["Beskrivelse"] == 1
