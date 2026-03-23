"""Tests for targeted re-check logic."""

import pytest

from backend.models import (
    EnrichmentSuggestion,
    FieldAnalysis,
    JeevesData,
    ProductAnalysis,
    ProductData,
    QualityStatus,
)
from backend.recheck import (
    get_recheck_candidates,
    filter_products_for_recheck,
    get_recheck_summary,
    RECHECK_PRESETS,
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

    return ProductAnalysis(
        article_number=artno,
        product_data=pd,
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


class TestGetRecheckCandidates:
    def test_preset_not_webshop_ready(self):
        results = [
            _make(artno="A1", webshop="Klar"),
            _make(artno="A2", webshop="Ikke klar"),
            _make(artno="A3", webshop="Delvis klar"),
        ]
        candidates = get_recheck_candidates(results, preset="not_webshop_ready")
        assert candidates == ["A2"]

    def test_preset_high_priority(self):
        results = [
            _make(artno="A1", priority="Høy"),
            _make(artno="A2", priority="Lav"),
        ]
        candidates = get_recheck_candidates(results, preset="high_priority")
        assert candidates == ["A1"]

    def test_preset_image_problems(self):
        results = [
            _make(artno="A1", img_status="MISSING", img_count=0),
            _make(artno="A2", img_status="PASS"),
        ]
        candidates = get_recheck_candidates(results, preset="image_problems")
        assert candidates == ["A1"]

    def test_preset_manual_review(self):
        results = [
            _make(artno="A1", manual_review=True),
            _make(artno="A2", manual_review=False),
        ]
        candidates = get_recheck_candidates(results, preset="manual_review")
        assert candidates == ["A1"]

    def test_custom_filters(self):
        results = [
            _make(artno="A1", webshop="Ikke klar", priority="Høy"),
            _make(artno="A2", webshop="Ikke klar", priority="Lav"),
            _make(artno="A3", webshop="Klar"),
        ]
        candidates = get_recheck_candidates(results, filters={
            "webshop_status": "Ikke klar",
            "priority": "Høy",
        })
        assert candidates == ["A1"]

    def test_no_filters_returns_empty(self):
        results = [_make(artno="A1")]
        candidates = get_recheck_candidates(results)
        assert candidates == []

    def test_unknown_preset_returns_empty(self):
        results = [_make(artno="A1")]
        candidates = get_recheck_candidates(results, preset="nonexistent")
        assert candidates == []

    def test_max_products_limit(self):
        results = [_make(artno=f"A{i}", webshop="Ikke klar") for i in range(100)]
        candidates = get_recheck_candidates(results, preset="not_webshop_ready", max_products=10)
        assert len(candidates) == 10

    def test_preset_with_additional_filters(self):
        results = [
            _make(artno="A1", webshop="Ikke klar", manufacturer="Ansell"),
            _make(artno="A2", webshop="Ikke klar", manufacturer="3M"),
        ]
        candidates = get_recheck_candidates(
            results,
            preset="not_webshop_ready",
            filters={"manufacturer": "Ansell"},
        )
        assert candidates == ["A1"]


class TestFilterProductsForRecheck:
    def test_by_webshop_status(self):
        results = [
            _make(artno="A1", webshop="Ikke klar"),
            _make(artno="A2", webshop="Klar"),
        ]
        candidates = filter_products_for_recheck(results, webshop_status="Ikke klar")
        assert candidates == ["A1"]

    def test_by_manufacturer(self):
        results = [
            _make(artno="A1", manufacturer="Ansell"),
            _make(artno="A2", manufacturer="3M"),
        ]
        candidates = filter_products_for_recheck(results, manufacturer="Ansell")
        assert candidates == ["A1"]

    def test_by_field_problem(self):
        results = [
            _make(artno="A1", field_statuses={"Beskrivelse": QualityStatus.MISSING}),
            _make(artno="A2", field_statuses={"Beskrivelse": QualityStatus.OK}),
        ]
        candidates = filter_products_for_recheck(results, field_name="Beskrivelse")
        assert candidates == ["A1"]

    def test_image_problems_only(self):
        results = [
            _make(artno="A1", img_status="MISSING", img_count=0),
            _make(artno="A2"),
        ]
        candidates = filter_products_for_recheck(results, only_image_problems=True)
        assert candidates == ["A1"]

    def test_missing_mfr_artno(self):
        results = [
            _make(artno="A1", manufacturer="Ansell", mfr_artno=""),
            _make(artno="A2", manufacturer="Ansell", mfr_artno="REF-1"),
        ]
        candidates = filter_products_for_recheck(results, only_missing_mfr_artno=True)
        assert candidates == ["A1"]

    def test_combined_criteria(self):
        results = [
            _make(artno="A1", webshop="Ikke klar", manufacturer="Ansell"),
            _make(artno="A2", webshop="Ikke klar", manufacturer="3M"),
            _make(artno="A3", webshop="Klar", manufacturer="Ansell"),
        ]
        candidates = filter_products_for_recheck(
            results,
            webshop_status="Ikke klar",
            manufacturer="Ansell",
        )
        assert candidates == ["A1"]


class TestRecheckSummary:
    def test_returns_all_presets(self):
        results = [
            _make(artno="A1", webshop="Ikke klar", priority="Høy"),
            _make(artno="A2", webshop="Klar"),
        ]
        summary = get_recheck_summary(results)
        assert "not_webshop_ready" in summary
        assert "high_priority" in summary
        for name, info in summary.items():
            assert "label" in info
            assert "description" in info
            assert "count" in info
            assert isinstance(info["count"], int)

    def test_counts_match_filters(self):
        results = [
            _make(artno="A1", webshop="Ikke klar"),
            _make(artno="A2", webshop="Ikke klar"),
            _make(artno="A3", webshop="Klar"),
        ]
        summary = get_recheck_summary(results)
        assert summary["not_webshop_ready"]["count"] == 2


class TestPresetsExist:
    def test_all_presets_have_required_fields(self):
        for name, preset in RECHECK_PRESETS.items():
            assert "label" in preset, f"Preset {name} mangler label"
            assert "description" in preset, f"Preset {name} mangler description"
            assert "filters" in preset, f"Preset {name} mangler filters"
            assert isinstance(preset["filters"], dict)
