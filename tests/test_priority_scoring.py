"""Tests for priority scoring."""

import pytest

from backend.models import (
    EnrichmentSuggestion,
    FieldAnalysis,
    JeevesData,
    ProductAnalysis,
    ProductData,
    QualityStatus,
)
from backend.priority_scoring import (
    PriorityResult,
    calculate_priority_score,
    get_priority_reasons,
    sort_products_by_priority,
)


def _make_analysis(
    name="Nitrilhanske Steril Str M Blå",
    description="Steril nitrilhanske for medisinsk bruk. Pudderfri og lateksfri.",
    category="Hansker > Engangshansker",
    manufacturer="Ansell",
    total_score=80.0,
    image_quality=None,
    webshop_status="Klar",
    auto_fix=False,
    mfr_contact=False,
    field_statuses=None,
    suggestions=None,
) -> ProductAnalysis:
    pd = ProductData(
        article_number="12345",
        product_name=name,
        description=description,
        category=category,
        manufacturer=manufacturer,
        found_on_onemed=True,
    )
    iq = image_quality or {"image_count_found": 2, "image_quality_status": "PASS"}
    fas = []
    if field_statuses:
        for fname, status in field_statuses.items():
            fas.append(FieldAnalysis(field_name=fname, status=status))
    else:
        fas = [
            FieldAnalysis(field_name="Produktnavn", status=QualityStatus.OK),
            FieldAnalysis(field_name="Beskrivelse", status=QualityStatus.OK),
        ]

    return ProductAnalysis(
        article_number="12345",
        product_data=pd,
        image_quality=iq,
        field_analyses=fas,
        total_score=total_score,
        webshop_status=webshop_status,
        auto_fix_possible=auto_fix,
        requires_manufacturer_contact=mfr_contact,
        enrichment_suggestions=suggestions or [],
    )


class TestBasicScoring:
    def test_good_product_gets_low_priority(self):
        a = _make_analysis(webshop_status="Klar", total_score=85.0)
        result = calculate_priority_score(a)
        assert result.score <= 30
        assert result.label == "Lav"

    def test_bad_product_gets_high_priority(self):
        a = _make_analysis(
            webshop_status="Ikke klar",
            total_score=20.0,
            image_quality={"image_count_found": 0, "image_quality_status": "MISSING"},
            field_statuses={
                "Produktnavn": QualityStatus.MISSING,
                "Beskrivelse": QualityStatus.MISSING,
                "Kategori": QualityStatus.MISSING,
            },
        )
        result = calculate_priority_score(a)
        assert result.score >= 60
        assert result.label == "Høy"


class TestImpactSignals:
    def test_webshop_not_ready_adds_30(self):
        a = _make_analysis(webshop_status="Ikke klar")
        result = calculate_priority_score(a)
        assert result.impact_score >= 30

    def test_webshop_partial_adds_15(self):
        a = _make_analysis(webshop_status="Delvis klar")
        result = calculate_priority_score(a)
        assert result.impact_score >= 15

    def test_missing_images_add_points(self):
        a = _make_analysis(
            image_quality={"image_count_found": 0, "image_quality_status": "MISSING"},
        )
        result = calculate_priority_score(a)
        assert result.impact_score >= 10

    def test_low_quality_score_adds_points(self):
        a = _make_analysis(total_score=25.0)
        result = calculate_priority_score(a)
        assert any("kvalitetsscore" in r.lower() for r in result.reasons)

    def test_field_problems_add_points(self):
        a = _make_analysis(field_statuses={
            "Produktnavn": QualityStatus.MISSING,
            "Beskrivelse": QualityStatus.PROBABLE_ERROR,
        })
        result = calculate_priority_score(a)
        assert any("felt med problemer" in r for r in result.reasons)


class TestEffortSignals:
    def test_auto_fix_adds_points(self):
        a = _make_analysis(auto_fix=True)
        result = calculate_priority_score(a)
        assert any("Auto-fix" in r for r in result.reasons)

    def test_manufacturer_contact_subtracts_points(self):
        a = _make_analysis(mfr_contact=True, webshop_status="Ikke klar")
        result_with = calculate_priority_score(a)

        a2 = _make_analysis(mfr_contact=False, webshop_status="Ikke klar")
        result_without = calculate_priority_score(a2)

        assert result_with.score < result_without.score


class TestReasons:
    def test_reasons_are_norwegian(self):
        a = _make_analysis(webshop_status="Ikke klar", total_score=20.0)
        result = calculate_priority_score(a)
        assert len(result.reasons) > 0
        # Should contain Norwegian text
        assert any("klar" in r.lower() or "mangler" in r.lower() for r in result.reasons)

    def test_get_priority_reasons(self):
        a = _make_analysis(webshop_status="Ikke klar")
        reasons = get_priority_reasons(a)
        assert isinstance(reasons, list)
        assert len(reasons) > 0


class TestSorting:
    def test_sort_highest_first(self):
        good = _make_analysis(webshop_status="Klar", total_score=90.0)
        good.article_number = "good"
        bad = _make_analysis(
            webshop_status="Ikke klar",
            total_score=20.0,
            image_quality={"image_count_found": 0, "image_quality_status": "MISSING"},
            field_statuses={
                "Produktnavn": QualityStatus.MISSING,
                "Beskrivelse": QualityStatus.MISSING,
            },
        )
        bad.article_number = "bad"

        sorted_list = sort_products_by_priority([good, bad])
        assert sorted_list[0][0].article_number == "bad"
        assert sorted_list[0][1].score > sorted_list[1][1].score

    def test_sort_returns_tuples(self):
        a = _make_analysis()
        sorted_list = sort_products_by_priority([a])
        assert len(sorted_list) == 1
        assert isinstance(sorted_list[0][1], PriorityResult)


class TestSummary:
    def test_summary_contains_score_and_label(self):
        a = _make_analysis(webshop_status="Ikke klar")
        result = calculate_priority_score(a)
        assert "Prioritet" in result.summary
        assert result.label in result.summary

    def test_summary_is_not_empty_for_good_product(self):
        a = _make_analysis()
        result = calculate_priority_score(a)
        assert len(result.summary) > 0


class TestEdgeCases:
    def test_empty_field_analyses(self):
        a = _make_analysis()
        a.field_analyses = []
        result = calculate_priority_score(a)
        assert isinstance(result.score, int)

    def test_no_enrichment_suggestions(self):
        a = _make_analysis()
        a.enrichment_suggestions = []
        result = calculate_priority_score(a)
        assert isinstance(result.score, int)

    def test_score_clamped_to_100(self):
        """Even with everything bad, score shouldn't exceed 100."""
        a = _make_analysis(
            webshop_status="Ikke klar",
            total_score=5.0,
            auto_fix=True,
            image_quality={"image_count_found": 0, "image_quality_status": "MISSING"},
            field_statuses={
                "Produktnavn": QualityStatus.MISSING,
                "Beskrivelse": QualityStatus.MISSING,
                "Kategori": QualityStatus.MISSING,
                "Produsent": QualityStatus.MISSING,
                "Spesifikasjon": QualityStatus.MISSING,
            },
        )
        result = calculate_priority_score(a)
        assert 0 <= result.score <= 100

    def test_score_clamped_to_0(self):
        """Score should never go negative even with manufacturer penalty."""
        a = _make_analysis(
            webshop_status="Klar",
            total_score=95.0,
            mfr_contact=True,
        )
        result = calculate_priority_score(a)
        assert result.score >= 0
