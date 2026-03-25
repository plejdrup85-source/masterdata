"""Tests for human-readable product explanations."""

import pytest

from backend.human_explainer import (
    HumanExplanation,
    build_human_readable_summary,
    explain_product_like_a_human,
    summarize_main_issues,
    summarize_recommended_actions,
    _join_list,
    _trust_label,
    _truncate,
)
from backend.models import (
    ApprovalStatus,
    EnrichmentSuggestion,
    FieldAnalysis,
    ProductAnalysis,
    ProductData,
    QualityStatus,
)


def _make_result(
    artno="12345",
    name="Nitrilhanske pudderfri",
    found=True,
    field_statuses=None,
    suggestions=None,
    webshop_status=None,
    priority_label=None,
    category_status=None,
    category_suggestion=None,
    image_quality=None,
    requires_manufacturer_contact=False,
):
    pd = ProductData(
        article_number=artno,
        product_name=name,
        found_on_onemed=found,
    )
    field_analyses = []
    if field_statuses:
        for fname, status in field_statuses.items():
            field_analyses.append(FieldAnalysis(
                field_name=fname,
                current_value="test verdi" if status != QualityStatus.MISSING else None,
                status=status,
            ))

    return ProductAnalysis(
        article_number=artno,
        product_data=pd,
        field_analyses=field_analyses,
        enrichment_suggestions=suggestions or [],
        webshop_status=webshop_status,
        priority_label=priority_label,
        category_status=category_status,
        category_suggestion=category_suggestion,
        image_quality=image_quality,
        requires_manufacturer_contact=requires_manufacturer_contact,
    )


class TestExplainProduct:
    def test_good_product(self):
        result = _make_result(
            field_statuses={
                "Produktnavn": QualityStatus.STRONG,
                "Beskrivelse": QualityStatus.OK,
                "Produsent": QualityStatus.OK,
            },
            webshop_status="Klar",
            category_status="OK",
            image_quality={"image_count_found": 3, "image_quality_status": "God"},
        )
        explanation = explain_product_like_a_human(result)

        assert explanation.article_number == "12345"
        assert len(explanation.whats_good) >= 2
        assert len(explanation.whats_wrong) == 0
        assert "bra ut" in explanation.overall_verdict.lower() or "klart" in explanation.overall_verdict.lower()

    def test_product_with_missing_fields(self):
        result = _make_result(
            field_statuses={
                "Produktnavn": QualityStatus.OK,
                "Beskrivelse": QualityStatus.MISSING,
                "Spesifikasjon": QualityStatus.MISSING,
            },
        )
        explanation = explain_product_like_a_human(result)

        assert len(explanation.whats_wrong) >= 1
        assert any("mangler" in issue.lower() for issue in explanation.whats_wrong)

    def test_product_with_suggestions(self):
        suggestions = [
            EnrichmentSuggestion(
                field_name="Beskrivelse",
                current_value="Kort tekst",
                suggested_value="En mye bedre og lengre beskrivelse av produktet",
                confidence=0.85,
                source="PDF",
            ),
        ]
        result = _make_result(
            field_statuses={"Beskrivelse": QualityStatus.WEAK},
            suggestions=suggestions,
        )
        explanation = explain_product_like_a_human(result)

        assert len(explanation.suggestions) >= 1
        assert any("beskrivelse" in s.lower() for s in explanation.suggestions)
        assert any("høy tillit" in s.lower() for s in explanation.suggestions)

    def test_product_needing_manufacturer(self):
        result = _make_result(
            field_statuses={
                "Produsent": QualityStatus.REQUIRES_MANUFACTURER,
            },
            requires_manufacturer_contact=True,
        )
        explanation = explain_product_like_a_human(result)

        assert any("produsent" in r.lower() for r in explanation.needs_manual_review)
        assert any("kontakt" in a.lower() for a in explanation.next_steps)

    def test_high_priority_product(self):
        result = _make_result(
            field_statuses={
                "Beskrivelse": QualityStatus.MISSING,
                "Produsent": QualityStatus.MISSING,
            },
            priority_label="Høy",
        )
        explanation = explain_product_like_a_human(result)

        assert "snart" in explanation.overall_verdict.lower() or "problemer" in explanation.overall_verdict.lower()

    def test_category_issues(self):
        result = _make_result(
            category_status="ATTRIBUTE_AS_CATEGORY",
            category_suggestion="Medisinsk > Hansker",
        )
        explanation = explain_product_like_a_human(result)

        assert any("filter" in issue.lower() for issue in explanation.whats_wrong)
        assert any("kategori" in s.lower() for s in explanation.suggestions)

    def test_not_on_website(self):
        result = _make_result(found=False)
        explanation = explain_product_like_a_human(result)

        assert any("nettbutikk" in issue.lower() for issue in explanation.whats_wrong)

    def test_source_conflict(self):
        result = _make_result(
            field_statuses={"Produktnavn": QualityStatus.SOURCE_CONFLICT},
        )
        explanation = explain_product_like_a_human(result)

        assert any("uenige" in issue.lower() for issue in explanation.whats_wrong)

    def test_no_images(self):
        result = _make_result(
            image_quality={"image_count_found": 0, "image_quality_status": "Mangler"},
        )
        explanation = explain_product_like_a_human(result)

        assert any("bilde" in issue.lower() for issue in explanation.whats_wrong)


class TestBuildSummary:
    def test_summary_is_concise(self):
        result = _make_result(
            field_statuses={
                "Beskrivelse": QualityStatus.MISSING,
                "Spesifikasjon": QualityStatus.WEAK,
            },
            suggestions=[
                EnrichmentSuggestion(
                    field_name="Beskrivelse",
                    suggested_value="Ny beskrivelse",
                    confidence=0.8,
                ),
            ],
        )
        summary = build_human_readable_summary(result)

        assert isinstance(summary, str)
        assert len(summary) > 10
        assert len(summary) < 500  # Should be concise

    def test_summary_for_perfect_product(self):
        result = _make_result(
            field_statuses={"Produktnavn": QualityStatus.STRONG},
            webshop_status="Klar",
        )
        summary = build_human_readable_summary(result)
        assert "bra" in summary.lower() or "klart" in summary.lower()


class TestRecommendedActions:
    def test_auto_approved_action(self):
        suggestions = [
            EnrichmentSuggestion(
                field_name="Beskrivelse",
                suggested_value="Ny tekst",
                confidence=0.9,
                approval_status=ApprovalStatus.AUTO_APPROVED,
            ),
        ]
        result = _make_result(suggestions=suggestions)
        actions = summarize_recommended_actions(result)

        assert any("automatisk" in a.lower() for a in actions)

    def test_pending_review_action(self):
        suggestions = [
            EnrichmentSuggestion(
                field_name="Beskrivelse",
                suggested_value="Ny tekst",
                confidence=0.6,
                review_required=True,
                approval_status=ApprovalStatus.NEEDS_REVIEW,
            ),
        ]
        result = _make_result(suggestions=suggestions)
        actions = summarize_recommended_actions(result)

        assert any("vurder" in a.lower() for a in actions)

    def test_no_action_needed(self):
        result = _make_result(
            webshop_status="Klar",
            field_statuses={"Produktnavn": QualityStatus.STRONG},
        )
        actions = summarize_recommended_actions(result)

        assert any("ingen tiltak" in a.lower() or "klart" in a.lower() for a in actions)

    def test_missing_critical_fields(self):
        result = _make_result(
            field_statuses={
                "Produktnavn": QualityStatus.MISSING,
                "Beskrivelse": QualityStatus.MISSING,
            },
        )
        actions = summarize_recommended_actions(result)

        assert any("kritisk" in a.lower() or "fyll inn" in a.lower() for a in actions)


class TestTextHelpers:
    def test_join_list_single(self):
        assert _join_list(["eple"]) == "eple"

    def test_join_list_two(self):
        assert _join_list(["eple", "pære"]) == "eple og pære"

    def test_join_list_three(self):
        assert _join_list(["eple", "pære", "banan"]) == "eple, pære og banan"

    def test_join_list_empty(self):
        assert _join_list([]) == ""

    def test_trust_label_high(self):
        assert "høy" in _trust_label(0.90).lower()

    def test_trust_label_medium(self):
        assert "middels" in _trust_label(0.75).lower()

    def test_trust_label_low(self):
        assert "lav" in _trust_label(0.55).lower()

    def test_trust_label_very_low(self):
        assert "veldig" in _trust_label(0.30).lower()

    def test_truncate_short(self):
        assert _truncate("kort", 80) == "kort"

    def test_truncate_long(self):
        result = _truncate("a" * 100, 50)
        assert len(result) == 50
        assert result.endswith("…")

    def test_truncate_empty(self):
        assert _truncate("") == ""


class TestConfidenceNote:
    def test_with_high_confidence_suggestions(self):
        suggestions = [
            EnrichmentSuggestion(
                field_name="Beskrivelse",
                suggested_value="God beskrivelse",
                confidence=0.90,
            ),
        ]
        result = _make_result(suggestions=suggestions)
        explanation = explain_product_like_a_human(result)
        assert "høy tillit" in explanation.confidence_note.lower()

    def test_with_low_confidence_suggestions(self):
        suggestions = [
            EnrichmentSuggestion(
                field_name="Beskrivelse",
                suggested_value="Usikker beskrivelse",
                confidence=0.40,
            ),
        ]
        result = _make_result(suggestions=suggestions)
        explanation = explain_product_like_a_human(result)
        assert "veldig lav" in explanation.confidence_note.lower()

    def test_no_suggestions(self):
        result = _make_result()
        explanation = explain_product_like_a_human(result)
        assert explanation.confidence_note == ""
