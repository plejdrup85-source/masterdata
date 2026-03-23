"""Tests for the approval workflow."""

import pytest

from backend.models import (
    ApprovalStatus,
    EnrichmentSuggestion,
    FieldAnalysis,
    ProductAnalysis,
    ProductData,
    QualityStatus,
)
from backend.approval import (
    mark_auto_approved,
    set_approval,
    bulk_set_approval,
    get_approval_summary,
    filter_by_approval,
)


def _make_suggestion(
    field="Beskrivelse",
    value="Ny beskrivelse",
    confidence=0.9,
    review_required=False,
    source="OneMed produktside",
    approval=ApprovalStatus.NOT_REVIEWED,
) -> EnrichmentSuggestion:
    return EnrichmentSuggestion(
        field_name=field,
        current_value="Gammel",
        suggested_value=value,
        source=source,
        confidence=confidence,
        review_required=review_required,
        approval_status=approval,
    )


def _make_result(
    artno="12345",
    suggestions=None,
    field_statuses=None,
) -> ProductAnalysis:
    pd = ProductData(
        article_number=artno,
        product_name="Test produkt",
        found_on_onemed=True,
    )
    fas = []
    if field_statuses:
        for fname, status in field_statuses.items():
            fas.append(FieldAnalysis(field_name=fname, status=status))
    else:
        fas = [FieldAnalysis(field_name="Beskrivelse", status=QualityStatus.SHOULD_IMPROVE)]

    return ProductAnalysis(
        article_number=artno,
        product_data=pd,
        field_analyses=fas,
        enrichment_suggestions=suggestions or [],
    )


class TestAutoApproval:
    def test_high_conf_suggestion_auto_approved(self):
        """High confidence, non-medical, source-grounded → auto-approved."""
        s = _make_suggestion(confidence=0.85, review_required=False)
        r = _make_result(suggestions=[s], field_statuses={"Beskrivelse": QualityStatus.SHOULD_IMPROVE})
        count = mark_auto_approved([r])
        assert count == 1
        assert s.approval_status == ApprovalStatus.AUTO_APPROVED
        assert s.approved_at is not None

    def test_low_conf_not_auto_approved(self):
        """Low confidence → NOT auto-approved."""
        s = _make_suggestion(confidence=0.3)
        r = _make_result(suggestions=[s])
        count = mark_auto_approved([r])
        assert count == 0
        assert s.approval_status != ApprovalStatus.AUTO_APPROVED

    def test_review_required_marked_needs_review(self):
        """review_required=True → NEEDS_REVIEW."""
        s = _make_suggestion(confidence=0.5, review_required=True)
        r = _make_result(suggestions=[s])
        mark_auto_approved([r])
        assert s.approval_status == ApprovalStatus.NEEDS_REVIEW

    def test_does_not_override_manual_decision(self):
        """Already-reviewed suggestions are not changed."""
        s = _make_suggestion(confidence=0.95, approval=ApprovalStatus.REJECTED)
        r = _make_result(suggestions=[s], field_statuses={"Beskrivelse": QualityStatus.SHOULD_IMPROVE})
        mark_auto_approved([r])
        assert s.approval_status == ApprovalStatus.REJECTED

    def test_empty_suggestion_skipped(self):
        s = _make_suggestion(value="")
        r = _make_result(suggestions=[s])
        count = mark_auto_approved([r])
        assert count == 0

    def test_ai_source_not_auto_approved(self):
        """AI-only source → NOT auto-approved (not a quick win)."""
        s = _make_suggestion(confidence=0.95, source="ai_enrichment")
        r = _make_result(suggestions=[s])
        count = mark_auto_approved([r])
        assert count == 0


class TestSetApproval:
    def test_approve_suggestion(self):
        s = _make_suggestion()
        r = _make_result(artno="A1", suggestions=[s])
        success = set_approval([r], "A1", 0, "Godkjent", comment="OK")
        assert success
        assert s.approval_status == ApprovalStatus.APPROVED
        assert s.approval_comment == "OK"
        assert s.approved_at is not None

    def test_reject_suggestion(self):
        s = _make_suggestion()
        r = _make_result(artno="A1", suggestions=[s])
        success = set_approval([r], "A1", 0, "Avvist", comment="Feil data")
        assert success
        assert s.approval_status == ApprovalStatus.REJECTED
        assert s.approval_comment == "Feil data"

    def test_invalid_article(self):
        s = _make_suggestion()
        r = _make_result(artno="A1", suggestions=[s])
        success = set_approval([r], "NONEXISTENT", 0, "Godkjent")
        assert not success

    def test_invalid_index(self):
        s = _make_suggestion()
        r = _make_result(artno="A1", suggestions=[s])
        success = set_approval([r], "A1", 99, "Godkjent")
        assert not success

    def test_invalid_status(self):
        s = _make_suggestion()
        r = _make_result(artno="A1", suggestions=[s])
        success = set_approval([r], "A1", 0, "INVALID")
        assert not success

    def test_reviewer_stored(self):
        s = _make_suggestion()
        r = _make_result(artno="A1", suggestions=[s])
        set_approval([r], "A1", 0, "Godkjent", reviewer="test_user")
        assert s.approved_by == "test_user"


class TestBulkSetApproval:
    def test_approve_all(self):
        s1 = _make_suggestion(field="Produktnavn")
        s2 = _make_suggestion(field="Beskrivelse")
        r = _make_result(artno="A1", suggestions=[s1, s2])
        count = bulk_set_approval([r], "A1", "Godkjent")
        assert count == 2
        assert s1.approval_status == ApprovalStatus.APPROVED
        assert s2.approval_status == ApprovalStatus.APPROVED

    def test_reject_all(self):
        s1 = _make_suggestion(field="Produktnavn")
        s2 = _make_suggestion(field="Beskrivelse")
        r = _make_result(artno="A1", suggestions=[s1, s2])
        count = bulk_set_approval([r], "A1", "Avvist")
        assert count == 2

    def test_skips_empty_suggestions(self):
        s1 = _make_suggestion(value="")
        s2 = _make_suggestion(value="Gyldig")
        r = _make_result(artno="A1", suggestions=[s1, s2])
        count = bulk_set_approval([r], "A1", "Godkjent")
        assert count == 1


class TestGetApprovalSummary:
    def test_all_statuses_counted(self):
        results = [_make_result(suggestions=[
            _make_suggestion(approval=ApprovalStatus.APPROVED),
            _make_suggestion(approval=ApprovalStatus.AUTO_APPROVED, field="Produsent"),
            _make_suggestion(approval=ApprovalStatus.REJECTED, field="Kategori"),
            _make_suggestion(approval=ApprovalStatus.NEEDS_REVIEW, field="Spesifikasjon"),
            _make_suggestion(approval=ApprovalStatus.NOT_REVIEWED, field="Pakningsinformasjon"),
        ])]
        summary = get_approval_summary(results)
        assert summary["approved"] == 1
        assert summary["auto_approved"] == 1
        assert summary["rejected"] == 1
        assert summary["needs_review"] == 1
        assert summary["not_reviewed"] == 1
        assert summary["approved_total"] == 2  # approved + auto_approved
        assert summary["total_suggestions"] == 5


class TestFilterByApproval:
    def test_approved_only(self):
        results = [
            _make_result(artno="A1", suggestions=[
                _make_suggestion(approval=ApprovalStatus.APPROVED),
            ]),
            _make_result(artno="A2", suggestions=[
                _make_suggestion(approval=ApprovalStatus.NOT_REVIEWED),
            ]),
        ]
        filtered = filter_by_approval(results, approved_only=True)
        assert len(filtered) == 1
        assert filtered[0].article_number == "A1"

    def test_exclude_rejected(self):
        s1 = _make_suggestion(approval=ApprovalStatus.APPROVED, field="Produktnavn")
        s2 = _make_suggestion(approval=ApprovalStatus.REJECTED, field="Beskrivelse")
        results = [_make_result(artno="A1", suggestions=[s1, s2])]
        filtered = filter_by_approval(results, exclude_rejected=True)
        assert len(filtered) == 1
        assert len(filtered[0].enrichment_suggestions) == 1
        assert filtered[0].enrichment_suggestions[0].field_name == "Produktnavn"

    def test_filter_by_specific_status(self):
        results = [
            _make_result(artno="A1", suggestions=[
                _make_suggestion(approval=ApprovalStatus.NEEDS_REVIEW),
            ]),
            _make_result(artno="A2", suggestions=[
                _make_suggestion(approval=ApprovalStatus.APPROVED),
            ]),
        ]
        filtered = filter_by_approval(results, status="Krever vurdering")
        assert len(filtered) == 1
        assert filtered[0].article_number == "A1"

    def test_auto_approved_counts_as_approved(self):
        results = [_make_result(suggestions=[
            _make_suggestion(approval=ApprovalStatus.AUTO_APPROVED),
        ])]
        filtered = filter_by_approval(results, approved_only=True)
        assert len(filtered) == 1

    def test_does_not_modify_original(self):
        s1 = _make_suggestion(approval=ApprovalStatus.APPROVED, field="X")
        s2 = _make_suggestion(approval=ApprovalStatus.REJECTED, field="Y")
        results = [_make_result(suggestions=[s1, s2])]
        filtered = filter_by_approval(results, exclude_rejected=True)
        # Original should still have both
        assert len(results[0].enrichment_suggestions) == 2
        assert len(filtered[0].enrichment_suggestions) == 1
