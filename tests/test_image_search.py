"""Tests for image search, improvement scoring, and approval workflow."""

import pytest
from backend.image_search import (
    build_search_urls,
    calculate_improvement_score,
    enhance_image_suggestion,
    approve_image_suggestion,
    get_approved_images,
    _confidence_label,
    _classify_source_type,
    _extract_domain,
    _guess_file_extension,
    _is_product_image_url,
    MIN_IMPROVEMENT_SCORE,
)
from backend.models import ImageSuggestion, ProductData, ProductAnalysis


class TestImprovementScoring:
    """Verify improvement scoring logic."""

    def test_missing_image_producer_source_high_score(self):
        """Missing image + producer source = high improvement score."""
        score, reason = calculate_improvement_score(
            current_status="missing",
            suggested_url="https://www.icumed.com/media/product.jpg",
            source_type="produsent",
            confidence=0.7,
        )
        assert score >= 70
        assert "mangler helt" in reason.lower()
        assert "produsent" in reason.lower()

    def test_low_quality_distributor_medium_score(self):
        score, reason = calculate_improvement_score(
            current_status="low_quality",
            suggested_url="https://www.norengros.no/image.jpg",
            source_type="distributør",
            confidence=0.5,
        )
        assert 40 <= score <= 70

    def test_no_url_gives_zero(self):
        score, reason = calculate_improvement_score(
            current_status="missing",
            suggested_url=None,
            source_type="produsent",
            confidence=0.9,
        )
        assert score == 0

    def test_review_status_low_confidence_low_score(self):
        score, reason = calculate_improvement_score(
            current_status="review",
            suggested_url="https://example.com/img.jpg",
            source_type="annen",
            confidence=0.2,
        )
        assert score < MIN_IMPROVEMENT_SCORE


class TestConfidenceLabels:
    def test_high(self):
        assert _confidence_label(0.80) == "Høy tillit"

    def test_medium(self):
        assert _confidence_label(0.55) == "Middels tillit"

    def test_low(self):
        assert _confidence_label(0.30) == "Lav tillit"

    def test_manual(self):
        assert _confidence_label(0.10) == "Krever manuell vurdering"


class TestSourceClassification:
    def test_manufacturer(self):
        assert _classify_source_type("manufacturer_website", "icumed.com") == "produsent"

    def test_norengros(self):
        assert _classify_source_type("norengros", "norengros.no") == "distributør"

    def test_unknown(self):
        assert _classify_source_type("other", "random.com") == "annen"

    def test_known_producer_domain(self):
        assert _classify_source_type("other", "molnlycke.com") == "produsent"


class TestHelpers:
    def test_extract_domain(self):
        assert _extract_domain("https://www.icumed.com/media/img.jpg") == "icumed.com"

    def test_guess_extension_jpg(self):
        assert _guess_file_extension("https://example.com/photo.jpg") == ".jpg"

    def test_guess_extension_webp(self):
        assert _guess_file_extension("https://example.com/photo?format=webp") == ".webp"

    def test_guess_extension_default(self):
        assert _guess_file_extension("https://example.com/photo") == ".jpg"

    def test_is_product_image(self):
        assert _is_product_image_url("https://www.icumed.com/media/product.jpg")
        assert not _is_product_image_url("https://www.example.com/favicon.ico")
        assert not _is_product_image_url("https://www.example.com/logo.png")
        assert not _is_product_image_url("")


class TestBuildSearchUrls:
    def test_with_producer_and_item(self):
        product = ProductData(
            article_number="N245100189090",
            product_name="Endotrakealtube SACETT",
            manufacturer="ICU Medical",
            manufacturer_article_number="REF12345",
        )
        result = build_search_urls(product)
        assert result["producer_name"] == "ICU Medical"
        assert result["supplier_article_number"] == "REF12345"
        assert result["google_image_search_url"]
        assert "google.com" in result["google_image_search_url"]
        # ICU Medical is in known producers
        assert result["producer_search_url"] is not None

    def test_icu_medical_matches_producer(self):
        """ICU Medical as manufacturer should match producer config."""
        product = ProductData(
            article_number="N245100189090",
            product_name="SACETT Endotrakealtube",
            manufacturer="ICU Medical",
        )
        result = build_search_urls(product)
        assert result["producer_search_url"] is not None
        assert "icumed.com" in result["producer_search_url"]

    def test_multiple_strategies(self):
        product = ProductData(
            article_number="222001",
            product_name="SELEFA Hanske Nitril",
            manufacturer="OneMed",
            manufacturer_article_number="SEL-001",
        )
        result = build_search_urls(product)
        assert len(result["all_strategies"]) >= 2

    def test_fallback_article_number(self):
        product = ProductData(article_number="123456")
        result = build_search_urls(product)
        assert "123456" in result["search_terms"]


class TestEnhanceImageSuggestion:
    def test_clears_cdn_self_reference(self):
        product = ProductData(
            article_number="222001",
            product_name="Hanske",
            manufacturer="TestProd",
        )
        suggestion = ImageSuggestion(
            current_image_url="https://res.onemed.com/NO/ARWebBig/222001.jpg",
            current_image_status="low_quality",
            suggested_image_url="https://res.onemed.com/NO/ARWebBig/222001.jpg",
            suggested_source="current_cdn",
            confidence=0.0,
        )
        result = enhance_image_suggestion(suggestion, product)
        assert result.suggested_image_url is None
        assert result.source_type == "manuelt_søk"
        assert result.improvement_score == 0
        assert result.confidence_label == "Krever manuell vurdering"
        assert result.google_search_url  # Always provides fallback search

    def test_keeps_real_image_and_scores(self):
        product = ProductData(
            article_number="N245100189090",
            product_name="Endotrakealtube SACETT",
            manufacturer="ICU Medical",
        )
        suggestion = ImageSuggestion(
            current_image_url=None,
            current_image_status="missing",
            suggested_image_url="https://www.icumed.com/media/sacett.jpg",
            suggested_source="manufacturer_website",
            confidence=0.7,
        )
        result = enhance_image_suggestion(suggestion, product)
        assert result.suggested_image_url == "https://www.icumed.com/media/sacett.jpg"
        assert result.source_type == "produsent"
        assert result.improvement_score >= MIN_IMPROVEMENT_SCORE
        assert result.confidence_label in ("Middels tillit", "Høy tillit")
        assert result.download_filename == "N245100189090.jpg"

    def test_rejects_low_improvement(self):
        """Images with low improvement score should be cleared."""
        product = ProductData(
            article_number="333",
            product_name="Test",
        )
        suggestion = ImageSuggestion(
            current_image_status="review",  # Only mildly bad
            suggested_image_url="https://random.com/maybe.jpg",
            suggested_source="other",
            confidence=0.15,  # Very low confidence
        )
        result = enhance_image_suggestion(suggestion, product)
        # Low improvement: review(10) + annen(5) + conf(3) + url(10) = 28 < 40
        assert result.suggested_image_url is None
        assert "ikke en klar nok forbedring" in result.improvement_reason

    def test_none_returns_none(self):
        product = ProductData(article_number="111")
        assert enhance_image_suggestion(None, product) is None


class TestApprovalWorkflow:
    def _make_result(self, art_nr, has_suggestion=True, url=None):
        sugg = ImageSuggestion(
            current_image_status="missing",
            suggested_image_url=url or "https://example.com/img.jpg",
            confidence=0.7,
        ) if has_suggestion else None
        return ProductAnalysis(
            article_number=art_nr,
            product_data=ProductData(article_number=art_nr),
            image_suggestion=sugg,
        )

    def test_approve_image(self):
        results = [self._make_result("111")]
        ok = approve_image_suggestion(results, "111", "godkjent", "bruker1")
        assert ok
        assert results[0].image_suggestion.approval_status == "godkjent"
        assert results[0].image_suggestion.approved_by == "bruker1"

    def test_reject_image(self):
        results = [self._make_result("222")]
        ok = approve_image_suggestion(results, "222", "avvist")
        assert ok
        assert results[0].image_suggestion.approval_status == "avvist"

    def test_invalid_article(self):
        results = [self._make_result("111")]
        ok = approve_image_suggestion(results, "999", "godkjent")
        assert not ok

    def test_invalid_status(self):
        results = [self._make_result("111")]
        ok = approve_image_suggestion(results, "111", "ugyldig_status")
        assert not ok

    def test_get_approved_images(self):
        r1 = self._make_result("111", url="https://a.com/1.jpg")
        r1.image_suggestion.approval_status = "godkjent"
        r1.image_suggestion.download_filename = "111.jpg"

        r2 = self._make_result("222", url="https://b.com/2.jpg")
        r2.image_suggestion.approval_status = "avvist"

        r3 = self._make_result("333", url="https://c.com/3.jpg")
        r3.image_suggestion.approval_status = "godkjent"
        r3.image_suggestion.download_filename = "333.jpg"

        approved = get_approved_images([r1, r2, r3])
        assert len(approved) == 2
        assert approved[0]["article_number"] == "111"
        assert approved[1]["article_number"] == "333"
        assert approved[0]["download_filename"] == "111.jpg"

    def test_no_suggestion_not_in_approved(self):
        r = self._make_result("444", has_suggestion=False)
        approved = get_approved_images([r])
        assert len(approved) == 0


class TestSACETTExample:
    """Verify the specific SACETT example from the task description works."""

    def test_sacett_image_scored_correctly(self):
        """The SACETT ICU Medical image should get a high improvement score."""
        score, reason = calculate_improvement_score(
            current_status="missing",
            suggested_url="https://www.icumed.com/media/nkykv5ll/sacett-suction-above-cuff-endotracheal-tube.jpg?format=webp",
            source_type="produsent",
            confidence=0.7,
        )
        assert score >= 70, f"Expected high score, got {score}: {reason}"
        assert "mangler" in reason.lower()
        assert "produsent" in reason.lower()

    def test_sacett_full_suggestion_flow(self):
        """Full flow: create suggestion, enhance, verify output."""
        product = ProductData(
            article_number="N245100189090",
            product_name="Endotrakealtube SACETT",
            manufacturer="ICU Medical",
            manufacturer_article_number="SACETT-01",
        )
        suggestion = ImageSuggestion(
            current_image_url=None,
            current_image_status="missing",
            suggested_image_url="https://www.icumed.com/media/nkykv5ll/sacett-suction-above-cuff-endotracheal-tube.jpg?format=webp",
            suggested_source="manufacturer_website",
            confidence=0.7,
        )
        result = enhance_image_suggestion(suggestion, product)

        # Verify all required fields are populated
        assert result.suggested_image_url is not None
        assert result.source_type == "produsent"
        assert result.source_domain == "icumed.com"
        assert result.improvement_score >= 70
        assert result.confidence_label in ("Middels tillit", "Høy tillit")
        assert result.download_filename == "N245100189090.webp"
        assert result.search_terms_used is not None
        assert result.google_search_url is not None
        assert result.improvement_reason is not None
        assert "produsent" in result.improvement_reason.lower()
