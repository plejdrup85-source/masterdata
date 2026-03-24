"""Tests for image search module."""

import pytest
from backend.image_search import build_image_search_urls, enhance_image_suggestion
from backend.models import ImageSuggestion, ProductData


class TestBuildImageSearchUrls:
    def test_with_producer_and_item(self):
        product = ProductData(
            article_number="222001",
            product_name="SELEFA Hanske Nitril",
            manufacturer="Mölnlycke",
            manufacturer_article_number="REF12345",
        )
        result = build_image_search_urls(product)
        assert result["producer_name"] == "Mölnlycke"
        assert result["supplier_article_number"] == "REF12345"
        assert result["google_image_search_url"]
        assert "google.com" in result["google_image_search_url"]
        # Mölnlycke is in the known producers list
        assert result["producer_search_url"] is not None
        assert "molnlycke.com" in result["producer_search_url"]

    def test_with_unknown_producer(self):
        product = ProductData(
            article_number="333001",
            product_name="Bandasje steril",
            manufacturer="UkjentProdusent AS",
        )
        result = build_image_search_urls(product)
        assert result["producer_search_url"] is None  # Unknown producer
        assert result["google_image_search_url"]

    def test_with_no_producer(self):
        product = ProductData(
            article_number="444001",
            product_name="Kompress",
        )
        result = build_image_search_urls(product)
        assert "Kompress" in result["search_terms"]
        assert result["google_image_search_url"]

    def test_with_jeeves_data(self):
        product = ProductData(
            article_number="555001",
            product_name="Plaster",
        )
        result = build_image_search_urls(
            product,
            jeeves_supplier="Hartmann",
            jeeves_supplier_item="REF99999",
        )
        assert result["producer_name"] == "Hartmann"
        assert result["supplier_article_number"] == "REF99999"
        assert result["producer_search_url"] is not None


class TestEnhanceImageSuggestion:
    def test_clears_cdn_self_reference(self):
        """When suggested URL is same as CDN, should clear it."""
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
            review_required=True,
        )
        result = enhance_image_suggestion(suggestion, product)
        assert result.suggested_image_url is None
        assert result.suggested_source == "manuelt_søk_påkrevd"
        assert "Ingen bedre bilde funnet automatisk" in result.reason
        assert "Google-bildesøk" in result.reason

    def test_keeps_real_image_url(self):
        """When suggestion has a real different URL, keep it."""
        product = ProductData(
            article_number="222001",
            product_name="Hanske",
            manufacturer="Mölnlycke",
        )
        suggestion = ImageSuggestion(
            current_image_url="https://res.onemed.com/NO/ARWebBig/222001.jpg",
            current_image_status="low_quality",
            suggested_image_url="https://www.molnlycke.com/images/product123.jpg",
            suggested_source="manufacturer",
            confidence=0.6,
            review_required=True,
        )
        result = enhance_image_suggestion(suggestion, product)
        assert result.suggested_image_url == "https://www.molnlycke.com/images/product123.jpg"
        assert result.suggested_source == "manufacturer"

    def test_none_suggestion_returns_none(self):
        product = ProductData(article_number="111")
        result = enhance_image_suggestion(None, product)
        assert result is None
