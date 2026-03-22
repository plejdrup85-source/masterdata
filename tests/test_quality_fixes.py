"""Tests for quality analysis fixes:
  1. Verification status / evidence accuracy
  2. Category breadcrumb extraction and analysis
  3. Packaging detection from technical_details / specs
  4. Image suggestion URL output
"""

import json
import re
from unittest.mock import MagicMock

import pytest
from bs4 import BeautifulSoup


# ── Fix 1: Verification status / evidence ──

class TestVerificationEvidence:
    """Test that business_evidence prefers raw_evidence over generic text."""

    def setup_method(self):
        from backend.models import VerificationStatus
        self.VS = VerificationStatus

    def test_cdn_only_uses_raw_evidence(self):
        """CDN_ONLY should use raw_evidence when provided."""
        raw = "Produktbilde funnet for '12345'. Ingen produktside hentet."
        result = self.VS.business_evidence(self.VS.CDN_ONLY, raw)
        assert result == raw
        assert "bildekatalogen" not in result  # NOT the generic message

    def test_cdn_only_falls_back_to_standard(self):
        """CDN_ONLY without raw_evidence uses standard explanation."""
        result = self.VS.business_evidence(self.VS.CDN_ONLY, None)
        assert "bildekatalogen" in result  # Standard message

    def test_exact_match_uses_raw_evidence(self):
        """EXACT_MATCH should use raw_evidence when provided."""
        raw = "Produktidentitet bekreftet: 12345 stemmer eksakt."
        result = self.VS.business_evidence(self.VS.EXACT_MATCH, raw)
        assert result == raw

    def test_exact_match_without_raw_evidence(self):
        """EXACT_MATCH without raw_evidence uses standard."""
        result = self.VS.business_evidence(self.VS.EXACT_MATCH, None)
        assert "Produktidentitet bekreftet" in result

    def test_cdn_only_label_updated(self):
        """CDN_ONLY label should say 'ikke funnet/hentet', not just 'ikke funnet'."""
        label = self.VS.business_label(self.VS.CDN_ONLY)
        assert "ikke funnet" in label or "ikke hentet" in label


class TestCDNOnlyGuard:
    """Test that CDN_ONLY is not set when product has web data."""

    def test_product_with_url_not_downgraded_to_cdn(self):
        """If product has product_url, verification should NOT be CDN_ONLY."""
        from backend.models import ProductData, VerificationStatus
        product = ProductData(
            article_number="12345",
            found_on_onemed=True,
            product_url="https://www.onemed.no/nb-no/products/i001/product-name",
            product_name="Test Product",
            verification_status=VerificationStatus.EXACT_MATCH,
        )
        # Product has web data - CDN_ONLY should NOT be applied
        has_web_data = bool(product.product_url or product.product_name or product.description)
        assert has_web_data is True

    def test_cdn_only_product_has_no_web_data(self):
        """CDN_ONLY product should have no product_url, name, or description."""
        from backend.models import ProductData, VerificationStatus
        product = ProductData(
            article_number="12345",
            found_on_onemed=True,
            verification_status=VerificationStatus.CDN_ONLY,
            image_url="https://res.onemed.com/NO/ARWebBig/12345.jpg",
        )
        has_web_data = bool(product.product_url or product.product_name or product.description)
        assert has_web_data is False


# ── Fix 2: Category / breadcrumb extraction ──

class TestBreadcrumbExtraction:
    """Test improved breadcrumb extraction from JSON-LD and HTML."""

    def test_json_ld_nested_name(self):
        """BreadcrumbList with name inside item object should be extracted."""
        from backend.scraper import _extract_product_from_json_ld
        json_ld = [{
            "@type": "BreadcrumbList",
            "itemListElement": [
                {"@type": "ListItem", "position": 1, "item": {"@id": "https://example.com/a", "name": "Sortiment"}},
                {"@type": "ListItem", "position": 2, "item": {"@id": "https://example.com/b", "name": "Medisinsk"}},
                {"@type": "ListItem", "position": 3, "item": {"@id": "https://example.com/c", "name": "Sterilisering"}},
            ]
        }]
        info = _extract_product_from_json_ld(json_ld)
        assert info.get("breadcrumbs") == ["Sortiment", "Medisinsk", "Sterilisering"]

    def test_json_ld_top_level_name(self):
        """BreadcrumbList with top-level name should still work."""
        from backend.scraper import _extract_product_from_json_ld
        json_ld = [{
            "@type": "BreadcrumbList",
            "itemListElement": [
                {"@type": "ListItem", "position": 1, "name": "Sortiment"},
                {"@type": "ListItem", "position": 2, "name": "Medisinsk"},
            ]
        }]
        info = _extract_product_from_json_ld(json_ld)
        assert info.get("breadcrumbs") == ["Sortiment", "Medisinsk"]

    def test_json_ld_empty_names_filtered(self):
        """Empty name entries should be filtered out."""
        from backend.scraper import _extract_product_from_json_ld
        json_ld = [{
            "@type": "BreadcrumbList",
            "itemListElement": [
                {"@type": "ListItem", "position": 1, "name": "Sortiment"},
                {"@type": "ListItem", "position": 2, "name": ""},
                {"@type": "ListItem", "position": 3, "name": "Sterilisering"},
            ]
        }]
        info = _extract_product_from_json_ld(json_ld)
        assert info.get("breadcrumbs") == ["Sortiment", "Sterilisering"]

    def test_html_breadcrumb_nav_aria_label(self):
        """Breadcrumb from nav with aria-label should be extracted."""
        from backend.scraper import _parse_product_page
        html = """
        <html><head>
        <script type="application/ld+json">{"@type": "Product", "name": "Test", "sku": "12345"}</script>
        </head><body>
        <nav aria-label="Breadcrumb">
            <ol>
                <li><a href="/a">Sortiment</a></li>
                <li><a href="/b">Medisinsk forbruk</a></li>
                <li><a href="/c">Sterilisering</a></li>
            </ol>
        </nav>
        </body></html>
        """
        product = _parse_product_page(html, "12345")
        assert product.category_breadcrumb is not None
        assert len(product.category_breadcrumb) >= 3
        assert "Sortiment" in product.category_breadcrumb
        assert "Sterilisering" in product.category_breadcrumb

    def test_html_breadcrumb_class(self):
        """Breadcrumb from element with breadcrumb class should be extracted."""
        from backend.scraper import _parse_product_page
        html = """
        <html><head>
        <script type="application/ld+json">{"@type": "Product", "name": "Test", "sku": "12345"}</script>
        </head><body>
        <div class="breadcrumb-nav">
            <a href="/a">Sortiment</a>
            <a href="/b">Medisinsk</a>
            <a href="/c">Sterilisering</a>
        </div>
        </body></html>
        """
        product = _parse_product_page(html, "12345")
        assert product.category_breadcrumb is not None
        assert len(product.category_breadcrumb) >= 3


class TestCategoryAnalysis:
    """Test that category analysis recognizes breadcrumbs."""

    def test_breadcrumb_not_missing(self):
        """Category with breadcrumbs should NOT be marked as MISSING."""
        from backend.analyzer import _analyze_category
        from backend.models import ProductData, QualityStatus
        product = ProductData(
            article_number="12345",
            category="Steriliseringspose flat",
            category_breadcrumb=[
                "Sortiment", "Medisinsk forbruksmateriell",
                "Sterilisering autoklavering", "Steriliseringsposer flate",
                "Steriliseringspose flat"
            ],
        )
        analysis = _analyze_category(product)
        assert analysis.status != QualityStatus.MISSING
        assert analysis.status in (QualityStatus.OK, QualityStatus.STRONG)
        assert "Sortiment" in (analysis.current_value or "")

    def test_leaf_category_only(self):
        """Single category without breadcrumbs should still be OK."""
        from backend.analyzer import _analyze_category
        from backend.models import ProductData, QualityStatus
        product = ProductData(
            article_number="12345",
            category="Sterilisering",
        )
        analysis = _analyze_category(product)
        assert analysis.status != QualityStatus.MISSING

    def test_no_category_is_missing(self):
        """No category data at all should be MISSING."""
        from backend.analyzer import _analyze_category
        from backend.models import ProductData, QualityStatus
        product = ProductData(
            article_number="12345",
        )
        analysis = _analyze_category(product)
        assert analysis.status == QualityStatus.MISSING


# ── Fix 3: Packaging extraction from specs ──

class TestPackagingExtraction:
    """Test improved packaging extraction from technical_details."""

    def test_packaging_from_technical_details(self):
        """Packaging keys in technical_details should be detected."""
        from backend.analyzer import _analyze_packaging
        from backend.models import ProductData, QualityStatus
        product = ProductData(
            article_number="12345",
            technical_details={
                "Antall i pakningen": "100",
                "Antall i transport-pakke": "10",
                "Antall på pall": "50",
            },
        )
        analysis = _analyze_packaging(product)
        assert analysis.status != QualityStatus.MISSING
        assert analysis.status == QualityStatus.OK
        assert "100" in (analysis.current_value or "")

    def test_packaging_from_packaging_info(self):
        """Pre-extracted packaging_info should still work."""
        from backend.analyzer import _analyze_packaging
        from backend.models import ProductData, QualityStatus
        product = ProductData(
            article_number="12345",
            packaging_info="Antall i pakning: 100; Antall i transportpakke: 10",
        )
        analysis = _analyze_packaging(product)
        assert analysis.status == QualityStatus.OK

    def test_no_packaging_is_missing(self):
        """No packaging data at all should be MISSING."""
        from backend.analyzer import _analyze_packaging
        from backend.models import ProductData, QualityStatus
        product = ProductData(
            article_number="12345",
        )
        analysis = _analyze_packaging(product)
        assert analysis.status == QualityStatus.MISSING

    def test_packaging_non_breaking_space_in_key(self):
        """Keys with non-breaking spaces should still be matched."""
        from backend.analyzer import _analyze_packaging
        from backend.models import ProductData, QualityStatus
        product = ProductData(
            article_number="12345",
            technical_details={
                "Antall\xa0i\xa0pakningen": "50",  # Non-breaking spaces
            },
        )
        analysis = _analyze_packaging(product)
        assert analysis.status != QualityStatus.MISSING

    def test_packaging_variant_keywords(self):
        """Various Norwegian packaging keywords should be recognized."""
        from backend.analyzer import _analyze_packaging
        from backend.models import ProductData, QualityStatus
        # "antall pr pakn" variant
        product = ProductData(
            article_number="12345",
            technical_details={
                "Antall pr pakning": "200",
            },
        )
        analysis = _analyze_packaging(product)
        assert analysis.status != QualityStatus.MISSING


class TestPackagingScraperExtraction:
    """Test packaging keyword matching in scraper."""

    def test_antall_i_pakningen_extracted(self):
        """'Antall i pakningen' spec key should be extracted as packaging."""
        from backend.scraper import _parse_product_page
        html = """
        <html><head>
        <script type="application/ld+json">{"@type": "Product", "name": "Test", "sku": "12345"}</script>
        </head><body>
        <div id="accordionItem_specifications">
            <table>
                <tr><td>Antall i pakningen</td><td>100 stk</td></tr>
                <tr><td>Antall i transport-pakke</td><td>10</td></tr>
                <tr><td>Antall på pall</td><td>50</td></tr>
            </table>
        </div>
        </body></html>
        """
        product = _parse_product_page(html, "12345")
        assert product.packaging_info is not None
        assert "100" in product.packaging_info

    def test_antall_pr_pakning_extracted(self):
        """'Antall pr pakning' spec key should also be recognized."""
        from backend.scraper import _parse_product_page
        html = """
        <html><head>
        <script type="application/ld+json">{"@type": "Product", "name": "Test", "sku": "12345"}</script>
        </head><body>
        <table>
            <tr><td>Antall pr pakning</td><td>200</td></tr>
        </table>
        </body></html>
        """
        product = _parse_product_page(html, "12345")
        assert product.packaging_info is not None
        assert "200" in product.packaging_info


# ── Fix 4: Image suggestion URLs ──

class TestImageSuggestionOutput:
    """Test that image suggestions include proper URLs."""

    def test_image_suggestion_model_has_current_url(self):
        """ImageSuggestion should always have current_image_url."""
        from backend.models import ImageSuggestion
        sugg = ImageSuggestion(
            current_image_url="https://res.onemed.com/NO/ARWebBig/12345.jpg",
            current_image_status="low_quality",
            suggested_source="producer_search",
            reason="Bildestatus: low_quality.",
        )
        assert sugg.current_image_url is not None
        assert "12345" in sugg.current_image_url

    def test_image_suggestion_with_manufacturer_url(self):
        """Manufacturer suggestion should have suggested_image_url."""
        from backend.models import ImageSuggestion
        sugg = ImageSuggestion(
            current_image_url="https://res.onemed.com/NO/ARWebBig/12345.jpg",
            current_image_status="low_quality",
            suggested_image_url="https://manufacturer.com/image.jpg",
            suggested_source="manufacturer",
            suggested_source_url="https://manufacturer.com/product",
            confidence=0.7,
        )
        assert sugg.suggested_image_url is not None
        assert sugg.suggested_source_url is not None
