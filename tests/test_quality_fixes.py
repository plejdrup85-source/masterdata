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


# ── END-TO-END FLOW TESTS ──
# These tests simulate the complete pipeline: HTML → parse → analyze → score → verify output


class TestEndToEndProductPage:
    """Test the FULL pipeline when a product page is found and parsed.

    Simulates: scraper finds page → parses HTML → analyzer evaluates fields → scorer scores areas.
    Verifies ALL fields flow correctly through the entire pipeline.
    """

    REALISTIC_HTML = """
    <html>
    <head>
        <script type="application/ld+json">{
            "@type": "Product",
            "name": "Steriliseringspose flat 100x250mm",
            "description": "Steriliseringspose flat for bruk med dampsterilisering. Indikatorfelt for prosessverifisering.",
            "sku": "12345",
            "image": "https://res.onemed.com/NO/ARWebBig/12345.jpg",
            "url": "https://www.onemed.no/nb-no/products/i001/steriliseringspose-flat",
            "brand": {"@type": "Brand", "name": "Steriking"}
        }</script>
        <script type="application/ld+json">{
            "@type": "BreadcrumbList",
            "itemListElement": [
                {"@type": "ListItem", "position": 1, "name": "Sortiment"},
                {"@type": "ListItem", "position": 2, "name": "Medisinsk forbruksmateriell"},
                {"@type": "ListItem", "position": 3, "name": "Sterilisering autoklavering"},
                {"@type": "ListItem", "position": 4, "name": "Steriliseringsposer flate"},
                {"@type": "ListItem", "position": 5, "name": "Steriliseringspose flat"}
            ]
        }</script>
    </head>
    <body>
        <h1>Steriliseringspose flat 100x250mm</h1>
        <div id="accordionItem_specifications">
            <table>
                <tr><td>Materiale</td><td>Papir/Plast</td></tr>
                <tr><td>Størrelse</td><td>100x250mm</td></tr>
                <tr><td>Steriliseringsmetode</td><td>Damp</td></tr>
                <tr><td>Antall i pakningen</td><td>200 stk</td></tr>
                <tr><td>Antall i transport-pakke</td><td>20</td></tr>
                <tr><td>Antall på pall</td><td>100</td></tr>
            </table>
        </div>
    </body>
    </html>
    """

    def _parse_and_verify(self):
        """Parse the realistic HTML and verify SKU match, return product + status."""
        from backend.scraper import _parse_product_page, _verify_sku_match
        from backend.models import VerificationStatus

        product = _parse_product_page(self.REALISTIC_HTML, "12345")
        v_status, v_evidence = _verify_sku_match(self.REALISTIC_HTML, "12345")
        product.verification_status = v_status
        product.verification_evidence = v_evidence
        product.product_url = "https://www.onemed.no/nb-no/products/i001/steriliseringspose-flat"
        return product

    def test_e2e_verification_status_is_exact_match(self):
        """When page is found and SKU matches, status must be EXACT_MATCH, not CDN_ONLY."""
        from backend.models import VerificationStatus
        product = self._parse_and_verify()
        assert product.verification_status == VerificationStatus.EXACT_MATCH
        assert product.found_on_onemed is True
        assert product.product_url is not None
        # Business label must NOT say "ingen produktside funnet"
        label = VerificationStatus.business_label(product.verification_status)
        assert "ikke funnet" not in label.lower()
        assert "Verifisert" in label

    def test_e2e_breadcrumbs_extracted(self):
        """Breadcrumbs must be extracted from JSON-LD BreadcrumbList."""
        product = self._parse_and_verify()
        assert product.category_breadcrumb is not None
        assert len(product.category_breadcrumb) >= 3
        assert "Sortiment" in product.category_breadcrumb
        assert "Steriliseringspose flat" in product.category_breadcrumb
        assert product.category == "Steriliseringspose flat"

    def test_e2e_category_not_missing_in_analysis(self):
        """Category must NOT be MISSING when breadcrumbs exist."""
        from backend.analyzer import _analyze_category
        from backend.models import QualityStatus
        product = self._parse_and_verify()
        analysis = _analyze_category(product)
        assert analysis.status != QualityStatus.MISSING
        assert analysis.status in (QualityStatus.OK, QualityStatus.STRONG)

    def test_e2e_category_not_missing_in_scoring(self):
        """Category must NOT be MISSING in scoring either."""
        from backend.scoring import _score_category_area, AreaStatus
        product = self._parse_and_verify()
        score = _score_category_area(product)
        assert score.status != AreaStatus.MISSING
        assert score.score >= 50

    def test_e2e_packaging_extracted(self):
        """Packaging info must be extracted from spec table."""
        product = self._parse_and_verify()
        assert product.packaging_info is not None
        assert "200" in product.packaging_info

    def test_e2e_packaging_not_missing_in_analysis(self):
        """Packaging must NOT be MISSING when spec table has packaging data."""
        from backend.analyzer import _analyze_packaging
        from backend.models import QualityStatus
        product = self._parse_and_verify()
        analysis = _analyze_packaging(product)
        assert analysis.status != QualityStatus.MISSING
        assert analysis.status == QualityStatus.OK

    def test_e2e_packaging_not_missing_in_scoring(self):
        """Packaging must NOT be MISSING in scoring either."""
        from backend.scoring import _score_packaging_area, AreaStatus
        product = self._parse_and_verify()
        score = _score_packaging_area(product)
        assert score.status != AreaStatus.MISSING
        assert score.score > 0

    def test_e2e_full_analysis_no_false_missing(self):
        """Full analyze_product() must not falsely report category/packaging as missing."""
        from backend.analyzer import analyze_product
        from backend.models import QualityStatus
        product = self._parse_and_verify()
        result = analyze_product(product)

        # Check field analyses
        field_by_name = {fa.field_name: fa for fa in result.field_analyses}

        # Category must not be missing
        cat_analysis = field_by_name.get("Kategori")
        assert cat_analysis is not None
        assert cat_analysis.status != QualityStatus.MISSING, \
            f"Category falsely reported as MISSING: {cat_analysis.comment}"

        # Packaging must not be missing
        pkg_analysis = field_by_name.get("Pakningsinformasjon")
        assert pkg_analysis is not None
        assert pkg_analysis.status != QualityStatus.MISSING, \
            f"Packaging falsely reported as MISSING: {pkg_analysis.comment}"

        # Overall score should be decent (product has good data)
        assert result.total_score > 50

    def test_e2e_nested_breadcrumb_json_ld(self):
        """BreadcrumbList with name inside item object must also work."""
        from backend.scraper import _parse_product_page
        from backend.analyzer import _analyze_category
        from backend.models import QualityStatus

        html = """
        <html><head>
        <script type="application/ld+json">{"@type": "Product", "name": "Test", "sku": "99999"}</script>
        <script type="application/ld+json">{
            "@type": "BreadcrumbList",
            "itemListElement": [
                {"@type": "ListItem", "position": 1, "item": {"@id": "/a", "name": "Sortiment"}},
                {"@type": "ListItem", "position": 2, "item": {"@id": "/b", "name": "Medisinsk"}},
                {"@type": "ListItem", "position": 3, "item": {"@id": "/c", "name": "Hansker"}}
            ]
        }</script>
        </head><body></body></html>
        """
        product = _parse_product_page(html, "99999")
        assert product.category_breadcrumb == ["Sortiment", "Medisinsk", "Hansker"]
        assert product.category == "Hansker"

        analysis = _analyze_category(product)
        assert analysis.status != QualityStatus.MISSING


class TestScoringConsistency:
    """Verify scoring.py and analyzer.py agree on packaging status."""

    def test_scoring_uses_technical_details_for_packaging(self):
        """scoring._score_packaging_area must check technical_details, not just packaging_info."""
        from backend.scoring import _score_packaging_area, AreaStatus
        from backend.models import ProductData

        # Product with packaging data only in technical_details
        product = ProductData(
            article_number="12345",
            technical_details={
                "Antall i pakningen": "100",
                "Antall i transport-pakke": "10",
            },
        )
        score = _score_packaging_area(product)
        assert score.status != AreaStatus.MISSING, \
            f"Scoring says packaging MISSING but it's in technical_details"
        assert score.score > 0
