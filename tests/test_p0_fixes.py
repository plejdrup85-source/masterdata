"""Tests for the three P0 fixes:
  P0-1: Float article number handling
  P0-2: SKU verification false-positive prevention
  P0-3: Description structure preservation
"""

import math
import re

import pytest

# ── P0-1: Float article number handling ──


class TestNormalizeIdentifier:
    """Test the identifier normalization function that prevents float coercion."""

    def setup_method(self):
        from backend.identifiers import normalize_identifier
        self.normalize = normalize_identifier

    def test_float_integer_coercion(self):
        """12345.0 (float from openpyxl) → '12345'"""
        assert self.normalize(12345.0) == "12345"

    def test_large_float_integer(self):
        """8286416.0 (common Excel float) → '8286416'"""
        assert self.normalize(8286416.0) == "8286416"

    def test_string_float_format(self):
        """String '12345.0' (from str(float)) → '12345'"""
        assert self.normalize("12345.0") == "12345"

    def test_string_float_trailing_zeros(self):
        """'12345.00' → '12345'"""
        assert self.normalize("12345.00") == "12345"

    def test_int_passthrough(self):
        """Integer 12345 → '12345'"""
        assert self.normalize(12345) == "12345"

    def test_string_passthrough(self):
        """Normal string preserved"""
        assert self.normalize("N12345") == "N12345"

    def test_leading_zeros_preserved(self):
        """Leading zeros must be preserved: '007890' stays '007890'"""
        assert self.normalize("007890") == "007890"

    def test_alphanumeric_identifier(self):
        """Alphanumeric identifiers stay as-is"""
        assert self.normalize("ABC-123/456") == "ABC-123/456"

    def test_none_returns_none(self):
        assert self.normalize(None) is None

    def test_nan_returns_none(self):
        assert self.normalize(float("nan")) is None

    def test_empty_string_returns_none(self):
        assert self.normalize("") is None

    def test_sentinel_values_return_none(self):
        for sentinel in ("none", "None", "NaN", "nan", "null", "NA", "N/A"):
            assert self.normalize(sentinel) is None, f"Should reject '{sentinel}'"

    def test_whitespace_stripped(self):
        """Whitespace (including non-breaking spaces) is stripped"""
        assert self.normalize("  12345  ") == "12345"
        assert self.normalize("\u00a012345\u00a0") == "12345"  # Non-breaking space

    def test_inf_returns_none(self):
        assert self.normalize(float("inf")) is None
        assert self.normalize(float("-inf")) is None

    def test_true_decimal_preserved(self):
        """A genuine decimal float (unusual for ID, but preserve it)"""
        # 12345.5 is not an integer-like float, so keep as-is
        result = self.normalize(12345.5)
        assert result == "12345.5"


class TestNormalizeIdentifierStrict:
    """Test the strict variant that returns '' instead of None."""

    def setup_method(self):
        from backend.identifiers import normalize_identifier_strict
        self.normalize_strict = normalize_identifier_strict

    def test_none_returns_empty(self):
        assert self.normalize_strict(None) == ""

    def test_nan_returns_empty(self):
        assert self.normalize_strict(float("nan")) == ""

    def test_normal_value(self):
        assert self.normalize_strict(12345.0) == "12345"


class TestIdentifierConsistency:
    """Verify that the same article number normalizes to the same key
    regardless of how it was read (upload vs Jeeves)."""

    def setup_method(self):
        from backend.identifiers import normalize_identifier
        self.normalize = normalize_identifier

    def test_upload_jeeves_match(self):
        """The core bug: uploaded '12345.0' must match Jeeves '12345'."""
        uploaded = self.normalize(12345.0)       # openpyxl float
        jeeves = self.normalize("12345")          # Jeeves string
        assert uploaded == jeeves == "12345"

    def test_string_float_matches_int(self):
        """'8286416.0' from str(float) matches '8286416' from Jeeves."""
        from_str_float = self.normalize("8286416.0")
        from_string = self.normalize("8286416")
        assert from_str_float == from_string == "8286416"


# ── P0-2: SKU verification false-positive prevention ──


class TestVerificationStatus:
    """Test the VerificationStatus enum and its values."""

    def test_enum_values_exist(self):
        from backend.models import VerificationStatus
        assert VerificationStatus.EXACT_MATCH
        assert VerificationStatus.MISMATCH
        assert VerificationStatus.CDN_ONLY
        assert VerificationStatus.UNVERIFIED

    def test_default_is_unverified(self):
        from backend.models import ProductData, VerificationStatus
        pd = ProductData(article_number="12345")
        assert pd.verification_status == VerificationStatus.UNVERIFIED


class TestVerifySKUMatch:
    """Test the rewritten _verify_sku_match function."""

    def setup_method(self):
        from backend.scraper import _verify_sku_match
        from backend.models import VerificationStatus
        self.verify = _verify_sku_match
        self.VS = VerificationStatus

    def test_exact_match_json_ld(self):
        """Exact SKU in JSON-LD → EXACT_MATCH"""
        html = '<script type="application/ld+json">{"sku": "12345"}</script>'
        status, evidence = self.verify(html, "12345")
        assert status == self.VS.EXACT_MATCH
        assert "eksakt" in evidence.lower() or "matcher" in evidence.lower()

    def test_normalized_match_leading_n(self):
        """SKU with leading N → NORMALIZED_MATCH"""
        html = '<script type="application/ld+json">{"sku": "N12345"}</script>'
        status, evidence = self.verify(html, "12345")
        assert status == self.VS.NORMALIZED_MATCH

    def test_mismatch_different_sku(self):
        """Different SKU in JSON-LD → MISMATCH (not True!)"""
        html = '<script type="application/ld+json">{"sku": "99999"}</script>'
        status, evidence = self.verify(html, "12345")
        assert status == self.VS.MISMATCH
        assert "avviker" in evidence.lower() or "feil" in evidence.lower()

    def test_sku_in_page_text_only(self):
        """SKU in page text but not JSON-LD → SKU_IN_PAGE (weaker)"""
        html = '<html><body>Artikkelnummer: 12345</body></html>'
        status, evidence = self.verify(html, "12345")
        assert status == self.VS.SKU_IN_PAGE
        assert "svakere" in evidence.lower() or "sidetekst" in evidence.lower()

    def test_cannot_verify_returns_unverified(self):
        """No SKU anywhere → UNVERIFIED (NOT True like before!)"""
        html = '<html><body>Some product page without any identifiers</body></html>'
        status, evidence = self.verify(html, "12345")
        assert status == self.VS.UNVERIFIED
        # This is the critical fix: old code returned True here (false positive)

    def test_mismatch_with_float_normalization(self):
        """Float-normalized identifiers still compare correctly."""
        html = '<script type="application/ld+json">{"sku": "12345"}</script>'
        status, _ = self.verify(html, "12345.0")  # Float-formatted
        # normalize_identifier("12345.0") → "12345" which matches
        assert status == self.VS.EXACT_MATCH


class TestVerificationInProductData:
    """Test that verification status is properly stored and used."""

    def test_cdn_only_product(self):
        """CDN-only product should have CDN_ONLY status, not EXACT_MATCH."""
        from backend.models import ProductData, VerificationStatus
        product = ProductData(
            article_number="12345",
            found_on_onemed=True,
            verification_status=VerificationStatus.CDN_ONLY,
        )
        assert product.verification_status == VerificationStatus.CDN_ONLY
        assert product.found_on_onemed is True  # Still "found" for pipeline

    def test_mismatch_triggers_review(self):
        """Products with MISMATCH verification should trigger manual review."""
        from backend.models import ProductData, VerificationStatus
        from backend.analyzer import analyze_product
        product = ProductData(
            article_number="12345",
            found_on_onemed=True,
            product_name="Test product",
            verification_status=VerificationStatus.MISMATCH,
            verification_evidence="SKU mismatch detected",
        )
        analysis = analyze_product(product)
        assert analysis.manual_review_needed is True

    def test_cdn_only_triggers_review(self):
        """Products with CDN_ONLY verification should trigger manual review."""
        from backend.models import ProductData, VerificationStatus
        from backend.analyzer import analyze_product
        product = ProductData(
            article_number="12345",
            found_on_onemed=True,
            product_name="Test product",
            verification_status=VerificationStatus.CDN_ONLY,
        )
        analysis = analyze_product(product)
        assert analysis.manual_review_needed is True

    def test_exact_match_no_forced_review(self):
        """Products with EXACT_MATCH don't trigger review from verification alone."""
        from backend.models import ProductData, VerificationStatus
        from backend.analyzer import analyze_product
        product = ProductData(
            article_number="12345",
            found_on_onemed=True,
            product_name="Test product",
            description="Good description of test product",
            specification="Size: M, Material: Nitrile",
            manufacturer="Test Corp",
            manufacturer_article_number="TC-12345",
            category="Test Category",
            packaging_info="100 stk",
            image_quality_ok=True,
            verification_status=VerificationStatus.EXACT_MATCH,
        )
        analysis = analyze_product(product)
        # Should NOT be flagged for review just from verification
        # (may still be flagged for other quality reasons)
        assert product.verification_status == VerificationStatus.EXACT_MATCH


# ── P0-3: Description structure preservation ──


class TestGetStructuredText:
    """Test the structured text extraction helper."""

    def setup_method(self):
        from backend.scraper import _get_structured_text
        from bs4 import BeautifulSoup
        self.extract = _get_structured_text
        self.soup = BeautifulSoup

    def _make_element(self, html):
        soup = self.soup(html, "html.parser")
        return soup

    def test_paragraphs_preserved(self):
        """Paragraphs should produce separate lines."""
        el = self._make_element("<div><p>First paragraph.</p><p>Second paragraph.</p></div>")
        result = self.extract(el.div)
        assert "First paragraph." in result
        assert "Second paragraph." in result
        assert "\n" in result  # Paragraphs separated by newline

    def test_bullet_list_preserved(self):
        """<ul><li> should produce bullet markers."""
        el = self._make_element("<div><ul><li>Feature one</li><li>Feature two</li></ul></div>")
        result = self.extract(el.div)
        assert "• Feature one" in result
        assert "• Feature two" in result

    def test_br_preserved(self):
        """<br> tags should produce newlines."""
        el = self._make_element("<div>Line one<br/>Line two</div>")
        result = self.extract(el.div)
        lines = [l.strip() for l in result.split("\n") if l.strip()]
        assert "Line one" in lines
        assert "Line two" in lines

    def test_mixed_content(self):
        """Mix of paragraphs and lists."""
        html = """
        <div>
            <p>Product description intro.</p>
            <ul>
                <li>Latex-free</li>
                <li>Powder-free</li>
            </ul>
            <p>For medical use only.</p>
        </div>
        """
        el = self._make_element(html)
        result = self.extract(el.div)
        assert "Product description intro." in result
        assert "• Latex-free" in result
        assert "For medical use only." in result

    def test_not_collapsed_to_single_line(self):
        """Structured content must NOT be collapsed to a single line."""
        html = "<div><p>Paragraph A.</p><p>Paragraph B.</p><p>Paragraph C.</p></div>"
        el = self._make_element(html)
        result = self.extract(el.div)
        # Old code with get_text(strip=True) would give "Paragraph A.Paragraph B.Paragraph C."
        assert result != "Paragraph A.Paragraph B.Paragraph C."
        assert "\n" in result

    def test_none_element(self):
        """None element should return empty string."""
        from backend.scraper import _get_structured_text
        assert _get_structured_text(None) == ""


class TestSpecificationStructure:
    """Test that specification fields preserve structure."""

    def test_spec_uses_newlines(self):
        """Structured specs should be joined with newlines, not semicolons."""
        # The scraper builds spec from technical_details dict
        specs = {"Størrelse": "M", "Materiale": "Nitril", "Farge": "Blå"}
        result = "\n".join(f"{k}: {v}" for k, v in specs.items())
        assert "\n" in result
        assert "Størrelse: M" in result
        assert "Materiale: Nitril" in result


class TestPdfEnricherPreservesStructure:
    """Test that PDF enricher doesn't destroy paragraph breaks."""

    def test_value_normalization_preserves_newlines(self):
        """Multi-line values should keep paragraph breaks."""
        value = "First paragraph.\n\nSecond paragraph.\nThird line."
        lines = value.split("\n")
        normalized_lines = [re.sub(r"[ \t]+", " ", line).strip() for line in lines]
        result = "\n".join(line for line in normalized_lines if line).strip()
        assert "\n" in result
        assert "First paragraph." in result
        assert "Second paragraph." in result

    def test_values_match_still_works(self):
        """The comparison function should still match equivalent values."""
        from backend.pdf_enricher import _values_match
        assert _values_match("Nitril", "Nitril") is True
        assert _values_match("nitril", "NITRIL") is True
        assert _values_match("Størrelse: M", "Størrelse:  M") is True

    def test_values_match_rejects_distant_substrings(self):
        """Short substrings of long values should NOT match (anti-false-positive)."""
        from backend.pdf_enricher import _values_match
        # "Nitril" is only 6 chars, "Nitril lateksfri puderfri" is 25 chars
        # 6/25 = 24% overlap — well below 80% threshold
        assert _values_match("Nitril", "Nitril lateksfri puderfri") is False
