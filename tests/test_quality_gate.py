"""Tests for the mandatory quality gate.

Verifies that garbage data is blocked before reaching Excel output.
Each test represents a real failure mode discovered in production output.
"""

import pytest
from backend.quality_gate import quality_gate_check, GateResult


class TestPhoneNumberRejection:
    """Phone numbers must NEVER appear in product suggestions."""

    def test_labeled_phone(self):
        result = quality_gate_check(
            "Kontakt oss: Tel: +47 22 04 72 00",
            "Beskrivelse",
        )
        assert not result.passed
        assert "telefon" in result.reason.lower()

    def test_bare_phone_with_spaces(self):
        result = quality_gate_check(
            "22 04 72 00",
            "Beskrivelse",
        )
        assert not result.passed

    def test_bare_phone_no_spaces(self):
        result = quality_gate_check(
            "22047200",
            "Spesifikasjon",
        )
        assert not result.passed

    def test_international_phone(self):
        result = quality_gate_check(
            "+47 905 12 345",
            "Beskrivelse",
        )
        assert not result.passed

    def test_fax_number(self):
        result = quality_gate_check(
            "Fax: 0800-123-456",
            "Beskrivelse",
        )
        assert not result.passed

    def test_phone_in_description(self):
        result = quality_gate_check(
            "Sterile hansker i nitril. For bestilling ring tlf 22 33 44 55.",
            "Beskrivelse",
        )
        assert not result.passed

    def test_valid_product_dimensions_not_rejected(self):
        """Ensure product dimensions like '100 x 200 mm' are NOT rejected as phones."""
        result = quality_gate_check(
            "Kompress, steril, 10 x 20 cm, lateksfri",
            "Beskrivelse",
            confidence=0.7,
        )
        assert result.passed

    def test_valid_measurement_not_rejected(self):
        """Ensure measurements like '25 mm' are NOT rejected."""
        result = quality_gate_check(
            "Diameter: 25 mm, lengde: 150 mm",
            "Spesifikasjon",
            confidence=0.7,
        )
        assert result.passed


class TestEmailRejection:
    def test_email_in_text(self):
        result = quality_gate_check(
            "Kontakt support@onemed.no for mer info",
            "Beskrivelse",
        )
        assert not result.passed
        assert "e-post" in result.reason.lower()


class TestArticleNumberDump:
    def test_article_number_list(self):
        result = quality_gate_check(
            "Produkter: 222001, 222002, 222003, 222004, 222005",
            "Beskrivelse",
            current_sku="100001",
        )
        assert not result.passed
        assert "artikkelnumre" in result.reason.lower()

    def test_own_sku_allowed(self):
        result = quality_gate_check(
            "Produkt 222001 er en steril hanske i nitril for undersøkelse.",
            "Beskrivelse",
            current_sku="222001",
            confidence=0.7,
        )
        assert result.passed


class TestURLInTextField:
    def test_url_in_description(self):
        result = quality_gate_check(
            "Se https://www.example.com/product for detaljer",
            "Beskrivelse",
        )
        assert not result.passed
        assert "URL" in result.reason

    def test_www_in_description(self):
        result = quality_gate_check(
            "Besøk www.molnlycke.com for mer informasjon",
            "Beskrivelse",
        )
        assert not result.passed


class TestContactSectionRejection:
    def test_contact_us(self):
        result = quality_gate_check(
            "For more information contact our customer support team.",
            "Beskrivelse",
        )
        assert not result.passed

    def test_approved_by(self):
        result = quality_gate_check(
            "Approved by John Smith, Quality Manager, 2024-01-15",
            "Beskrivelse",
        )
        assert not result.passed


class TestPDFNoiseRejection:
    def test_page_number(self):
        result = quality_gate_check(
            "Side 3 av 12. Produktdatablad versjon 2.1",
            "Beskrivelse",
        )
        assert not result.passed
        assert "PDF-støy" in result.reason

    def test_date_version(self):
        result = quality_gate_check(
            "Dato: 2024-01-15. Version: 3.2",
            "Beskrivelse",
        )
        assert not result.passed


class TestDrawingNoiseRejection:
    def test_view_labels(self):
        result = quality_gate_check(
            "Side view. Front view. Total length 250mm. Scale: 1:2",
            "Beskrivelse",
        )
        assert not result.passed
        assert "tegning" in result.reason.lower()


class TestVariantTableRejection:
    def test_size_code_table(self):
        result = quality_gate_check(
            "Størrelseskode: XS=5, S=6, M=7, L=8, XL=9",
            "Beskrivelse",
        )
        assert not result.passed
        assert "varianttabell" in result.reason.lower()


class TestEmptyAndTrivial:
    def test_empty_string(self):
        result = quality_gate_check("", "Beskrivelse")
        assert not result.passed

    def test_whitespace_only(self):
        result = quality_gate_check("   ", "Beskrivelse")
        assert not result.passed

    def test_too_short_description(self):
        result = quality_gate_check("Kort", "Beskrivelse")
        assert not result.passed

    def test_only_numbers(self):
        result = quality_gate_check("12345 67890", "Beskrivelse")
        assert not result.passed

    def test_only_punctuation(self):
        result = quality_gate_check("---/---", "Spesifikasjon")
        assert not result.passed


class TestDuplicateRejection:
    def test_identical_to_current(self):
        result = quality_gate_check(
            "Nitrilhansker, pudderfri, blå",
            "Produktnavn",
            current_value="Nitrilhansker, pudderfri, blå",
        )
        assert not result.passed
        assert "identisk" in result.reason.lower()

    def test_identical_after_normalization(self):
        result = quality_gate_check(
            "  Nitrilhansker, pudderfri, blå.  ",
            "Produktnavn",
            current_value="Nitrilhansker, pudderfri, blå",
        )
        assert not result.passed


class TestFieldSpecificValidation:
    def test_product_name_too_long(self):
        result = quality_gate_check(
            "A" * 250,
            "Produktnavn",
        )
        assert not result.passed
        assert "for langt" in result.reason.lower()

    def test_product_name_with_sentences(self):
        result = quality_gate_check(
            "Hanske. For undersøkelse. Laget av nitril. Pudderfri. Steril.",
            "Produktnavn",
        )
        assert not result.passed

    def test_description_too_few_words(self):
        result = quality_gate_check(
            "Hanske blå",
            "Beskrivelse",
        )
        assert not result.passed

    def test_packaging_without_packaging_data(self):
        result = quality_gate_check(
            "Hansken er designet for undersøkelse av pasienter med sensitiv hud og allergier",
            "Pakningsinformasjon",
        )
        assert not result.passed

    def test_valid_packaging(self):
        result = quality_gate_check(
            "100 stk per eske, 10 esker per kartong",
            "Pakningsinformasjon",
            confidence=0.7,
        )
        assert result.passed


class TestConfidenceFloor:
    def test_very_low_confidence_rejected(self):
        result = quality_gate_check(
            "Steril kompress for sårbehandling",
            "Beskrivelse",
            confidence=0.15,
        )
        assert not result.passed
        assert "confidence" in result.reason.lower()

    def test_acceptable_confidence_passes(self):
        result = quality_gate_check(
            "Steril kompress for sårbehandling, lateksfri og hypoallergen.",
            "Beskrivelse",
            confidence=0.60,
        )
        assert result.passed


class TestValidSuggestionsPass:
    """Verify that good suggestions pass the quality gate."""

    def test_good_description(self):
        result = quality_gate_check(
            "Steril kompress i bomull for sårbehandling. Lateksfri og hypoallergen. "
            "Egnet for primær sårbehandling og postoperativ pleie.",
            "Beskrivelse",
            confidence=0.75,
        )
        assert result.passed

    def test_good_product_name(self):
        result = quality_gate_check(
            "SELEFA Undersøkelseshanske Nitril Blå S",
            "Produktnavn",
            confidence=0.80,
        )
        assert result.passed

    def test_good_specification(self):
        result = quality_gate_check(
            "Materiale: Nitril; Størrelse: S; Farge: Blå; Pudderfri: Ja",
            "Spesifikasjon",
            confidence=0.70,
        )
        assert result.passed

    def test_good_manufacturer(self):
        result = quality_gate_check(
            "Mölnlycke Health Care",
            "Produsent",
            confidence=0.90,
        )
        assert result.passed


class TestGateResult:
    def test_bool_true(self):
        r = GateResult(True)
        assert r
        assert bool(r) is True

    def test_bool_false(self):
        r = GateResult(False, "test reason")
        assert not r
        assert bool(r) is False
        assert r.reason == "test reason"
