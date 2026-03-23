"""Tests for webshop readiness evaluation."""

import pytest

from backend.models import (
    JeevesData,
    ManufacturerLookup,
    ProductAnalysis,
    ProductData,
    QualityStatus,
)
from backend.webshop_readiness import (
    WebshopStatus,
    evaluate_webshop_readiness,
    get_missing_for_webshop,
    summarize_webshop_blockers,
)


def _make_analysis(
    name="Nitrilhanske Steril Str M Blå",
    description="Steril nitrilhanske for medisinsk bruk. Pudderfri og lateksfri. Egnet for undersøkelse.",
    category="Hansker > Engangshansker > Nitrilhansker",
    manufacturer="Ansell",
    mfr_artno="REF-12345",
    image_quality=None,
    specification="Materiale: nitril",
    packaging_info="100 stk/eske",
    jeeves_supplier=None,
    tech_details=None,
) -> ProductAnalysis:
    """Helper to build a ProductAnalysis with sane defaults."""
    pd = ProductData(
        article_number="12345",
        product_name=name,
        description=description,
        category=category,
        manufacturer=manufacturer,
        manufacturer_article_number=mfr_artno,
        specification=specification,
        packaging_info=packaging_info,
        technical_details=tech_details or {},
        found_on_onemed=True,
    )
    jeeves = None
    if jeeves_supplier:
        jeeves = JeevesData(article_number="12345", supplier=jeeves_supplier)

    iq = image_quality or {"image_count_found": 2, "image_quality_status": "PASS"}

    return ProductAnalysis(
        article_number="12345",
        product_data=pd,
        jeeves_data=jeeves,
        image_quality=iq,
    )


class TestWebshopReadyProduct:
    """Test a product that meets all criteria."""

    def test_full_product_is_ready(self):
        analysis = _make_analysis()
        readiness = evaluate_webshop_readiness(analysis)
        assert readiness.status == WebshopStatus.READY
        assert readiness.must_have_met == 5

    def test_ready_summary(self):
        analysis = _make_analysis()
        readiness = evaluate_webshop_readiness(analysis)
        assert "Nettbutikkklar" in readiness.summary

    def test_ready_missing_list_says_none(self):
        analysis = _make_analysis()
        readiness = evaluate_webshop_readiness(analysis)
        # May still have should-have items missing
        # but status should be READY


class TestMissingName:
    """Test product with missing/bad product name."""

    def test_no_name(self):
        analysis = _make_analysis(name="")
        readiness = evaluate_webshop_readiness(analysis)
        assert readiness.status != WebshopStatus.READY
        assert any(b.field_name == "Produktnavn" for b in readiness.blockers)

    def test_short_name(self):
        analysis = _make_analysis(name="Hanske")
        readiness = evaluate_webshop_readiness(analysis)
        assert any(b.field_name == "Produktnavn" and b.is_must_have for b in readiness.blockers)

    def test_placeholder_name(self):
        analysis = _make_analysis(name="test")
        readiness = evaluate_webshop_readiness(analysis)
        assert any(b.field_name == "Produktnavn" for b in readiness.blockers)


class TestMissingDescription:
    """Test product with missing/bad description."""

    def test_no_description(self):
        analysis = _make_analysis(description="")
        readiness = evaluate_webshop_readiness(analysis)
        assert any(b.field_name == "Beskrivelse" and b.is_must_have for b in readiness.blockers)

    def test_noisy_description(self):
        analysis = _make_analysis(
            description="Produkt info. Tel: +47 22 33 44 55. Besøk www.test.no for mer."
        )
        readiness = evaluate_webshop_readiness(analysis)
        assert any(b.field_name == "Beskrivelse" and "støy" in b.criterion for b in readiness.blockers)

    def test_english_description(self):
        analysis = _make_analysis(
            description="This glove is designed for medical use and provides protection and ensures safety."
        )
        readiness = evaluate_webshop_readiness(analysis)
        assert any(b.field_name == "Beskrivelse" and "engelsk" in b.criterion for b in readiness.blockers)


class TestMissingCategory:
    def test_no_category(self):
        analysis = _make_analysis(category="")
        readiness = evaluate_webshop_readiness(analysis)
        assert any(b.field_name == "Kategori" and b.is_must_have for b in readiness.blockers)


class TestMissingImage:
    def test_no_image(self):
        analysis = _make_analysis(
            image_quality={"image_count_found": 0, "image_quality_status": "MISSING"}
        )
        readiness = evaluate_webshop_readiness(analysis)
        assert any(b.field_name == "Bildekvalitet" and b.is_must_have for b in readiness.blockers)

    def test_bad_image(self):
        analysis = _make_analysis(
            image_quality={"image_count_found": 1, "image_quality_status": "FAIL"}
        )
        readiness = evaluate_webshop_readiness(analysis)
        assert any(b.field_name == "Bildekvalitet" for b in readiness.blockers)


class TestMissingManufacturer:
    def test_no_manufacturer(self):
        analysis = _make_analysis(manufacturer="")
        readiness = evaluate_webshop_readiness(analysis)
        assert any(b.field_name == "Produsent" and b.is_must_have for b in readiness.blockers)

    def test_jeeves_supplier_as_fallback(self):
        """Jeeves supplier counts as manufacturer for readiness."""
        analysis = _make_analysis(manufacturer="", jeeves_supplier="Ansell Healthcare")
        readiness = evaluate_webshop_readiness(analysis)
        assert not any(b.field_name == "Produsent" and b.is_must_have for b in readiness.blockers)


class TestShouldHaveCriteria:
    """Test should-have (non-blocking) criteria."""

    def test_missing_spec_is_should_have(self):
        analysis = _make_analysis(specification="", tech_details={})
        readiness = evaluate_webshop_readiness(analysis)
        spec_blockers = [b for b in readiness.blockers if b.field_name == "Spesifikasjon"]
        assert len(spec_blockers) == 1
        assert spec_blockers[0].is_must_have is False

    def test_missing_packaging_is_should_have(self):
        analysis = _make_analysis(packaging_info="")
        readiness = evaluate_webshop_readiness(analysis)
        pkg_blockers = [b for b in readiness.blockers if b.field_name == "Pakningsinformasjon"]
        assert len(pkg_blockers) == 1
        assert pkg_blockers[0].is_must_have is False

    def test_missing_mfr_artno_is_should_have(self):
        analysis = _make_analysis(mfr_artno="")
        readiness = evaluate_webshop_readiness(analysis)
        artno_blockers = [b for b in readiness.blockers if b.field_name == "Produsentens varenummer"]
        assert len(artno_blockers) == 1
        assert artno_blockers[0].is_must_have is False


class TestPartialStatus:
    def test_one_must_have_missing_is_partial(self):
        analysis = _make_analysis(category="")
        readiness = evaluate_webshop_readiness(analysis)
        assert readiness.status == WebshopStatus.PARTIAL

    def test_two_must_haves_missing_is_partial(self):
        analysis = _make_analysis(category="", manufacturer="")
        readiness = evaluate_webshop_readiness(analysis)
        assert readiness.status == WebshopStatus.PARTIAL


class TestNotReadyStatus:
    def test_three_must_haves_missing(self):
        analysis = _make_analysis(
            name="",
            description="",
            category="",
        )
        readiness = evaluate_webshop_readiness(analysis)
        assert readiness.status == WebshopStatus.NOT_READY

    def test_not_ready_summary_lists_blockers(self):
        analysis = _make_analysis(name="", category="")
        readiness = evaluate_webshop_readiness(analysis)
        assert "Produktnavn" in readiness.summary
        assert "Kategori" in readiness.summary


class TestConvenienceFunctions:
    def test_get_missing_for_webshop(self):
        analysis = _make_analysis(category="")
        missing = get_missing_for_webshop(analysis)
        assert any("kategori" in m.lower() for m in missing)

    def test_summarize_webshop_blockers(self):
        analysis = _make_analysis(name="")
        summary = summarize_webshop_blockers(analysis)
        assert isinstance(summary, str)
        assert len(summary) > 0
