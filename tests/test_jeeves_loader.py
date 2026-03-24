"""Tests for Jeeves Excel loader — column mapping and supplier field flow."""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from openpyxl import Workbook

from backend.jeeves_loader import (
    JeevesIndex,
    _match_header,
    _normalize_header,
    _HEADER_VARIANTS,
    _POSITION_FALLBACK,
)


@pytest.fixture
def tmp_excel(tmp_path):
    """Create a temporary Excel file for testing."""
    def _make(headers, rows):
        wb = Workbook()
        ws = wb.active
        ws.append(headers)
        for row in rows:
            ws.append(row)
        path = tmp_path / "test_catalog.xlsx"
        wb.save(str(path))
        return str(path)
    return _make


class TestHeaderMatching:
    """Test that all expected header variants match correctly."""

    def test_english_headers(self):
        assert _match_header("Item. No") == "article_number"
        assert _match_header("Supplier") == "supplier"
        assert _match_header("Supplier Item.no") == "supplier_item_no"
        assert _match_header("Product Brand") == "product_brand"
        assert _match_header("Web Title") == "web_title"
        assert _match_header("Web Text") == "web_text"

    def test_norwegian_headers(self):
        """The user's definitive column names must work."""
        assert _match_header("Vårt art.nr") == "article_number"
        assert _match_header("Vårt GID nr") == "gid"
        assert _match_header("Varebeskrivelse") == "item_description"
        assert _match_header("Spesifikasjon") == "specification"
        assert _match_header("Produsent") == "supplier"
        assert _match_header("Produsent art.nr") == "supplier_item_no"

    def test_parenthetical_variants(self):
        """Headers with parenthetical annotations must work."""
        assert _match_header("Produsent (Supplier)") == "supplier"
        assert _match_header("Produsent art.nr (Supplier item number)") == "supplier_item_no"

    def test_case_insensitive(self):
        assert _match_header("SUPPLIER") == "supplier"
        assert _match_header("supplier") == "supplier"
        assert _match_header("Supplier Item.No") == "supplier_item_no"
        assert _match_header("ITEM. NO") == "article_number"

    def test_whitespace_tolerance(self):
        assert _match_header("  Supplier  ") == "supplier"
        assert _match_header("Web Title ") == "web_title"  # trailing space

    def test_unknown_header_returns_none(self):
        assert _match_header("Foobar") is None
        assert _match_header("") is None
        assert _match_header(None) is None


class TestJeevesLoaderEnglishHeaders:
    """Test loading with English headers (the original format)."""

    def test_loads_supplier_correctly(self, tmp_excel):
        path = tmp_excel(
            ["Item. No", "GID", "Item description", "Specification",
             "Supplier", "Supplier Item.no", "Product Brand", "Web Title", "Web Text"],
            [
                ["N245100189090", "GID001", "Endotrakealtube SACETT", "Størrelse 7.0",
                 "ICU Medical International Limited", "SACETT-01", "SACETT", "SACETT tube", "Desc"],
                ["222001", "GID002", "Hanske Nitril", "Str M",
                 "OneMed AB", "SEL-001", "Selefa", "Hanske", "Text"],
            ]
        )
        idx = JeevesIndex()
        count = idx.load(path)
        assert count == 2

        jd = idx.get("N245100189090")
        assert jd is not None
        assert jd.supplier == "ICU Medical International Limited"
        assert jd.supplier_item_no == "SACETT-01"
        assert jd.item_description == "Endotrakealtube SACETT"

    def test_missing_supplier_is_none(self, tmp_excel):
        path = tmp_excel(
            ["Item. No", "GID", "Item description", "Specification",
             "Supplier", "Supplier Item.no", "Product Brand", "Web Title", "Web Text"],
            [["111", "G1", "Produkt 1", "Spec", None, None, None, None, None]],
        )
        idx = JeevesIndex()
        idx.load(path)
        jd = idx.get("111")
        assert jd is not None
        assert jd.supplier is None
        assert jd.supplier_item_no is None


class TestJeevesLoaderNorwegianHeaders:
    """Test loading with Norwegian headers (the user's FASIT format)."""

    def test_norwegian_headers_map_correctly(self, tmp_excel):
        path = tmp_excel(
            ["Vårt art.nr", "Vårt GID nr", "Varebeskrivelse", "Spesifikasjon",
             "Produsent", "Produsent art.nr", "Product brand", "Web Title", "Web Text"],
            [
                ["N245100189090", "GID001", "Endotrakealtube SACETT", "Størrelse 7.0",
                 "ICU Medical International Limited", "SACETT-01", "SACETT", "SACETT tube", "Desc"],
            ]
        )
        idx = JeevesIndex()
        count = idx.load(path)
        assert count == 1

        jd = idx.get("N245100189090")
        assert jd is not None
        assert jd.supplier == "ICU Medical International Limited"
        assert jd.supplier_item_no == "SACETT-01"
        assert jd.item_description == "Endotrakealtube SACETT"
        assert jd.specification == "Størrelse 7.0"
        assert jd.product_brand == "SACETT"

    def test_parenthetical_headers(self, tmp_excel):
        """Headers like 'Produsent (Supplier)' should work."""
        path = tmp_excel(
            ["Vårt art.nr", "Vårt GID nr", "Varebeskrivelse", "Spesifikasjon",
             "Produsent (Supplier)", "Produsent art.nr (Supplier item number)",
             "Product brand", "Web Title", "Web Text"],
            [
                ["999", "G1", "Test", "Spec", "TestProd", "TP-001", "Brand", "Title", "Text"],
            ]
        )
        idx = JeevesIndex()
        idx.load(path)
        jd = idx.get("999")
        assert jd.supplier == "TestProd"
        assert jd.supplier_item_no == "TP-001"


class TestPositionFallback:
    """Test that column position fallback works when headers don't match."""

    def test_fallback_to_positions(self, tmp_excel):
        """Completely unknown headers should fall back to position mapping."""
        path = tmp_excel(
            ["Kol A", "Kol B", "Kol C", "Kol D",
             "Kol E", "Kol F", "Kol G", "Kol H", "Kol I"],
            [
                ["N245100189090", "GID001", "Endotrakealtube SACETT", "Størrelse 7.0",
                 "ICU Medical International Limited", "SACETT-01", "SACETT", "SACETT tube", "Desc"],
            ]
        )
        idx = JeevesIndex()
        count = idx.load(path)
        assert count == 1

        jd = idx.get("N245100189090")
        assert jd is not None
        assert jd.supplier == "ICU Medical International Limited"
        assert jd.supplier_item_no == "SACETT-01"


class TestSupplierNotLostInPipeline:
    """Verify supplier fields flow all the way from JeevesData to output."""

    def test_resolve_manufacturer_uses_jeeves_supplier(self):
        from backend.models import JeevesData, ProductData
        from backend.content_validator import resolve_manufacturer

        jeeves = JeevesData(
            article_number="N245100189090",
            supplier="ICU Medical International Limited",
            supplier_item_no="SACETT-01",
        )
        product = ProductData(
            article_number="N245100189090",
            product_name="Endotrakealtube SACETT",
            manufacturer=None,  # Website didn't find manufacturer
        )

        name, artnr, source = resolve_manufacturer(product, jeeves)
        assert name == "ICU Medical International Limited"
        assert artnr == "SACETT-01"
        assert source == "katalog"

    def test_get_best_producer_info_uses_jeeves(self):
        from backend.models import JeevesData, ProductData
        from backend.content_validator import get_best_producer_info

        jeeves = JeevesData(
            article_number="N245100189090",
            supplier="ICU Medical International Limited",
            supplier_item_no="SACETT-01",
        )
        product = ProductData(article_number="N245100189090")

        name, artnr = get_best_producer_info(product, jeeves)
        assert name == "ICU Medical International Limited"
        assert artnr == "SACETT-01"

    def test_jeeves_takes_priority_over_website(self):
        from backend.models import JeevesData, ProductData
        from backend.content_validator import resolve_manufacturer

        jeeves = JeevesData(
            article_number="111",
            supplier="Riktig Produsent AS",
            supplier_item_no="RP-001",
        )
        product = ProductData(
            article_number="111",
            manufacturer="Feil Produsent",
            manufacturer_article_number="FP-999",
        )

        name, artnr, source = resolve_manufacturer(product, jeeves)
        assert name == "Riktig Produsent AS"
        assert artnr == "RP-001"
        assert source == "katalog"

    def test_website_used_when_jeeves_empty(self):
        from backend.models import JeevesData, ProductData
        from backend.content_validator import resolve_manufacturer

        jeeves = JeevesData(article_number="222")  # No supplier
        product = ProductData(
            article_number="222",
            manufacturer="Website Manufacturer",
            manufacturer_article_number="WM-001",
        )

        name, artnr, source = resolve_manufacturer(product, jeeves)
        assert name == "Website Manufacturer"
        assert artnr == "WM-001"
        assert source == "nettside"


class TestSupplierStats:
    def test_supplier_stats(self, tmp_excel):
        path = tmp_excel(
            ["Item. No", "GID", "Item description", "Specification",
             "Supplier", "Supplier Item.no", "Product Brand", "Web Title", "Web Text"],
            [
                ["111", None, None, None, "Prod A", "PA-01", None, None, None],
                ["222", None, None, None, None, None, None, None, None],
                ["333", None, None, None, "Prod B", None, None, None, None],
            ]
        )
        idx = JeevesIndex()
        idx.load(path)
        stats = idx.supplier_stats()
        assert stats["total"] == 3
        assert stats["with_supplier"] == 2
        assert stats["with_supplier_item_no"] == 1
        assert stats["without_supplier"] == 1
