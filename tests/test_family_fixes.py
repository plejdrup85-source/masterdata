"""Tests for the family detector P0/P1 fixes:
  P0-1: Specification field parsed for variant dimensions
  P0-2: Zero-dimension families capped at 0.40 confidence
  P0-3: Unknown-brand families conservative
  P1-3: Family naming uses original capitalization
"""

from backend.family_detector import (
    _extract_variant_dimensions,
    _score_family,
    _build_family_name,
    _ProductRecord,
    detect_families,
)


class TestSpecificationParsing:
    """P0-1: Specification text is now parsed for variant dimensions."""

    def test_size_from_spec(self):
        """Size in spec field: 'M rosa AQL 1,5' → Størrelse=M"""
        dims = _extract_variant_dimensions("Hanske us nitril evercare", "M rosa AQL 1,5")
        dim_names = {d.dimension_name for d in dims}
        assert "Størrelse" in dim_names
        size_dim = next(d for d in dims if d.dimension_name == "Størrelse")
        assert size_dim.value == "M"
        assert size_dim.source == "spec"

    def test_gauge_from_spec(self):
        """Gauge in spec: '18G x 40mm Rosa' → Gauge=18G"""
        dims = _extract_variant_dimensions("Kanyle KD-Fine", "18G x 40mm Rosa")
        dim_names = {d.dimension_name for d in dims}
        assert "Gauge" in dim_names
        assert "Farge" in dim_names
        gauge = next(d for d in dims if d.dimension_name == "Gauge")
        assert gauge.value == "18G"
        assert gauge.source == "spec"

    def test_dimension_from_spec(self):
        """Dimensions in spec: '5x5cm usteril dispenserboks' → Dimensjon=5x5cm"""
        dims = _extract_variant_dimensions("Kompress NW 4-lag Embra", "5x5cm usteril dispenserboks")
        dim_names = {d.dimension_name for d in dims}
        assert "Dimensjon" in dim_names

    def test_volume_and_color_from_spec(self):
        """Volume + color: '30ml smal blå' → Volum=30ml, Farge=Blå"""
        dims = _extract_variant_dimensions("Medisinbeger plast Eco", "30ml smal blå")
        dim_names = {d.dimension_name for d in dims}
        assert "Volum" in dim_names
        assert "Farge" in dim_names

    def test_ch_from_spec(self):
        """CH size: '10ml ch14 40cm' → CH=CH14"""
        dims = _extract_variant_dimensions("Kateter 2v AquaFlate", "10ml ch14 40cm helsilikon Nel")
        dim_names = {d.dimension_name for d in dims}
        assert "CH" in dim_names

    def test_name_takes_priority_over_spec(self):
        """If dimension is in both name and spec, name wins."""
        dims = _extract_variant_dimensions("Hanske L nitril", "L rosa")
        size_dims = [d for d in dims if d.dimension_name == "Størrelse"]
        assert len(size_dims) == 1  # Not duplicated
        assert size_dims[0].source == "name"

    def test_empty_spec_still_works(self):
        """Empty spec doesn't break extraction."""
        dims = _extract_variant_dimensions("Hanske nitril M", "")
        assert any(d.dimension_name == "Størrelse" for d in dims)

    def test_no_spec_no_name_dims(self):
        """Product with no variant info in either field."""
        dims = _extract_variant_dimensions("Stetoskop", "Standard")
        assert len(dims) == 0


class TestConfidenceCap:
    """P0-2: Families with zero variant dims capped at 0.40."""

    def _make_records(self, names, brand="TestBrand", specs=None):
        records = []
        for i, name in enumerate(names):
            spec = (specs[i] if specs else "") or ""
            records.append(_ProductRecord(
                article_number=f"T{i:04d}",
                product_name=name,
                brand=brand,
                specification=spec,
                technical_details={},
                category="Test",
                base_name=name.lower(),
                variant_dims=_extract_variant_dimensions(name, spec),
            ))
        return records

    def test_zero_dims_capped(self):
        """Family with identical names, no dims in name or spec → capped at 0.40."""
        records = self._make_records(
            ["ProductX"] * 5,
            specs=["variant A", "variant B", "variant C", "variant D", "variant E"],
        )
        score, reason, signals = _score_family(records)
        assert score <= 0.40
        assert any("Konfidenstak" in s or "maks 0.40" in s for s in signals)

    def test_with_dims_not_capped(self):
        """Family with real variant dims → NOT capped."""
        records = self._make_records(
            ["Hanske nitril"] * 4,
            specs=["S rosa", "M rosa", "L rosa", "XL rosa"],
        )
        score, reason, signals = _score_family(records)
        assert score > 0.40  # Has size dims → not capped


class TestUnknownBrandConservative:
    """P0-3: Unknown-brand families are more conservative."""

    def _make_records(self, names, brand="", specs=None):
        records = []
        for i, name in enumerate(names):
            spec = (specs[i] if specs else "") or ""
            records.append(_ProductRecord(
                article_number=f"T{i:04d}",
                product_name=name,
                brand=brand,
                specification=spec,
                technical_details={},
                category="",
                base_name=name.lower(),
                variant_dims=_extract_variant_dimensions(name, spec),
            ))
        return records

    def test_no_brand_no_dims_capped_at_030(self):
        """No brand + no dims → capped at 0.30."""
        records = self._make_records(["GenericProduct"] * 5)
        score, reason, signals = _score_family(records)
        assert score <= 0.30
        assert any("maks 0.30" in s for s in signals)

    def test_no_brand_with_dims_still_ok(self):
        """No brand but real dims → still allowed (dims are strong evidence)."""
        records = self._make_records(
            ["Kompress NW"] * 3,
            specs=["5x5cm", "10x10cm", "10x20cm"],
        )
        score, _, _ = _score_family(records)
        # Has dimension variants, so not capped at 0.30
        assert score > 0.30


class TestFamilyNaming:
    """P1-3: Family names use original capitalization."""

    def test_original_capitalization_preserved(self):
        records = [
            _ProductRecord("A1", "Hanske us nitril Evercare", "", "", {}, "", "hanske us nitril evercare", []),
            _ProductRecord("A2", "Hanske us nitril Evercare", "", "", {}, "", "hanske us nitril evercare", []),
        ]
        name = _build_family_name(records)
        assert name == "Hanske us nitril Evercare"  # Original case, not lowercased

    def test_most_common_name_wins(self):
        records = [
            _ProductRecord("A1", "Bandasje Mepilex Border", "", "", {}, "", "", []),
            _ProductRecord("A2", "Bandasje Mepilex Border", "", "", {}, "", "", []),
            _ProductRecord("A3", "bandasje mepilex border", "", "", {}, "", "", []),
        ]
        name = _build_family_name(records)
        assert name == "Bandasje Mepilex Border"  # The majority form


class TestDetectFamiliesIntegration:
    """Integration tests for detect_families with spec parsing."""

    def test_gloves_from_spec(self):
        """Gloves: name identical, sizes in spec → detect Størrelse dimension."""
        products = [
            {"article_number": f"G{i}", "product_name": "Hanske us nitril evercare",
             "brand": "Evercare", "supplier": "", "specification": spec,
             "technical_details": {}, "category": "Hansker"}
            for i, spec in enumerate([
                "XS rosa AQL 1,5", "S rosa AQL 1,5", "M rosa AQL 1,5",
                "L rosa AQL 1,5", "XL rosa AQL 1,5",
            ])
        ]
        families, members = detect_families(products)
        assert len(families) == 1
        f = families[0]
        assert "Størrelse" in f.variant_dimension_names
        assert f.confidence >= 0.65
        assert not f.review_required

    def test_needles_from_spec(self):
        """Needles: name identical, gauge+length+color in spec."""
        products = [
            {"article_number": f"N{i}", "product_name": "Kanyle KD-Fine",
             "brand": "KD", "supplier": "", "specification": spec,
             "technical_details": {}, "category": "Kanyler"}
            for i, spec in enumerate([
                "18G x 40mm Rosa", "21G x 40mm Grønn", "23G x 25mm Blå",
            ])
        ]
        families, _ = detect_families(products)
        assert len(families) == 1
        f = families[0]
        assert "Gauge" in f.variant_dimension_names

    def test_zero_dim_family_capped_in_output(self):
        """Family with no parseable dims gets capped confidence in output."""
        products = [
            {"article_number": f"X{i}", "product_name": "Spesialprodukt ABC",
             "brand": "ABC Corp", "supplier": "", "specification": spec,
             "technical_details": {}, "category": ""}
            for i, spec in enumerate(["variant A", "variant B", "variant C"])
        ]
        families, _ = detect_families(products)
        assert len(families) == 1
        assert families[0].confidence <= 0.40
        assert families[0].review_required is True
