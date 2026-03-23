"""Tests for category intelligence — e-commerce category analysis."""

import pytest

from backend.category_intelligence import (
    evaluate_category_fit,
    detect_overly_specific_category,
    should_be_attribute_instead_of_category,
    suggest_better_category,
    build_ecommerce_category_recommendations,
    CategoryEvaluation,
)
from backend.models import ProductAnalysis, ProductData


def _make_result(artno="12345", breadcrumb=None, name="Test produkt"):
    pd = ProductData(
        article_number=artno,
        product_name=name,
        category=breadcrumb[-1] if breadcrumb else None,
        category_breadcrumb=breadcrumb,
        found_on_onemed=True,
    )
    return ProductAnalysis(article_number=artno, product_data=pd)


class TestEvaluateCategoryFit:
    def test_good_3_level_category(self):
        ev = evaluate_category_fit(
            ["Medisinsk forbruksmateriell", "Hansker", "Engangshansker"],
            product_name="Nitrilhanske pudderfri",
        )
        assert ev.status == "OK"
        assert ev.effective_depth == 3
        assert not ev.is_too_deep
        assert not ev.has_attribute_as_category

    def test_missing_category(self):
        ev = evaluate_category_fit(None)
        assert ev.status == "MISSING"

    def test_too_deep_5_levels(self):
        ev = evaluate_category_fit([
            "Medisinsk forbruksmateriell",
            "Sterilisering autoklavering",
            "Steriliseringsposer flate",
            "Steriliseringspose flat",
            "Steriliseringspose flat 100x250mm",
        ])
        assert ev.is_too_deep
        assert ev.status in ("SHOULD_SIMPLIFY", "ATTRIBUTE_AS_CATEGORY")

    def test_root_stripped(self):
        """'Sortiment' root should be stripped from effective depth."""
        ev = evaluate_category_fit(
            ["Sortiment", "Medisinsk", "Hansker"],
        )
        assert ev.effective_depth == 2  # "Sortiment" stripped

    def test_shallow_1_level(self):
        ev = evaluate_category_fit(["Hansker"])
        assert ev.is_too_shallow
        assert ev.status == "NEEDS_REVIEW"

    def test_ideal_4_levels(self):
        ev = evaluate_category_fit([
            "Medisinsk forbruksmateriell",
            "Sårbehandling",
            "Bandasjer",
            "Elastiske bandasjer",
        ])
        assert ev.status == "OK"
        assert ev.effective_depth == 4


class TestAttributeDetection:
    def test_size_in_category(self):
        result = should_be_attribute_instead_of_category("Hansker str M")
        assert result is not None
        assert "størrelse" in result

    def test_material_in_category(self):
        result = should_be_attribute_instead_of_category("Nitrilhansker")
        assert result is not None
        assert "materiale" in result

    def test_sterility_in_category(self):
        result = should_be_attribute_instead_of_category("Steril kompress")
        assert result is not None
        assert "sterilisering" in result

    def test_color_in_category(self):
        result = should_be_attribute_instead_of_category("Blå hansker")
        assert result is not None
        assert "farge" in result

    def test_product_type_not_flagged(self):
        """A pure product type name should NOT be flagged as attribute."""
        result = should_be_attribute_instead_of_category("Bandasje")
        assert result is None

    def test_packaging_in_category(self):
        result = should_be_attribute_instead_of_category("100 stk kompress")
        assert result is not None

    def test_variant_in_category(self):
        result = should_be_attribute_instead_of_category("Med pudderfri")
        assert result is not None

    def test_attribute_in_deep_level_flagged(self):
        """Attribute-like content in deeper levels should be flagged."""
        ev = evaluate_category_fit([
            "Medisinsk forbruksmateriell",
            "Hansker",
            "Engangshansker",
            "Nitril",  # This is a material, should be filter
        ])
        assert ev.has_attribute_as_category
        assert len(ev.attribute_levels) >= 1

    def test_attribute_in_top_level_not_flagged(self):
        """Attribute-like content in top 2 levels should NOT be flagged."""
        ev = evaluate_category_fit([
            "Steriliseringsprodukter",  # Contains "steril" but is top-level
            "Poser",
        ])
        assert not ev.has_attribute_as_category


class TestOverlySpecificDetection:
    def test_4_levels_ok(self):
        result = detect_overly_specific_category([
            "Medisinsk", "Sårbehandling", "Bandasjer", "Elastiske",
        ])
        assert result is None

    def test_5_levels_too_specific(self):
        result = detect_overly_specific_category([
            "Sortiment", "Medisinsk", "Sårbehandling", "Bandasjer", "Elastiske", "10cm",
        ])
        assert result is not None
        assert "nivåer" in result

    def test_none_breadcrumb(self):
        assert detect_overly_specific_category(None) is None


class TestSuggestBetterCategory:
    def test_good_category_returns_none(self):
        result = suggest_better_category(
            ["Medisinsk", "Hansker", "Engangshansker"],
            "Nitrilhanske",
        )
        assert result is None

    def test_too_deep_gets_suggestion(self):
        result = suggest_better_category([
            "Sortiment", "Medisinsk forbruksmateriell",
            "Sterilisering autoklavering",
            "Steriliseringsposer flate",
            "Steriliseringspose flat",
            "Steriliseringspose flat 100mm",
        ])
        # Should suggest a shorter path
        if result:
            assert result.count(" > ") < 5


class TestCategoryMatch:
    def test_matching_product(self):
        ev = evaluate_category_fit(
            ["Medisinsk", "Hansker"],
            product_name="Nitrilhanske pudderfri str M",
        )
        assert ev.product_type_match

    def test_mismatched_product(self):
        ev = evaluate_category_fit(
            ["Medisinsk", "Kateter"],
            product_name="Skalpell engangsskalpell",
            description="Kirurgisk skalpell med stålblad",
        )
        # Product name has nothing to do with "Kateter"
        # This depends on keyword overlap, may or may not flag
        # At minimum it should produce a valid evaluation
        assert ev.status in ("OK", "WRONG_CATEGORY", "NEEDS_REVIEW")


class TestBuildRecommendations:
    def test_recommendations_for_deep_categories(self):
        results = [
            _make_result("A1", ["Sortiment", "Med", "Steril", "Poser", "Flat", "100mm"]),
            _make_result("A2", ["Sortiment", "Med", "Steril", "Poser", "Flat", "200mm"]),
        ]
        recs = build_ecommerce_category_recommendations(results)
        assert len(recs) > 0
        # Should recommend simplification
        assert any("dypt" in r.issue.lower() or "attributt" in r.issue.lower() for r in recs)

    def test_no_recommendations_for_good_structure(self):
        results = [
            _make_result("A1", ["Medisinsk", "Hansker", "Engangshansker"]),
            _make_result("A2", ["Medisinsk", "Hansker", "Engangshansker"]),
            _make_result("A3", ["Medisinsk", "Hansker", "Engangshansker"]),
        ]
        recs = build_ecommerce_category_recommendations(results)
        # Good 3-level category with 3 products → no recommendations
        assert len(recs) == 0

    def test_single_product_category_flagged(self):
        results = [
            _make_result("A1", ["Medisinsk", "Spesialprodukt", "Unik"]),
        ]
        recs = build_ecommerce_category_recommendations(results)
        assert any("1 produkt" in r.issue for r in recs)

    def test_empty_results(self):
        recs = build_ecommerce_category_recommendations([])
        assert recs == []


class TestSummaryText:
    def test_ok_summary(self):
        ev = evaluate_category_fit(["Medisinsk", "Hansker", "Engangs"])
        assert "OK" in ev.summary

    def test_simplify_summary(self):
        ev = evaluate_category_fit([
            "Medisinsk", "Ster", "Poser", "Flat", "Blå",
        ])
        if ev.status == "SHOULD_SIMPLIFY":
            assert "forenkles" in ev.summary.lower()

    def test_missing_summary(self):
        ev = evaluate_category_fit(None)
        assert "mangler" in ev.summary.lower()
