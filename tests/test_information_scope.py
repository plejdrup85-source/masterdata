"""Tests for information scope detection — variant, family, SKU, and packaging levels."""

import pytest

from backend.information_scope import (
    InformationScope,
    ScopeResult,
    adjust_confidence_for_scope,
    block_family_content_for_sku,
    detect_information_scope,
    is_family_level_content,
    is_packaging_level_content,
    is_variant_specific_content,
)


class TestDetectInformationScope:
    """Test the core detect_information_scope function."""

    def test_empty_text_returns_unknown(self):
        result = detect_information_scope("")
        assert result.scope == InformationScope.UNKNOWN

    def test_none_like_text(self):
        result = detect_information_scope("   ")
        assert result.scope == InformationScope.UNKNOWN

    def test_sku_specific_text(self):
        text = "Dette produktet er en steril kompress. Art.nr: 12345."
        result = detect_information_scope(text, current_sku="12345")
        assert result.scope == InformationScope.SKU
        assert result.sku_mentioned is True

    def test_sku_no_family_markers(self):
        text = "Nitrilhanske, pudderfri, størrelse M. Blå farge."
        result = detect_information_scope(text, current_sku="55555")
        assert result.scope == InformationScope.SKU

    def test_family_level_series_language(self):
        text = (
            "SELEFA Kompresser er en produktserie som finnes i flere størrelser. "
            "Alle størrelser er laget av 100% bomull."
        )
        result = detect_information_scope(text, current_sku="12345")
        assert result.scope == InformationScope.FAMILY
        assert result.confidence >= 0.40

    def test_family_level_variant_table(self):
        text = (
            "Tilgjengelig i:\n"
            "Størrelse: S, M, L, XL\n"
            "Art.nr 10001 - S\n"
            "Art.nr 10002 - M\n"
            "Art.nr 10003 - L\n"
            "Art.nr 10004 - XL\n"
        )
        result = detect_information_scope(text, current_sku="10002")
        assert result.scope == InformationScope.FAMILY
        assert result.sku_mentioned is True

    def test_family_multi_dimensions(self):
        text = "Leveres i størrelser: S, M, L, XL. Gauge: 18G, 21G, 23G."
        result = detect_information_scope(text)
        assert result.scope == InformationScope.FAMILY

    def test_packaging_level_transport(self):
        text = "Transportkartong: 20 esker á 100 stk. Pall: 40 kartonger."
        result = detect_information_scope(text)
        assert result.scope == InformationScope.PACKAGING
        assert result.blocking is True

    def test_packaging_level_hierarchy(self):
        text = "Pakningshierarki: Enhet: EA, Eske: 50 stk, Kartong: 10 esker."
        result = detect_information_scope(text)
        assert result.scope == InformationScope.PACKAGING

    def test_variant_in_family_context(self):
        text = (
            "Størrelse: M\n"
            "Art.nr 10001 - S\n"
            "Art.nr 10002 - M\n"
        )
        result = detect_information_scope(
            text, current_sku="10002", known_variant_dims=["M"]
        )
        assert result.scope in (InformationScope.VARIANT, InformationScope.FAMILY)

    def test_simple_product_description_is_sku(self):
        text = "Sterile kompresser i usterilt papir. 100% bomull. Myk og absorberende."
        result = detect_information_scope(text, current_sku="12345")
        assert result.scope == InformationScope.SKU

    def test_series_reference_triggers_family(self):
        text = "Produktene i denne serien er designet for engangsbruk."
        result = detect_information_scope(text)
        assert result.scope == InformationScope.FAMILY

    def test_available_in_multiple_triggers_family(self):
        text = "Finnes i flere størrelser og farger."
        result = detect_information_scope(text)
        assert result.scope == InformationScope.FAMILY


class TestIsFamilyLevelContent:
    """Test the boolean convenience function."""

    def test_family_text_returns_true(self):
        text = "Hele serien er laget av lateksfritt materiale."
        assert is_family_level_content(text) is True

    def test_sku_text_returns_false(self):
        text = "Sterilt plaster, 5x5cm."
        assert is_family_level_content(text) is False

    def test_packaging_returns_false(self):
        # Packaging is packaging, not family
        text = "Transportkartong: 20 esker."
        assert is_family_level_content(text) is False


class TestIsVariantSpecificContent:
    """Test variant-specificity detection."""

    def test_single_size_mention(self):
        text = "Størrelse M hanske. Art.nr 12345."
        assert is_variant_specific_content(text, "12345") is True

    def test_family_text_not_variant_specific(self):
        text = "Hele sortimentet finnes i flere størrelser."
        assert is_variant_specific_content(text) is False


class TestIsPackagingLevelContent:
    def test_packaging_text(self):
        text = "Masterpakk: 10 esker per kartong. Pall: 50 kartonger."
        assert is_packaging_level_content(text) is True

    def test_product_text_not_packaging(self):
        text = "Nitrilhanske, pudderfri, størrelse M."
        assert is_packaging_level_content(text) is False


class TestBlockFamilyContentForSKU:
    """Test the blocking decision function."""

    def test_sku_content_not_blocked(self):
        text = "Dette produktet er et sterilt plaster."
        blocked, reason, result = block_family_content_for_sku(
            text, "12345", "Beskrivelse"
        )
        assert blocked is False

    def test_family_content_blocked_for_description(self):
        text = "Produktserien finnes i flere størrelser. Alle størrelser er av bomull."
        blocked, reason, result = block_family_content_for_sku(
            text, "99999", "Beskrivelse"
        )
        assert blocked is True
        assert "familie" in reason.lower() or "serie" in reason.lower()

    def test_family_content_with_sku_not_blocked(self):
        text = (
            "Produktserien finnes i flere størrelser. "
            "Art.nr: 12345 er størrelse M."
        )
        blocked, reason, result = block_family_content_for_sku(
            text, "12345", "Beskrivelse"
        )
        assert blocked is False

    def test_packaging_blocked_for_spec(self):
        text = "Transportkartong: 20 esker á 100 stk."
        blocked, reason, result = block_family_content_for_sku(
            text, "12345", "Spesifikasjon"
        )
        assert blocked is True

    def test_packaging_allowed_for_packaging_field(self):
        text = "Transportkartong: 20 esker á 100 stk."
        blocked, reason, result = block_family_content_for_sku(
            text, "12345", "Pakningsinformasjon"
        )
        assert blocked is False

    def test_category_field_not_scope_checked(self):
        """Category is not a content field — scope check should not block."""
        text = "Hele serien er klassifisert under hansker."
        blocked, reason, result = block_family_content_for_sku(
            text, "12345", "Kategori"
        )
        # block_family_content_for_sku doesn't auto-block category
        assert blocked is False


class TestAdjustConfidenceForScope:
    """Test confidence adjustment based on scope."""

    def test_sku_scope_no_change(self):
        result = ScopeResult(scope=InformationScope.SKU, confidence=0.8, reason="")
        assert adjust_confidence_for_scope(0.75, result) == 0.75

    def test_variant_scope_slight_penalty(self):
        result = ScopeResult(scope=InformationScope.VARIANT, confidence=0.6, reason="")
        adjusted = adjust_confidence_for_scope(0.80, result)
        assert 0.60 < adjusted < 0.80

    def test_family_scope_significant_penalty(self):
        result = ScopeResult(
            scope=InformationScope.FAMILY, confidence=0.7, reason="",
            sku_mentioned=False,
        )
        adjusted = adjust_confidence_for_scope(0.80, result)
        assert adjusted < 0.50  # 0.80 * 0.45 = 0.36

    def test_family_with_sku_mentioned_less_penalty(self):
        result = ScopeResult(
            scope=InformationScope.FAMILY, confidence=0.7, reason="",
            sku_mentioned=True,
        )
        adjusted = adjust_confidence_for_scope(0.80, result)
        assert adjusted > 0.50  # 0.80 * 0.65 = 0.52

    def test_packaging_scope_heavy_penalty(self):
        result = ScopeResult(scope=InformationScope.PACKAGING, confidence=0.9, reason="")
        adjusted = adjust_confidence_for_scope(0.80, result)
        assert adjusted < 0.30  # 0.80 * 0.30 = 0.24


class TestIntegrationWithValidator:
    """Test that scope checks are wired into validate_suggestion_output."""

    def test_family_content_rejected_by_validator(self):
        from backend.content_validator import validate_suggestion_output
        text = "Produktserien finnes i flere størrelser. Alle størrelser er av høykvalitets bomull."
        ok, reason = validate_suggestion_output(text, "Beskrivelse", "99999")
        assert ok is False
        assert "familie" in reason.lower() or "serie" in reason.lower()

    def test_normal_description_passes_validator(self):
        from backend.content_validator import validate_suggestion_output
        text = "Sterile kompresser laget av 100% bomull. Egnet for sårpleie."
        ok, reason = validate_suggestion_output(text, "Beskrivelse", "12345")
        assert ok is True

    def test_packaging_in_spec_rejected(self):
        from backend.content_validator import validate_suggestion_output
        text = "Transportkartong: 20 esker á 100 stk per pall."
        ok, reason = validate_suggestion_output(text, "Spesifikasjon", "12345")
        assert ok is False
