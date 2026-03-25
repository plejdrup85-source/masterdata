"""Tests for diff display — human-readable field change summaries."""

import pytest

from backend.diff_display import (
    ChangeScope,
    ChangeType,
    build_field_diff,
    detect_change_scope,
    summarize_change_type,
)


class TestBuildFieldDiff:
    """Test the compact diff representation."""

    def test_empty_to_value_shows_new(self):
        diff = build_field_diff("", "Steril kompress 10x10 cm")
        assert diff.startswith("Ny verdi:")
        assert "kompress" in diff

    def test_none_to_value_shows_new(self):
        diff = build_field_diff(None, "Steril kompress 10x10 cm")
        assert diff.startswith("Ny verdi:")

    def test_value_to_empty_shows_empty(self):
        diff = build_field_diff("Kompress", "")
        assert "tom" in diff.lower()

    def test_identical_values_show_no_change(self):
        diff = build_field_diff("Kompress", "kompress")
        assert "formatering" in diff.lower() or "endring" in diff.lower()

    def test_both_empty_returns_empty(self):
        diff = build_field_diff("", "")
        assert diff == ""

    def test_partial_change_shows_added_removed(self):
        current = "Steril kompress"
        proposed = "Steril kompress for sårpleie"
        diff = build_field_diff(current, proposed)
        assert "Lagt til:" in diff
        assert "sårpleie" in diff

    def test_full_replacement_shows_new_text(self):
        current = "Short placeholder text here"
        proposed = "Nitrilhanske, pudderfri, størrelse M, blå farge, 100 stk per eske"
        diff = build_field_diff(current, proposed)
        assert "Helt ny tekst:" in diff or "Lagt til:" in diff

    def test_long_diff_is_truncated(self):
        current = "A"
        proposed = " ".join(f"word{i}" for i in range(100))
        diff = build_field_diff(current, proposed, max_length=200)
        assert len(diff) <= 201  # +1 for possible truncation char

    def test_removed_words_shown(self):
        current = "Steril engangskompress av bomull"
        proposed = "Steril kompress"
        diff = build_field_diff(current, proposed)
        assert "Fjernet:" in diff

    def test_long_new_value_truncated(self):
        diff = build_field_diff(None, "A" * 200)
        assert "…" in diff


class TestSummarizeChangeType:
    """Test change type classification."""

    def test_empty_to_value_is_new(self):
        ct = summarize_change_type("", "Kompress")
        assert ct == ChangeType.NEW_VALUE.value

    def test_none_to_value_is_new(self):
        ct = summarize_change_type(None, "Kompress")
        assert ct == ChangeType.NEW_VALUE.value

    def test_identical_is_minor(self):
        ct = summarize_change_type("Kompress", "kompress")
        assert ct == ChangeType.MINOR_EDIT.value

    def test_extension_detected(self):
        current = "Steril kompress."
        proposed = "Steril kompress. Absorberende og hypoallergen. Størrelse 10x10 cm."
        ct = summarize_change_type(current, proposed)
        assert ct in (ChangeType.EXTENSION.value, ChangeType.NEW_INFORMATION.value)

    def test_english_to_norwegian_is_language_fix(self):
        current = "This glove is designed for medical use and provides protection"
        proposed = "Denne hansken er designet for medisinsk bruk og gir beskyttelse"
        ct = summarize_change_type(current, proposed)
        assert ct == ChangeType.LANGUAGE_FIX.value

    def test_short_different_values_is_correction(self):
        ct = summarize_change_type("Mölnlyke", "Mölnlycke")
        assert ct in (ChangeType.CORRECTION.value, ChangeType.MINOR_EDIT.value)

    def test_total_replacement(self):
        ct = summarize_change_type(
            "Placeholder tekst som ikke betyr noe",
            "Nitrilhanske pudderfri steril størrelse medium blå farge engangs",
        )
        assert ct in (ChangeType.REPLACEMENT.value, ChangeType.NEW_INFORMATION.value)

    def test_empty_proposed_returns_empty(self):
        ct = summarize_change_type("Kompress", "")
        assert ct == ""


class TestDetectChangeScope:
    """Test change scope detection."""

    def test_empty_to_value_is_full(self):
        scope = detect_change_scope("", "Kompress")
        assert scope == ChangeScope.FULL.value

    def test_identical_is_minor(self):
        scope = detect_change_scope("Kompress", "kompress")
        assert scope == ChangeScope.MINOR.value

    def test_small_addition_is_minor_or_moderate(self):
        scope = detect_change_scope(
            "Steril kompress for sårpleie",
            "Steril kompress for sårpleie. Størrelse 10x10 cm.",
        )
        assert scope in (ChangeScope.MINOR.value, ChangeScope.MODERATE.value)

    def test_major_change_detected(self):
        scope = detect_change_scope(
            "Kort tekst",
            "Steril engangskompress av ikke-vevd materiale. Absorberende og hypoallergen. "
            "Størrelse 10x10 cm. Egnet for sårpleie og postoperativ behandling.",
        )
        assert scope in (ChangeScope.MAJOR.value, ChangeScope.FULL.value)

    def test_full_replacement(self):
        scope = detect_change_scope(
            "Alpha beta gamma delta epsilon",
            "Helt annerledes tekst om noe annet helt nytt",
        )
        assert scope in (ChangeScope.MAJOR.value, ChangeScope.FULL.value)

    def test_empty_proposed_returns_empty(self):
        scope = detect_change_scope("Kompress", "")
        assert scope == ""
