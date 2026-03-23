"""Tests for regression guard — ensures suggestions don't make data worse."""

import pytest

from backend.regression_guard import (
    ComparisonResult,
    QualityScore,
    block_regressive_suggestion,
    compare_current_vs_proposed,
    is_proposed_value_better,
    score_text_quality,
)


class TestScoreTextQuality:
    """Test the multi-factor quality scoring."""

    def test_empty_text_scores_zero(self):
        qs = score_text_quality("")
        assert qs.total == 0.0

    def test_none_text_scores_zero(self):
        qs = score_text_quality(None)
        assert qs.total == 0.0

    def test_good_norwegian_description_scores_high(self):
        text = (
            "Steril kompress av ikke-vevd materiale. Absorberende og hypoallergen. "
            "Størrelse 10x10 cm. Egnet for sårpleie og postoperativ behandling."
        )
        qs = score_text_quality(text, "Beskrivelse")
        assert qs.total > 0.5
        assert qs.language_quality > 0.5
        assert qs.precision > 0.3

    def test_noisy_text_scores_low_on_noise(self):
        text = (
            "Produkt info. Tel: +47 123 45 678. "
            "Besøk oss på www.example.com. "
            "Side 3 av 12. Copyright 2024."
        )
        qs = score_text_quality(text)
        assert qs.noise_level < 0.5

    def test_english_text_lower_language_quality(self):
        text = (
            "This sterile compress is designed for wound care. "
            "It provides excellent absorption and ensures patient comfort."
        )
        qs = score_text_quality(text, "Beskrivelse")
        assert qs.language_quality < 0.5

    def test_structured_spec_scores_well(self):
        text = "Materiale: nitril; Størrelse: M; Lengde: 240 mm; Pudderfri: ja"
        qs = score_text_quality(text, "Spesifikasjon")
        assert qs.structure > 0.3
        assert qs.precision > 0.3


class TestIsProposedValueBetter:
    """Test the main regression guard entry point."""

    def test_missing_current_any_proposal_is_better(self):
        is_better, reason = is_proposed_value_better(
            None, "Steril kompress 10x10 cm", "Beskrivelse"
        )
        assert is_better is True

    def test_empty_current_any_proposal_is_better(self):
        is_better, reason = is_proposed_value_better(
            "", "Steril kompress 10x10 cm", "Beskrivelse"
        )
        assert is_better is True

    def test_empty_proposal_is_not_better(self):
        is_better, reason = is_proposed_value_better(
            "Kompress", "", "Beskrivelse"
        )
        assert is_better is False

    def test_noisy_proposal_blocked(self):
        current = "Steril kompress for sårpleie. Størrelse 10x10 cm."
        proposed = (
            "Kompress. Tel: +47 22 33 44 55. "
            "www.leverandor.no. Side 2. Copyright 2024."
        )
        is_better, reason = is_proposed_value_better(current, proposed, "Beskrivelse")
        assert is_better is False

    def test_english_replacing_norwegian_blocked(self):
        current = "Nitrilhanske, pudderfri, steril. Egnet for medisinsk undersøkelse."
        proposed = "This glove is designed for medical examinations. It provides excellent protection and ensures safety."
        is_better, reason = is_proposed_value_better(current, proposed, "Beskrivelse")
        assert is_better is False

    def test_genuinely_better_description_passes(self):
        current = "Kompress"
        proposed = (
            "Steril kompress av ikke-vevd materiale. Absorberende og hypoallergen. "
            "Størrelse 10x10 cm. Egnet for sårpleie."
        )
        is_better, reason = is_proposed_value_better(
            current, proposed, "Beskrivelse", confidence=0.80
        )
        assert is_better is True

    def test_shorter_but_noisier_current_replaced(self):
        """A shorter current with noise should be replaceable by clean proposed."""
        current = "Kompress, se www.test.no for mer info."
        proposed = "Steril kompress for sårpleie. Materiale: ikke-vevd. Størrelse 10x10 cm."
        is_better, reason = is_proposed_value_better(
            current, proposed, "Beskrivelse", confidence=0.85
        )
        assert is_better is True

    def test_generic_replacing_specific_blocked(self):
        current = "Nitrilhanske, pudderfri, størrelse M, 240 mm, blå, 100 stk/eske."
        proposed = "Hanske for medisinsk bruk."
        is_better, reason = is_proposed_value_better(
            current, proposed, "Beskrivelse", confidence=0.70
        )
        assert is_better is False

    def test_much_shorter_without_density_gain_blocked(self):
        current = (
            "Steril engangskompress av ikke-vevd materiale. "
            "Absorberende og hypoallergen. Størrelse 10x10 cm. "
            "Egnet for sårpleie og postoperativ behandling."
        )
        proposed = "Kompress 10x10."
        is_better, reason = is_proposed_value_better(
            current, proposed, "Beskrivelse", confidence=0.80
        )
        assert is_better is False


class TestBlockRegressiveSuggestion:
    """Test the block_regressive_suggestion function (inverted logic)."""

    def test_good_suggestion_not_blocked(self):
        should_block, reason = block_regressive_suggestion(
            None, "Steril kompress 10x10 cm", "Beskrivelse"
        )
        assert should_block is False

    def test_regressive_suggestion_blocked(self):
        current = "Nitrilhanske, pudderfri, størrelse M, 240 mm, blå."
        proposed = "Hanske."
        should_block, reason = block_regressive_suggestion(
            current, proposed, "Produktnavn", confidence=0.60
        )
        assert should_block is True
        assert reason  # Should have an explanation

    def test_low_confidence_requires_bigger_margin(self):
        """Low confidence proposals need to be much better to pass."""
        current = "Kompress for sårpleie."
        proposed = "Steril kompress for sårpleie. Størrelse 10x10 cm."
        # With high confidence, this should pass
        should_block_high, _ = block_regressive_suggestion(
            current, proposed, "Beskrivelse", confidence=0.90
        )
        # With very low confidence, it might not clear the threshold
        should_block_low, _ = block_regressive_suggestion(
            current, proposed, "Beskrivelse", confidence=0.30
        )
        # At minimum, high confidence should not be more blocked than low
        assert not (should_block_high and not should_block_low)


class TestCompareCurrentVsProposed:
    """Test detailed comparison results."""

    def test_returns_comparison_result(self):
        result = compare_current_vs_proposed(
            "Kompress", "Steril kompress for sårpleie", "Beskrivelse"
        )
        assert isinstance(result, ComparisonResult)
        assert isinstance(result.current_score, QualityScore)
        assert isinstance(result.proposed_score, QualityScore)
        assert isinstance(result.reason, str)
        assert result.reason  # Never empty

    def test_factors_tracked(self):
        result = compare_current_vs_proposed(
            "Kort tekst",
            "Steril kompress av ikke-vevd materiale. Størrelse 10x10 cm. Absorberende.",
            "Beskrivelse",
        )
        # At least one factor should be better
        assert result.factors_better or result.is_improvement

    def test_delta_positive_for_improvement(self):
        result = compare_current_vs_proposed(
            "Kort",
            "Steril kompress av ikke-vevd materiale. Størrelse 10x10 cm. Absorberende.",
            "Beskrivelse",
        )
        if result.is_improvement:
            assert result.delta > 0


class TestFieldSpecificScoring:
    """Test that scoring adapts to field type."""

    def test_product_name_length_scoring(self):
        short = score_text_quality("X", "Produktnavn")
        good = score_text_quality("Nitrilhanske Steril Str M", "Produktnavn")
        too_long = score_text_quality("A" * 200, "Produktnavn")
        assert good.length_adequacy > short.length_adequacy
        assert good.length_adequacy > too_long.length_adequacy

    def test_spec_structure_bonus(self):
        plain = score_text_quality("nitril blå pudderfri", "Spesifikasjon")
        structured = score_text_quality(
            "Materiale: nitril; Farge: blå; Pudderfri: ja", "Spesifikasjon"
        )
        assert structured.structure > plain.structure

    def test_description_sentence_bonus(self):
        fragment = score_text_quality("kompress steril", "Beskrivelse")
        sentences = score_text_quality(
            "Steril kompress for sårpleie. Egnet for daglig bruk.", "Beskrivelse"
        )
        assert sentences.structure >= fragment.structure
