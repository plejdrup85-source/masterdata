"""Tests for two-dimensional quality scoring: content quality vs source conformity."""

import pytest

from backend.models import FieldAnalysis, QualityStatus
from backend.quality_dimensions import (
    ContentQualityScore,
    ConformityQualityScore,
    compute_quality_dimensions,
    quality_summary_label,
    score_conformity_quality,
    score_content_quality,
)


class TestContentQualityScoring:
    """Test content quality dimension."""

    def test_empty_value_scores_zero(self):
        fa = FieldAnalysis(field_name="Beskrivelse", current_value="")
        cq = score_content_quality(fa)
        assert cq.total == 0

    def test_none_value_scores_zero(self):
        fa = FieldAnalysis(field_name="Beskrivelse", current_value=None)
        cq = score_content_quality(fa)
        assert cq.total == 0

    def test_good_norwegian_description_scores_high(self):
        fa = FieldAnalysis(
            field_name="Beskrivelse",
            current_value=(
                "Steril kompress av ikke-vevd materiale. Absorberende og hypoallergen. "
                "Størrelse 10x10 cm. Egnet for sårpleie og postoperativ behandling."
            ),
        )
        cq = score_content_quality(fa)
        assert cq.total >= 55
        assert cq.language >= 50
        assert cq.completeness >= 50

    def test_noisy_text_scores_low_on_noise(self):
        fa = FieldAnalysis(
            field_name="Beskrivelse",
            current_value=(
                "Produkt info. Tel: +47 123 45 678. "
                "Besøk oss på www.example.com. Side 3. Copyright 2024."
            ),
        )
        cq = score_content_quality(fa)
        assert cq.noise_level < 50

    def test_english_text_lower_language_score(self):
        fa = FieldAnalysis(
            field_name="Beskrivelse",
            current_value=(
                "This sterile compress is designed for wound care. "
                "It provides excellent absorption and ensures patient comfort."
            ),
        )
        cq = score_content_quality(fa)
        assert cq.language < 50

    def test_structured_spec_scores_well(self):
        fa = FieldAnalysis(
            field_name="Spesifikasjon",
            current_value="Materiale: nitril; Størrelse: M; Lengde: 240 mm; Pudderfri: ja; Farge: blå",
        )
        cq = score_content_quality(fa)
        assert cq.structure >= 40
        assert cq.completeness >= 50

    def test_product_name_adequate_length(self):
        fa = FieldAnalysis(
            field_name="Produktnavn",
            current_value="Nitrilhanske Steril Str M Blå",
        )
        cq = score_content_quality(fa)
        assert cq.completeness >= 70

    def test_details_string_populated(self):
        fa = FieldAnalysis(
            field_name="Beskrivelse",
            current_value="Steril kompress for sårpleie.",
        )
        cq = score_content_quality(fa)
        assert cq.details
        assert "Lesbarhet:" in cq.details
        assert "Språk:" in cq.details


class TestConformityQualityScoring:
    """Test source conformity dimension."""

    def test_empty_value_scores_zero(self):
        fa = FieldAnalysis(field_name="Beskrivelse", current_value="")
        conf = score_conformity_quality(fa)
        assert conf.total == 0

    def test_matching_website_and_jeeves_scores_high(self):
        fa = FieldAnalysis(
            field_name="Produktnavn",
            current_value="Nitrilhanske Steril Str M",
            website_value="Nitrilhanske Steril Str M",
            jeeves_value="Nitrilhanske Steril Str M",
            value_origin="nettside",
        )
        conf = score_conformity_quality(fa)
        assert conf.website_match == 100
        assert conf.catalog_match == 100

    def test_conflicting_sources_scores_low(self):
        fa = FieldAnalysis(
            field_name="Produktnavn",
            current_value="Nitrilhanske Steril Str M",
            website_value="Nitrilhanske Steril Str M",
            jeeves_value="Vinylhanske Usteril Str L",
            value_origin="nettside",
        )
        conf = score_conformity_quality(fa)
        assert conf.catalog_match < 50

    def test_no_references_gives_neutral(self):
        fa = FieldAnalysis(
            field_name="Beskrivelse",
            current_value="Steril kompress.",
            website_value=None,
            jeeves_value=None,
        )
        conf = score_conformity_quality(fa)
        # Neutral when no references available
        assert conf.website_match == 50
        assert conf.catalog_match == 50

    def test_family_data_lowers_scope_match(self):
        fa = FieldAnalysis(
            field_name="Beskrivelse",
            current_value=(
                "Denne serien finnes i flere størrelser. "
                "Velg mellom alle varianter. Art 12345, 12346, 12347."
            ),
        )
        conf = score_conformity_quality(fa)
        assert conf.scope_match < 60

    def test_details_string_populated(self):
        fa = FieldAnalysis(
            field_name="Produktnavn",
            current_value="Nitrilhanske",
            website_value="Nitrilhanske",
        )
        conf = score_conformity_quality(fa)
        assert conf.details
        assert "Kilde:" in conf.details
        assert "Nettside:" in conf.details


class TestComputeQualityDimensions:
    """Test combined computation."""

    def test_returns_both_scores(self):
        fa = FieldAnalysis(
            field_name="Beskrivelse",
            current_value="Steril kompress for sårpleie. Størrelse 10x10 cm.",
        )
        cq, conf = compute_quality_dimensions(fa)
        assert isinstance(cq, ContentQualityScore)
        assert isinstance(conf, ConformityQualityScore)
        assert 0 <= cq.total <= 100
        assert 0 <= conf.total <= 100


class TestQualitySummaryLabel:
    """Test the quadrant label function."""

    def test_high_high_production_ready(self):
        label = quality_summary_label(80, 80)
        assert label == "Klar for produksjon"

    def test_high_content_low_conformity(self):
        label = quality_summary_label(80, 40)
        assert "samsvar" in label.lower()

    def test_low_content_high_conformity(self):
        label = quality_summary_label(40, 80)
        assert "språkvask" in label.lower()

    def test_low_low_rework(self):
        label = quality_summary_label(30, 30)
        assert "omarbeiding" in label.lower()


class TestIndependentDimensions:
    """Test that content and conformity are truly independent."""

    def test_good_content_bad_conformity(self):
        """Well-written Norwegian text that doesn't match sources."""
        fa = FieldAnalysis(
            field_name="Beskrivelse",
            current_value=(
                "Steril kompress av ikke-vevd materiale. Absorberende og hypoallergen. "
                "Størrelse 10x10 cm. Egnet for sårpleie og postoperativ behandling."
            ),
            website_value="Kompress for engangsbruk",  # Different
            jeeves_value="Sårkompress 10x10",  # Different
        )
        cq = score_content_quality(fa)
        conf = score_conformity_quality(fa)
        # Content should be high (well-written)
        assert cq.total >= 50
        # Conformity should be lower (doesn't match sources closely)
        assert conf.website_match < 60
        assert conf.catalog_match < 60

    def test_bad_content_good_conformity(self):
        """Noisy text that matches sources exactly."""
        noisy = "Kompress. Tel: +47 22 33 44 55. Side 3."
        fa = FieldAnalysis(
            field_name="Beskrivelse",
            current_value=noisy,
            website_value=noisy,
            jeeves_value=noisy,
        )
        cq = score_content_quality(fa)
        conf = score_conformity_quality(fa)
        # Content should be low (noisy)
        assert cq.noise_level < 50
        # Conformity should be high (matches sources)
        assert conf.website_match == 100
        assert conf.catalog_match == 100
