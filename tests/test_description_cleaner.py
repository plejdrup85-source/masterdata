"""Tests for description_cleaner.py — content filtering and quality gate."""

from backend.description_cleaner import (
    clean_description_source,
    validate_webshop_description,
    _is_junk_line,
)


class TestJunkLineDetection:
    """Test that individual junk lines are correctly identified."""

    def test_table_header(self):
        assert _is_junk_line("Størrelseskode  dispenser/kartong  lengde") is True

    def test_bestillingsnummer_header(self):
        assert _is_junk_line("Bestillingsnummer  Gauge  Lengde  Farge") is True

    def test_article_number_line(self):
        assert _is_junk_line("874900062         18G    40mm    Rosa") is True

    def test_metadata_produktdatablad(self):
        assert _is_junk_line("Produktdatablad") is True

    def test_metadata_side(self):
        assert _is_junk_line("Side 1") is True

    def test_date_line(self):
        assert _is_junk_line("2026-02-27") is True

    def test_packaging_qty(self):
        assert _is_junk_line("150 / 1500") is True

    def test_url_line(self):
        assert _is_junk_line("www.onemed.no") is True

    def test_copyright(self):
        assert _is_junk_line("© OneMed AS 2026") is True
        assert _is_junk_line("Copyright © Mölnlycke Health Care 2024") is True

    def test_producer_metadata(self):
        assert _is_junk_line("Produsent: KD Medical GmbH") is True

    def test_antall_line(self):
        assert _is_junk_line("Antall pr eske: 100") is True
        assert _is_junk_line("Antall pr kartong: 2400") is True

    def test_mostly_numeric(self):
        assert _is_junk_line("2231951203      150 / 1500         5x5cm") is True

    # Lines that should NOT be junk
    def test_description_sentence_kept(self):
        assert _is_junk_line("Embra kompresser i non-woven er myke og absorberende.") is False

    def test_product_property_kept(self):
        assert _is_junk_line("Lateksfri og hypoallergen.") is False

    def test_fargekodede_not_fargekode(self):
        """'Fargekodede' (color-coded) is NOT a table header 'Fargekode'."""
        assert _is_junk_line("Fargekodede kanyler etter ISO-standard.") is False

    def test_short_product_name_kept(self):
        assert _is_junk_line("Kompress NW 4-lag Embra") is False


class TestCleanDescriptionSource:
    """Test the main content filtering function."""

    def test_removes_table_and_metadata(self):
        raw = """Produktdatablad
Kompress NW 4-lag Embra
2026-02-27
Embra kompresser er myke og absorberende.
Egnet for sårbehandling.
Størrelseskode  dispenser/kartong
2231951203      150 / 1500
Side 1
www.onemed.no"""
        cleaned = clean_description_source(raw)
        assert cleaned is not None
        assert "Produktdatablad" not in cleaned
        assert "2026-02-27" not in cleaned
        assert "Størrelseskode" not in cleaned
        assert "2231951203" not in cleaned
        assert "Side 1" not in cleaned
        assert "www.onemed.no" not in cleaned
        assert "Embra kompresser" in cleaned

    def test_pure_table_returns_none(self):
        raw = """50x250mm
75x150mm
100x200mm
150x300mm"""
        assert clean_description_source(raw) is None

    def test_merges_broken_sentences(self):
        raw = """Mepilex Border er en selvklebende skumbandasje med
safetac teknologi som sikrer skånsom festing."""
        cleaned = clean_description_source(raw)
        assert "skumbandasje med safetac" in cleaned  # Merged lowercase continuation

    def test_empty_input(self):
        assert clean_description_source("") is None
        assert clean_description_source(None) is None

    def test_clean_text_passes_through(self):
        good = "Pudderfri hansker i nitril. Egnet for medisinsk undersøkelse."
        cleaned = clean_description_source(good)
        assert cleaned is not None
        assert "Pudderfri" in cleaned

    def test_deduplicates_repeated_lines(self):
        raw = """Kompress NW 4-lag Embra
Kompress NW 4-lag Embra
Egnet for medisinsk bruk.
Lateksfri og hypoallergen."""
        cleaned = clean_description_source(raw)
        assert cleaned.count("Kompress NW 4-lag Embra") == 1


class TestValidateWebshopDescription:
    """Test the quality gate for generated descriptions."""

    def test_good_description_passes(self):
        text = ("Undersøkelseshanske i nitril. Pudderfri og lateksfri. "
                "Teksturert fingertupp for bedre grep. Egnet for medisinsk bruk.")
        ok, reason = validate_webshop_description(text)
        assert ok is True

    def test_too_short_rejected(self):
        ok, reason = validate_webshop_description("Kort tekst.")
        assert ok is False
        assert "kort" in reason.lower()

    def test_sku_in_text_rejected(self):
        text = ("Fin hanske for medisinsk bruk. Artikkelnummer 874900062 for bestilling. "
                "Pudderfri og lateksfri for sikker bruk.")
        ok, reason = validate_webshop_description(text)
        assert ok is False
        assert "874900062" in reason

    def test_packaging_pattern_rejected(self):
        text = "God hanske for medisinsk bruk. Pakket 150 / 1500 stk."
        ok, reason = validate_webshop_description(text)
        assert ok is False

    def test_pdf_artifact_rejected(self):
        text = "Fin bandasje for sårbehandling. Se side 1 for mer info."
        ok, reason = validate_webshop_description(text)
        assert ok is False

    def test_empty_rejected(self):
        ok, _ = validate_webshop_description("")
        assert ok is False

    def test_table_like_rejected(self):
        text = "XS\nS\nM\nL\nXL\nXXL\n5cm\n10cm\n15cm"
        ok, reason = validate_webshop_description(text)
        assert ok is False


class TestEndToEnd:
    """Integration: raw → clean → gate."""

    def test_compress_pdf_cleanup(self):
        raw = """Produktdatablad
Embra kompresser i non-woven er myke og absorberende.
Egnet for sårbehandling og generell pleie.
Lateksfri og hypoallergen.
Størrelseskode  dispenser/kartong
2231951203      150 / 1500
Side 1"""
        cleaned = clean_description_source(raw)
        assert cleaned is not None
        ok, _ = validate_webshop_description(cleaned)
        assert ok is True
        assert "Størrelseskode" not in cleaned
        assert "2231951203" not in cleaned

    def test_needle_pdf_cleanup(self):
        raw = """Injeksjonskanyle for subkutan og intramuskulær injeksjon.
Silikonbehandlet for smidig penetrering.
Fargekodede kanyler etter ISO-standard.
Bestillingsnummer  Gauge  Lengde  Farge
874900062         18G    40mm    Rosa
Produsent: KD Medical GmbH"""
        cleaned = clean_description_source(raw)
        assert cleaned is not None
        ok, _ = validate_webshop_description(cleaned)
        assert ok is True
        assert "874900062" not in cleaned
        assert "Produsent:" not in cleaned
        assert "Injeksjonskanyle" in cleaned
        assert "Fargekodede" in cleaned
