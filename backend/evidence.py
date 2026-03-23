"""Structured evidence builder for enrichment suggestions.

Replaces ad-hoc f-string evidence with a consistent, machine-readable
tag system. Each suggestion gets both:
  - evidence_structured: dict of typed tags (for filtering/sorting)
  - evidence: human-readable summary (for Excel display)

Tags are standardized across all field types so the Excel output,
Inriver logic, and UI can programmatically inspect WHY a suggestion
was made.

Usage:
    eb = EvidenceBuilder("Beskrivelse")
    eb.set_source("datablad (PDF)", "https://example.com/spec.pdf", tier=3)
    eb.set_language("sv", translated=True)
    eb.set_variant_match(True, "SKU bekreftet i dokument")
    eb.set_classification("beskrivelse", 0.85)
    eb.note_noise_filtered(["kontaktinfo", "varianttabell"])
    eb.set_confidence(0.78, auto_inriver=False)
    suggestion.evidence = eb.build_text()
    suggestion.evidence_structured = eb.build_dict()
"""

from typing import Optional


class EvidenceBuilder:
    """Builds structured evidence for one enrichment suggestion."""

    def __init__(self, field_name: str):
        self.field_name = field_name
        self._source_label: str = ""
        self._source_url: Optional[str] = None
        self._source_tier: Optional[int] = None
        self._source_lang: Optional[str] = None
        self._translated: bool = False
        self._translation_note: str = ""
        self._variant_matched: Optional[bool] = None
        self._variant_note: str = ""
        self._noise_filtered: list[str] = []
        self._classification: Optional[str] = None
        self._classification_score: Optional[float] = None
        self._confidence: Optional[float] = None
        self._auto_inriver: Optional[bool] = None
        self._medical_flag: Optional[str] = None
        self._conflict_sources: list[str] = []
        self._extra_notes: list[str] = []

    # ── Setters ──

    def set_source(
        self, label: str, url: Optional[str] = None, tier: Optional[int] = None
    ) -> "EvidenceBuilder":
        """Set the winning source."""
        self._source_label = label
        self._source_url = url
        self._source_tier = tier
        return self

    def set_language(
        self, lang_code: str, translated: bool = False, note: str = ""
    ) -> "EvidenceBuilder":
        """Set source language and translation status."""
        self._source_lang = lang_code
        self._translated = translated
        self._translation_note = note
        return self

    def set_variant_match(
        self, matched: bool, note: str = ""
    ) -> "EvidenceBuilder":
        """Set whether the correct product variant was identified."""
        self._variant_matched = matched
        self._variant_note = note
        return self

    def set_classification(
        self, field_type: str, score: Optional[float] = None
    ) -> "EvidenceBuilder":
        """Set how this content was classified (description vs spec etc.)."""
        self._classification = field_type
        self._classification_score = score
        return self

    def note_noise_filtered(self, noise_types: list[str]) -> "EvidenceBuilder":
        """Record what types of noise were filtered out."""
        self._noise_filtered.extend(noise_types)
        return self

    def set_confidence(
        self, confidence: float, auto_inriver: bool = False
    ) -> "EvidenceBuilder":
        """Set confidence and Inriver eligibility."""
        self._confidence = confidence
        self._auto_inriver = auto_inriver
        return self

    def set_medical_flag(self, note: str) -> "EvidenceBuilder":
        """Set medical safety flag."""
        self._medical_flag = note
        return self

    def note_conflict(self, sources: list[str]) -> "EvidenceBuilder":
        """Record which sources had conflicting values."""
        self._conflict_sources.extend(sources)
        return self

    def add_note(self, note: str) -> "EvidenceBuilder":
        """Add a free-text note."""
        self._extra_notes.append(note)
        return self

    # ── Builders ──

    def build_dict(self) -> dict:
        """Build a machine-readable dict of evidence tags.

        All keys use Norwegian labels for consistency with the Excel output.
        Values are typed: str, bool, float, list[str], or None.
        """
        d = {}

        # Source
        d["Kilde"] = self._source_label or "ukjent"
        if self._source_url:
            d["Kilde-URL"] = self._source_url
        if self._source_tier is not None:
            from backend.golden_source import get_tier_label
            d["Kildenivå"] = get_tier_label(self._source_tier)

        # Language
        if self._source_lang:
            lang_names = {"no": "norsk", "sv": "svensk", "da": "dansk", "en": "engelsk"}
            d["Kildespråk"] = lang_names.get(self._source_lang, self._source_lang)
        d["Oversatt til norsk"] = "ja" if self._translated else "nei"

        # Variant
        if self._variant_matched is not None:
            d["Variant sikkert identifisert"] = "ja" if self._variant_matched else "nei"
            if self._variant_note:
                d["Variant-detalj"] = self._variant_note

        # Noise
        if self._noise_filtered:
            d["Støy filtrert bort"] = "ja"
            d["Filtrert støytyper"] = ", ".join(sorted(set(self._noise_filtered)))
        else:
            d["Støy filtrert bort"] = "nei"

        # Classification
        if self._classification:
            d["Felttypeklassifisering"] = self._classification
            if self._classification_score is not None:
                d["Klassifiseringsscore"] = round(self._classification_score, 2)

        # Confidence / risk
        if self._confidence is not None:
            d["Confidence"] = round(self._confidence, 2)
        if self._auto_inriver is not None:
            d["Auto-egnet for Inriver"] = "ja" if self._auto_inriver else "nei"

        # Medical
        if self._medical_flag:
            d["Medisinsk flagg"] = self._medical_flag

        # Conflicts
        if self._conflict_sources:
            d["Kildekonflikter"] = ", ".join(self._conflict_sources)

        return d

    def build_text(self) -> str:
        """Build a concise human-readable evidence string.

        Uses ` | ` as separator for easy scanning in Excel.
        """
        parts = []

        # Source
        source_part = f"Kilde: {self._source_label}"
        if self._source_url:
            source_part += f" ({self._source_url})"
        parts.append(source_part)

        # Language / translation
        if self._translated and self._source_lang:
            lang_names = {"sv": "svensk", "da": "dansk", "en": "engelsk"}
            lang_name = lang_names.get(self._source_lang, self._source_lang)
            parts.append(f"Oversatt fra {lang_name}")
        elif self._source_lang == "en":
            parts.append("Engelsk kilde — manuell oversettelse påkrevet")
        elif self._source_lang and self._source_lang not in ("no", "unknown", None):
            lang_names = {"sv": "svensk", "da": "dansk"}
            parts.append(f"Kildespråk: {lang_names.get(self._source_lang, self._source_lang)}")

        # Variant
        if self._variant_matched is True:
            parts.append("Variant bekreftet")
        elif self._variant_matched is False:
            parts.append("Variant IKKE sikkert identifisert")

        # Noise
        if self._noise_filtered:
            parts.append(f"Filtrert: {', '.join(sorted(set(self._noise_filtered)))}")

        # Classification
        if self._classification:
            cl = f"Klassifisert som: {self._classification}"
            if self._classification_score is not None:
                cl += f" ({self._classification_score:.0%})"
            parts.append(cl)

        # Confidence + Inriver
        if self._confidence is not None:
            conf_part = f"Confidence: {self._confidence:.0%}"
            if self._auto_inriver is True:
                conf_part += " (Inriver-klar)"
            elif self._auto_inriver is False:
                conf_part += " (manuell vurdering)"
            parts.append(conf_part)

        # Medical
        if self._medical_flag:
            parts.append(f"Medisinsk: {self._medical_flag}")

        # Conflicts
        if self._conflict_sources:
            parts.append(f"Avvik fra: {', '.join(self._conflict_sources)}")

        # Extra notes
        parts.extend(self._extra_notes)

        return " | ".join(parts)


def build_evidence(
    field_name: str,
    source_label: str,
    source_url: Optional[str] = None,
    source_tier: Optional[int] = None,
    lang: Optional[str] = None,
    translated: bool = False,
    translate_note: str = "",
    variant_matched: Optional[bool] = None,
    variant_note: str = "",
    noise_filtered: Optional[list[str]] = None,
    classification: Optional[str] = None,
    classification_score: Optional[float] = None,
    confidence: Optional[float] = None,
    auto_inriver: Optional[bool] = None,
    medical_flag: Optional[str] = None,
    conflict_sources: Optional[list[str]] = None,
    notes: Optional[list[str]] = None,
) -> tuple[str, dict]:
    """One-shot convenience function to build evidence text + dict.

    Returns (evidence_text, evidence_dict).
    """
    eb = EvidenceBuilder(field_name)
    eb.set_source(source_label, source_url, source_tier)
    if lang:
        eb.set_language(lang, translated, translate_note)
    if variant_matched is not None:
        eb.set_variant_match(variant_matched, variant_note)
    if noise_filtered:
        eb.note_noise_filtered(noise_filtered)
    if classification:
        eb.set_classification(classification, classification_score)
    if confidence is not None:
        eb.set_confidence(confidence, auto_inriver if auto_inriver is not None else (confidence >= 0.75))
    if medical_flag:
        eb.set_medical_flag(medical_flag)
    if conflict_sources:
        eb.note_conflict(conflict_sources)
    if notes:
        for n in notes:
            eb.add_note(n)
    return eb.build_text(), eb.build_dict()
