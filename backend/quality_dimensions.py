"""Two-dimensional quality scoring: content quality vs source conformity.

Replaces the single blended confidence score with two distinct dimensions
that answer different questions:

**Innholdskvalitet (Content Quality)** — 0–100
  "Is this text well-written and suitable for a webshop?"
  Components:
    - Lesbarhet (readability): sentence structure, punctuation, flow
    - Struktur (structure): key-value pairs, bullets, paragraphs
    - Språk (language): Norwegian, no language mixing
    - Kompletthet (completeness): adequate length, detail level
    - Nettbutikkegnethet (webshop suitability): product-focused, no noise
    - Støynivå (noise level): free of contact info, PDF artifacts

**Samsvarskvalitet (Conformity Quality)** — 0–100
  "Does this value match what authoritative sources say?"
  Components:
    - Kildesamsvar (source match): agrees with golden source
    - Katalogsamsvar (catalog match): agrees with Jeeves ERP
    - Nettsidesamsvar (website match): agrees with onemed.no
    - Databladsamsvar (datasheet match): agrees with PDF
    - Produsentkildesamsvar (manufacturer match): agrees with manufacturer
    - Variant/produktnivå (scope match): correct product, not family data

A field can score:
  - High content + High conformity: Ready for production
  - High content + Low conformity: Well-written but might be wrong product
  - Low content + High conformity: Correct but poorly presented
  - Low content + Low conformity: Needs complete rework
"""

import re
from dataclasses import dataclass, field
from typing import Optional

from backend.models import FieldAnalysis, QualityStatus


@dataclass
class ContentQualityScore:
    """Content quality assessment: is the text well-written for a webshop?"""
    readability: int = 0       # Sentence structure, punctuation, flow
    structure: int = 0         # Key-value pairs, bullets, paragraphs
    language: int = 0          # Norwegian, no mixing
    completeness: int = 0      # Adequate length, detail level
    webshop_suitability: int = 0  # Product-focused, customer-oriented
    noise_level: int = 0       # Free of artifacts (inverted: 100 = clean)
    total: int = 0             # Weighted composite
    details: str = ""          # Human-readable breakdown

    def compute_total(self, field_name: str = "") -> int:
        weights = _CONTENT_WEIGHTS.get(field_name, _CONTENT_WEIGHTS_DEFAULT)
        self.total = round(
            self.readability * weights["readability"]
            + self.structure * weights["structure"]
            + self.language * weights["language"]
            + self.completeness * weights["completeness"]
            + self.webshop_suitability * weights["webshop_suitability"]
            + self.noise_level * weights["noise_level"]
        )
        self.total = max(0, min(100, self.total))
        self._build_details()
        return self.total

    def _build_details(self) -> None:
        parts = [
            f"Lesbarhet: {self.readability}",
            f"Struktur: {self.structure}",
            f"Språk: {self.language}",
            f"Komplett: {self.completeness}",
            f"Nettbutikk: {self.webshop_suitability}",
            f"Renhet: {self.noise_level}",
        ]
        self.details = " | ".join(parts)


@dataclass
class ConformityQualityScore:
    """Source conformity assessment: does the value match authoritative sources?"""
    source_match: int = 0       # Matches golden source for this field
    catalog_match: int = 0      # Matches Jeeves ERP catalog
    website_match: int = 0      # Matches onemed.no
    datasheet_match: int = 0    # Matches PDF datasheet
    manufacturer_match: int = 0  # Matches manufacturer source
    scope_match: int = 0        # Correct product scope (not family data)
    total: int = 0              # Weighted composite
    details: str = ""           # Human-readable breakdown

    def compute_total(self, field_name: str = "") -> int:
        weights = _CONFORMITY_WEIGHTS.get(field_name, _CONFORMITY_WEIGHTS_DEFAULT)
        self.total = round(
            self.source_match * weights["source_match"]
            + self.catalog_match * weights["catalog_match"]
            + self.website_match * weights["website_match"]
            + self.datasheet_match * weights["datasheet_match"]
            + self.manufacturer_match * weights["manufacturer_match"]
            + self.scope_match * weights["scope_match"]
        )
        self.total = max(0, min(100, self.total))
        self._build_details()
        return self.total

    def _build_details(self) -> None:
        parts = [
            f"Kilde: {self.source_match}",
            f"Katalog: {self.catalog_match}",
            f"Nettside: {self.website_match}",
            f"Datablad: {self.datasheet_match}",
            f"Produsent: {self.manufacturer_match}",
            f"Omfang: {self.scope_match}",
        ]
        self.details = " | ".join(parts)


# ── Content quality weights per field type ──

_CONTENT_WEIGHTS_DEFAULT = {
    "readability": 0.20, "structure": 0.15, "language": 0.20,
    "completeness": 0.20, "webshop_suitability": 0.15, "noise_level": 0.10,
}

_CONTENT_WEIGHTS = {
    "Produktnavn": {
        "readability": 0.15, "structure": 0.10, "language": 0.20,
        "completeness": 0.25, "webshop_suitability": 0.20, "noise_level": 0.10,
    },
    "Beskrivelse": {
        "readability": 0.25, "structure": 0.15, "language": 0.20,
        "completeness": 0.20, "webshop_suitability": 0.10, "noise_level": 0.10,
    },
    "Spesifikasjon": {
        "readability": 0.10, "structure": 0.30, "language": 0.10,
        "completeness": 0.30, "webshop_suitability": 0.10, "noise_level": 0.10,
    },
    "Pakningsinformasjon": {
        "readability": 0.10, "structure": 0.20, "language": 0.10,
        "completeness": 0.30, "webshop_suitability": 0.10, "noise_level": 0.20,
    },
    "Produsent": {
        "readability": 0.05, "structure": 0.05, "language": 0.10,
        "completeness": 0.40, "webshop_suitability": 0.10, "noise_level": 0.30,
    },
    "Produsentens varenummer": {
        "readability": 0.00, "structure": 0.05, "language": 0.00,
        "completeness": 0.50, "webshop_suitability": 0.05, "noise_level": 0.40,
    },
    "Kategori": {
        "readability": 0.05, "structure": 0.30, "language": 0.10,
        "completeness": 0.35, "webshop_suitability": 0.10, "noise_level": 0.10,
    },
}

# ── Conformity weights per field type ──

_CONFORMITY_WEIGHTS_DEFAULT = {
    "source_match": 0.30, "catalog_match": 0.15, "website_match": 0.15,
    "datasheet_match": 0.15, "manufacturer_match": 0.15, "scope_match": 0.10,
}

_CONFORMITY_WEIGHTS = {
    "Produktnavn": {
        "source_match": 0.30, "catalog_match": 0.20, "website_match": 0.20,
        "datasheet_match": 0.10, "manufacturer_match": 0.10, "scope_match": 0.10,
    },
    "Beskrivelse": {
        "source_match": 0.25, "catalog_match": 0.10, "website_match": 0.25,
        "datasheet_match": 0.15, "manufacturer_match": 0.10, "scope_match": 0.15,
    },
    "Spesifikasjon": {
        "source_match": 0.20, "catalog_match": 0.10, "website_match": 0.15,
        "datasheet_match": 0.25, "manufacturer_match": 0.15, "scope_match": 0.15,
    },
    "Produsent": {
        "source_match": 0.25, "catalog_match": 0.25, "website_match": 0.15,
        "datasheet_match": 0.10, "manufacturer_match": 0.15, "scope_match": 0.10,
    },
    "Produsentens varenummer": {
        "source_match": 0.25, "catalog_match": 0.25, "website_match": 0.10,
        "datasheet_match": 0.15, "manufacturer_match": 0.20, "scope_match": 0.05,
    },
    "Kategori": {
        "source_match": 0.35, "catalog_match": 0.20, "website_match": 0.20,
        "datasheet_match": 0.05, "manufacturer_match": 0.05, "scope_match": 0.15,
    },
    "Pakningsinformasjon": {
        "source_match": 0.20, "catalog_match": 0.10, "website_match": 0.20,
        "datasheet_match": 0.25, "manufacturer_match": 0.10, "scope_match": 0.15,
    },
}


# ── Norwegian medical/product terms ──

_NO_MEDICAL_TERMS = re.compile(
    r"(?i)\b(?:hanske|hansker|bandasje|kompress|plaster|sprøyte|kanyle|kateter|"
    r"sutur|frakk|munnbind|steril|usteril|pudderfri|lateksfri|nitril|vinyl|"
    r"sårpleie|beskyttelse|absorberende|hypoallergen|elastisk|kirurgisk|"
    r"medisinsk|engangs|flergangs|materiale|størrelse|lengde|bredde|pakning)\b"
)

_EN_INDICATORS = re.compile(
    r"(?i)\b(?:the|designed|intended|provides|ensures|available|suitable|"
    r"glove|sterile|disposable|latex-free|powder-free|protection)\b"
)

_SV_INDICATORS = re.compile(
    r"(?i)\b(?:och|för|inte|användas|engångs|handske|storlek|förpackning)\b"
)

_NOISE_PATTERNS = [
    re.compile(r"(?i)(?:tel|telefon|tlf|fax|phone)\s*[.:]?\s*[\+\d\(\)\s\-]{7,}"),
    re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"),
    re.compile(r"(?i)(?:www\.[a-z0-9\-]+\.[a-z]{2,}|https?://[^\s]+)"),
    re.compile(r"(?i)\b(?:side|page)\s+\d+\b"),
    re.compile(r"(?i)(?:copyright|©|\(c\))"),
    re.compile(r"(?i)\b(?:produktdatablad|technical\s+data\s*sheet)\b"),
]

_WEBSHOP_POSITIVE = re.compile(
    r"(?i)\b(?:egnet for|brukes til|passer til|ideell for|designet for|"
    r"enkel å bruke|komfortabel|holdbar|allergivennlig|CE-merket|"
    r"medisinsk klasse|godkjent|sertifisert)\b"
)

_SPEC_KV_PATTERN = re.compile(
    r"^[A-ZÆØÅ\u00C0-\u00FF][a-zæøåa-z\u00E0-\u00FF\s]+\s*:\s*.+",
    re.MULTILINE,
)

_PRECISION_UNITS = re.compile(
    r"\b\d+[\.,]?\d*\s*(?:mm|cm|m|ml|l|g|kg|µm|stk|pk|%|°C|bar|kPa|Fr|Ch|Ga)\b",
    re.IGNORECASE,
)


# ═══════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════


def score_content_quality(fa: FieldAnalysis) -> ContentQualityScore:
    """Score the content quality of a field's current value.

    Measures how well the text is written, structured, and suited for
    a webshop — independent of whether it matches source data.
    """
    cq = ContentQualityScore()
    val = (fa.current_value or "").strip()

    if not val:
        cq.compute_total(fa.field_name)
        return cq

    cq.readability = _score_readability(val, fa.field_name)
    cq.structure = _score_structure(val, fa.field_name)
    cq.language = _score_language(val)
    cq.completeness = _score_completeness(val, fa.field_name)
    cq.webshop_suitability = _score_webshop_suitability(val, fa.field_name)
    cq.noise_level = _score_noise_level(val)

    cq.compute_total(fa.field_name)
    return cq


def score_conformity_quality(
    fa: FieldAnalysis,
    enrichment_results: Optional[list] = None,
    manufacturer_data: Optional[object] = None,
) -> ConformityQualityScore:
    """Score how well a field's value conforms to authoritative sources.

    Measures agreement with Jeeves, website, PDF, manufacturer —
    independent of whether the text is well-written.
    """
    conf = ConformityQualityScore()
    val = (fa.current_value or "").strip()

    if not val:
        conf.compute_total(fa.field_name)
        return conf

    # Source match: use golden source hierarchy
    conf.source_match = _score_source_match(fa)

    # Catalog match: compare with Jeeves value
    conf.catalog_match = _score_text_agreement(val, fa.jeeves_value)

    # Website match: compare with website value
    conf.website_match = _score_text_agreement(val, fa.website_value)

    # Datasheet match: check enrichment results from PDF
    conf.datasheet_match = _score_datasheet_match(fa, enrichment_results)

    # Manufacturer match: check manufacturer data
    conf.manufacturer_match = _score_manufacturer_match(fa, manufacturer_data)

    # Scope match: is this the right product (not family data)?
    conf.scope_match = _score_scope_match(val, fa.field_name)

    conf.compute_total(fa.field_name)
    return conf


def compute_quality_dimensions(
    fa: FieldAnalysis,
    enrichment_results: Optional[list] = None,
    manufacturer_data: Optional[object] = None,
) -> tuple[ContentQualityScore, ConformityQualityScore]:
    """Compute both quality dimensions for a field analysis.

    Returns (content_quality, conformity_quality).
    """
    cq = score_content_quality(fa)
    conf = score_conformity_quality(fa, enrichment_results, manufacturer_data)
    return cq, conf


def quality_summary_label(content_score: int, conformity_score: int) -> str:
    """Return a human-readable Norwegian label for the quality quadrant.

    The four quadrants:
      High/High: "Klar for produksjon"
      High/Low:  "Godt skrevet, usikkert samsvar"
      Low/High:  "Korrekt kilde, trenger språkvask"
      Low/Low:   "Krever omarbeiding"
    """
    high_threshold = 65

    if content_score >= high_threshold and conformity_score >= high_threshold:
        return "Klar for produksjon"
    elif content_score >= high_threshold and conformity_score < high_threshold:
        return "Godt skrevet, usikkert samsvar"
    elif content_score < high_threshold and conformity_score >= high_threshold:
        return "Korrekt kilde, trenger språkvask"
    else:
        return "Krever omarbeiding"


# ═══════════════════════════════════════════════════════════
# CONTENT QUALITY SCORERS (each 0–100)
# ═══════════════════════════════════════════════════════════


def _score_readability(text: str, field_name: str = "") -> int:
    """Score sentence structure, punctuation, and flow."""
    if len(text) < 10:
        return 50  # Too short to judge

    score = 50  # Baseline

    # Sentence endings
    sentences = re.findall(r"[.!?]\s", text + " ")
    if len(sentences) >= 3:
        score += 30
    elif len(sentences) >= 1:
        score += 15

    # Starts with a capital letter
    if text[0].isupper():
        score += 5

    # Ends with proper punctuation
    if text.rstrip()[-1] in ".!?)":
        score += 10

    # Penalize very long runs without punctuation
    words = text.split()
    if len(words) > 20 and not sentences:
        score -= 20

    # Product name: short and clean is fine
    if field_name == "Produktnavn" and len(text) < 80:
        score = max(score, 70)

    return max(0, min(100, score))


def _score_structure(text: str, field_name: str = "") -> int:
    """Score key-value pairs, bullets, paragraph structure."""
    score = 40  # Baseline for plain text

    # Key-value pairs
    kv_matches = _SPEC_KV_PATTERN.findall(text)
    if kv_matches:
        score += min(30, len(kv_matches) * 8)

    # Bullet/numbered lists
    bullets = re.findall(r"^\s*[•\-\*►]\s+.+", text, re.MULTILINE)
    numbered = re.findall(r"^\s*\d+[.)]\s+.+", text, re.MULTILINE)
    if bullets or numbered:
        score += min(20, (len(bullets) + len(numbered)) * 5)

    # Paragraphs (multiple sections)
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    if len(paragraphs) >= 2:
        score += 10

    # Spec fields reward structure highly
    if field_name == "Spesifikasjon" and kv_matches:
        score += 10

    # Category: hierarchy bonus
    if field_name == "Kategori" and " > " in text:
        depth = text.count(" > ") + 1
        score += min(30, depth * 10)

    return max(0, min(100, score))


def _score_language(text: str) -> int:
    """Score Norwegian language quality."""
    if len(text) < 10:
        return 70  # Too short to judge

    score = 50  # Baseline

    no_count = len(_NO_MEDICAL_TERMS.findall(text))
    en_count = len(_EN_INDICATORS.findall(text))
    sv_count = len(_SV_INDICATORS.findall(text))

    # Norwegian boost
    if no_count >= 3:
        score += 35
    elif no_count >= 1:
        score += 20

    # English penalty
    if en_count >= 3:
        score -= 30
    elif en_count >= 1:
        score -= 10

    # Swedish penalty
    if sv_count >= 2:
        score -= 20

    return max(0, min(100, score))


def _score_completeness(text: str, field_name: str = "") -> int:
    """Score detail level and length adequacy."""
    length = len(text)

    if field_name == "Produktnavn":
        if 15 <= length <= 100:
            return 95
        elif length >= 10:
            return 75
        elif length >= 5:
            return 55
        return 25

    if field_name == "Beskrivelse":
        has_sentences = bool(re.search(r"[.!?]\s", text))
        has_details = bool(_PRECISION_UNITS.search(text))
        if length >= 100 and has_sentences and has_details:
            return 95
        if length >= 80 and has_sentences:
            return 80
        if length >= 40:
            return 55
        if length >= 15:
            return 35
        return 15

    if field_name == "Spesifikasjon":
        kv_count = len(re.findall(r"\w+\s*:\s*\w+", text))
        has_units = bool(_PRECISION_UNITS.search(text))
        if kv_count >= 4 and has_units:
            return 95
        if kv_count >= 3:
            return 80
        if kv_count >= 1 or has_units:
            return 55
        return 30

    if field_name == "Kategori":
        if " > " in text and text.count(" > ") >= 2:
            return 95
        if " > " in text:
            return 75
        if length >= 5:
            return 50
        return 25

    if field_name in ("Produsent", "Produsentens varenummer"):
        if length >= 3:
            return 85
        return 40

    if field_name == "Pakningsinformasjon":
        has_qty = bool(re.search(r"\d+\s*(?:stk|pk|per|pr|x)", text, re.I))
        if has_qty and length >= 10:
            return 90
        if length >= 10:
            return 65
        return 35

    # Default
    if length >= 20:
        return 70
    if length >= 5:
        return 45
    return 20


def _score_webshop_suitability(text: str, field_name: str = "") -> int:
    """Score how suitable the text is for a webshop product page."""
    score = 50  # Baseline

    # Positive: customer-facing product language
    positive_hits = len(_WEBSHOP_POSITIVE.findall(text))
    if positive_hits >= 2:
        score += 25
    elif positive_hits >= 1:
        score += 15

    # Positive: has specific measurements (customers care about this)
    if _PRECISION_UNITS.search(text):
        score += 10

    # Negative: internal codes / raw data
    if re.search(r"\b(?:GID|EAN|GTIN|UDI|LOT)\b", text):
        score -= 15

    # Negative: PDF metadata language
    if re.search(r"(?i)\b(?:produktdatablad|data\s*sheet|revision|version)\b", text):
        score -= 20

    # Negative: very long unstructured text (customer won't read)
    if len(text) > 500 and "\n" not in text:
        score -= 15

    return max(0, min(100, score))


def _score_noise_level(text: str) -> int:
    """Score cleanliness (inverted: 100 = clean, 0 = all noise)."""
    hits = 0
    for pattern in _NOISE_PATTERNS:
        if pattern.search(text):
            hits += 1

    # Special characters ratio
    special_ratio = len(re.findall(r"[^\w\s.,;:\-/()\n]", text)) / max(len(text), 1)
    if special_ratio > 0.10:
        hits += 1

    if hits == 0:
        return 100
    elif hits == 1:
        return 70
    elif hits == 2:
        return 40
    else:
        return 15


# ═══════════════════════════════════════════════════════════
# CONFORMITY QUALITY SCORERS (each 0–100)
# ═══════════════════════════════════════════════════════════


def _score_source_match(fa: FieldAnalysis) -> int:
    """Score based on golden source hierarchy position."""
    if not fa.current_value:
        return 0

    try:
        from backend.golden_source import (
            get_source_priority, get_tier_for_origin, TIER_NONE,
        )
    except ImportError:
        return 50  # Can't evaluate without golden source module

    origin = fa.value_origin or ""
    tier = get_tier_for_origin(origin)
    priority = get_source_priority(fa.field_name)

    if tier != TIER_NONE and tier in priority:
        position = priority.index(tier)
        return max(40, 95 - position * 15)
    elif tier != TIER_NONE:
        return 50
    return 30


def _score_text_agreement(current: str, reference: Optional[str]) -> int:
    """Score agreement between current value and a reference value."""
    if not reference or not reference.strip():
        return 50  # No reference — neutral (can't confirm or deny)

    current_clean = current.strip().lower()
    ref_clean = reference.strip().lower()

    if current_clean == ref_clean:
        return 100

    # One contains the other
    if current_clean in ref_clean or ref_clean in current_clean:
        return 80

    # Word overlap
    current_words = set(current_clean.split())
    ref_words = set(ref_clean.split())
    if current_words and ref_words:
        overlap = current_words & ref_words
        ratio = len(overlap) / max(len(current_words), len(ref_words))
        if ratio >= 0.7:
            return 75
        if ratio >= 0.4:
            return 55
        if ratio >= 0.2:
            return 35

    return 15  # Clearly different


def _score_datasheet_match(
    fa: FieldAnalysis,
    enrichment_results: Optional[list] = None,
) -> int:
    """Score agreement with PDF datasheet enrichment results."""
    if not enrichment_results or not fa.current_value:
        return 50  # No datasheet data — neutral

    val = fa.current_value.strip().lower()

    # Map field names to enrichment result field names
    field_map = {
        "Produktnavn": "product_name",
        "Beskrivelse": "description",
        "Spesifikasjon": "specifications",
        "Pakningsinformasjon": "packaging_info",
        "Produsent": "manufacturer",
        "Produsentens varenummer": "manufacturer_article_number",
    }

    er_field = field_map.get(fa.field_name)
    if not er_field:
        return 50

    for er in enrichment_results:
        if er.field_name == er_field and er.suggested_value:
            er_val = er.suggested_value.strip().lower()
            if val == er_val:
                return 100
            if val in er_val or er_val in val:
                return 80
            # Word overlap
            val_words = set(val.split())
            er_words = set(er_val.split())
            if val_words and er_words:
                overlap = val_words & er_words
                ratio = len(overlap) / max(len(val_words), len(er_words))
                if ratio >= 0.5:
                    return 65
                if ratio >= 0.2:
                    return 40
            return 20  # PDF says something different

    return 50  # No PDF data for this field


def _score_manufacturer_match(
    fa: FieldAnalysis,
    manufacturer_data: Optional[object] = None,
) -> int:
    """Score agreement with manufacturer source data."""
    if not manufacturer_data or not fa.current_value:
        return 50  # No manufacturer data — neutral

    # Check if manufacturer_data has the relevant field
    mfr_val = None
    if hasattr(manufacturer_data, "found") and manufacturer_data.found:
        field_attr_map = {
            "Produktnavn": "product_name",
            "Beskrivelse": "description",
        }
        attr = field_attr_map.get(fa.field_name)
        if attr:
            mfr_val = getattr(manufacturer_data, attr, None)

        if fa.field_name == "Spesifikasjon" and hasattr(manufacturer_data, "specifications"):
            specs = manufacturer_data.specifications
            if specs:
                mfr_val = "; ".join(f"{k}: {v}" for k, v in specs.items())

    if not mfr_val:
        return 50  # No manufacturer data for this field

    return _score_text_agreement(fa.current_value, mfr_val)


def _score_scope_match(text: str, field_name: str = "") -> int:
    """Score whether text is product-specific (not family/variant data)."""
    if not text or field_name not in (
        "Produktnavn", "Beskrivelse", "Spesifikasjon", "Pakningsinformasjon",
    ):
        return 80  # Not applicable or neutral

    score = 85  # Baseline: assume product-specific

    # Family/variant indicators reduce score
    family_indicators = re.findall(
        r"(?i)\b(?:serie|sortiment|familie|produktlinje|finnes i|"
        r"tilgjengelig i|flere størrelser|alle varianter|velg mellom)\b",
        text,
    )
    if family_indicators:
        score -= min(40, len(family_indicators) * 15)

    # Multiple article numbers suggest family data
    art_numbers = re.findall(r"\b[A-Z]{0,3}\d{5,8}\b", text)
    if len(art_numbers) >= 3:
        score -= 30
    elif len(art_numbers) >= 2:
        score -= 15

    # Variant table indicators
    if re.search(r"(?i)\b(?:størrelseskode|size\s*code|salgsenhet|transportkartong)\b", text):
        score -= 25

    return max(0, min(100, score))
