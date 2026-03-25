"""Regression guard — prevents enrichment suggestions that make data worse.

Every suggestion must pass a multi-factor quality comparison against the
current value.  A suggestion is blocked if the proposed value is:

  - shorter AND less informative
  - more generic (fewer specific details)
  - noisier (PDF artifacts, contact info, variant tables)
  - worse language (English replacing Norwegian, broken grammar)
  - worse structure (unstructured replacing structured)
  - less precise (vague replacing specific measurements/materials)
  - less suitable for a webshop product page

The guard runs as a final check inside the enricher *before* a suggestion
is emitted, and again in ``final_quality_gate`` as a safety net.

Design principles:
  - Conservative: when in doubt, block the suggestion
  - Field-aware: different fields have different quality criteria
  - Transparent: every block produces a human-readable reason
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ── Weights for each quality factor (sum = 1.0) ──

_WEIGHTS = {
    "information_density": 0.25,
    "language_quality": 0.15,
    "precision": 0.20,
    "structure": 0.15,
    "noise_level": 0.15,
    "length_adequacy": 0.10,
}

# ── Norwegian medical/product keywords that indicate domain relevance ──

_NORWEGIAN_MEDICAL_TERMS = re.compile(
    r"(?i)\b(?:"
    r"hanske|hansker|bandasje|kompress|plaster|sprøyte|kanyle|kateter|"
    r"sutur|frakk|munnbind|stetoskop|termometer|skalpell|pinsett|saks|"
    r"fikseringstape|forbinding|engangs|flergangs|steril|usteril|"
    r"pudderfri|lateksfri|nitril|vinyl|polyester|silikon|polyuretan|"
    r"sårpleie|beskyttelse|absorberende|hypoallergen|ikke-vevd|"
    r"selvklebende|elastisk|kirurgisk|medisinsk|undersøkelse|"
    r"behandling|pasient|infeksjon|hygiene|desinfeksjon|"
    r"materiale|størrelse|lengde|bredde|tykkelse|diameter|volum|"
    r"pakning|forpakning|pakke|eske|kartong|stykk|antall"
    r")\b"
)

# ── Precision indicators: specific measurements, codes, materials ──

_PRECISION_PATTERNS = [
    re.compile(r"\b\d+[\.,]?\d*\s*(?:mm|cm|m|ml|l|g|kg|µm|µl|stk|pk|%|°C|bar|kPa|Fr|Ch|Ga)\b", re.I),
    re.compile(r"\b(?:REF|LOT|EAN|GTIN|UDI)\s*[:\-]?\s*[\d\-]+\b", re.I),
    re.compile(r"\b(?:EN|ISO|IEC)\s*\d+", re.I),
    re.compile(r"\b(?:klasse|class|type|kategori)\s+[IVXivx\d]+\b", re.I),
    re.compile(r"\b\d+\s*x\s*\d+(?:\s*(?:mm|cm|m))?\b", re.I),
    re.compile(r"\b(?:nitril|vinyl|latex|silikon|polyuretan|PVC|PE|PP|HDPE|LDPE)\b", re.I),
]

# ── Noise indicators: things that should NOT be in webshop content ──

_NOISE_PATTERNS = [
    re.compile(r"(?i)(?:tel|telefon|tlf|fax|phone|mob)\s*[.:]?\s*[\+\d\(\)\s\-]{7,}"),
    re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"),
    re.compile(r"(?i)(?:postboks|pb|p\.?o\.?\s*box)\s+\d+"),
    re.compile(r"(?i)(?:www\.[a-z0-9\-]+\.[a-z]{2,}|https?://[^\s]+)"),
    re.compile(r"(?i)\b(?:side|page)\s+\d+\b"),
    re.compile(r"(?i)\b(?:copyright|©|\(c\))\b"),
    re.compile(r"(?i)\b(?:versjon|version|rev\.?\s*\d|dato\s*:|date\s*:)\b"),
    re.compile(r"(?i)\b(?:printed\s+in|all\s+rights?\s+reserved)\b"),
    re.compile(r"(?i)\b(?:produktdatablad|technical\s+data\s*sheet|product\s+data\s*sheet)\b"),
]

# ── Structure indicators: key-value pairs, bullet lists, proper formatting ──

_STRUCTURE_PATTERNS = [
    re.compile(r"^[A-ZÆØÅ\u00C0-\u00FF][a-zæøåa-z\u00E0-\u00FF\s]+\s*:\s*.+", re.MULTILINE),
    re.compile(r"^\s*[•\-\*►]\s+.+", re.MULTILINE),
    re.compile(r"^\s*\d+[.)]\s+.+", re.MULTILINE),
]

# ── English indicators (for detecting language regression) ──

_ENGLISH_STRONG = re.compile(
    r"(?i)\b(?:the|designed|intended|provides|ensures|available|suitable|"
    r"used\s+for|made\s+of|features|compatible|offering|recommended)\b"
)


@dataclass
class QualityScore:
    """Multi-factor quality score for a text value."""
    information_density: float = 0.0  # unique meaningful tokens / length
    language_quality: float = 0.0     # Norwegian, good grammar, no mixing
    precision: float = 0.0           # specific measurements, materials, codes
    structure: float = 0.0           # key-value, bullets, proper formatting
    noise_level: float = 0.0         # inverse: 1.0 = no noise, 0.0 = all noise
    length_adequacy: float = 0.0     # appropriate length for field type
    total: float = 0.0               # weighted composite

    details: dict = field(default_factory=dict)

    def compute_total(self) -> float:
        self.total = sum(
            getattr(self, factor) * weight
            for factor, weight in _WEIGHTS.items()
        )
        return self.total


@dataclass
class ComparisonResult:
    """Result of comparing current vs proposed value."""
    current_score: QualityScore
    proposed_score: QualityScore
    is_improvement: bool
    delta: float             # proposed.total - current.total
    reason: str              # human-readable explanation
    factors_worse: list[str] = field(default_factory=list)
    factors_better: list[str] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════


def score_text_quality(
    text: str,
    field_name: str = "",
) -> QualityScore:
    """Score a text value on multiple quality dimensions (each 0.0–1.0)."""
    if not text or not text.strip():
        return QualityScore()

    text = text.strip()
    qs = QualityScore()

    # ── 1. Information density ──
    qs.information_density = _score_information_density(text)

    # ── 2. Language quality ──
    qs.language_quality = _score_language_quality(text)

    # ── 3. Precision ──
    qs.precision = _score_precision(text)

    # ── 4. Structure ──
    qs.structure = _score_structure(text, field_name)

    # ── 5. Noise level (inverse: high = clean) ──
    qs.noise_level = _score_noise_level(text)

    # ── 6. Length adequacy ──
    qs.length_adequacy = _score_length_adequacy(text, field_name)

    qs.compute_total()
    return qs


def compare_current_vs_proposed(
    current_value: str,
    proposed_value: str,
    field_name: str = "",
    confidence: float = 1.0,
) -> ComparisonResult:
    """Compare current and proposed values across all quality factors.

    Returns a ComparisonResult with detailed factor-by-factor analysis
    and an overall verdict on whether the proposal is an improvement.
    """
    current_score = score_text_quality(current_value, field_name)
    proposed_score = score_text_quality(proposed_value, field_name)

    # Apply confidence discount to proposed score
    # Low-confidence suggestions need a bigger quality margin to pass
    confidence_factor = max(0.5, confidence)
    adjusted_proposed_total = proposed_score.total * confidence_factor

    delta = adjusted_proposed_total - current_score.total

    # Factor-by-factor comparison
    factors_worse = []
    factors_better = []
    for factor in _WEIGHTS:
        current_val = getattr(current_score, factor)
        proposed_val = getattr(proposed_score, factor)
        if proposed_val < current_val - 0.05:  # tolerance
            factors_worse.append(factor)
        elif proposed_val > current_val + 0.05:
            factors_better.append(factor)

    # ── Decision logic ──
    is_improvement = True
    reason = ""

    # Rule 1: If noise increases, block
    if proposed_score.noise_level < current_score.noise_level - 0.10:
        is_improvement = False
        reason = "Forslaget er mer støyete enn nåværende verdi (PDF-artefakter, kontaktinfo, metadata)"

    # Rule 2: If language quality drops significantly, block
    elif proposed_score.language_quality < current_score.language_quality - 0.15:
        is_improvement = False
        reason = "Forslaget har dårligere språkkvalitet (feil språk, blandet språk, dårlig grammatikk)"

    # Rule 3: If precision drops AND information density drops, block
    elif (proposed_score.precision < current_score.precision - 0.05
          and proposed_score.information_density < current_score.information_density - 0.05):
        is_improvement = False
        reason = "Forslaget er mindre presist og mindre informativt enn nåværende verdi"

    # Rule 4: If multiple factors are worse and none are significantly better, block
    elif len(factors_worse) >= 3 and not factors_better:
        is_improvement = False
        worse_labels = _translate_factor_names(factors_worse)
        reason = f"Forslaget er dårligere på flere områder: {', '.join(worse_labels)}"

    # Rule 5: Overall weighted score must be an improvement (with margin)
    elif delta < 0.02:
        # Not enough improvement to justify the change
        is_improvement = False
        if delta < 0:
            reason = (
                f"Forslaget har lavere kvalitet enn nåværende verdi "
                f"(Δ={delta:+.3f})"
            )
        else:
            reason = (
                f"Forslaget gir ingen vesentlig forbedring "
                f"(Δ={delta:+.3f}, under terskel 0.02)"
            )

    # Rule 6: If proposed is much shorter and not more dense, likely info loss
    elif (current_value and proposed_value
          and len(proposed_value) < len(current_value) * 0.5
          and proposed_score.information_density <= current_score.information_density + 0.10):
        is_improvement = False
        reason = (
            "Forslaget er vesentlig kortere uten å være mer informasjonstett — "
            "sannsynlig informasjonstap"
        )

    if is_improvement:
        better_labels = _translate_factor_names(factors_better) if factors_better else ["samlet kvalitet"]
        reason = f"Forslaget er bedre på: {', '.join(better_labels)} (Δ={delta:+.3f})"

    return ComparisonResult(
        current_score=current_score,
        proposed_score=proposed_score,
        is_improvement=is_improvement,
        delta=delta,
        reason=reason,
        factors_worse=factors_worse,
        factors_better=factors_better,
    )


def is_proposed_value_better(
    current_value: Optional[str],
    proposed_value: Optional[str],
    field_name: str = "",
    confidence: float = 1.0,
) -> tuple[bool, str]:
    """Quick check: is the proposed value better than current?

    Returns (is_better, reason).
    Use this as the main entry point for the regression guard.
    """
    # If no current value, any non-empty proposal is better
    if not current_value or not current_value.strip():
        if proposed_value and proposed_value.strip():
            return True, "Nåværende verdi mangler — forslaget fyller et hull"
        return False, "Ingen verdi foreslått"

    # If no proposed value, nothing to do
    if not proposed_value or not proposed_value.strip():
        return False, "Foreslått verdi er tom"

    result = compare_current_vs_proposed(
        current_value, proposed_value, field_name, confidence,
    )
    return result.is_improvement, result.reason


def block_regressive_suggestion(
    current_value: Optional[str],
    proposed_value: Optional[str],
    field_name: str = "",
    confidence: float = 1.0,
) -> tuple[bool, str]:
    """Check if a suggestion should be blocked as regressive.

    Returns (should_block, reason).
    True = the suggestion makes data worse and should be blocked.
    """
    is_better, reason = is_proposed_value_better(
        current_value, proposed_value, field_name, confidence,
    )
    if is_better:
        return False, ""
    return True, reason


# ═══════════════════════════════════════════════════════════
# SCORING FUNCTIONS (each returns 0.0–1.0)
# ═══════════════════════════════════════════════════════════


def _score_information_density(text: str) -> float:
    """Score information density: unique meaningful tokens relative to length.

    High score = lots of distinct, meaningful words per character.
    Low score = repetitive, filler-heavy, or very sparse.
    """
    words = re.findall(r"\b[a-zæøåA-ZÆØÅ\d]{2,}\b", text)
    if not words:
        return 0.0

    # Unique meaningful words (exclude very common stopwords)
    stopwords = {
        "og", "i", "er", "en", "et", "den", "det", "de", "som", "til",
        "for", "med", "av", "på", "fra", "har", "kan", "vil", "skal",
        "the", "and", "for", "with", "from", "are", "was", "has", "is",
        "of", "in", "to", "at", "by", "or", "an", "be", "it",
    }
    meaningful = {w.lower() for w in words if w.lower() not in stopwords and len(w) > 2}

    # Density = unique meaningful tokens / total length (normalized)
    raw_density = len(meaningful) / max(len(text), 1) * 100

    # Map to 0-1 (sweet spot around 5-15 unique words per 100 chars)
    if raw_density < 1:
        return 0.1
    elif raw_density < 3:
        return 0.3
    elif raw_density < 8:
        return 0.7
    elif raw_density < 15:
        return 1.0
    else:
        return 0.8  # Very dense can mean telegraphic/fragmentary


def _score_language_quality(text: str) -> float:
    """Score language quality: Norwegian, proper grammar, no mixing."""
    score = 0.5  # Baseline

    # Norwegian medical terms → boost
    no_terms = len(_NORWEGIAN_MEDICAL_TERMS.findall(text))
    if no_terms >= 3:
        score += 0.3
    elif no_terms >= 1:
        score += 0.15

    # English indicators → penalty
    en_terms = len(_ENGLISH_STRONG.findall(text))
    if en_terms >= 3:
        score -= 0.3
    elif en_terms >= 1:
        score -= 0.1

    # Sentence endings (proper punctuation)
    sentences = re.findall(r"[.!?]\s", text + " ")
    if sentences:
        score += 0.1

    # Very short fragments without sentence structure
    if len(text) > 30 and not sentences and not re.search(r"[.!?]$", text):
        score -= 0.1

    return max(0.0, min(1.0, score))


def _score_precision(text: str) -> float:
    """Score precision: specific measurements, materials, codes, standards."""
    hits = 0
    for pattern in _PRECISION_PATTERNS:
        matches = pattern.findall(text)
        hits += len(matches)

    if hits >= 5:
        return 1.0
    elif hits >= 3:
        return 0.8
    elif hits >= 1:
        return 0.5
    return 0.15  # No specific data at all


def _score_structure(text: str, field_name: str = "") -> float:
    """Score structure: key-value pairs, bullets, proper formatting."""
    score = 0.3  # Baseline for plain text

    for pattern in _STRUCTURE_PATTERNS:
        matches = pattern.findall(text)
        if matches:
            score += min(0.25, len(matches) * 0.08)

    # Specs benefit more from key-value structure
    if field_name in ("Spesifikasjon",):
        kv_pairs = re.findall(r"[A-Za-zÆØÅæøå]+\s*:\s*\S+", text)
        if kv_pairs:
            score += min(0.3, len(kv_pairs) * 0.06)

    # Descriptions benefit from sentence structure
    if field_name in ("Beskrivelse",):
        sentences = [s.strip() for s in re.split(r"[.!?]", text) if len(s.strip()) > 10]
        if len(sentences) >= 2:
            score += 0.2
        elif len(sentences) == 1:
            score += 0.1

    return max(0.0, min(1.0, score))


def _score_noise_level(text: str) -> float:
    """Score noise level (inverted: 1.0 = clean, 0.0 = all noise)."""
    noise_hits = 0
    for pattern in _NOISE_PATTERNS:
        if pattern.search(text):
            noise_hits += 1

    if noise_hits == 0:
        return 1.0
    elif noise_hits == 1:
        return 0.7
    elif noise_hits == 2:
        return 0.4
    else:
        return 0.15


def _score_length_adequacy(text: str, field_name: str = "") -> float:
    """Score length adequacy for the field type."""
    length = len(text)

    if field_name == "Produktnavn":
        # Product names: 10-100 chars ideal
        if 10 <= length <= 100:
            return 1.0
        elif length < 10:
            return 0.3
        elif length <= 150:
            return 0.6
        else:
            return 0.2

    elif field_name == "Beskrivelse":
        # Descriptions: 50-500 chars ideal
        if 50 <= length <= 500:
            return 1.0
        elif 30 <= length < 50:
            return 0.6
        elif length < 30:
            return 0.2
        elif length <= 1000:
            return 0.7
        else:
            return 0.3

    elif field_name == "Spesifikasjon":
        # Specs: 20-800 chars ideal
        if 20 <= length <= 800:
            return 1.0
        elif length < 20:
            return 0.3
        elif length <= 1500:
            return 0.6
        else:
            return 0.3

    elif field_name == "Pakningsinformasjon":
        # Packaging: 10-200 chars ideal
        if 10 <= length <= 200:
            return 1.0
        elif length < 10:
            return 0.3
        else:
            return 0.5

    else:
        # Generic: 5-500 chars
        if 5 <= length <= 500:
            return 0.8
        elif length < 5:
            return 0.2
        else:
            return 0.5


# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════


def _translate_factor_names(factors: list[str]) -> list[str]:
    """Translate factor names to Norwegian for user-facing messages."""
    translations = {
        "information_density": "informasjonstetthet",
        "language_quality": "språkkvalitet",
        "precision": "presisjon",
        "structure": "struktur",
        "noise_level": "støynivå",
        "length_adequacy": "lengdetilpasning",
    }
    return [translations.get(f, f) for f in factors]
