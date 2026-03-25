"""Mandatory quality gate for all enrichment suggestions before Excel export.

This module is the FINAL checkpoint before any suggestion is written to output.
It enforces hard rules that prevent garbage data from reaching the user.

Design principle: CONSERVATIVE. Better to output nothing than garbage.
If in doubt, reject and flag for manual review.

This is for medical/healthcare products — factual accuracy is paramount.
"""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# ── Hard-reject patterns: these NEVER belong in product data ──

# Standalone phone numbers (with or without label)
_PHONE_PATTERNS = [
    re.compile(r"(?i)(?:tel|telefon|tlf|fax|phone|mob|mobil)\s*[.:]?\s*[\+\d\(\)\s\-]{7,}"),
    re.compile(r"\+\d{1,3}\s*[\d\s\-\(\)]{7,}"),  # +47 123 45 678
    re.compile(r"\b0\d{1,3}[\s\-]\d{2,3}[\s\-]\d{2,4}\b"),  # 0800-123-456
    # Bare phone numbers: exactly 2-4 digit groups separated by spaces (22 04 72 00)
    # Must have at least 3 groups to avoid matching product codes
    re.compile(r"(?<!\w)\d{2,4}\s\d{2,4}\s\d{2,4}(?:\s\d{2,4})?(?!\w)"),
    # 10+ consecutive digits (definitely not a product code)
    re.compile(r"(?<!\d)\d{10,}(?!\d)"),
]

_EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

_POSTAL_PATTERN = re.compile(
    r"(?i)(?:postboks|pb|p\.?o\.?\s*box)\s+\d+|"
    r"\b\d{4}\s+[A-ZÆØÅ][a-zæøå]+\b|"
    r"(?:gate|gata|veien|vei|vegen|veg|allé|plass)\s+\d+"
)

_URL_IN_TEXT = re.compile(r"(?:https?://|www\.)[^\s]+", re.IGNORECASE)

# Contact/support section indicators
_CONTACT_SECTION_PATTERNS = [
    re.compile(r"(?i)for\s+(?:more|mer)\s+(?:information|informasjon|info)\s+(?:contact|kontakt)"),
    re.compile(r"(?i)customer\s+(?:support|service|care)"),
    re.compile(r"(?i)(?:kundeservice|kundestøtte|kontakt\s+oss)"),
    re.compile(r"(?i)(?:approved|godkjent)\s+(?:by|av)\s+[A-Z]"),
    re.compile(r"(?i)(?:visit|besøk)\s+(?:us|oss)\s+(?:at|på)"),
]

# PDF/document noise
_PDF_NOISE_PATTERNS = [
    re.compile(r"(?i)\bside\s+\d+\b"),
    re.compile(r"(?i)\bpage\s+\d+\b"),
    re.compile(r"(?i)\bproduktdatablad\b"),
    re.compile(r"(?i)\btechnical\s+data\s*sheet\b"),
    re.compile(r"(?i)\bproduct\s+data\s*sheet\b"),
    re.compile(r"(?i)\bcopyright\b|©|\(c\)"),
    re.compile(r"(?i)\ball\s+rights?\s+reserved\b"),
    re.compile(r"(?i)\bprinted\s+in\b"),
    re.compile(r"(?i)\bdato\s*:\s*\d"),
    re.compile(r"(?i)\bdate\s*:\s*\d"),
    re.compile(r"(?i)\bversjon\s*:\s*\d"),
    re.compile(r"(?i)\bversion\s*:\s*\d"),
    re.compile(r"(?i)\brev\.\s*\d"),
]

# Drawing/technical document noise
_DRAWING_NOISE = [
    re.compile(r"(?i)\b(?:side|front|rear|top|bottom)\s+view\b"),
    re.compile(r"(?i)\btotal\s+length\b"),
    re.compile(r"(?i)\b(?:scale|målestokk)\s*[:=]\s*\d"),
    re.compile(r"(?i)\b(?:drawing|tegning)\s*(?:no|nr|number)"),
    re.compile(r"(?i)\b(?:tolerance|toleranse)\s*[:=±]"),
]

# CE/regulatory boilerplate
_BOILERPLATE_PATTERNS = [
    re.compile(r"(?i)\bce[\s\-]?(?:mark|merke|marked|merket)\b.*\b(?:93|2017)/"),
    re.compile(r"(?i)\bmedical\s+device\s+(?:regulation|directive)\b"),
    re.compile(r"(?i)\b(?:class|klasse)\s+(?:I|II|III)\s+(?:medical|medisinsk)\b"),
    re.compile(r"(?i)\bnotified\s+body\b"),
    re.compile(r"(?i)\biso\s+\d{4,5}\s*:\s*\d{4}\b"),  # ISO standard references
]

# Article number dump pattern (multiple article numbers listed)
_ARTICLE_NUMBER_RE = re.compile(r"\b[A-Z]{0,3}\d{5,8}\b")

# Variant table / product code list indicators
_VARIANT_TABLE_PATTERNS = [
    re.compile(r"(?i)\bstørrelseskode\b"),
    re.compile(r"(?i)\bsize\s*code\b"),
    re.compile(r"(?i)\bdispenser\s*/?\s*kartong\b"),
    re.compile(r"(?i)\bantall\s*(?:pr|per|i)\s*(?:eske|kartong|pakk)"),
    re.compile(r"(?i)\bsalgsenhet\b"),
    re.compile(r"(?i)\btransportkartong\b"),
]


class GateResult:
    """Result of a quality gate check."""

    __slots__ = ("passed", "reason", "severity")

    def __init__(self, passed: bool, reason: str = "", severity: str = "reject"):
        self.passed = passed
        self.reason = reason
        self.severity = severity  # "reject" or "flag_review"

    def __bool__(self):
        return self.passed


# ══════════════════════════════════════════════════════════════
# HARD REJECT CHECKS — these always block the suggestion
# ══════════════════════════════════════════════════════════════


def _check_phone_numbers(text: str) -> GateResult:
    """Reject text containing phone/fax numbers."""
    for pattern in _PHONE_PATTERNS:
        m = pattern.search(text)
        if m:
            matched = m.group().strip()
            # Exclude false positives: measurements like "100 x 200 x 300"
            if re.match(r"\d+\s*x\s*\d+", matched):
                continue
            # Exclude product dimensions/measurements with units nearby
            if re.match(r"\d+\s*(?:mm|cm|ml|mg|g|kg|µm|stk|pk)\b", matched, re.IGNORECASE):
                continue
            # Check context: if the matched number is followed by a unit, it's a measurement
            end_pos = m.end()
            after = text[end_pos:end_pos + 10].strip().lower()
            if after and re.match(r"(?:mm|cm|ml|mg|g|kg|µm|stk|pk|%|°c|bar|kpa|fr|ch)\b", after):
                continue
            # Check context: if preceded by measurement-related words, skip
            start_pos = m.start()
            before = text[max(0, start_pos - 20):start_pos].strip().lower()
            if re.search(r"(?:diameter|lengde|bredde|størrelse|vekt|volum|tykkelse|x)\s*[:=]?\s*$", before):
                continue
            # Exclude numbers that appear in typical product data contexts
            # (e.g., "10 x 20 cm" or "100 stk per eske")
            context = text[max(0, start_pos - 30):min(len(text), end_pos + 30)].lower()
            if re.search(r"\d+\s*x\s*\d+", context):
                continue
            if re.search(r"(?:stk|pk|per|eske|kartong|pakning)", context):
                continue
            return GateResult(False, f"Inneholder telefonnummer: '{matched[:30]}'")
    return GateResult(True)


def _check_email(text: str) -> GateResult:
    """Reject text containing email addresses."""
    m = _EMAIL_PATTERN.search(text)
    if m:
        return GateResult(False, f"Inneholder e-postadresse: '{m.group()}'")
    return GateResult(True)


def _check_postal_address(text: str) -> GateResult:
    """Reject short text that's primarily a postal address."""
    m = _POSTAL_PATTERN.search(text)
    if m and len(text) < 200:
        return GateResult(False, f"Inneholder postadresse: '{m.group()[:40]}'")
    return GateResult(True)


def _check_url_in_text_field(text: str, field_name: str) -> GateResult:
    """Reject URLs in text fields (they don't belong in descriptions/specs)."""
    if field_name in ("Bilde-URL", "PDF-URL", "Produkt-URL"):
        return GateResult(True)
    m = _URL_IN_TEXT.search(text)
    if m:
        return GateResult(False, f"Inneholder URL i tekstfelt: '{m.group()[:50]}'")
    return GateResult(True)


def _check_contact_section(text: str) -> GateResult:
    """Reject text that's primarily a contact/support section."""
    for pattern in _CONTACT_SECTION_PATTERNS:
        if pattern.search(text):
            return GateResult(False, f"Inneholder kontakt-/supporttekst")
    return GateResult(True)


def _check_article_number_dump(text: str, current_sku: str) -> GateResult:
    """Reject text that's mainly a list of article numbers."""
    all_skus = _ARTICLE_NUMBER_RE.findall(text)
    if len(all_skus) < 3:
        return GateResult(True)

    # Check how many are NOT the current SKU
    current_clean = re.sub(r"^[A-Za-z]+", "", current_sku).strip() if current_sku else ""
    other_skus = [s for s in all_skus if re.sub(r"^[A-Za-z]+", "", s).strip() != current_clean]

    if len(other_skus) >= 3:
        return GateResult(False, f"Inneholder {len(other_skus)} andre artikkelnumre — variantliste/produktdump")

    return GateResult(True)


def _check_drawing_noise(text: str) -> GateResult:
    """Reject technical drawing annotations."""
    count = sum(1 for p in _DRAWING_NOISE if p.search(text))
    if count >= 2:
        return GateResult(False, "Inneholder teknisk tegningsnotasjon")
    return GateResult(True)


def _check_pdf_noise(text: str) -> GateResult:
    """Reject text dominated by PDF artifacts."""
    count = sum(1 for p in _PDF_NOISE_PATTERNS if p.search(text))
    if count >= 2:
        return GateResult(False, f"Inneholder {count} PDF-støymønstre")
    return GateResult(True)


def _check_boilerplate(text: str) -> GateResult:
    """Flag but don't reject CE/regulatory boilerplate unless it's the entire text."""
    count = sum(1 for p in _BOILERPLATE_PATTERNS if p.search(text))
    if count >= 2 and len(text) < 300:
        return GateResult(False, "Inneholder hovedsakelig regulatorisk boilerplate")
    return GateResult(True)


def _check_variant_table(text: str) -> GateResult:
    """Reject variant table data."""
    for p in _VARIANT_TABLE_PATTERNS:
        if p.search(text):
            return GateResult(False, "Inneholder varianttabell-data")
    return GateResult(True)


def _check_empty_or_trivial(text: str, field_name: str) -> GateResult:
    """Reject empty, whitespace-only, or trivially short text."""
    if not text or not text.strip():
        return GateResult(False, "Tom verdi")

    stripped = text.strip()

    # Minimum lengths per field
    min_lengths = {
        "Produktnavn": 3,
        "Beskrivelse": 20,
        "Spesifikasjon": 5,
        "Kategori": 2,
        "Pakningsinformasjon": 3,
        "Produsent": 2,
        "Produsentens varenummer": 2,
    }
    min_len = min_lengths.get(field_name, 2)
    if len(stripped) < min_len:
        return GateResult(False, f"For kort for {field_name} ({len(stripped)} tegn, minimum {min_len})")

    # Reject if it's just numbers/punctuation (no actual words)
    if re.match(r"^[\d\s\-/.,;:=+*#()]+$", stripped):
        return GateResult(False, "Inneholder kun tall/tegn, ingen tekst")

    return GateResult(True)


def _check_just_repeats_current(text: str, current_value: Optional[str]) -> GateResult:
    """Reject if suggestion is identical or near-identical to current value."""
    if not current_value:
        return GateResult(True)

    def _normalize(s: str) -> str:
        return re.sub(r"\s+", " ", s.strip().lower().rstrip(".,;:"))

    if _normalize(text) == _normalize(current_value):
        return GateResult(False, "Foreslått verdi er identisk med nåværende verdi")

    return GateResult(True)


# ══════════════════════════════════════════════════════════════
# FIELD-SPECIFIC CHECKS
# ══════════════════════════════════════════════════════════════


def _check_product_name(text: str) -> GateResult:
    """Validate product name specifics."""
    if len(text) > 200:
        return GateResult(False, "Produktnavn er for langt (over 200 tegn) — ser ut som en beskrivelse")
    if text.count(".") > 3:
        return GateResult(False, "Produktnavn inneholder flere setninger")
    if text.count("\n") > 1:
        return GateResult(False, "Produktnavn inneholder linjeskift — ser ut som flerlinjetekst")
    return GateResult(True)


def _check_description(text: str) -> GateResult:
    """Validate description specifics."""
    # Description should not be a single repeated product name
    words = text.split()
    if len(words) < 3:
        return GateResult(False, "Beskrivelse er for kort (under 3 ord)")

    # Check for excessive article number content
    sku_count = len(_ARTICLE_NUMBER_RE.findall(text))
    word_count = len(words)
    if word_count > 0 and sku_count / word_count > 0.3:
        return GateResult(False, "Beskrivelsen inneholder for mange artikkelnumre i forhold til tekst")

    return GateResult(True)


def _check_specification(text: str) -> GateResult:
    """Validate specification specifics."""
    # Specs should ideally have key:value structure or technical content
    # But don't reject just because it lacks this — let it through with a note
    return GateResult(True)


def _check_packaging(text: str) -> GateResult:
    """Validate packaging info specifics."""
    packaging_indicators = [
        r"(?i)\d+\s*(?:stk|pk|stykk|per|pr|i\s+pakning)",
        r"(?i)(?:eske|kartong|pall|pose|boks|pakke|forpakning)\b",
        r"(?i)\d+\s*(?:x\s*\d+)",
        r"(?i)(?:inner|outer|master|transport)\s*(?:pak|box|cart)",
    ]
    has_packaging = any(re.search(p, text) for p in packaging_indicators)

    non_packaging = [
        r"(?i)(?:oppbevar|lagr)\w*\s+(?:tørt|kjølig|mørkt)",
        r"(?i)(?:brukes?\s+til|designed\s+for|intended\s+for)",
        r"(?i)(?:fordeler|benefits|advantages|features)\b",
    ]
    has_non_packaging = any(re.search(p, text) for p in non_packaging)

    if has_non_packaging and not has_packaging:
        return GateResult(False, "Inneholder lagrings-/bruksinformasjon, ikke pakningsdata")
    if not has_packaging and len(text) > 80:
        return GateResult(False, "Lang tekst uten gjenkjennelig pakningsdata")

    return GateResult(True)


# ══════════════════════════════════════════════════════════════
# MAIN QUALITY GATE FUNCTION
# ══════════════════════════════════════════════════════════════


def quality_gate_check(
    suggested_value: str,
    field_name: str,
    current_value: Optional[str] = None,
    current_sku: str = "",
    confidence: float = 0.0,
    source: str = "",
) -> GateResult:
    """Run ALL quality checks on a suggestion before it reaches output.

    This is the mandatory quality gate. Every suggestion MUST pass this
    before being written to Excel or shown to the user.

    Returns GateResult with passed=True if acceptable, or passed=False with reason.
    """
    text = suggested_value.strip() if suggested_value else ""

    # ── Universal checks (apply to all fields) ──
    checks = [
        _check_empty_or_trivial(text, field_name),
        _check_phone_numbers(text),
        _check_email(text),
        _check_postal_address(text),
        _check_url_in_text_field(text, field_name),
        _check_contact_section(text),
        _check_article_number_dump(text, current_sku),
        _check_drawing_noise(text),
        _check_pdf_noise(text),
        _check_boilerplate(text),
        _check_variant_table(text),
        _check_just_repeats_current(text, current_value),
    ]

    for result in checks:
        if not result:
            logger.info(
                f"[quality-gate] REJECTED {field_name} (SKU={current_sku}): "
                f"{result.reason} | value={text[:80]!r}"
            )
            return result

    # ── Field-specific checks ──
    field_checks = {
        "Produktnavn": _check_product_name,
        "Beskrivelse": _check_description,
        "Spesifikasjon": _check_specification,
        "Pakningsinformasjon": _check_packaging,
    }

    field_check = field_checks.get(field_name)
    if field_check:
        result = field_check(text)
        if not result:
            logger.info(
                f"[quality-gate] REJECTED {field_name} (SKU={current_sku}): "
                f"{result.reason} | value={text[:80]!r}"
            )
            return result

    # ── Confidence floor ──
    if confidence < 0.30:
        return GateResult(
            False,
            f"Confidence for lav ({confidence:.2f}) — forslaget er for usikkert",
            severity="flag_review",
        )

    return GateResult(True)


def run_quality_gate_on_suggestions(
    suggestions: list,
    results_for_logging: str = "",
) -> tuple[list, list]:
    """Run quality gate on a list of EnrichmentSuggestion objects.

    Returns (passed_suggestions, rejected_log_entries).
    """
    passed = []
    rejected_log = []

    for s in suggestions:
        if not s.suggested_value:
            continue

        result = quality_gate_check(
            suggested_value=s.suggested_value,
            field_name=s.field_name,
            current_value=s.current_value,
            current_sku=getattr(s, "_sku", ""),
            confidence=s.confidence,
            source=s.source or "",
        )

        if result:
            passed.append(s)
        else:
            rejected_log.append({
                "field": s.field_name,
                "reason": result.reason,
                "value_preview": (s.suggested_value or "")[:100],
                "confidence": s.confidence,
            })

    if rejected_log:
        logger.info(
            f"[quality-gate] {results_for_logging}: "
            f"{len(passed)} passed, {len(rejected_log)} rejected"
        )
        for entry in rejected_log:
            logger.info(
                f"  REJECTED {entry['field']}: {entry['reason']} "
                f"(conf={entry['confidence']:.2f}, preview={entry['value_preview'][:60]!r})"
            )

    return passed, rejected_log
