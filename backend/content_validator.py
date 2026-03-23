"""Content validation, cleaning, and classification for webshop-ready product data.

Central module that ensures all enrichment output is:
- Free from contact information (phone, email, address)
- Free from other products' article numbers
- In Norwegian (or flagged for translation)
- Correctly classified as description vs. specification
- Product-specific (not family/variant-level noise)
- Webshop-ready (no raw PDF artifacts, table headers, footers)

Used by enricher.py before any EnrichmentSuggestion is created,
and by excel_handler.py before writing to Excel output.
"""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# ── Contact information patterns ──

_PHONE_PATTERN = re.compile(
    r"(?i)(?:"
    r"(?:tel|telefon|tlf|fax|phone|mob|mobil)\s*[.:]?\s*"
    r"[\+\d\(\)\s\-]{7,}"
    r"|"
    r"\+\d{1,3}\s*[\d\s\-\(\)]{7,}"  # +47 123 45 678
    r"|"
    r"\b0\d{1,3}[\s\-]\d{2,3}[\s\-]\d{2,4}\b"  # 0800-123-456
    r")"
)

_EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)

_POSTAL_ADDRESS_PATTERN = re.compile(
    r"(?i)(?:"
    r"(?:postboks|pb|p\.?o\.?\s*box)\s+\d+"
    r"|"
    r"\b\d{4}\s+[A-ZÆØÅ][a-zæøå]+\b"  # 0153 Oslo, 4028 Stavanger
    r"|"
    r"(?:gate|gata|veien|vei|vegen|veg|allé|plass)\s+\d+"
    r")"
)

_WEBSITE_FOOTER_PATTERN = re.compile(
    r"(?i)(?:"
    r"www\.[a-z0-9\-]+\.[a-z]{2,}"
    r"|"
    r"https?://[^\s]+"
    r"|"
    r"(?:besøk|visit|se)\s+(?:oss\s+på\s+)?(?:www|http)"
    r")"
)

# ── PDF/catalog noise patterns ──

_PDF_NOISE_PATTERNS = re.compile(
    r"(?i)(?:"
    r"\bside\s+\d+\b"
    r"|\bpage\s+\d+\b"
    r"|\bproduktdatablad\b"
    r"|\btechnical\s+data\s*sheet\b"
    r"|\bproduct\s+data\s*sheet\b"
    r"|\bvelg\s+mellom\b"
    r"|\bart\.?\s*nr\.?\s*[:.]"
    r"|\bref\.?\s*(?:nr|no|nummer)\.?\s*[:.]"
    r"|\bbestillingsnummer\b"
    r"|\border\s+(?:number|code|no)\b"
    r"|\bcopyright\b|©|\(c\)"
    r"|\ball\s+rights?\s+reserved\b"
    r"|\bprinted\s+in\b"
    r"|\bdato\s*:"
    r"|\bdate\s*:"
    r"|\bversjon\s*:"
    r"|\bversion\s*:"
    r"|\brev\.\s*\d+"
    r")"
)

# ── Multi-SKU / variant table patterns ──

_ARTICLE_NUMBER_RE = re.compile(r"\b[A-Z]{0,3}\d{5,8}\b")

_VARIANT_TABLE_INDICATORS = re.compile(
    r"(?i)(?:"
    r"\bstørrelseskode\b"
    r"|\bsize\s*code\b"
    r"|\bdispenser\s*/?\s*kartong\b"
    r"|\bantall\s*(?:pr|per|i)\s*(?:eske|kartong|pakk)"
    r"|\bsalgsenhet\b"
    r"|\btransportkartong\b"
    r"|\binnhold\b.*\bantall\b"
    r")"
)

# ── Language detection patterns ──

_SWEDISH_INDICATORS = re.compile(
    r"(?i)\b(?:och|för|med|som|kan|har|inte|eller|detta|dessa|alla|utan|också|"
    r"användas|storlek|förpackning|handske|handskar|skydd|steril|engångs|"
    r"material|längd|bredd|tjocklek|vikt)\b"
)

_DANISH_INDICATORS = re.compile(
    r"(?i)\b(?:og|til|med|som|kan|har|ikke|eller|dette|disse|alle|uden|også|"
    r"bruges|størrelse|emballage|handske|handsker|beskyttelse|"
    r"materiale|længde|bredde|tykkelse|vægt)\b"
)

_ENGLISH_INDICATORS = re.compile(
    r"(?i)\b(?:the|and|for|with|this|that|from|are|was|has|have|will|can|"
    r"used|using|designed|intended|suitable|available|provides|ensures|"
    r"glove|gloves|bandage|sterile|non-sterile|disposable|latex-free|"
    r"powder-free|protection|material|length|width|thickness|weight)\b"
)

_NORWEGIAN_INDICATORS = re.compile(
    r"(?i)\b(?:og|til|med|som|kan|har|ikke|eller|dette|disse|alle|uten|også|"
    r"brukes|størrelse|pakning|hanske|hansker|beskyttelse|steril|engangs|"
    r"materiale|lengde|bredde|tykkelse|vekt|lateksfri|pudderfri|"
    r"sårpleie|bandasje|kompress|plaster|sprøyte|kateter)\b"
)

# ── Description vs. specification classification ──

_SPEC_KEY_VALUE_PATTERN = re.compile(
    r"^[A-ZÆØÅ\u00C0-\u00FF][a-zæøåa-z\u00E0-\u00FF\s]+\s*:\s*.+",
    re.MULTILINE,
)

_TECHNICAL_UNITS = re.compile(
    r"\b\d+[\.,]?\d*\s*(?:mm|cm|m|ml|l|g|kg|µm|stk|pk|%|°C|bar|kPa|Fr|Ch|Ga)\b",
    re.IGNORECASE,
)

_TECHNICAL_KEYWORDS = re.compile(
    r"(?i)\b(?:latex|nitril|vinyl|polyester|silikon|polyuretan|PVC|"
    r"steril|usteril|pudderfri|lateksfri|engangs|flergangs|"
    r"materiale|material|diameter|lengde|bredde|volum|kapasitet|"
    r"temperatur|trykk|pH|absorpsjon|dimensjon)\b"
)


# ═══════════════════════════════════════════════════════════
# PUBLIC API — Cleaning functions
# ═══════════════════════════════════════════════════════════


def clean_contact_noise(text: str) -> str:
    """Remove phone numbers, email addresses, postal addresses, and web footers."""
    if not text:
        return text

    # Remove full lines containing contact info
    lines = text.split("\n")
    clean_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            clean_lines.append(line)
            continue
        # Skip entire line if it's primarily contact info
        if _PHONE_PATTERN.search(stripped):
            logger.debug(f"Removed contact line (phone): {stripped[:60]}")
            continue
        if _EMAIL_PATTERN.search(stripped):
            logger.debug(f"Removed contact line (email): {stripped[:60]}")
            continue
        if _POSTAL_ADDRESS_PATTERN.search(stripped) and len(stripped) < 80:
            logger.debug(f"Removed contact line (address): {stripped[:60]}")
            continue
        if _WEBSITE_FOOTER_PATTERN.search(stripped) and len(stripped) < 100:
            logger.debug(f"Removed contact line (web footer): {stripped[:60]}")
            continue
        clean_lines.append(line)

    return "\n".join(clean_lines).strip()


def clean_variant_table_noise(text: str, current_sku: Optional[str] = None) -> str:
    """Remove variant tables and multi-product listings from text.

    Keeps only content relevant to the current SKU.
    """
    if not text:
        return text

    # If text contains variant table indicators, strip aggressively
    if _VARIANT_TABLE_INDICATORS.search(text):
        lines = text.split("\n")
        clean_lines = []
        in_table = False
        for line in lines:
            stripped = line.strip()
            if _VARIANT_TABLE_INDICATORS.search(stripped):
                in_table = True
                continue
            if in_table:
                # Table rows are typically short with numbers/codes
                if len(stripped) < 60 and (
                    re.match(r"^\s*[\d\w\-]+\s", stripped)
                    or re.match(r"^\s*\d", stripped)
                    or not stripped
                ):
                    continue
                else:
                    in_table = False
            clean_lines.append(line)
        text = "\n".join(clean_lines).strip()

    return text


def remove_other_skus(text: str, current_sku: str) -> str:
    """Remove lines containing article numbers that don't match the current product.

    Preserves lines containing the current SKU but removes references to
    other products' article numbers.
    """
    if not text or not current_sku:
        return text

    # Normalize current SKU for comparison
    current_clean = re.sub(r"^[A-Za-z]+", "", current_sku).strip()

    lines = text.split("\n")
    clean_lines = []
    for line in lines:
        skus_in_line = _ARTICLE_NUMBER_RE.findall(line)
        if skus_in_line:
            # Check if any SKU in this line is NOT the current product
            has_other_sku = False
            for sku in skus_in_line:
                sku_clean = re.sub(r"^[A-Za-z]+", "", sku).strip()
                if sku_clean != current_clean and sku != current_sku:
                    has_other_sku = True
                    break
            if has_other_sku:
                logger.debug(
                    f"Removed line with other SKU(s): {line.strip()[:60]}"
                )
                continue
        clean_lines.append(line)

    return "\n".join(clean_lines).strip()


def clean_pdf_noise(text: str) -> str:
    """Remove common PDF artifacts: page numbers, headers, footers, metadata."""
    if not text:
        return text

    lines = text.split("\n")
    clean_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            clean_lines.append(line)
            continue
        # Skip PDF metadata lines
        if _PDF_NOISE_PATTERNS.match(stripped):
            continue
        # Skip very short lines that are just numbers (page numbers)
        if re.match(r"^\s*\d{1,3}\s*$", stripped):
            continue
        clean_lines.append(line)

    return "\n".join(clean_lines).strip()


def clean_all_noise(text: str, current_sku: Optional[str] = None) -> str:
    """Apply all cleaning functions in sequence.

    This is the primary cleaning function that should be used before
    any text is written to an enrichment suggestion.
    """
    if not text:
        return text

    text = clean_contact_noise(text)
    text = clean_pdf_noise(text)
    text = clean_variant_table_noise(text, current_sku)
    if current_sku:
        text = remove_other_skus(text, current_sku)

    # Final cleanup: collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ═══════════════════════════════════════════════════════════
# PUBLIC API — Validation functions (blockers)
# ═══════════════════════════════════════════════════════════


def validate_no_contact_info(text: str) -> tuple[bool, str]:
    """Check that text contains no contact information.

    Returns (is_valid, reject_reason).
    """
    if not text:
        return True, ""

    if _PHONE_PATTERN.search(text):
        return False, "Inneholder telefonnummer"
    if _EMAIL_PATTERN.search(text):
        return False, "Inneholder e-postadresse"
    if _POSTAL_ADDRESS_PATTERN.search(text):
        # Only flag if it's a short text (likely primarily an address)
        addr_match = _POSTAL_ADDRESS_PATTERN.search(text)
        if addr_match and len(text) < 200:
            return False, "Inneholder postadresse"

    return True, ""


def validate_single_product_scope(
    text: str, current_sku: str
) -> tuple[bool, str]:
    """Check that text only references the current product, not other SKUs.

    Returns (is_valid, reject_reason).
    """
    if not text or not current_sku:
        return True, ""

    current_clean = re.sub(r"^[A-Za-z]+", "", current_sku).strip()
    skus_found = _ARTICLE_NUMBER_RE.findall(text)
    other_skus = set()

    for sku in skus_found:
        sku_clean = re.sub(r"^[A-Za-z]+", "", sku).strip()
        if sku_clean != current_clean and sku != current_sku:
            other_skus.add(sku)

    if len(other_skus) > 0:
        return False, f"Inneholder artikkelnumre for andre produkter: {', '.join(sorted(other_skus))}"

    return True, ""


def validate_no_pdf_noise(text: str) -> tuple[bool, str]:
    """Check that text doesn't contain PDF artifacts.

    Returns (is_valid, reject_reason).
    """
    if not text:
        return True, ""

    m = _PDF_NOISE_PATTERNS.search(text)
    if m:
        return False, f"Inneholder PDF-støy: '{m.group().strip()}'"

    return True, ""


def validate_suggestion_output(
    text: str, field_name: str, current_sku: str
) -> tuple[bool, str]:
    """Master validation for any enrichment suggestion before it's written.

    Runs all blocker checks. Returns (is_valid, reject_reason).
    If not valid, the suggestion should be rejected or flagged for manual review.
    """
    if not text or not text.strip():
        return False, "Tom verdi"

    text = text.strip()

    # Contact info check
    ok, reason = validate_no_contact_info(text)
    if not ok:
        return False, reason

    # Multi-SKU check
    ok, reason = validate_single_product_scope(text, current_sku)
    if not ok:
        return False, reason

    # PDF noise check (only for text fields, not for URLs)
    if field_name not in ("Bilde-URL", "PDF-URL", "Produkt-URL"):
        ok, reason = validate_no_pdf_noise(text)
        if not ok:
            return False, reason

    # Variant table indicators in final output
    if _VARIANT_TABLE_INDICATORS.search(text):
        return False, "Inneholder varianttabell-data"

    return True, ""


# ═══════════════════════════════════════════════════════════
# PUBLIC API — Language detection and classification
# ═══════════════════════════════════════════════════════════


def detect_language(text: str) -> str:
    """Detect the primary language of text.

    Returns one of: 'no' (Norwegian), 'sv' (Swedish), 'da' (Danish),
    'en' (English), 'unknown'.
    """
    if not text or len(text) < 15:
        return "unknown"

    text_lower = text.lower()
    words = text_lower.split()
    if len(words) < 3:
        return "unknown"

    no_count = len(_NORWEGIAN_INDICATORS.findall(text_lower))
    sv_count = len(_SWEDISH_INDICATORS.findall(text_lower))
    da_count = len(_DANISH_INDICATORS.findall(text_lower))
    en_count = len(_ENGLISH_INDICATORS.findall(text_lower))

    # Norwegian and Danish share many words; use distinctive words
    # Swedish has distinctive words like "och", "för", "inte", "användas"
    scores = {
        "no": no_count,
        "sv": sv_count,
        "da": da_count,
        "en": en_count,
    }

    # Boost Swedish for distinctive markers
    if re.search(r"\b(?:och|för|inte|användas|engångs)\b", text_lower):
        scores["sv"] += 3
    # Boost Danish for distinctive markers
    if re.search(r"\b(?:bruges|emballage|vægt|tykkelse)\b", text_lower):
        scores["da"] += 3
    # Boost English for distinctive markers
    if re.search(r"\b(?:the|designed|intended|provides|ensures|available)\b", text_lower):
        scores["en"] += 3
    # Boost Norwegian for distinctive markers
    if re.search(r"\b(?:brukes|pakning|hanske|sårpleie|lateksfri|pudderfri)\b", text_lower):
        scores["no"] += 3

    best_lang = max(scores, key=scores.get)
    best_score = scores[best_lang]

    # Need meaningful signal
    if best_score < 2:
        return "unknown"

    # If Norwegian is close to the best, prefer Norwegian (since much content
    # is Norwegian with occasional technical English terms)
    if best_lang != "no" and scores["no"] >= best_score * 0.7:
        return "no"

    return best_lang


def validate_language_is_norwegian(text: str) -> tuple[bool, str, str]:
    """Check if text is in Norwegian.

    Returns (is_norwegian, language_code, message).
    If not Norwegian, the suggestion should be flagged for translation.
    """
    lang = detect_language(text)

    if lang == "no" or lang == "unknown":
        return True, lang, ""

    lang_names = {"sv": "svensk", "da": "dansk", "en": "engelsk"}
    lang_name = lang_names.get(lang, lang)

    return False, lang, f"Teksten er på {lang_name} — oversettelse til norsk påkrevet"


# ── Swedish → Norwegian word mapping ──
# Swedish and Norwegian are mutually intelligible; these are the most common
# divergent words in medical/product contexts.
_SV_TO_NO: dict[str, str] = {
    # Conjunctions & common words
    "och": "og", "för": "for", "inte": "ikke", "eller": "eller",
    "detta": "dette", "dessa": "disse", "alla": "alle", "utan": "uten",
    "också": "også", "från": "fra", "som": "som", "med": "med",
    "kan": "kan", "har": "har", "vara": "være", "är": "er",
    "den": "den", "det": "det", "de": "de", "att": "å",
    "ska": "skal", "skulle": "skulle", "bara": "bare",
    "mycket": "mye", "mer": "mer", "mest": "mest",
    "sedan": "siden", "efter": "etter", "innan": "før",
    "under": "under", "över": "over", "mellan": "mellom",
    "genom": "gjennom", "vid": "ved", "till": "til",
    "hos": "hos", "här": "her", "där": "der",
    "vilken": "hvilken", "vilka": "hvilke", "vilkas": "hvis",
    "varje": "hver", "annan": "annen", "andra": "andre",
    "samma": "samme", "sådana": "slike", "sådan": "slik",
    "både": "både", "antingen": "enten", "varken": "verken",
    "nej": "nei", "ja": "ja", "inte": "ikke",
    # Medical / product terms
    "användas": "brukes", "använd": "bruk", "användning": "bruk",
    "handske": "hanske", "handskar": "hansker",
    "storlek": "størrelse", "storlekar": "størrelser",
    "förpackning": "forpakning", "förpackningar": "forpakninger",
    "skydd": "beskyttelse", "steril": "steril",
    "engångs": "engangs", "engångshandske": "engangshanske",
    "materiale": "materiale", "material": "materiale",
    "längd": "lengde", "bredd": "bredde",
    "tjocklek": "tykkelse", "vikt": "vekt",
    "färg": "farge", "färger": "farger",
    "innehåller": "inneholder", "innehåll": "innhold",
    "tillverkad": "laget", "tillverkare": "produsent",
    "egenskaper": "egenskaper", "egenskap": "egenskap",
    "passande": "passende", "lämplig": "egnet",
    "sjukvård": "helsevesen", "sjukhus": "sykehus",
    "patient": "pasient", "patienter": "pasienter",
    "behandling": "behandling",
    "sårvård": "sårpleie", "sår": "sår",
    "bandage": "bandasje", "kompress": "kompress",
    "plåster": "plaster", "spruta": "sprøyte",
    "nål": "nål", "nålar": "nåler",
    "kanyl": "kanyle", "kanyler": "kanyler",
    "kateter": "kateter",
    "undersökning": "undersøkelse", "undersöknings": "undersøkelses",
    "latexfri": "lateksfri", "puderfri": "pudderfri",
    "nitril": "nitril", "vinyl": "vinyl",
    "polyester": "polyester", "bomull": "bomull",
    "vit": "hvit", "vitt": "hvitt", "vita": "hvite",
    "blå": "blå", "grön": "grønn", "gröna": "grønne",
    "svart": "svart", "svarta": "svarte",
    "rosa": "rosa", "röd": "rød", "röda": "røde",
    "ytterförpackning": "ytterforpakning",
    "innerförpackning": "innerforpakning",
    "stycke": "stykk", "stycken": "stykker",
    "kartong": "kartong", "låda": "eske",
    "produktblad": "produktdatablad",
    "produktbeskrivning": "produktbeskrivelse",
    "tekniska": "tekniske", "teknisk": "teknisk",
    "specifikation": "spesifikasjon", "specifikationer": "spesifikasjoner",
    "beskrivning": "beskrivelse", "beskrivningar": "beskrivelser",
    "information": "informasjon",
    "rekommenderas": "anbefales", "rekommendation": "anbefaling",
    "avsedd": "beregnet", "avsedda": "beregnet",
    "godkänd": "godkjent", "godkända": "godkjente",
    "certifierad": "sertifisert",
    "kvalitet": "kvalitet", "kvalitetskrav": "kvalitetskrav",
    "säkerhet": "sikkerhet", "säker": "sikker",
    "hygien": "hygiene", "hygienisk": "hygienisk",
    "hållbarhet": "holdbarhet",
    "temperatur": "temperatur",
    "förvaras": "oppbevares", "förvaring": "oppbevaring",
    "torrt": "tørt", "svalt": "kjølig",
    "mörkt": "mørkt",
}

# ── Danish → Norwegian word mapping ──
# Danish and Norwegian Bokmål are extremely close; only the most
# divergent words need mapping.
_DA_TO_NO: dict[str, str] = {
    "bruges": "brukes", "brug": "bruk",
    "emballage": "emballasje",
    "vægt": "vekt",
    "tykkelse": "tykkelse",
    "længde": "lengde",
    "bredde": "bredde",
    "handske": "hanske", "handsker": "hansker",
    "størrelse": "størrelse",
    "pakke": "pakning", "pakker": "pakninger",
    "beskyttelse": "beskyttelse",
    "materiale": "materiale",
    "anvendes": "brukes", "anvendelse": "bruk",
    "beregnet": "beregnet",
    "egnet": "egnet",
    "farve": "farge", "farver": "farger",
    "hvid": "hvit", "hvide": "hvite",
    "blød": "myk", "bløde": "myke",
    "sygepleje": "sykepleie", "sygehus": "sykehus",
    "forbinding": "bandasje",
    "såpleje": "sårpleie",
    "plastik": "plast",
    "gummi": "gummi",
    "steriliseret": "sterilisert",
    "beskrivelse": "beskrivelse",
    "specifikation": "spesifikasjon", "specifikationer": "spesifikasjoner",
    "kvalitet": "kvalitet",
    "sikkerhed": "sikkerhet",
    "temperatur": "temperatur",
    "opbevares": "oppbevares", "opbevaring": "oppbevaring",
    "tørt": "tørt", "køligt": "kjølig",
    "mørkt": "mørkt",
    "anbefales": "anbefales",
    "godkendt": "godkjent", "godkendte": "godkjente",
    "certificeret": "sertifisert",
    "holdbarhed": "holdbarhet",
    "indhold": "innhold", "indeholder": "inneholder",
    "engangs": "engangs",
    "stykke": "stykk", "stykker": "stykker",
    "æske": "eske",
}


def _translate_word_sv_to_no(word: str) -> str:
    """Translate a single Swedish word to Norwegian, preserving case."""
    lower = word.lower()
    replacement = _SV_TO_NO.get(lower)
    if replacement is None:
        return word
    # Preserve original capitalization
    if word[0].isupper() and not word.isupper():
        return replacement.capitalize()
    if word.isupper():
        return replacement.upper()
    return replacement


def _translate_word_da_to_no(word: str) -> str:
    """Translate a single Danish word to Norwegian, preserving case."""
    lower = word.lower()
    replacement = _DA_TO_NO.get(lower)
    if replacement is None:
        return word
    if word[0].isupper() and not word.isupper():
        return replacement.capitalize()
    if word.isupper():
        return replacement.upper()
    return replacement


def translate_to_norwegian_if_needed(
    text: str, source_lang: Optional[str] = None
) -> tuple[str, str, bool]:
    """Translate text to Norwegian if it's in Swedish, Danish, or English.

    For Swedish and Danish: performs rule-based word-level translation
    (these languages are mutually intelligible with Norwegian).

    For English: flags for manual translation (too different for rule-based).

    Args:
        text: The text to translate.
        source_lang: If known, the source language code ('sv', 'da', 'en').
            If None, auto-detected.

    Returns:
        (translated_text, language_code, was_translated)
        - translated_text: The Norwegian text (or original if English/unknown)
        - language_code: Detected/provided language code
        - was_translated: True if text was actually modified
    """
    if not text or len(text.strip()) < 5:
        return text, "unknown", False

    # Detect language if not provided
    lang = source_lang or detect_language(text)

    if lang in ("no", "unknown"):
        return text, lang, False

    if lang == "sv":
        # Swedish → Norwegian: word-by-word replacement
        words = re.split(r"(\W+)", text)  # Split keeping delimiters
        translated_words = [
            _translate_word_sv_to_no(w) if w.strip() else w
            for w in words
        ]
        result = "".join(translated_words)
        changed = result != text
        if changed:
            logger.info(
                f"Translated Swedish → Norwegian: "
                f"'{text[:60]}' → '{result[:60]}'"
            )
        return result, lang, changed

    if lang == "da":
        # Danish → Norwegian: word-by-word replacement
        words = re.split(r"(\W+)", text)
        translated_words = [
            _translate_word_da_to_no(w) if w.strip() else w
            for w in words
        ]
        result = "".join(translated_words)
        changed = result != text
        if changed:
            logger.info(
                f"Translated Danish → Norwegian: "
                f"'{text[:60]}' → '{result[:60]}'"
            )
        return result, lang, changed

    if lang == "en":
        # English → Norwegian: cannot do rule-based translation.
        # Return original with flag so caller can mark for manual review.
        return text, lang, False

    return text, lang, False


def classify_text_as_description_candidate(text: str) -> float:
    """Score how well text fits as a product description (0.0-1.0).

    High score = prose-like, narrative, suitable for webshop description.
    Low score = structured, technical, better suited as specification.
    """
    if not text or len(text.strip()) < 10:
        return 0.0

    text = text.strip()
    score = 0.0

    # Factor 1: Has complete sentences (sentences end with period/exclamation)
    sentences = re.split(r"[.!?]+", text)
    real_sentences = [s.strip() for s in sentences if len(s.strip()) > 15]
    if len(real_sentences) >= 2:
        score += 0.30
    elif len(real_sentences) == 1:
        score += 0.15

    # Factor 2: Prose-like structure (not key-value pairs)
    kv_matches = _SPEC_KEY_VALUE_PATTERN.findall(text)
    total_lines = len([l for l in text.split("\n") if l.strip()])
    if total_lines > 0:
        kv_ratio = len(kv_matches) / total_lines
        if kv_ratio < 0.3:
            score += 0.25  # Mostly prose
        elif kv_ratio > 0.6:
            score -= 0.15  # Mostly key-value → spec-like

    # Factor 3: Descriptive language (words like "egnet for", "brukes til", "gir")
    descriptive_patterns = re.findall(
        r"(?i)\b(?:egnet|brukes|gir|sikrer|beskytter|passer|ideell|"
        r"komfortabel|praktisk|enkel|effektiv|god|høy|lav|lett)\b",
        text,
    )
    if len(descriptive_patterns) >= 2:
        score += 0.20
    elif len(descriptive_patterns) >= 1:
        score += 0.10

    # Factor 4: Adequate length for description (50-500 chars ideal)
    length = len(text)
    if 50 <= length <= 500:
        score += 0.15
    elif 30 <= length < 50 or 500 < length <= 1000:
        score += 0.05

    # Factor 5: Not just a list of bullet points
    bullet_lines = len(re.findall(r"^\s*[•\-\*]\s", text, re.MULTILINE))
    if total_lines > 0 and bullet_lines / total_lines > 0.7:
        score -= 0.10  # Heavy bullet list → more spec-like

    return max(0.0, min(1.0, score))


def classify_text_as_spec_candidate(text: str) -> float:
    """Score how well text fits as a product specification (0.0-1.0).

    High score = structured, technical attributes, key-value pairs.
    Low score = prose-like, better suited as description.
    """
    if not text or len(text.strip()) < 5:
        return 0.0

    text = text.strip()
    score = 0.0

    # Factor 1: Key-value pairs (e.g., "Materiale: Nitril")
    kv_matches = _SPEC_KEY_VALUE_PATTERN.findall(text)
    if len(kv_matches) >= 3:
        score += 0.35
    elif len(kv_matches) >= 1:
        score += 0.20

    # Factor 2: Technical units (measurements)
    unit_matches = _TECHNICAL_UNITS.findall(text)
    if len(unit_matches) >= 2:
        score += 0.25
    elif len(unit_matches) >= 1:
        score += 0.15

    # Factor 3: Technical keywords
    tech_matches = _TECHNICAL_KEYWORDS.findall(text)
    if len(tech_matches) >= 3:
        score += 0.20
    elif len(tech_matches) >= 1:
        score += 0.10

    # Factor 4: Short, structured lines (semicolon-separated or bullet points)
    if ";" in text:
        parts = text.split(";")
        if len(parts) >= 3:
            score += 0.15
    bullet_lines = len(re.findall(r"^\s*[•\-\*]\s", text, re.MULTILINE))
    if bullet_lines >= 2:
        score += 0.10

    # Factor 5: Penalty for long prose paragraphs
    sentences = re.split(r"[.!?]+", text)
    long_sentences = [s for s in sentences if len(s.strip()) > 80]
    if len(long_sentences) >= 2:
        score -= 0.15  # Long prose → more description-like

    return max(0.0, min(1.0, score))


def should_swap_description_and_spec(
    desc_text: Optional[str], spec_text: Optional[str]
) -> bool:
    """Determine if description and specification content should be swapped.

    Returns True if the current description looks like a specification
    and the current specification looks like a description.
    """
    if not desc_text or not spec_text:
        return False

    desc_as_desc = classify_text_as_description_candidate(desc_text)
    desc_as_spec = classify_text_as_spec_candidate(desc_text)
    spec_as_desc = classify_text_as_description_candidate(spec_text)
    spec_as_spec = classify_text_as_spec_candidate(spec_text)

    # Swap only if both are clearly misclassified
    return (
        desc_as_spec > desc_as_desc + 0.2
        and spec_as_desc > spec_as_spec + 0.2
    )


# ═══════════════════════════════════════════════════════════
# PUBLIC API — Normalization for webshop output
# ═══════════════════════════════════════════════════════════


def normalize_for_webshop_description(text: str) -> str:
    """Normalize text for use as a webshop product description.

    Ensures clean prose suitable for display on a product page.
    """
    if not text:
        return text

    # Clean all noise
    text = clean_contact_noise(text)
    text = clean_pdf_noise(text)

    # Remove raw table-like content
    lines = text.split("\n")
    clean_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        # Skip lines that are just numbers or codes
        if re.match(r"^[\d\s\-/.,]+$", stripped):
            continue
        # Skip very short non-sentence fragments
        if len(stripped) < 5 and not stripped.endswith((".", ":", "!")):
            continue
        clean_lines.append(stripped)

    text = "\n".join(clean_lines).strip()

    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text


def normalize_for_webshop_specification(text: str) -> str:
    """Normalize text for use as a webshop product specification.

    Ensures clean, structured key-value format.
    """
    if not text:
        return text

    # Clean all noise
    text = clean_contact_noise(text)
    text = clean_pdf_noise(text)

    # If it's already semicolon-separated key-value, clean each pair
    if ";" in text and ":" in text:
        pairs = text.split(";")
        clean_pairs = []
        for pair in pairs:
            pair = pair.strip()
            if not pair:
                continue
            # Validate it looks like a key-value pair
            if ":" in pair:
                key, _, val = pair.partition(":")
                key = key.strip()
                val = val.strip()
                if key and val and len(key) < 50:
                    clean_pairs.append(f"{key}: {val}")
            elif len(pair) > 3:
                clean_pairs.append(pair)
        if clean_pairs:
            return "; ".join(clean_pairs)

    return text.strip()


def get_best_producer_info(
    product_data, jeeves_data=None, manufacturer_lookup=None
) -> tuple[Optional[str], Optional[str]]:
    """Get best available producer and producer article number.

    Priority:
    1. Jeeves ERP data (most authoritative for internal use)
    2. Product page data (from onemed.no scraping)
    3. Manufacturer lookup (from enrichment)

    Returns (producer_name, producer_article_number).
    """
    producer = None
    producer_artnr = None

    # Priority 1: Jeeves
    if jeeves_data:
        if jeeves_data.supplier and jeeves_data.supplier.strip():
            candidate = jeeves_data.supplier.strip()
            if candidate.lower() not in ("ukjent", "unknown", "n/a", "-", ""):
                producer = candidate
        if jeeves_data.supplier_item_no and jeeves_data.supplier_item_no.strip():
            candidate = jeeves_data.supplier_item_no.strip()
            if candidate.lower() not in ("ukjent", "unknown", "n/a", "-", ""):
                producer_artnr = candidate

    # Priority 2: Product page
    if not producer and product_data:
        if product_data.manufacturer and product_data.manufacturer.strip():
            candidate = product_data.manufacturer.strip()
            if candidate.lower() not in ("ukjent", "unknown", "n/a", "-", ""):
                producer = candidate
    if not producer_artnr and product_data:
        if product_data.manufacturer_article_number and product_data.manufacturer_article_number.strip():
            candidate = product_data.manufacturer_article_number.strip()
            if candidate.lower() not in ("ukjent", "unknown", "n/a", "-", ""):
                producer_artnr = candidate

    # Priority 3: Manufacturer lookup
    if not producer and manufacturer_lookup and manufacturer_lookup.found:
        if manufacturer_lookup.source_url:
            # Infer from URL domain
            from backend.enricher import _infer_manufacturer_from_url
            inferred = _infer_manufacturer_from_url(manufacturer_lookup.source_url)
            if inferred:
                producer = inferred

    return producer, producer_artnr
