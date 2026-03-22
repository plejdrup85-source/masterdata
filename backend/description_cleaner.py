"""Description cleaning and quality gate for webshop-ready product descriptions.

Medical product PDFs and web pages often contain tables, metadata, variant lists,
and other non-description content. This module filters raw extracted text down to
clean, human-readable content suitable for webshop descriptions.

Pipeline:
  RAW TEXT → clean_description_source() → AI REWRITE → validate_webshop_description() → OUTPUT

If the quality gate fails, the description is rejected (None) rather than outputting
garbage. A bad description is worse than no description.
"""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)


# ── Patterns for lines to DROP (non-description content) ──

# Table headers common in medical product PDFs
# Match lines that START with a table header keyword (may have more columns after)
_TABLE_HEADER_PATTERNS = re.compile(
    r"(?i)^(?:"
    r"størrelseskode|størrelses?\s*kode|size\s*code"
    r"|dispenser\s*/?\s*kartong|dispenser\s*/?\s*carton"
    r"|lengde\s*dispenser|bredde\s*dispenser"
    r"|bestillingsnummer|order\s*(?:number|code|no)"
    r"|art\.?\s*(?:nr|nummer|no)|varenummer|item\s*no"
    r"|ref\.?\s*(?:nr|no|nummer|kode)"
    r"|farge\s*kode\b|colour\s*code|color\s*code"
    r"|antall\s*(?:pr|per|i)\s*(?:eske|kartong|pall|kolli)"
    r"|enhet|unit|qty|mengde"
    r"|innhold\b|contents\b"
    r")",
)

# Metadata lines (PDF headers, footers, dates)
_METADATA_PATTERNS = re.compile(
    r"(?i)^(?:"
    r"produktdatablad|product\s*data\s*sheet|technical\s*data\s*sheet"
    r"|side\s+\d+|page\s+\d+"
    r"|dato\s*:|date\s*:"
    r"|versjon|version|rev\b"
    r"|www\.\S+"
    r"|copyright|©|\(c\)"
    r"|all\s*rights?\s*reserved"
    r"|printed\s+in"
    r"|onemed\s*(?:ab|as|oy|norge)"
    r")\b"
)

# Date patterns: 2024-01-15, 15.01.2024, 01/15/2024
_DATE_PATTERN = re.compile(
    r"^\s*\d{2,4}[.\-/]\d{2}[.\-/]\d{2,4}\s*$"
)

# Packaging quantity patterns: "150 / 1500", "100 / 6000"
_PACKAGING_QTY_PATTERN = re.compile(
    r"^\s*\d+\s*/\s*\d+\s*$"
)

# Lines that START with article numbers / SKUs (may have more data after)
_ARTICLE_NUMBER_PATTERN = re.compile(
    r"^\s*(?:N?\d{5,}[\-\s]?\d*|[A-Z]{1,3}\d{5,})"
)

# Lines that look like table rows (multiple tab/space-separated numbers)
_TABLE_ROW_PATTERN = re.compile(
    r"^\s*(?:\d[\d.,]*\s+){2,}"
)

# Variant row: article number + product text + packaging ratio (e.g. "222001 SELEFA® ... 120 / 10800")
_VARIANT_ROW_INLINE = re.compile(
    r"\b\d{4,7}\s+\S+.*?\d+\s*/\s*\d+"
)

# Section markers that signal end of description content
_SECTION_END_MARKERS = re.compile(
    r"(?i)\b(?:salgsenhet|transportkartong|produktdatablad)\b"
)


def _is_junk_line(line: str) -> bool:
    """Determine if a line is non-description content that should be dropped."""
    stripped = line.strip()
    if not stripped:
        return True
    if len(stripped) < 3:
        return True

    # Table headers
    if _TABLE_HEADER_PATTERNS.match(stripped):
        return True

    # Metadata
    if _METADATA_PATTERNS.match(stripped):
        return True

    # Pure date
    if _DATE_PATTERN.match(stripped):
        return True

    # Packaging quantity like "150 / 1500"
    if _PACKAGING_QTY_PATTERN.match(stripped):
        return True

    # Pure article number
    if _ARTICLE_NUMBER_PATTERN.match(stripped):
        return True

    # Table row (multiple numbers separated by whitespace)
    if _TABLE_ROW_PATTERN.match(stripped):
        return True

    # Section-end markers: "Salgsenhet:", "Transportkartong", "Produktdatablad"
    if _SECTION_END_MARKERS.search(stripped):
        return True

    # Lines starting with "Produsent:", "Antall pr", "Art.nr" etc. — metadata, not description
    if re.match(r"(?i)^\s*(?:produsent|manufacturer|leverandør|supplier)\s*:", stripped):
        return True
    if re.match(r"(?i)^\s*antall\s*(?:pr|per|i|/)\s*", stripped):
        return True

    # Variant row inline: article number + text + packaging ratio anywhere in line
    # e.g. "222001 SELEFA® Optimia gaskompresser, Hvit 120 / 10800"
    if _VARIANT_ROW_INLINE.search(stripped):
        return True

    # >40% of tokens are numeric → probably a data row
    tokens = stripped.split()
    if tokens:
        numeric_count = sum(1 for t in tokens if re.match(r"^[\d.,/\-x×]+$", t))
        if len(tokens) >= 3 and numeric_count / len(tokens) > 0.4:
            return True

    # Copyright lines that don't start with ©
    if re.match(r"(?i)^\s*(?:©|copyright|\(c\))", stripped):
        return True

    # Lines with multiple "/" separators that look like size tables: "XS / S / M / L / XL"
    # Allow this ONLY if it contains size labels — otherwise it's a table
    if stripped.count("/") >= 3:
        # Check if it's a size selector line (OK to keep in some contexts)
        if not re.search(r"\b(?:XS|S|M|L|XL|XXL)\b", stripped):
            return True

    return False


def _strip_variant_blocks(text: str) -> str:
    """Remove inline variant table data that PDF extraction merged into description text.

    Medical product PDFs list all size/color variants in a table. When pdfplumber
    extracts this, the table rows can merge with description text into one block:
      "...beskyttelse osv. 222001 SELEFA® Optimia gaskompresser, Hvit 120 / 10800
       7,5 x 7,5 cm, 222002 SELEFA® ..."

    This function detects sequences of article-number + packaging-ratio patterns
    embedded in text and removes the variant block while preserving descriptive text
    both before and after it.
    """
    # Pattern: 4-7 digit article number followed by text and packaging ratio (N/N)
    # This catches variant rows like "222001 SELEFA® Optimia gaskompresser, Hvit 120 / 10800"
    variant_entry = re.compile(
        r"\b(\d{4,7})\s+[A-ZÆØÅa-zæøå®].{5,}?\d+\s*/\s*\d+"
    )

    matches = list(variant_entry.finditer(text))
    if len(matches) >= 2:
        # Multiple variant entries found — likely a variant table block.
        # Remove the region from first variant to last variant (inclusive),
        # keeping text before AND after the block.
        first_start = matches[0].start()
        last_end = matches[-1].end()

        before = text[:first_start].rstrip()
        after = text[last_end:].lstrip()

        # Strip trailing table headers before the variant block
        before = re.sub(r"\s*(?:Beskrivelse|Størrelse|Total|Antall|REF)\s*$",
                        "", before, flags=re.IGNORECASE).rstrip()

        parts = [p for p in [before, after] if p.strip()]
        if parts:
            result = "\n".join(parts)
            logger.debug(
                f"Stripped variant block: {len(matches)} variant entries removed, "
                f"kept {len(result)} of {len(text)} chars"
            )
            return result
        # If nothing meaningful remains, return original for _is_junk_line to handle

    return text


def clean_description_source(raw_text: str) -> Optional[str]:
    """Filter raw PDF/web text to extract only clean descriptive content.

    Removes:
    - Table headers and data rows
    - Article numbers and SKU patterns
    - Packaging quantity patterns (e.g., "150 / 1500")
    - PDF metadata (page numbers, dates, URLs)
    - Duplicated and fragmented lines
    - Lines that are mostly numeric
    - Variant table blocks (other sizes/colors of same product)

    Keeps:
    - Full sentences describing the product
    - Material, usage, properties, benefits, compliance info
    - Structured paragraphs

    Returns None if no usable descriptive content remains.
    """
    if not raw_text or not raw_text.strip():
        return None

    # Step 0: Strip inline variant blocks before line-by-line filtering
    raw_text = _strip_variant_blocks(raw_text)

    lines = raw_text.split("\n")

    # Step 1: Drop junk lines
    clean_lines = []
    for line in lines:
        if not _is_junk_line(line):
            clean_lines.append(line.strip())

    if not clean_lines:
        return None

    # Step 2: Merge broken sentences
    # If a line ends without punctuation and the next starts with lowercase,
    # join them (PDF line breaks inside sentences)
    merged = []
    buffer = clean_lines[0]
    for i in range(1, len(clean_lines)):
        line = clean_lines[i]
        # If previous line doesn't end with sentence-ending punctuation
        # and current line starts with lowercase → continuation
        if buffer and buffer[-1] not in ".!?:;)" and line and line[0].islower():
            buffer += " " + line
        else:
            merged.append(buffer)
            buffer = line
    if buffer:
        merged.append(buffer)

    # Step 3: Remove near-duplicate lines (PDF headers repeated on each page)
    seen = set()
    deduped = []
    for line in merged:
        normalized = re.sub(r"\s+", " ", line.lower().strip())
        if normalized not in seen:
            seen.add(normalized)
            deduped.append(line)

    # Step 4: Keep only lines with some sentence quality
    # A "sentence-like" line has at least a few words and isn't just a label
    descriptive_lines = []
    for line in deduped:
        words = line.split()
        if len(words) >= 3:
            descriptive_lines.append(line)
        elif len(words) >= 1 and line.endswith((".", ":", "!")):
            # Short but complete (e.g., "Lateksfri." or "Steril:")
            descriptive_lines.append(line)

    if not descriptive_lines:
        return None

    result = "\n".join(descriptive_lines).strip()

    # Final sanity: if result is too short, not useful
    if len(result) < 20:
        return None

    return result


# ── Quality Gate: validate generated webshop descriptions ──

# Patterns that should NEVER appear in a webshop description
_REJECT_PATTERNS = [
    # SKU-like numbers (5+ digits)
    re.compile(r"\b\d{5,}\b"),
    # Packaging "X / Y" patterns
    re.compile(r"\b\d+\s*/\s*\d+\b"),
    # PDF artifacts
    re.compile(r"(?i)\bside\s+\d+\b"),
    re.compile(r"\b\d{4}[-/.]\d{2}[-/.]\d{2}\b"),
    # Table-like content
    re.compile(r"(?i)(?:størrelseskode|dispenser\s*/\s*kartong|bestillingsnummer)"),
    # Raw technical sheet references
    re.compile(r"(?i)produktdatablad"),
    # Section markers that should not be in descriptions
    re.compile(r"(?i)\bsalgsenhet\b"),
    re.compile(r"(?i)\btransportkartong\b"),
]


def validate_webshop_description(text: str) -> tuple[bool, str]:
    """Quality gate: validate that a description is suitable for webshop display.

    Returns (is_valid, reject_reason).
    If not valid, the description should be set to None with Review_Required=True.
    """
    if not text or not text.strip():
        return False, "Tom beskrivelse"

    text = text.strip()

    # Too short
    sentences = re.split(r"[.!?]+", text)
    real_sentences = [s.strip() for s in sentences if len(s.strip()) > 10]
    if len(real_sentences) < 2:
        return False, "For kort — mindre enn 2 meningsfulle setninger"

    # Too long and unfocused (>1500 chars is suspicious for a product description)
    if len(text) > 1500:
        return False, "For lang — mulig ukuratert råtekst"

    # Check for reject patterns
    for pattern in _REJECT_PATTERNS:
        m = pattern.search(text)
        if m:
            return False, f"Inneholder uønsket innhold: '{m.group()}'"

    # Check for table-like structure (many lines with similar short length)
    lines = text.strip().split("\n")
    if len(lines) > 5:
        short_lines = sum(1 for l in lines if len(l.strip()) < 30)
        if short_lines / len(lines) > 0.6:
            return False, "Ser ut som en tabell — for mange korte linjer"

    # Broken sentences: lines ending mid-word or without closure
    for line in lines:
        stripped = line.strip()
        if len(stripped) > 50 and stripped[-1] not in ".!?:;)\"'0123456789%":
            # Allow bullet-like lines starting with • or -
            if not stripped.startswith(("•", "-", "*")):
                # Only reject if it really looks like a broken sentence
                if re.search(r"\b(?:og|for|som|med|til|av|er|i)\s*$", stripped):
                    return False, f"Avbrutt setning: '...{stripped[-40:]}'"

    return True, ""
