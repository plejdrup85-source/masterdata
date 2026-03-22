"""PDF-based product data enrichment pipeline.

Source-priority enrichment:
1. Internal product sheet (PDF from OneMed CDN) - PRIMARY source
2. Manufacturer website lookup - FALLBACK only

For medical products, this follows evidence-based enrichment:
- No enrichment without a documented source
- No auto-approval without clear evidence
- Conflicting sources flagged for manual review

PDF URL pattern: https://res.onemed.com/NO/Produktblad/{ARTNR}.pdf
"""

import asyncio
import logging
import re
from io import BytesIO
from typing import Optional

import httpx

from backend.models import (
    EnrichmentMatchStatus,
    EnrichmentResult,
    EnrichmentSourceLevel,
    ProductData,
)

logger = logging.getLogger(__name__)

# ── PDF source URL ──
PDF_BASE_URL = "https://res.onemed.com/NO/Produktblad"

# ── HTTP settings ──
PDF_TIMEOUT = 20  # seconds - PDFs can be larger
PDF_MAX_SIZE = 10 * 1024 * 1024  # 10 MB max PDF size
PDF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/pdf,*/*;q=0.8",
}

# ── Fields we attempt to extract from PDFs ──
ENRICHMENT_FIELDS = [
    "product_name",
    "description",
    "manufacturer",
    "manufacturer_article_number",
    "specifications",
    "packaging_info",
    "materials",
    "dimensions",
]

# ── Extraction patterns (Norwegian + English) ──
# These patterns look for labeled sections in PDF text
FIELD_PATTERNS = {
    "product_name": [
        r"(?:produktnavn|product\s*name|varenavn|betegnelse)\s*[:\-]?\s*(.+?)(?:\n|$)",
    ],
    "description": [
        # Multi-paragraph: capture until double newline or end
        r"(?:beskrivelse|description|produktbeskrivelse|product\s*description)\s*[:\-]?\s*((?:(?!\n\n).)+)",
        # Fallback: single line
        r"(?:beskrivelse|description|produktbeskrivelse|product\s*description)\s*[:\-]?\s*(.+?)(?:\n|$)",
    ],
    "manufacturer": [
        r"(?:produsent|manufacturer|leverand.r|supplier|fabrikant)\s*[:\-]?\s*(.+?)(?:\n|$)",
    ],
    "manufacturer_article_number": [
        r"(?:produsentens?\s*(?:vare|artikkel)?\s*(?:nummer|nr|kode)|manufacturer\s*(?:article|item|ref)?\s*(?:number|no|code|nr))\s*[:\-]?\s*(.+?)(?:\n|$)",
        r"(?:ref[\.\s]*(?:nr|no|number|kode))\s*[:\-]?\s*(.+?)(?:\n|$)",
    ],
    "packaging_info": [
        # Strict packaging patterns only — no "innhold"/"contents" (too broad)
        r"(?:pakning|emballasje|packaging|pack\s*size|pakningsstørrelse|forpakning)\s*[:\-]?\s*(.+?)(?:\n|$)",
        r"(?:antall\s*(?:i|per|pr)?\s*(?:pakning|eske|kartong|pall))\s*[:\-]?\s*(.+?)(?:\n|$)",
    ],
    "materials": [
        r"(?:materiale?|material|sammensetning|composition)\s*[:\-]?\s*(.+?)(?:\n|$)",
    ],
    "dimensions": [
        r"(?:dimensjoner?|dimensions?|st.rrelse|size|m.l)\s*[:\-]?\s*(.+?)(?:\n|$)",
    ],
}

# Per-field max value lengths (prevents accepting irrelevant long text blocks)
FIELD_MAX_LENGTHS = {
    "product_name": 200,
    "description": 2000,
    "manufacturer": 100,
    "manufacturer_article_number": 60,
    "packaging_info": 200,
    "materials": 300,
    "dimensions": 200,
}
FIELD_MAX_LENGTH_DEFAULT = 500

# ── Content quality rules ──
# Boilerplate / irrelevant text patterns that should be rejected from ANY field
_BOILERPLATE_PATTERNS = [
    r"(?i)(?:oppbevar|lagr)\w*\s+(?:tørt|kjølig|mørkt|romtemperatur)",  # storage instructions
    r"(?i)(?:best\s*før|holdbar|expir|shelf\s*life)",  # expiry info
    r"(?i)(?:les\s+bruksanvisning|read\s+instructions)",
    r"(?i)(?:kontakt\s+(?:lege|helsepersonell)|consult\s+(?:doctor|physician))",
    r"(?i)(?:www\.\S+\.(?:com|no|se|dk))",  # URLs
    r"(?i)(?:copyright|©|\ball\s+rights\s+reserved)",
]

# Packaging-specific: patterns that look like packaging content
_PACKAGING_VALID_PATTERNS = [
    r"(?i)\d+\s*(?:stk|pk|stykk|per|i\s+(?:pakning|eske|kartong|pall))",
    r"(?i)(?:eske|kartong|pall|pose|boks|pakke|forpakning|inner|outer|master)\b",
    r"(?i)\d+\s*(?:x\s*\d+)",  # e.g. "10 x 50"
]

# Specification table patterns (key: value or key\tvalue)
SPEC_TABLE_PATTERNS = [
    r"^([A-Z\u00C0-\u00FF][a-z\u00C0-\u00FF\s]+?)\s*[:\t]\s*(.+?)$",  # "Label: Value" or "Label\tValue"
]


def _extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF bytes using pdfplumber.

    Returns None if extraction fails or produces no usable text.
    Handles:
    - Scanned PDFs (returns None - no OCR attempted)
    - Corrupted PDFs (returns None with warning)
    - Empty PDFs (returns None)
    - Multi-page PDFs (concatenates all pages)
    """
    try:
        import pdfplumber

        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            if not pdf.pages:
                return None

            texts = []
            for page in pdf.pages:
                try:
                    page_text = page.extract_text()
                    if page_text:
                        texts.append(page_text)
                except Exception as e:
                    logger.debug(f"Failed to extract text from page: {e}")
                    continue

            if not texts:
                return None

            full_text = "\n\n".join(texts)

            # Sanity check: if text is very short or mostly garbage, consider it unreadable
            clean = full_text.strip()
            if len(clean) < 20:
                return None
            # Check for excessive non-printable characters (likely scanned PDF)
            printable_ratio = sum(1 for c in clean if c.isprintable() or c in "\n\t") / len(clean)
            if printable_ratio < 0.7:
                logger.warning("PDF text appears to be mostly non-printable (possibly scanned)")
                return None

            return clean

    except ImportError:
        logger.error("pdfplumber not installed. Install with: pip install pdfplumber")
        return None
    except Exception as e:
        logger.warning(f"PDF text extraction failed: {e}")
        return None


def _extract_tables_from_pdf(pdf_bytes: bytes) -> list[dict[str, str]]:
    """Extract tabular data from PDF (specifications, properties).

    Returns a list of key-value dicts from tables found in the PDF.
    """
    try:
        import pdfplumber

        tables_data = []
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                try:
                    tables = page.extract_tables()
                    for table in tables:
                        if not table:
                            continue
                        for row in table:
                            if row and len(row) >= 2:
                                key = str(row[0] or "").strip()
                                val = str(row[1] or "").strip()
                                if key and val and len(key) < 100 and len(val) < 500:
                                    tables_data.append({"key": key, "value": val})
                except Exception:
                    continue

        return tables_data

    except Exception as e:
        logger.debug(f"Table extraction failed: {e}")
        return []


def _extract_field_from_text(text: str, field_name: str) -> Optional[tuple[str, str, float]]:
    """Try to extract a specific field value from PDF text.

    Returns (value, evidence_snippet, quality_score) or None if not found.
    quality_score is 0.0-1.0 reflecting extraction confidence.
    Uses regex patterns to find labeled values.
    """
    patterns = FIELD_PATTERNS.get(field_name, [])
    max_len = FIELD_MAX_LENGTHS.get(field_name, FIELD_MAX_LENGTH_DEFAULT)

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE | re.DOTALL)
        if match:
            value = match.group(1).strip()
            # Normalize whitespace within lines but preserve paragraph breaks
            lines = value.split("\n")
            normalized_lines = [re.sub(r"[ \t]+", " ", line).strip() for line in lines]
            value = "\n".join(line for line in normalized_lines if line).strip()
            if not value or len(value) <= 1:
                continue
            if len(value) > max_len:
                logger.debug(
                    f"Extracted {field_name} too long ({len(value)} > {max_len}), "
                    f"truncating to field limit"
                )
                # Truncate at last sentence boundary within limit
                truncated = value[:max_len]
                last_period = truncated.rfind(".")
                if last_period > max_len // 2:
                    value = truncated[:last_period + 1]
                else:
                    value = truncated.rstrip()

            # Quality scoring
            quality = _score_extraction_quality(value, field_name)
            if quality < 0.1:
                logger.debug(f"Rejected {field_name} extraction: quality {quality:.2f}")
                continue

            # Evidence: ±80 chars for meaningful context
            start = max(0, match.start() - 80)
            end = min(len(text), match.end() + 80)
            snippet = text[start:end].strip()
            # Normalize whitespace in snippet (spaces only, preserve newlines for readability)
            snippet = re.sub(r"[ \t]+", " ", snippet)

            return value, snippet, quality

    return None


def _score_extraction_quality(value: str, field_name: str) -> float:
    """Score the quality of an extracted value (0.0 = reject, 1.0 = perfect).

    Checks for:
    - Fragment detection (cut-off mid-sentence)
    - Boilerplate/irrelevant content
    - Field-specific validity
    """
    score = 1.0

    # Reject boilerplate text in any field
    for bp_pattern in _BOILERPLATE_PATTERNS:
        if re.search(bp_pattern, value):
            score *= 0.3
            break

    # Fragment detection: cut off mid-sentence
    # Only penalize longer text that looks like a truncated sentence
    if len(value) > 40 and value[-1] not in ".!?)\"':;,0123456789%":
        has_sentence_words = bool(re.search(r"\b(?:og|for|som|med|til|av|er|i|and|for|with|the|is)\b", value))
        if has_sentence_words:
            score *= 0.5
    # Very short for a description-type field
    if field_name == "description" and len(value) < 20:
        score *= 0.3

    # Packaging-specific validation
    if field_name == "packaging_info":
        has_packaging_content = any(
            re.search(p, value) for p in _PACKAGING_VALID_PATTERNS
        )
        if not has_packaging_content:
            # Text matched the packaging label but doesn't contain actual packaging data
            score *= 0.2
            logger.debug(f"Packaging value rejected — no packaging content: {value[:80]}")

    # Short values for fields that should be substantive
    if field_name in ("description", "specifications") and len(value) < 10:
        score *= 0.3

    return score


def _extract_specifications_from_text(text: str) -> dict[str, str]:
    """Extract specification-like key:value pairs from text."""
    specs = {}
    for pattern in SPEC_TABLE_PATTERNS:
        for match in re.finditer(pattern, text, re.MULTILINE):
            key = match.group(1).strip()
            val = match.group(2).strip()
            if key and val and len(key) < 60 and len(val) < 300:
                # Avoid picking up headers, footers, etc.
                if not any(skip in key.lower() for skip in ["side", "page", "dato", "date", "rev"]):
                    specs[key] = val
    return specs


def parse_pdf_content(
    pdf_bytes: bytes,
    article_number: str,
    pdf_url: str,
) -> list[EnrichmentResult]:
    """Parse PDF and extract enrichment data for all fields.

    This is the core extraction function. It:
    1. Extracts full text from PDF
    2. Extracts tables from PDF
    3. Attempts field-by-field extraction using patterns
    4. Returns EnrichmentResult per field with evidence

    AI does NOT guess. Only explicitly found values are returned.
    """
    results = []
    source_level = EnrichmentSourceLevel.INTERNAL_PRODUCT_SHEET.value
    source_type = "PDF"

    # Extract text
    text = _extract_text_from_pdf(pdf_bytes)
    if not text:
        # PDF is unreadable or empty - return NOT_FOUND for all fields
        for field in ENRICHMENT_FIELDS:
            results.append(EnrichmentResult(
                artnr=article_number,
                field_name=field,
                source_level=source_level,
                source_url=pdf_url,
                source_type=source_type,
                match_status=EnrichmentMatchStatus.NOT_FOUND.value,
                evidence_snippet="PDF kunne ikke leses (mulig skannet dokument)",
            ))
        return results

    # Extract tables for specifications
    tables = _extract_tables_from_pdf(pdf_bytes)
    spec_from_tables = {t["key"]: t["value"] for t in tables}
    spec_from_text = _extract_specifications_from_text(text)
    all_specs = {**spec_from_text, **spec_from_tables}

    # Extract each field
    for field in ENRICHMENT_FIELDS:
        if field == "specifications":
            # Specifications are handled as a group
            if all_specs:
                # Create one result per spec key-value pair
                for spec_key, spec_val in all_specs.items():
                    results.append(EnrichmentResult(
                        artnr=article_number,
                        field_name=f"spec:{spec_key}",
                        suggested_value=spec_val,
                        source_level=source_level,
                        source_url=pdf_url,
                        source_type=source_type,
                        evidence_snippet=f"{spec_key}: {spec_val}",
                        confidence=0.7,
                        match_status=EnrichmentMatchStatus.FOUND_IN_INTERNAL_PDF.value,
                    ))
            else:
                results.append(EnrichmentResult(
                    artnr=article_number,
                    field_name="specifications",
                    source_level=source_level,
                    source_url=pdf_url,
                    source_type=source_type,
                    match_status=EnrichmentMatchStatus.NOT_FOUND.value,
                ))
            continue

        extraction = _extract_field_from_text(text, field)
        if extraction:
            value, snippet, quality = extraction
            # Confidence is quality-weighted: base 0.75 * quality score
            # High-quality extraction → 0.75, low quality → 0.15-0.45
            field_confidence = round(min(0.85, 0.75 * quality), 2)
            needs_review = quality < 0.7 or field_confidence < 0.60
            results.append(EnrichmentResult(
                artnr=article_number,
                field_name=field,
                suggested_value=value,
                source_level=source_level,
                source_url=pdf_url,
                source_type=source_type,
                evidence_snippet=snippet,
                confidence=field_confidence,
                match_status=EnrichmentMatchStatus.FOUND_IN_INTERNAL_PDF.value,
                review_status="needs_review" if needs_review else "auto",
            ))
        else:
            results.append(EnrichmentResult(
                artnr=article_number,
                field_name=field,
                source_level=source_level,
                source_url=pdf_url,
                source_type=source_type,
                match_status=EnrichmentMatchStatus.NOT_FOUND.value,
            ))

    return results


def merge_enrichment_sources(
    pdf_results: list[EnrichmentResult],
    manufacturer_results: list[EnrichmentResult],
    current_data: ProductData,
) -> list[EnrichmentResult]:
    """Merge PDF (primary) and manufacturer (fallback) enrichment results.

    Source priority:
    1. Internal PDF (primary)
    2. Manufacturer source (fallback)

    If both have a value for the same field:
    - If they match → FOUND_IN_BOTH_MATCH, confidence boosted
    - If they conflict → FOUND_IN_BOTH_CONFLICT, flagged for review

    If only one source has it → that source's result is used.
    If neither has it → NOT_FOUND.
    """
    # Index by field_name
    pdf_by_field = {}
    for r in pdf_results:
        pdf_by_field[r.field_name] = r

    mfr_by_field = {}
    for r in manufacturer_results:
        mfr_by_field[r.field_name] = r

    all_fields = set(pdf_by_field.keys()) | set(mfr_by_field.keys())
    merged = []

    for field in sorted(all_fields):
        pdf_r = pdf_by_field.get(field)
        mfr_r = mfr_by_field.get(field)

        pdf_found = pdf_r and pdf_r.suggested_value and pdf_r.match_status != EnrichmentMatchStatus.NOT_FOUND.value
        mfr_found = mfr_r and mfr_r.suggested_value and mfr_r.match_status != EnrichmentMatchStatus.NOT_FOUND.value

        # Map field name to ProductData attribute for current_value
        current_val = _get_current_value(current_data, field)

        if pdf_found and mfr_found:
            # Both sources have a value - compare
            pdf_val = (pdf_r.suggested_value or "").strip().lower()
            mfr_val = (mfr_r.suggested_value or "").strip().lower()

            if _values_match(pdf_val, mfr_val):
                # Match - boost confidence, use PDF as primary
                result = pdf_r.model_copy()
                result.current_value = current_val
                result.confidence = min(0.95, pdf_r.confidence + 0.15)
                result.match_status = EnrichmentMatchStatus.FOUND_IN_BOTH_MATCH.value
                result.review_status = "auto"
                result.evidence_snippet = (
                    f"PDF: {pdf_r.evidence_snippet or pdf_r.suggested_value} | "
                    f"Produsent: {mfr_r.evidence_snippet or mfr_r.suggested_value}"
                )
                merged.append(result)
            else:
                # Conflict - flag for review
                result = pdf_r.model_copy()
                result.current_value = current_val
                result.match_status = EnrichmentMatchStatus.FOUND_IN_BOTH_CONFLICT.value
                result.review_status = "conflict"
                result.confidence = max(pdf_r.confidence, mfr_r.confidence) * 0.6
                result.evidence_snippet = (
                    f"KONFLIKT - PDF: '{pdf_r.suggested_value}' vs "
                    f"Produsent: '{mfr_r.suggested_value}'"
                )
                merged.append(result)

        elif pdf_found:
            # Only PDF has the value - primary source
            result = pdf_r.model_copy()
            result.current_value = current_val
            merged.append(result)

        elif mfr_found:
            # Only manufacturer has the value - fallback
            result = mfr_r.model_copy()
            result.current_value = current_val
            merged.append(result)

        else:
            # Neither source found this field
            base = pdf_r or mfr_r
            if base:
                result = base.model_copy()
                result.current_value = current_val
                result.match_status = EnrichmentMatchStatus.NOT_FOUND.value
                result.suggested_value = None
                result.confidence = 0.0
                merged.append(result)

    return merged


def _values_match(val_a: str, val_b: str) -> bool:
    """Check if two extracted values are essentially the same.

    Handles minor formatting differences, whitespace, case.
    Note: this collapses whitespace for COMPARISON only — original values are preserved.
    """
    if val_a == val_b:
        return True
    # Normalize for comparison: collapse whitespace, common punctuation, case
    norm_a = re.sub(r"[\s\-_/.,;:]+", " ", val_a).strip().lower()
    norm_b = re.sub(r"[\s\-_/.,;:]+", " ", val_b).strip().lower()
    if norm_a == norm_b:
        return True
    # Substring match: only for short values (single attributes, not full descriptions)
    # For medical products, "Nitril" should not match "Nitril lateksfri" — require >80% overlap
    if len(norm_a) > 5 and len(norm_b) > 5:
        shorter, longer = (norm_a, norm_b) if len(norm_a) <= len(norm_b) else (norm_b, norm_a)
        if shorter in longer and len(shorter) >= len(longer) * 0.8:
            return True
    return False


def _get_current_value(data: ProductData, field_name: str) -> Optional[str]:
    """Get the current value from ProductData for a given enrichment field name."""
    field_map = {
        "product_name": data.product_name,
        "description": data.description,
        "manufacturer": data.manufacturer,
        "manufacturer_article_number": data.manufacturer_article_number,
        "packaging_info": data.packaging_info or data.packaging_unit,
        "materials": None,  # Not in ProductData; extracted only from PDF/manufacturer
        "dimensions": None,
    }
    if field_name.startswith("spec:"):
        spec_key = field_name[5:]
        if data.technical_details:
            return data.technical_details.get(spec_key)
        return None
    return field_map.get(field_name)


def build_manufacturer_enrichment_results(
    product: ProductData,
    mfr_data,  # ManufacturerLookup
) -> list[EnrichmentResult]:
    """Convert manufacturer lookup data into EnrichmentResult format for merging."""
    results = []
    source_level = EnrichmentSourceLevel.MANUFACTURER_SOURCE.value
    source_type = "website"
    source_url = mfr_data.source_url or ""

    if not mfr_data.found:
        return results

    if mfr_data.product_name:
        results.append(EnrichmentResult(
            artnr=product.article_number,
            field_name="product_name",
            current_value=product.product_name,
            suggested_value=mfr_data.product_name,
            source_level=source_level,
            source_url=source_url,
            source_type=source_type,
            evidence_snippet=f"Fra produsent: {mfr_data.product_name}",
            confidence=mfr_data.confidence * 0.9,
            match_status=EnrichmentMatchStatus.FOUND_IN_MANUFACTURER_SOURCE.value,
        ))

    if mfr_data.description:
        results.append(EnrichmentResult(
            artnr=product.article_number,
            field_name="description",
            current_value=product.description,
            suggested_value=mfr_data.description,
            source_level=source_level,
            source_url=source_url,
            source_type=source_type,
            evidence_snippet=f"Fra produsent: {mfr_data.description[:100]}{'...' if len(mfr_data.description) > 100 else ''}",
            confidence=mfr_data.confidence * 0.8,
            match_status=EnrichmentMatchStatus.FOUND_IN_MANUFACTURER_SOURCE.value,
        ))

    if mfr_data.specifications:
        for key, val in mfr_data.specifications.items():
            results.append(EnrichmentResult(
                artnr=product.article_number,
                field_name=f"spec:{key}",
                suggested_value=val,
                source_level=source_level,
                source_url=source_url,
                source_type=source_type,
                evidence_snippet=f"{key}: {val}",
                confidence=mfr_data.confidence * 0.7,
                match_status=EnrichmentMatchStatus.FOUND_IN_MANUFACTURER_SOURCE.value,
            ))

    return results


async def check_pdf_exists(
    article_number: str,
) -> tuple[bool, Optional[str]]:
    """Lightweight HEAD check for PDF existence without downloading/parsing.

    Used by audit mode to score document presence without the cost of full extraction.
    Returns (exists, pdf_url).
    """
    clean = article_number.strip()
    pdf_url = f"{PDF_BASE_URL}/{clean}.pdf"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.head(pdf_url, headers=PDF_HEADERS, follow_redirects=True)
            if response.status_code == 200:
                content_type = response.headers.get("content-type", "")
                if "pdf" in content_type.lower() or "octet-stream" in content_type.lower():
                    return True, pdf_url
            return False, pdf_url
    except Exception as e:
        logger.debug(f"PDF existence check failed for {article_number}: {e}")
        return False, pdf_url


async def fetch_and_parse_product_pdf(
    article_number: str,
    client: Optional[httpx.AsyncClient] = None,
) -> tuple[bool, Optional[str], list[EnrichmentResult]]:
    """Fetch product PDF from OneMed CDN and extract enrichment data.

    Returns (pdf_exists, pdf_url, enrichment_results).
    """
    clean = article_number.strip()
    pdf_url = f"{PDF_BASE_URL}/{clean}.pdf"

    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=PDF_TIMEOUT)

    try:
        response = await client.get(pdf_url, headers=PDF_HEADERS, follow_redirects=True)

        if response.status_code != 200:
            logger.debug(f"PDF not found for {article_number}: HTTP {response.status_code}")
            return False, pdf_url, []

        # Check content type
        content_type = response.headers.get("content-type", "")
        if "pdf" not in content_type.lower() and "octet-stream" not in content_type.lower():
            # Might be an HTML error page
            if len(response.content) < 5000 and b"<html" in response.content[:200].lower():
                logger.debug(f"PDF URL returned HTML for {article_number}")
                return False, pdf_url, []

        # Check size
        if len(response.content) < 500:
            logger.debug(f"PDF too small for {article_number}: {len(response.content)} bytes")
            return False, pdf_url, []

        if len(response.content) > PDF_MAX_SIZE:
            logger.warning(f"PDF too large for {article_number}: {len(response.content)} bytes")
            return True, pdf_url, []

        # Parse in thread executor to avoid blocking
        loop = asyncio.get_running_loop()
        results = await loop.run_in_executor(
            None,
            parse_pdf_content,
            response.content,
            article_number,
            pdf_url,
        )

        return True, pdf_url, results

    except httpx.TimeoutException:
        logger.warning(f"PDF fetch timeout for {article_number}")
        return False, pdf_url, []
    except Exception as e:
        logger.warning(f"PDF fetch error for {article_number}: {e}")
        return False, pdf_url, []
    finally:
        if own_client:
            await client.aclose()


async def run_enrichment_pipeline(
    article_number: str,
    product_data: ProductData,
    manufacturer_data=None,  # Optional ManufacturerLookup
    client: Optional[httpx.AsyncClient] = None,
) -> tuple[bool, Optional[str], list[EnrichmentResult]]:
    """Run the full source-priority enrichment pipeline.

    Steps:
    1. Fetch and parse internal product sheet PDF (primary source)
    2. Convert manufacturer data to enrichment format (fallback)
    3. Merge with source priority: PDF > manufacturer
    4. Return merged enrichment results with full traceability

    Returns (pdf_exists, pdf_url, merged_enrichment_results).
    """
    # Step 1: Internal product sheet (primary)
    pdf_exists, pdf_url, pdf_results = await fetch_and_parse_product_pdf(
        article_number, client=client
    )

    # Step 2: Manufacturer enrichment results (fallback)
    mfr_results = []
    if manufacturer_data and manufacturer_data.found:
        mfr_results = build_manufacturer_enrichment_results(product_data, manufacturer_data)

    # Step 3: Merge with source priority
    if pdf_results or mfr_results:
        merged = merge_enrichment_sources(pdf_results, mfr_results, product_data)
    else:
        merged = []

    return pdf_exists, pdf_url, merged
