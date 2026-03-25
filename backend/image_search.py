"""Active image search, improvement scoring, and approval for product images.

When a product's current image is missing or low quality, this module:
1. Searches for better alternatives from producer websites
2. Scores the improvement (0-100) comparing new vs existing
3. Assigns confidence with labels
4. Supports approval workflow and ZIP download

Sources are prioritized:
  1. Producer's own website (highest trust)
  2. Official distributors
  3. Generic web search (manual follow-up)

Design: CONSERVATIVE. Only suggest images that are a CLEAR improvement.
Better to say "manual search needed" than suggest a wrong image.
"""

import logging
import re
from typing import Optional
from urllib.parse import quote_plus, urlparse

import httpx

from backend.models import ImageSuggestion, ManufacturerLookup, ProductData

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,image/*",
}

# ── Improvement score thresholds ──
MIN_IMPROVEMENT_SCORE = 40  # Below this, don't suggest the image
HIGH_IMPROVEMENT_SCORE = 70  # Above this, high confidence suggestion

# ── Known producer websites with search capabilities ──
KNOWN_PRODUCERS = {
    "molnlycke": {"base": "https://www.molnlycke.com", "search": "https://www.molnlycke.com/search/?q={q}"},
    "mölnlycke": {"base": "https://www.molnlycke.com", "search": "https://www.molnlycke.com/search/?q={q}"},
    "coloplast": {"base": "https://www.coloplast.com", "search": "https://www.coloplast.com/search/?q={q}"},
    "essity": {"base": "https://www.essity.com", "search": "https://www.essity.com/search/?q={q}"},
    "bsn medical": {"base": "https://www.bsnmedical.com", "search": "https://www.bsnmedical.com/search/?q={q}"},
    "hartmann": {"base": "https://www.hartmann.info", "search": "https://www.hartmann.info/en/search?q={q}"},
    "medline": {"base": "https://www.medline.com", "search": "https://www.medline.com/search/?q={q}"},
    "ansell": {"base": "https://www.ansell.com", "search": "https://www.ansell.com/search?q={q}"},
    "barrier": {"base": "https://www.molnlycke.com", "search": "https://www.molnlycke.com/search/?q={q}"},
    "selefa": {"base": "https://www.onemed.com", "search": "https://www.onemed.com/search?q={q}"},
    "icu medical": {"base": "https://www.icumed.com", "search": "https://www.icumed.com/search?q={q}"},
    "icumed": {"base": "https://www.icumed.com", "search": "https://www.icumed.com/search?q={q}"},
    "bd": {"base": "https://www.bd.com", "search": "https://www.bd.com/en-us/search#q={q}"},
    "becton dickinson": {"base": "https://www.bd.com", "search": "https://www.bd.com/en-us/search#q={q}"},
    "cardinal health": {"base": "https://www.cardinalhealth.com", "search": "https://www.cardinalhealth.com/en/search.html#q={q}"},
    "smith & nephew": {"base": "https://www.smith-nephew.com", "search": "https://www.smith-nephew.com/search?q={q}"},
    "convatec": {"base": "https://www.convatec.com", "search": "https://www.convatec.com/search?q={q}"},
    "3m": {"base": "https://www.3m.com", "search": "https://www.3m.com/3M/en_US/search/?Ntt={q}"},
    "abena": {"base": "https://www.abena.com", "search": "https://www.abena.com/search?q={q}"},
    "sca": {"base": "https://www.sca.com", "search": "https://www.sca.com/en/search/?q={q}"},
    "tena": {"base": "https://www.tena.no", "search": "https://www.tena.no/search?q={q}"},
    "kimberly-clark": {"base": "https://www.kimberly-clark.com", "search": "https://www.kimberly-clark.com/search?q={q}"},
    "halyard": {"base": "https://www.halyardhealth.com", "search": "https://www.halyardhealth.com/search?q={q}"},
    "medela": {"base": "https://www.medela.com", "search": "https://www.medela.com/search?q={q}"},
    "b. braun": {"base": "https://www.bbraun.com", "search": "https://www.bbraun.com/en/search.html?q={q}"},
    "bbraun": {"base": "https://www.bbraun.com", "search": "https://www.bbraun.com/en/search.html?q={q}"},
    "stryker": {"base": "https://www.stryker.com", "search": "https://www.stryker.com/search?q={q}"},
    "medtronic": {"base": "https://www.medtronic.com", "search": "https://www.medtronic.com/search?q={q}"},
    "sacett": {"base": "https://www.icumed.com", "search": "https://www.icumed.com/search?q={q}"},
}

# Image file extensions
_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tiff"}

# Skip patterns for non-product images
_SKIP_URL_PATTERNS = {"icon", "logo", "favicon", "placeholder", "1x1", "pixel",
                       "spacer", "blank", "arrow", "flag", "banner", "spinner",
                       "loading", "avatar", "social", "share", "button"}


def _normalize_producer(name: str) -> str:
    """Normalize producer name for lookup."""
    if not name:
        return ""
    return name.strip().lower().replace("®", "").replace("™", "").replace("(", "").replace(")", "").strip()


def _extract_domain(url: str) -> str:
    """Extract domain from URL."""
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def _guess_file_extension(url: str) -> str:
    """Guess file extension from image URL."""
    url_lower = url.lower()
    # Check query params first (e.g. ?format=webp)
    if "format=webp" in url_lower:
        return ".webp"
    if "format=png" in url_lower:
        return ".png"
    if "format=jpg" in url_lower or "format=jpeg" in url_lower:
        return ".jpg"
    # Check file extension in path
    path_lower = url_lower.split("?")[0]
    for ext in _IMAGE_EXTENSIONS:
        if path_lower.endswith(ext):
            return ext
    return ".jpg"  # Default


def _is_product_image_url(url: str) -> bool:
    """Check if URL looks like a product image (not icon/logo/placeholder)."""
    if not url:
        return False
    url_lower = url.lower()
    if any(skip in url_lower for skip in _SKIP_URL_PATTERNS):
        return False
    # Must look like an image
    has_image_ext = any(ext in url_lower for ext in _IMAGE_EXTENSIONS)
    has_image_path = any(kw in url_lower for kw in ["image", "img", "photo", "media", "product", "asset"])
    if not has_image_ext and not has_image_path and "format=" not in url_lower:
        return False
    return True


def _classify_source_type(source: str, domain: str) -> str:
    """Classify the source type of an image."""
    if not source:
        return "annen"
    source_lower = source.lower()
    if source_lower in ("manufacturer", "manufacturer_website", "producer_website"):
        return "produsent"
    if source_lower == "norengros":
        return "distributør"
    if domain:
        # Check if domain matches a known producer
        for key in KNOWN_PRODUCERS:
            base = KNOWN_PRODUCERS[key]["base"].replace("https://www.", "").replace("https://", "")
            if base in domain:
                return "produsent"
    return "annen"


def _confidence_label(confidence: float) -> str:
    """Map confidence score to human-readable label."""
    if confidence >= 0.75:
        return "Høy tillit"
    elif confidence >= 0.50:
        return "Middels tillit"
    elif confidence >= 0.25:
        return "Lav tillit"
    return "Krever manuell vurdering"


# ══════════════════════════════════════════════════════════════
# IMPROVEMENT SCORING
# ══════════════════════════════════════════════════════════════


def calculate_improvement_score(
    current_status: str,
    suggested_url: Optional[str],
    source_type: str,
    confidence: float,
    current_image_score: float = 0.0,
) -> tuple[int, str]:
    """Calculate how much better a suggested image is vs. current (0-100).

    Returns (score, reason_text).

    Factors:
    - Current image status (missing = highest possible improvement)
    - Source reliability (producer > distributor > other)
    - Confidence in product match
    - Whether URL looks like a real product image
    """
    if not suggested_url:
        return 0, "Ingen bilde-URL tilgjengelig"

    score = 0
    reasons = []

    # Factor 1: Current image status (0-40 points)
    if current_status == "missing":
        score += 40
        reasons.append("Eksisterende bilde mangler helt")
    elif current_status == "low_quality":
        score += 30
        reasons.append("Eksisterende bilde har lav kvalitet")
    elif current_status == "poor_background":
        score += 20
        reasons.append("Eksisterende bilde har dårlig bakgrunn")
    elif current_status == "review":
        score += 10
        reasons.append("Eksisterende bilde bør gjennomgås")

    # Factor 2: Source reliability (0-30 points)
    if source_type == "produsent":
        score += 30
        reasons.append("Bildet kommer fra produsentens nettside")
    elif source_type == "distributør":
        score += 15
        reasons.append("Bildet kommer fra offisiell distributør")
    else:
        score += 5
        reasons.append("Bildet kommer fra ekstern kilde")

    # Factor 3: Confidence in match (0-20 points)
    conf_points = int(confidence * 20)
    score += conf_points
    if confidence >= 0.7:
        reasons.append("Høy sikkerhet på produktmatch")
    elif confidence >= 0.4:
        reasons.append("Middels sikkerhet på produktmatch")
    else:
        reasons.append("Lav sikkerhet på produktmatch — krever manuell kontroll")

    # Factor 4: URL quality (0-10 points)
    if _is_product_image_url(suggested_url):
        score += 10
        reasons.append("URL ser ut som et reelt produktbilde")

    return min(100, score), "; ".join(reasons)


# ══════════════════════════════════════════════════════════════
# SEARCH URL BUILDING
# ══════════════════════════════════════════════════════════════


def build_search_urls(
    product: ProductData,
    manufacturer_data: Optional[ManufacturerLookup] = None,
    jeeves_supplier: Optional[str] = None,
    jeeves_supplier_item: Optional[str] = None,
) -> dict:
    """Build multiple search URL strategies for finding product images.

    Returns dict with search terms, producer URL, Google URL, and metadata.
    """
    producer = (
        jeeves_supplier
        or product.manufacturer
        or (manufacturer_data.manufacturer_name if manufacturer_data and manufacturer_data.found else None)
        or ""
    )
    supplier_item = (
        jeeves_supplier_item
        or product.manufacturer_article_number
        or (manufacturer_data.manufacturer_article_number if manufacturer_data and manufacturer_data.found else None)
        or ""
    )
    product_name = product.product_name or ""

    # Build multiple search term strategies
    strategies = []

    # Strategy 1: Producer + supplier article number (most specific)
    if producer and supplier_item:
        strategies.append(f"{producer} {supplier_item}")

    # Strategy 2: Full product name + producer
    if product_name and producer:
        strategies.append(f"{producer} {product_name}")

    # Strategy 3: Product name + "product image"
    if product_name:
        strategies.append(f"{product_name} product image")

    # Strategy 4: Supplier item alone
    if supplier_item:
        strategies.append(supplier_item)

    # Fallback: article number
    if not strategies:
        strategies.append(product.article_number)

    primary_query = strategies[0] if strategies else product.article_number

    result = {
        "search_terms": primary_query,
        "all_strategies": strategies,
        "producer_name": producer,
        "supplier_article_number": supplier_item,
        "producer_search_url": None,
        "google_image_search_url": f"https://www.google.com/search?tbm=isch&q={quote_plus(primary_query)}",
    }

    # Find producer-specific search URL
    norm_producer = _normalize_producer(producer)
    # Also check product name for brand matches (e.g. "SACETT" → ICU Medical)
    norm_name = _normalize_producer(product_name)
    for key, config in KNOWN_PRODUCERS.items():
        if key in norm_producer or norm_producer in key or key in norm_name:
            query_term = supplier_item or product_name or product.article_number
            result["producer_search_url"] = config["search"].format(q=quote_plus(query_term))
            break

    return result


# ══════════════════════════════════════════════════════════════
# ACTIVE IMAGE SEARCH
# ══════════════════════════════════════════════════════════════


async def search_producer_image(
    product: ProductData,
    manufacturer_data: Optional[ManufacturerLookup] = None,
    jeeves_supplier: Optional[str] = None,
    jeeves_supplier_item: Optional[str] = None,
) -> Optional[dict]:
    """Try to find a product image on the producer's website.

    Returns dict with image_url, source_url, source, confidence, domain if found.
    Returns None if no image could be found automatically.
    """
    producer = (
        jeeves_supplier
        or product.manufacturer
        or (manufacturer_data.manufacturer_name if manufacturer_data and manufacturer_data.found else None)
        or ""
    )
    supplier_item = (
        jeeves_supplier_item
        or product.manufacturer_article_number
        or (manufacturer_data.manufacturer_article_number if manufacturer_data and manufacturer_data.found else None)
        or ""
    )

    # If manufacturer_data already has an image, use it directly
    if manufacturer_data and manufacturer_data.found and manufacturer_data.image_url:
        domain = _extract_domain(manufacturer_data.image_url)
        return {
            "image_url": manufacturer_data.image_url,
            "source_url": manufacturer_data.source_url,
            "source": "manufacturer_website",
            "domain": domain,
            "confidence": manufacturer_data.confidence * 0.8,
        }

    if not producer:
        return None

    # Find the producer config
    norm_producer = _normalize_producer(producer)
    norm_name = _normalize_producer(product.product_name or "")
    search_config = None
    for key, config in KNOWN_PRODUCERS.items():
        if key in norm_producer or norm_producer in key or key in norm_name:
            search_config = config
            break

    if not search_config:
        return None

    # Try multiple search queries
    queries = []
    if supplier_item:
        queries.append(supplier_item)
    if product.product_name:
        queries.append(product.product_name)
    if supplier_item and producer:
        queries.append(f"{supplier_item} {producer}")

    for query in queries[:3]:
        search_url = search_config["search"].format(q=quote_plus(query))
        try:
            async with httpx.AsyncClient(
                headers=HEADERS, timeout=10.0, follow_redirects=True
            ) as client:
                response = await client.get(search_url)
                if response.status_code != 200:
                    continue

                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, "html.parser")

                # Strategy 1: og:image meta tag (most reliable)
                og_image = soup.find("meta", attrs={"property": "og:image"})
                if og_image:
                    img_url = og_image.get("content", "")
                    if img_url and _is_product_image_url(img_url):
                        if not img_url.startswith("http"):
                            img_url = f"https:{img_url}" if img_url.startswith("//") else search_config["base"] + img_url
                        domain = _extract_domain(img_url)
                        return {
                            "image_url": img_url,
                            "source_url": search_url,
                            "source": "producer_website",
                            "domain": domain,
                            "confidence": 0.55,
                        }

                # Strategy 2: Search for img tags matching product terms
                search_terms_lower = [t.lower() for t in [supplier_item, product.product_name or ""] if t]
                for img in soup.find_all("img"):
                    src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
                    alt = (img.get("alt") or "").lower()

                    if not src or src.startswith("data:"):
                        continue
                    if not _is_product_image_url(src):
                        continue

                    # Make absolute URL
                    if src.startswith("//"):
                        src = "https:" + src
                    elif src.startswith("/"):
                        src = search_config["base"] + src

                    # Check if matches product
                    matches = any(
                        term in src.lower() or term in alt
                        for term in search_terms_lower
                    )

                    if matches:
                        # Skip tiny images
                        width = img.get("width")
                        height = img.get("height")
                        if width and height:
                            try:
                                if int(width) < 100 or int(height) < 100:
                                    continue
                            except ValueError:
                                pass

                        domain = _extract_domain(src)
                        return {
                            "image_url": src,
                            "source_url": search_url,
                            "source": "producer_website",
                            "domain": domain,
                            "confidence": 0.45,
                        }

        except Exception as e:
            logger.debug(f"Image search failed for query '{query}': {e}")
            continue

    return None


# ══════════════════════════════════════════════════════════════
# ENHANCE IMAGE SUGGESTION
# ══════════════════════════════════════════════════════════════


def enhance_image_suggestion(
    suggestion: Optional[ImageSuggestion],
    product: ProductData,
    manufacturer_data: Optional[ManufacturerLookup] = None,
    jeeves_supplier: Optional[str] = None,
    jeeves_supplier_item: Optional[str] = None,
) -> Optional[ImageSuggestion]:
    """Enhance an image suggestion with scores, labels, search URLs, and download info.

    This is the final enrichment step before an image suggestion reaches output.
    It adds: improvement_score, confidence_label, source_type, source_domain,
    download_filename, search URLs, and quality checks.
    """
    if not suggestion:
        return None

    # Build search URLs
    search_info = build_search_urls(
        product, manufacturer_data, jeeves_supplier, jeeves_supplier_item,
    )

    # Store search metadata
    suggestion.search_terms_used = search_info["search_terms"]
    suggestion.producer_search_url = search_info["producer_search_url"]
    suggestion.google_search_url = search_info["google_image_search_url"]

    # Detect CDN self-reference
    current_cdn_url = f"https://res.onemed.com/NO/ARWebBig/{product.article_number}.jpg"
    is_just_cdn = (
        not suggestion.suggested_image_url
        or suggestion.suggested_image_url == current_cdn_url
        or suggestion.suggested_image_url == suggestion.current_image_url
    )

    if is_just_cdn:
        # No real image found — provide search guidance
        suggestion.suggested_image_url = None
        suggestion.suggested_source = "manuelt_søk_påkrevd"
        suggestion.source_type = "manuelt_søk"
        suggestion.source_domain = None
        suggestion.improvement_score = 0
        suggestion.improvement_reason = "Ingen bedre bilde funnet automatisk"
        suggestion.confidence = 0.0
        suggestion.confidence_label = "Krever manuell vurdering"
        suggestion.review_required = True

        parts = [f"Bildestatus: {suggestion.current_image_status}."]
        parts.append("Ingen bedre bilde funnet automatisk.")
        if search_info["producer_name"]:
            parts.append(
                f"Produsent: {search_info['producer_name']}"
                + (f" (art.nr: {search_info['supplier_article_number']})" if search_info["supplier_article_number"] else "")
                + "."
            )
        if search_info["producer_search_url"]:
            parts.append(f"Søk hos produsent: {search_info['producer_search_url']}")
        parts.append(f"Google-bildesøk: {search_info['google_image_search_url']}")
        suggestion.reason = " ".join(parts)
    else:
        # Real image found — calculate scores
        suggestion.source_domain = _extract_domain(suggestion.suggested_image_url or "")
        suggestion.source_type = _classify_source_type(
            suggestion.suggested_source, suggestion.source_domain
        )

        # Calculate improvement score
        current_img_score = 0.0  # Could be enhanced with actual CV score
        imp_score, imp_reason = calculate_improvement_score(
            current_status=suggestion.current_image_status,
            suggested_url=suggestion.suggested_image_url,
            source_type=suggestion.source_type,
            confidence=suggestion.confidence,
            current_image_score=current_img_score,
        )
        suggestion.improvement_score = imp_score
        suggestion.improvement_reason = imp_reason

        # Quality gate: only suggest if improvement is meaningful
        if imp_score < MIN_IMPROVEMENT_SCORE:
            logger.info(
                f"Image suggestion rejected: improvement score {imp_score} < {MIN_IMPROVEMENT_SCORE} "
                f"for {product.article_number}"
            )
            suggestion.suggested_image_url = None
            suggestion.improvement_reason = (
                f"Foreslått bilde er ikke en klar nok forbedring (score {imp_score}/100, "
                f"minimum {MIN_IMPROVEMENT_SCORE})"
            )
            suggestion.source_type = "manuelt_søk"
            suggestion.confidence = 0.0

        # Set confidence label
        suggestion.confidence_label = _confidence_label(suggestion.confidence)

        # Build reason if not already set
        if not suggestion.reason and suggestion.suggested_image_url:
            suggestion.reason = (
                f"Bedre bilde funnet hos {suggestion.source_type} "
                f"({suggestion.source_domain or 'ukjent'}). "
                f"Forbedringsscore: {imp_score}/100. "
                f"{imp_reason}"
            )

    # Set download filename
    ext = _guess_file_extension(suggestion.suggested_image_url or "")
    suggestion.download_filename = f"{product.article_number}{ext}"

    return suggestion


# ══════════════════════════════════════════════════════════════
# APPROVAL WORKFLOW
# ══════════════════════════════════════════════════════════════


def approve_image_suggestion(
    results: list,
    article_number: str,
    status: str = "godkjent",
    approved_by: Optional[str] = None,
) -> bool:
    """Set approval status on an image suggestion for a specific product.

    Valid statuses: "godkjent", "avvist", "manuell_kontroll", "ikke_vurdert"
    """
    valid_statuses = {"godkjent", "avvist", "manuell_kontroll", "ikke_vurdert"}
    if status not in valid_statuses:
        return False

    for result in results:
        if result.article_number == article_number and result.image_suggestion:
            result.image_suggestion.approval_status = status
            if approved_by:
                result.image_suggestion.approved_by = approved_by
            return True
    return False


def get_approved_images(results: list) -> list[dict]:
    """Get all image suggestions with 'godkjent' status.

    Returns list of dicts with article_number, image_url, download_filename.
    """
    approved = []
    for result in results:
        sugg = result.image_suggestion
        if sugg and sugg.approval_status == "godkjent" and sugg.suggested_image_url:
            approved.append({
                "article_number": result.article_number,
                "image_url": sugg.suggested_image_url,
                "download_filename": sugg.download_filename or f"{result.article_number}.jpg",
                "source": sugg.suggested_source,
                "source_domain": sugg.source_domain,
                "confidence": sugg.confidence,
            })
    return approved


async def download_approved_images_as_zip(
    results: list,
    output_path: str,
    jpeg_quality: int = 90,
) -> dict:
    """Download all approved images, convert to JPEG, and package as a ZIP file.

    ALL images are converted to JPEG (.jpg) regardless of source format.
    This ensures consistent output for PIM/Inriver import.

    Conversion handles:
      - WEBP → JPEG
      - PNG (with transparency) → JPEG with white background
      - GIF (first frame) → JPEG
      - RGBA/P/CMYK → RGB → JPEG
      - Already-JPEG → re-saved with consistent quality settings

    Returns dict with:
      - zip_path: path to created ZIP file
      - downloaded: list of successfully downloaded filenames
      - failed: list of failed downloads with reasons
      - total: total number of approved images
    """
    import zipfile
    import io
    from PIL import Image

    approved = get_approved_images(results)

    if not approved:
        return {
            "zip_path": None,
            "downloaded": [],
            "failed": [],
            "total": 0,
        }

    downloaded = []
    failed = []
    filename_counter: dict[str, int] = {}  # Track duplicates

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
        async with httpx.AsyncClient(
            headers=HEADERS, timeout=15.0, follow_redirects=True
        ) as client:
            for item in approved:
                url = item["image_url"]
                base_name = item["article_number"]

                # Always .jpg extension
                if base_name in filename_counter:
                    filename_counter[base_name] += 1
                    filename = f"{base_name}-{filename_counter[base_name]}.jpg"
                else:
                    filename_counter[base_name] = 1
                    filename = f"{base_name}.jpg"

                original_format = _guess_file_extension(url).lstrip(".")

                try:
                    response = await client.get(url)
                    if response.status_code != 200:
                        failed.append({
                            "article_number": base_name,
                            "url": url,
                            "reason": f"HTTP {response.status_code}",
                        })
                        continue

                    # Verify it's actually image data
                    content_type = response.headers.get("content-type", "")
                    if not content_type.startswith("image/") and len(response.content) < 1000:
                        failed.append({
                            "article_number": base_name,
                            "url": url,
                            "reason": f"Not an image (content-type: {content_type})",
                        })
                        continue

                    # Convert to JPEG
                    jpeg_bytes = _convert_to_jpeg(
                        response.content, quality=jpeg_quality
                    )
                    if jpeg_bytes is None:
                        failed.append({
                            "article_number": base_name,
                            "url": url,
                            "reason": "Konvertering til JPEG feilet",
                        })
                        logger.warning(
                            f"JPEG conversion failed for {base_name} "
                            f"(original: {original_format}, url: {url})"
                        )
                        continue

                    # Validate the output is valid JPEG before adding to ZIP
                    if not _validate_jpeg(jpeg_bytes):
                        failed.append({
                            "article_number": base_name,
                            "url": url,
                            "reason": "JPEG-validering feilet etter konvertering",
                        })
                        logger.warning(
                            f"JPEG validation failed for {base_name} after conversion"
                        )
                        continue

                    zf.writestr(filename, jpeg_bytes)
                    downloaded.append(filename)
                    logger.info(
                        f"Image OK: {filename} "
                        f"(original: {original_format}, "
                        f"size: {len(response.content) // 1024}KB → "
                        f"{len(jpeg_bytes) // 1024}KB JPEG) "
                        f"from {url}"
                    )

                except Exception as e:
                    failed.append({
                        "article_number": base_name,
                        "url": url,
                        "reason": str(e),
                    })
                    logger.warning(f"Failed to download image {url}: {e}")

    if downloaded:
        logger.info(
            f"ZIP created: {output_path} — "
            f"{len(downloaded)} images OK, {len(failed)} failed"
        )
    else:
        logger.warning(
            f"ZIP not created — all {len(failed)} images failed"
        )

    return {
        "zip_path": output_path if downloaded else None,
        "downloaded": downloaded,
        "failed": failed,
        "total": len(approved),
    }


def _convert_to_jpeg(
    image_bytes: bytes,
    quality: int = 90,
    max_dimension: int = 4096,
) -> Optional[bytes]:
    """Convert any image format to JPEG with consistent settings.

    Handles:
      - RGBA / P with transparency → white background compositing
      - CMYK → RGB conversion
      - GIF → first frame only
      - WEBP → full decode and re-encode
      - Very large images → downscaled to max_dimension

    Returns JPEG bytes or None if conversion fails.
    """
    import io
    from PIL import Image, UnidentifiedImageError

    try:
        img = Image.open(io.BytesIO(image_bytes))
    except (UnidentifiedImageError, Exception) as e:
        logger.warning(f"Could not open image: {e}")
        return None

    try:
        original_mode = img.mode
        original_format = img.format or "unknown"

        # For animated GIF/WEBP, use first frame
        if hasattr(img, "n_frames") and img.n_frames > 1:
            img.seek(0)

        # Handle palette mode (P) — may have transparency
        if img.mode == "P":
            img = img.convert("RGBA")

        # Handle transparency: composite onto white background
        if img.mode in ("RGBA", "LA"):
            background = Image.new("RGB", img.size, (255, 255, 255))
            # Split out alpha channel for compositing
            if img.mode == "LA":
                img = img.convert("RGBA")
            background.paste(img, mask=img.split()[3])
            img = background
        elif img.mode == "CMYK":
            img = img.convert("RGB")
        elif img.mode not in ("RGB",):
            img = img.convert("RGB")

        # Downscale very large images
        w, h = img.size
        if max(w, h) > max_dimension:
            ratio = max_dimension / max(w, h)
            new_size = (int(w * ratio), int(h * ratio))
            img = img.resize(new_size, Image.LANCZOS)
            logger.info(
                f"Downscaled image from {w}x{h} to {new_size[0]}x{new_size[1]}"
            )

        # Save as JPEG
        buffer = io.BytesIO()
        img.save(
            buffer,
            format="JPEG",
            quality=quality,
            optimize=True,
            progressive=True,
        )
        jpeg_bytes = buffer.getvalue()

        logger.debug(
            f"Converted {original_format} ({original_mode}) → JPEG "
            f"({len(image_bytes)} → {len(jpeg_bytes)} bytes)"
        )
        return jpeg_bytes

    except Exception as e:
        logger.warning(f"JPEG conversion error: {e}")
        return None


def _validate_jpeg(data: bytes) -> bool:
    """Validate that bytes are a valid, openable JPEG image."""
    import io
    from PIL import Image

    if not data or len(data) < 100:
        return False
    # Check JPEG magic bytes (SOI marker)
    if data[:2] != b"\xff\xd8":
        return False
    try:
        img = Image.open(io.BytesIO(data))
        img.verify()
        return True
    except Exception:
        return False
