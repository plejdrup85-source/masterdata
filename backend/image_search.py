"""Active image search for better product images.

When a product's current image is missing or low quality, this module
searches for better alternatives from:
1. Producer's own website (highest priority)
2. Known medical product image sources
3. Generates actionable search URLs for manual follow-up

Design: returns concrete URLs where possible, or structured search hints
when automatic retrieval isn't feasible.
"""

import logging
import re
from typing import Optional
from urllib.parse import quote_plus

import httpx

from backend.models import ImageSuggestion, ManufacturerLookup, ProductData

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,image/*",
}

# Known producer image URL patterns — these reliably serve product images
PRODUCER_IMAGE_PATTERNS = {
    "molnlycke": {
        "base": "https://www.molnlycke.com",
        "search_url": "https://www.molnlycke.com/search/?q={query}",
    },
    "mölnlycke": {
        "base": "https://www.molnlycke.com",
        "search_url": "https://www.molnlycke.com/search/?q={query}",
    },
    "coloplast": {
        "base": "https://www.coloplast.com",
        "search_url": "https://www.coloplast.com/search/?q={query}",
    },
    "essity": {
        "base": "https://www.essity.com",
        "search_url": "https://www.essity.com/search/?q={query}",
    },
    "bsn medical": {
        "base": "https://www.bsnmedical.com",
        "search_url": "https://www.bsnmedical.com/search/?q={query}",
    },
    "hartmann": {
        "base": "https://www.hartmann.info",
        "search_url": "https://www.hartmann.info/en/search?q={query}",
    },
    "medline": {
        "base": "https://www.medline.com",
        "search_url": "https://www.medline.com/search/?q={query}",
    },
    "ansell": {
        "base": "https://www.ansell.com",
        "search_url": "https://www.ansell.com/search?q={query}",
    },
    "barrier": {
        "base": "https://www.molnlycke.com",
        "search_url": "https://www.molnlycke.com/search/?q={query}",
    },
    "selefa": {
        "base": "https://www.onemed.com",
        "search_url": "https://www.onemed.com/search?q={query}",
    },
}


def _normalize_producer(name: str) -> str:
    """Normalize producer name for lookup."""
    if not name:
        return ""
    return name.strip().lower().replace("®", "").replace("™", "").strip()


def build_image_search_urls(
    product: ProductData,
    manufacturer_data: Optional[ManufacturerLookup] = None,
    jeeves_supplier: Optional[str] = None,
    jeeves_supplier_item: Optional[str] = None,
) -> dict:
    """Build actionable search URLs for finding better product images.

    Returns dict with:
      - producer_search_url: Direct search on producer's website
      - google_image_search_url: Google Images search
      - search_terms: Terms used for searching
      - producer_name: Identified producer
      - supplier_article_number: Producer's own article number
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

    # Build search terms
    search_terms = []
    if producer:
        search_terms.append(producer)
    if supplier_item:
        search_terms.append(supplier_item)
    elif product_name:
        # Use first 3 significant words of product name
        words = [w for w in product_name.split() if len(w) > 2 and not w.isdigit()]
        search_terms.extend(words[:3])

    if not search_terms:
        search_terms = [product.article_number]

    query = " ".join(search_terms)

    result = {
        "search_terms": query,
        "producer_name": producer,
        "supplier_article_number": supplier_item,
        "producer_search_url": None,
        "google_image_search_url": f"https://www.google.com/search?tbm=isch&q={quote_plus(query + ' product image')}",
    }

    # Try to find producer-specific search URL
    norm_producer = _normalize_producer(producer)
    for key, config in PRODUCER_IMAGE_PATTERNS.items():
        if key in norm_producer or norm_producer in key:
            result["producer_search_url"] = config["search_url"].format(
                query=quote_plus(supplier_item or product_name)
            )
            break

    return result


async def search_producer_image(
    product: ProductData,
    manufacturer_data: Optional[ManufacturerLookup] = None,
    jeeves_supplier: Optional[str] = None,
    jeeves_supplier_item: Optional[str] = None,
) -> Optional[dict]:
    """Try to find a product image on the producer's website.

    Returns dict with image_url, source_url, confidence if found.
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

    if not producer or not supplier_item:
        return None

    # If manufacturer_data already has an image, use it
    if manufacturer_data and manufacturer_data.found and manufacturer_data.image_url:
        return {
            "image_url": manufacturer_data.image_url,
            "source_url": manufacturer_data.source_url,
            "source": "manufacturer_website",
            "confidence": manufacturer_data.confidence * 0.8,
        }

    # Try to fetch producer page and find image
    norm_producer = _normalize_producer(producer)
    search_config = None
    for key, config in PRODUCER_IMAGE_PATTERNS.items():
        if key in norm_producer or norm_producer in key:
            search_config = config
            break

    if not search_config:
        return None

    search_url = search_config["search_url"].format(
        query=quote_plus(supplier_item)
    )

    try:
        async with httpx.AsyncClient(
            headers=HEADERS, timeout=10.0, follow_redirects=True
        ) as client:
            response = await client.get(search_url)
            if response.status_code != 200:
                return None

            # Look for product images in the response
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Find product images — look for img tags with product-related attributes
            for img in soup.find_all("img"):
                src = img.get("src") or img.get("data-src") or ""
                alt = (img.get("alt") or "").lower()

                if not src or src.startswith("data:"):
                    continue

                # Make absolute URL
                if src.startswith("//"):
                    src = "https:" + src
                elif src.startswith("/"):
                    src = search_config["base"] + src

                # Check if this looks like a product image
                if any(
                    term.lower() in src.lower() or term.lower() in alt
                    for term in [supplier_item, product.product_name or ""]
                    if term
                ):
                    # Verify it's a real image (not a tiny icon)
                    width = img.get("width")
                    height = img.get("height")
                    if width and height:
                        try:
                            w, h = int(width), int(height)
                            if w < 100 or h < 100:
                                continue
                        except ValueError:
                            pass

                    return {
                        "image_url": src,
                        "source_url": search_url,
                        "source": "producer_website",
                        "confidence": 0.5,
                    }

    except Exception as e:
        logger.debug(f"Image search failed for {producer}/{supplier_item}: {e}")

    return None


def enhance_image_suggestion(
    suggestion: Optional[ImageSuggestion],
    product: ProductData,
    manufacturer_data: Optional[ManufacturerLookup] = None,
    jeeves_supplier: Optional[str] = None,
    jeeves_supplier_item: Optional[str] = None,
) -> Optional[ImageSuggestion]:
    """Enhance an image suggestion with search URLs and structured info.

    Ensures that even when no automatic image is found, the user gets
    actionable search URLs and clear guidance.
    """
    if not suggestion:
        return None

    # Build search URLs
    search_info = build_image_search_urls(
        product, manufacturer_data, jeeves_supplier, jeeves_supplier_item,
    )

    # If the suggestion has no real suggested URL (or points to CDN = current),
    # add the search URLs as guidance
    current_cdn_url = (
        f"https://res.onemed.com/NO/ARWebBig/{product.article_number}.jpg"
    )
    suggestion_is_just_cdn = (
        not suggestion.suggested_image_url
        or suggestion.suggested_image_url == current_cdn_url
        or suggestion.suggested_image_url == suggestion.current_image_url
    )

    if suggestion_is_just_cdn:
        # No actual better image found — provide search guidance
        suggestion.suggested_image_url = None  # Clear CDN self-reference
        suggestion.suggested_source = "manuelt_søk_påkrevd"

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
        if search_info["google_image_search_url"]:
            parts.append(f"Google-bildesøk: {search_info['google_image_search_url']}")

        suggestion.reason = " ".join(parts)
        suggestion.confidence = 0.0
        suggestion.review_required = True
    else:
        # We have a real suggested image URL — add source info
        if search_info["producer_name"] and not suggestion.reason:
            suggestion.reason = (
                f"Bedre bilde funnet hos {search_info['producer_name']}. "
                f"Verifiser at bildet viser riktig produkt."
            )

    return suggestion
