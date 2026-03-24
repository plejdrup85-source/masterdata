"""Broad web image search for product images.

Searches multiple sources to find better product images when the current
CDN image is missing or low quality. Uses manufacturer article numbers,
product names, and specifications to find and verify candidate images.

Source priority:
1. Manufacturer media banks (highest confidence)
2. Manufacturer product pages
3. Distributor / catalog sites
4. General web search results

All candidates are verified against known product attributes before
being suggested.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse, quote_plus

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/*,*/*;q=0.8",
    "Accept-Language": "nb-NO,nb;q=0.9,no;q=0.8,en;q=0.5",
}

# Known manufacturer media bank URL patterns
# Maps manufacturer name fragments → media bank base URLs and search patterns
MEDIA_BANKS: dict[str, list[dict]] = {
    "abena": [
        {
            "name": "Abena Mediacenter",
            "base": "https://mediacenter.abena.com",
            "search_url": "https://mediacenter.abena.com/search?q={query}",
            # Site-specific search: search engine with site: filter
            "site_search_domains": ["mediacenter.abena.com", "abena.com"],
            "source_type": "manufacturer_mediabank",
        },
        {
            "name": "Abena Website",
            "base": "https://www.abena.com",
            "search_url": "https://www.abena.com/search?q={query}",
            "source_type": "manufacturer_website",
        },
    ],
    "molnlycke": [
        {
            "name": "Mölnlycke",
            "base": "https://www.molnlycke.com",
            "search_url": "https://www.molnlycke.com/search/?q={query}",
            "source_type": "manufacturer_website",
        },
    ],
    "coloplast": [
        {
            "name": "Coloplast",
            "base": "https://www.coloplast.com",
            "search_url": "https://www.coloplast.com/search/?q={query}",
            "source_type": "manufacturer_website",
        },
    ],
    "essity": [
        {
            "name": "Essity / TENA",
            "base": "https://www.essity.com",
            "search_url": "https://www.essity.com/search/?q={query}",
            "source_type": "manufacturer_website",
        },
    ],
    "hartmann": [
        {
            "name": "Hartmann",
            "base": "https://www.hartmann.info",
            "search_url": "https://www.hartmann.info/search?q={query}",
            "source_type": "manufacturer_website",
        },
    ],
    "bbraun": [
        {
            "name": "B.Braun",
            "base": "https://www.bbraun.com",
            "search_url": "https://www.bbraun.com/search?q={query}",
            "source_type": "manufacturer_website",
        },
    ],
    "convatec": [
        {
            "name": "ConvaTec",
            "base": "https://www.convatec.com",
            "search_url": "https://www.convatec.com/search?q={query}",
            "source_type": "manufacturer_website",
        },
    ],
    "3m": [
        {
            "name": "3M",
            "base": "https://www.3m.com",
            "search_url": "https://www.3m.com/3M/en_US/search/?Ntt={query}",
            "source_type": "manufacturer_website",
        },
    ],
    "sca": [
        {
            "name": "SCA / TENA",
            "base": "https://www.tena.no",
            "search_url": "https://www.tena.no/sok/?q={query}",
            "source_type": "manufacturer_website",
        },
    ],
    "dansac": [
        {
            "name": "Dansac",
            "base": "https://www.dansac.com",
            "search_url": "https://www.dansac.com/search?q={query}",
            "source_type": "manufacturer_website",
        },
    ],
    "hollister": [
        {
            "name": "Hollister",
            "base": "https://www.hollister.com",
            "search_url": "https://www.hollister.com/search?q={query}",
            "source_type": "manufacturer_website",
        },
    ],
    "medela": [
        {
            "name": "Medela",
            "base": "https://www.medela.com",
            "search_url": "https://www.medela.com/search?q={query}",
            "source_type": "manufacturer_website",
        },
    ],
}

# Image file extensions considered valid
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".tiff", ".bmp"}

# URL patterns that indicate non-product images
SKIP_PATTERNS = frozenset([
    "icon", "logo", "favicon", "placeholder", "1x1", "pixel", "spacer",
    "blank", "arrow", "flag", "banner", "cart", "checkout", "avatar",
    "social", "facebook", "twitter", "instagram", "linkedin", "youtube",
    "spinner", "loading", "thumb_small", "thumbnail_tiny",
])

# Source type confidence multipliers (higher = more trusted)
SOURCE_CONFIDENCE: dict[str, float] = {
    "manufacturer_mediabank": 1.0,
    "manufacturer_website": 0.85,
    "official_distributor": 0.7,
    "catalog_site": 0.6,
    "web_search": 0.5,
}


@dataclass
class ImageCandidate:
    """A candidate product image found during search."""
    image_url: str
    source_url: str  # Page where image was found
    source_domain: str
    source_type: str  # manufacturer_mediabank, manufacturer_website, etc.
    source_name: str  # Human-readable source name

    # Verification signals
    artnr_in_url: bool = False
    artnr_in_filename: bool = False
    artnr_in_page_text: bool = False
    artnr_in_alt_text: bool = False
    manufacturer_match: bool = False
    description_match: bool = False
    spec_match: bool = False

    # Computed scores
    identity_score: float = 0.0  # 0-1: how confident we are this is the right product
    improvement_score: float = 0.0  # 0-1: how much better this is than current
    confidence: float = 0.0  # Final combined confidence

    reason: str = ""
    verification_details: list[str] = field(default_factory=list)


def _normalize_artnr(artnr: str) -> str:
    """Normalize an article number for comparison."""
    if not artnr:
        return ""
    # Strip leading N (OneMed convention), spaces, dashes
    clean = artnr.strip().upper()
    if clean.startswith("N") and len(clean) > 1 and clean[1:].isdigit():
        clean = clean[1:]
    return re.sub(r'[\s\-\.]', '', clean)


def _find_media_banks(manufacturer_name: str) -> list[dict]:
    """Find known media bank configurations for a manufacturer."""
    if not manufacturer_name:
        return []
    clean = manufacturer_name.lower().strip()
    # Remove common corporate suffixes
    for suffix in [" as", " ab", " gmbh", " inc", " ltd", " ag", " sa", " norge", " norway"]:
        if clean.endswith(suffix):
            clean = clean[:-len(suffix)].strip()

    results = []
    for key, banks in MEDIA_BANKS.items():
        if key in clean or clean in key:
            results.extend(banks)
    return results


def _is_image_url(url: str) -> bool:
    """Check if a URL points to an image file."""
    if not url:
        return False
    url_lower = url.lower()
    parsed = urlparse(url_lower)
    path = parsed.path

    # Check explicit image extensions
    for ext in IMAGE_EXTENSIONS:
        if path.endswith(ext):
            return True

    # Check for image-serving URL patterns (dynamic image servers)
    image_indicators = ["/image/", "/img/", "/photo/", "/media/", "/preview/",
                        "/product-image/", "/produktbild/", "imagehandler",
                        "/picture/", "/asset/"]
    if any(ind in url_lower for ind in image_indicators):
        return True

    return False


def _is_product_image(url: str, alt_text: str = "") -> bool:
    """Check if a URL is likely a product image (not decoration)."""
    if not _is_image_url(url):
        return False
    url_lower = url.lower()
    alt_lower = (alt_text or "").lower()

    # Skip known non-product patterns
    for pattern in SKIP_PATTERNS:
        if pattern in url_lower or pattern in alt_lower:
            return False

    return True


def _check_artnr_in_text(text: str, artnr: str) -> bool:
    """Check if an article number appears in text (with normalization)."""
    if not text or not artnr:
        return False
    norm_artnr = _normalize_artnr(artnr)
    if not norm_artnr:
        return False

    # Clean text for comparison
    clean_text = re.sub(r'[\s\-\.]', '', text.upper())
    if norm_artnr in clean_text:
        return True

    # Also check with common variants (e.g., 103014 in "10301402")
    if len(norm_artnr) >= 5 and norm_artnr in text.replace(" ", "").replace("-", ""):
        return True

    return False


def _description_overlap(product_desc: str, page_text: str) -> float:
    """Calculate word overlap between product description and page text.

    Returns 0-1 score based on how many significant description words
    appear in the page text.
    """
    if not product_desc or not page_text:
        return 0.0

    # Extract significant words (skip short/common words)
    stop_words = {"og", "i", "med", "for", "av", "en", "et", "den", "det",
                  "er", "på", "til", "fra", "som", "the", "and", "or", "with",
                  "for", "in", "of", "a", "an", "mm", "stk", "pk"}
    desc_words = {
        w.lower() for w in re.findall(r'\b\w+\b', product_desc)
        if len(w) >= 3 and w.lower() not in stop_words
    }
    if not desc_words:
        return 0.0

    page_lower = page_text.lower()
    matches = sum(1 for w in desc_words if w in page_lower)
    return matches / len(desc_words)


def _verify_candidate(
    candidate: ImageCandidate,
    manufacturer_artnr: str,
    our_artnr: str,
    manufacturer_name: str,
    product_description: str,
    specification: str,
    page_text: str = "",
) -> ImageCandidate:
    """Verify an image candidate against known product attributes.

    Builds an identity score based on multiple signals. The more signals
    match, the higher the confidence that this image shows the right product.
    """
    signals = []
    norm_mfr_artnr = _normalize_artnr(manufacturer_artnr)
    norm_our_artnr = _normalize_artnr(our_artnr)

    # Signal 1: Manufacturer art.nr in image URL/filename (VERY strong)
    if norm_mfr_artnr:
        url_path = urlparse(candidate.image_url).path
        filename = url_path.split("/")[-1] if "/" in url_path else url_path
        if _check_artnr_in_text(candidate.image_url, manufacturer_artnr):
            candidate.artnr_in_url = True
            signals.append(("artnr_in_url", 0.35))
        if _check_artnr_in_text(filename, manufacturer_artnr):
            candidate.artnr_in_filename = True
            signals.append(("artnr_in_filename", 0.30))

    # Signal 2: Our art.nr in URL (moderate signal)
    if norm_our_artnr and _check_artnr_in_text(candidate.image_url, our_artnr):
        signals.append(("our_artnr_in_url", 0.20))

    # Signal 3: Manufacturer art.nr in page text (strong)
    if norm_mfr_artnr and page_text and _check_artnr_in_text(page_text, manufacturer_artnr):
        candidate.artnr_in_page_text = True
        signals.append(("artnr_in_page_text", 0.25))

    # Signal 4: Manufacturer art.nr in alt text (strong)
    # (set by caller if found during extraction)
    if candidate.artnr_in_alt_text:
        signals.append(("artnr_in_alt_text", 0.25))

    # Signal 5: Manufacturer name match (moderate)
    if manufacturer_name:
        mfr_clean = manufacturer_name.lower().strip()
        for suffix in [" as", " ab", " gmbh", " inc", " ltd", " ag", " sa", " norge"]:
            if mfr_clean.endswith(suffix):
                mfr_clean = mfr_clean[:-len(suffix)].strip()
        if mfr_clean and (
            mfr_clean in candidate.source_domain.lower()
            or mfr_clean in (page_text or "").lower()
        ):
            candidate.manufacturer_match = True
            signals.append(("manufacturer_match", 0.15))

    # Signal 6: Description overlap (supporting)
    if product_description and page_text:
        overlap = _description_overlap(product_description, page_text)
        if overlap >= 0.4:
            candidate.description_match = True
            signals.append(("description_match", min(overlap * 0.25, 0.20)))

    # Signal 7: Specification terms in page (supporting)
    if specification and page_text:
        # Extract dimension-like terms from spec (e.g., "240x350mm", "35my")
        spec_terms = re.findall(r'\d+x\d+\s*mm|\d+\s*my|\d+\s*ml|\d+\s*g\b', specification.lower())
        page_lower = page_text.lower()
        spec_hits = sum(1 for t in spec_terms if t.replace(" ", "") in page_lower.replace(" ", ""))
        if spec_terms and spec_hits > 0:
            candidate.spec_match = True
            ratio = spec_hits / len(spec_terms)
            signals.append(("spec_match", min(ratio * 0.15, 0.15)))

    # Compute identity score (capped at 1.0)
    if signals:
        raw_score = sum(s[1] for s in signals)
        # Bonus: if mfr artnr is found in BOTH URL and page text, strong combo
        signal_names = {s[0] for s in signals}
        if ("artnr_in_url" in signal_names or "artnr_in_filename" in signal_names) and "artnr_in_page_text" in signal_names:
            raw_score += 0.10  # Combo bonus
        if ("artnr_in_url" in signal_names or "artnr_in_filename" in signal_names) and "manufacturer_match" in signal_names:
            raw_score += 0.10  # Manufacturer + artnr combo bonus
        candidate.identity_score = min(raw_score, 1.0)
        candidate.verification_details = [s[0] for s in signals]
    else:
        candidate.identity_score = 0.0

    # Apply source type confidence multiplier
    # For high identity scores on trusted sources, be less conservative
    source_mult = SOURCE_CONFIDENCE.get(candidate.source_type, 0.5)
    if candidate.identity_score >= 0.5 and source_mult >= 0.7:
        # Strong identity + trusted source: boost confidence
        candidate.confidence = min(candidate.identity_score * (source_mult + 0.15), 1.0)
    else:
        candidate.confidence = min(candidate.identity_score * source_mult, 1.0)

    # Build human-readable reason
    details = []
    if candidate.artnr_in_url or candidate.artnr_in_filename:
        details.append(f"Produsent art.nr {manufacturer_artnr} gjenfunnet i bilde-URL")
    if candidate.artnr_in_page_text:
        details.append(f"Produsent art.nr bekreftet i sidetekst")
    if candidate.manufacturer_match:
        details.append(f"Produsentnavn matcher kilde ({candidate.source_domain})")
    if candidate.description_match:
        details.append("Produktbeskrivelse samsvarer med kildeinnhold")
    if candidate.spec_match:
        details.append("Spesifikasjonsdetaljer bekreftet i kilde")
    if not details:
        details.append("Ingen sterke verifiseringssignaler funnet")

    candidate.reason = ". ".join(details) + "."

    return candidate


def _extract_images_from_page(
    html: str,
    page_url: str,
    manufacturer_artnr: str,
) -> list[tuple[str, str]]:
    """Extract image URLs from an HTML page.

    Returns list of (image_url, alt_text) tuples.
    Prioritizes images where the manufacturer art.nr appears in URL or alt text.
    """
    soup = BeautifulSoup(html, "lxml")
    candidates: list[tuple[str, str, int]] = []  # (url, alt, priority)
    seen_urls = set()

    # 1. Check og:image (most reliable for product pages)
    og_img = soup.find("meta", attrs={"property": "og:image"})
    if og_img:
        url = og_img.get("content", "")
        if url and _is_product_image(url):
            if not url.startswith("http"):
                url = f"https:{url}" if url.startswith("//") else f"https://{urlparse(page_url).netloc}{url}"
            if url not in seen_urls:
                seen_urls.add(url)
                candidates.append((url, "", 10))

    # 2. All img tags
    for img in soup.find_all("img", src=True):
        src = img.get("src", "")
        alt = img.get("alt", "") or ""
        if not src or not _is_product_image(src, alt):
            continue

        # Resolve relative URLs
        if not src.startswith("http"):
            if src.startswith("//"):
                src = f"https:{src}"
            else:
                base = f"https://{urlparse(page_url).netloc}"
                src = f"{base}/{src.lstrip('/')}"

        if src in seen_urls:
            continue
        seen_urls.add(src)

        # Prioritize images with art.nr in URL or alt text
        priority = 0
        norm_artnr = _normalize_artnr(manufacturer_artnr)
        if norm_artnr:
            if _check_artnr_in_text(src, manufacturer_artnr):
                priority += 5
            if _check_artnr_in_text(alt, manufacturer_artnr):
                priority += 5

        candidates.append((src, alt, priority))

    # Also check srcset for higher-res versions
    for img in soup.find_all("img", srcset=True):
        srcset = img.get("srcset", "")
        for part in srcset.split(","):
            url_part = part.strip().split(" ")[0]
            if url_part and _is_product_image(url_part) and url_part not in seen_urls:
                if not url_part.startswith("http"):
                    if url_part.startswith("//"):
                        url_part = f"https:{url_part}"
                    else:
                        base = f"https://{urlparse(page_url).netloc}"
                        url_part = f"{base}/{url_part.lstrip('/')}"
                seen_urls.add(url_part)
                candidates.append((url_part, "", 1))

    # Check data-src and data-zoom-image (lazy-loaded / zoom images)
    for img in soup.find_all(["img", "a", "div"], attrs=True):
        for attr in ["data-src", "data-zoom-image", "data-full-size", "data-large"]:
            url = img.get(attr, "")
            if url and _is_product_image(url) and url not in seen_urls:
                if not url.startswith("http"):
                    if url.startswith("//"):
                        url = f"https:{url}"
                    else:
                        base = f"https://{urlparse(page_url).netloc}"
                        url = f"{base}/{url.lstrip('/')}"
                seen_urls.add(url)
                alt = img.get("alt", "") or ""
                priority = 0
                norm_artnr = _normalize_artnr(manufacturer_artnr)
                if norm_artnr and (_check_artnr_in_text(url, manufacturer_artnr) or _check_artnr_in_text(alt, manufacturer_artnr)):
                    priority += 5
                candidates.append((url, alt, priority))

    # Check link tags that point to images (e.g., <a href="...jpg">)
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if href and _is_product_image(href) and href not in seen_urls:
            if not href.startswith("http"):
                if href.startswith("//"):
                    href = f"https:{href}"
                else:
                    base = f"https://{urlparse(page_url).netloc}"
                    href = f"{base}/{href.lstrip('/')}"
            seen_urls.add(href)
            priority = 0
            norm_artnr = _normalize_artnr(manufacturer_artnr)
            if norm_artnr and _check_artnr_in_text(href, manufacturer_artnr):
                priority += 5
            candidates.append((href, "", priority))

    # Sort by priority (highest first) and return top candidates
    candidates.sort(key=lambda x: -x[2])
    return [(url, alt) for url, alt, _ in candidates[:10]]


async def _search_media_bank(
    bank: dict,
    manufacturer_artnr: str,
    product_name: str,
    client: httpx.AsyncClient,
    specification: str = "",
) -> list[tuple[str, str, str]]:
    """Search a manufacturer media bank for product images.

    Returns list of (image_url, page_text_snippet, source_url) tuples.
    """
    results = []
    queries = []
    norm_artnr = _normalize_artnr(manufacturer_artnr)

    if norm_artnr:
        queries.append(norm_artnr)
        # Also try with common suffixes (e.g., 103014 -> 10301402)
        queries.append(f"{norm_artnr}02")
    if product_name and norm_artnr:
        words = [w for w in product_name.split() if len(w) >= 3][:3]
        if words:
            queries.append(f"{norm_artnr} {' '.join(words)}")
    if product_name:
        words = [w for w in product_name.split() if len(w) >= 3][:4]
        if words:
            queries.append(" ".join(words))

    search_url_template = bank.get("search_url", "")

    # Stage A: Direct media bank search
    for query in queries[:3]:
        if not search_url_template:
            continue
        search_url = search_url_template.format(query=quote_plus(query))
        try:
            logger.info(f"Image search: media bank={bank['name']} query='{query}'")
            response = await client.get(search_url, headers=HEADERS, follow_redirects=True)
            if response.status_code != 200:
                logger.debug(f"Media bank {bank['name']} returned {response.status_code}")
                continue

            page_text = BeautifulSoup(response.text, "lxml").get_text(" ", strip=True)[:2000]
            images = _extract_images_from_page(response.text, str(response.url), manufacturer_artnr)
            for img_url, alt_text in images:
                results.append((img_url, page_text, str(response.url)))

        except Exception as e:
            logger.debug(f"Media bank search error ({bank['name']}): {e}")
            continue

    # Stage B: Site-specific web search (e.g., site:mediacenter.abena.com 103014)
    site_domains = bank.get("site_search_domains", [])
    if not results and norm_artnr and site_domains:
        for domain in site_domains[:2]:
            site_query = f"site:{domain} {norm_artnr}"
            try:
                ddg_url = f"https://html.duckduckgo.com/html/?q={quote_plus(site_query)}"
                logger.info(f"Image search: site-specific query='{site_query}'")
                response = await client.get(ddg_url, headers=HEADERS, follow_redirects=True, timeout=15)
                if response.status_code != 200:
                    continue

                soup = BeautifulSoup(response.text, "lxml")
                for link in soup.select("a.result__a")[:3]:
                    href = link.get("href", "")
                    if not href or "duckduckgo" in href:
                        continue
                    try:
                        page_resp = await client.get(href, headers=HEADERS, follow_redirects=True, timeout=10)
                        if page_resp.status_code != 200:
                            continue
                        page_text = BeautifulSoup(page_resp.text, "lxml").get_text(" ", strip=True)[:2000]
                        images = _extract_images_from_page(page_resp.text, str(page_resp.url), manufacturer_artnr)
                        for img_url, alt_text in images:
                            results.append((img_url, page_text, str(page_resp.url)))
                        if results:
                            break
                    except Exception:
                        continue
                if results:
                    break
            except Exception as e:
                logger.debug(f"Site-specific search error ({domain}): {e}")
                continue

    return results


async def _search_manufacturer_site(
    manufacturer_name: str,
    manufacturer_artnr: str,
    product_name: str,
    client: httpx.AsyncClient,
) -> list[tuple[str, str, str]]:
    """Search a manufacturer's website directly for product images.

    Tries to construct the manufacturer's website URL and search for the product.
    Returns list of (image_url, page_text_snippet, source_url) tuples.
    """
    results = []
    if not manufacturer_name:
        return results

    # Clean manufacturer name to guess domain
    clean = manufacturer_name.lower().strip()
    for suffix in [" as", " ab", " gmbh", " inc", " ltd", " ag", " sa", " norge", " norway"]:
        if clean.endswith(suffix):
            clean = clean[:-len(suffix)].strip()

    # Try common domain patterns
    domains = []
    if clean and len(clean) >= 2:
        base = re.sub(r'[^a-z0-9]', '', clean)
        if base:
            domains.extend([
                f"www.{base}.com",
                f"www.{base}.no",
                f"www.{base}.eu",
            ])

    norm_artnr = _normalize_artnr(manufacturer_artnr)
    queries = []
    if norm_artnr:
        queries.append(norm_artnr)
    if product_name and norm_artnr:
        words = [w for w in product_name.split() if len(w) >= 3][:3]
        if words:
            queries.append(f"{norm_artnr} {' '.join(words)}")

    for domain in domains[:2]:
        for query in queries[:2]:
            search_url = f"https://{domain}/search?q={quote_plus(query)}"
            try:
                logger.info(f"Image search: manufacturer site={domain} query='{query}'")
                response = await client.get(search_url, headers=HEADERS, follow_redirects=True, timeout=10)
                if response.status_code != 200:
                    continue

                page_text = BeautifulSoup(response.text, "lxml").get_text(" ", strip=True)[:2000]
                images = _extract_images_from_page(response.text, str(response.url), manufacturer_artnr)
                for img_url, alt_text in images:
                    results.append((img_url, page_text, str(response.url)))

                if results:
                    return results  # Found on first working domain — stop

            except Exception as e:
                logger.debug(f"Manufacturer site search error ({domain}): {e}")
                continue

    return results


async def _search_web_broad(
    manufacturer_name: str,
    manufacturer_artnr: str,
    product_description: str,
    specification: str,
    our_artnr: str,
    client: httpx.AsyncClient,
) -> list[tuple[str, str, str, str]]:
    """Perform broad web search for product images.

    Uses multiple search query strategies to find product images from
    any source. Results are verified separately.

    Returns list of (image_url, page_text, source_url, source_type) tuples.
    """
    results = []
    norm_mfr_artnr = _normalize_artnr(manufacturer_artnr)
    mfr_clean = ""
    if manufacturer_name:
        mfr_clean = manufacturer_name.strip()
        for suffix in [" AS", " Ab", " GmbH", " Inc", " Ltd", " AG", " SA", " Norge", " Norway"]:
            if mfr_clean.endswith(suffix):
                mfr_clean = mfr_clean[:-len(suffix)].strip()

    # Build search queries — most specific first, many strategies
    search_queries = []
    if mfr_clean and norm_mfr_artnr:
        search_queries.append(f"{mfr_clean} {norm_mfr_artnr} product image")
        search_queries.append(f"{mfr_clean} {norm_mfr_artnr}")
    if product_description and norm_mfr_artnr:
        # Use first few words of description + art.nr
        desc_words = product_description.split()[:4]
        search_queries.append(f"{' '.join(desc_words)} {norm_mfr_artnr}")
    if mfr_clean and product_description:
        desc_short = " ".join(product_description.split()[:5])
        search_queries.append(f"{mfr_clean} {desc_short}")
    # Additional: specification-based queries
    if norm_mfr_artnr and specification:
        spec_short = specification[:40].strip()
        search_queries.append(f"{norm_mfr_artnr} {spec_short}")
    # Additional: our article number as last resort
    if our_artnr and mfr_clean:
        norm_our = _normalize_artnr(our_artnr)
        if norm_our:
            search_queries.append(f"{mfr_clean} {norm_our} product")

    # Try DuckDuckGo HTML search (no API key needed)
    for query in search_queries[:4]:
        try:
            ddg_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
            logger.info(f"Image search: web query='{query}'")
            response = await client.get(ddg_url, headers=HEADERS, follow_redirects=True, timeout=15)
            if response.status_code != 200:
                continue

            soup = BeautifulSoup(response.text, "lxml")

            # Extract result links
            for link in soup.select("a.result__a"):
                href = link.get("href", "")
                if not href or "duckduckgo" in href:
                    continue

                # Follow the result link to find images
                try:
                    page_response = await client.get(
                        href, headers=HEADERS, follow_redirects=True, timeout=10
                    )
                    if page_response.status_code != 200:
                        continue

                    page_text = BeautifulSoup(page_response.text, "lxml").get_text(" ", strip=True)[:2000]
                    images = _extract_images_from_page(
                        page_response.text, str(page_response.url), manufacturer_artnr
                    )

                    domain = urlparse(str(page_response.url)).netloc
                    source_type = "web_search"
                    # Upgrade source type if it's a known trustworthy domain
                    if manufacturer_name and any(
                        frag in domain.lower() for frag in mfr_clean.lower().split()
                        if len(frag) >= 3
                    ):
                        source_type = "manufacturer_website"

                    for img_url, alt_text in images[:3]:
                        results.append((img_url, page_text, str(page_response.url), source_type))

                    if len(results) >= 6:
                        return results

                except Exception as e:
                    logger.debug(f"Error following search result {href}: {e}")
                    continue

        except Exception as e:
            logger.debug(f"Web search error for query '{query}': {e}")
            continue

    return results


def _compute_improvement_score(
    current_status: str,
    candidate: ImageCandidate,
) -> float:
    """Compute how much better a candidate image is vs current situation.

    Returns 0-1 score.
    """
    # Base improvement by current status
    base_scores = {
        "missing": 0.95,       # Missing → any image is a huge improvement
        "low_quality": 0.70,   # Low quality → better image is significant
        "poor_background": 0.50,  # Background issue → moderate improvement
        "review": 0.30,        # Needs review → smaller improvement
    }
    base = base_scores.get(current_status, 0.2)

    # Boost for high-confidence sources
    if candidate.source_type == "manufacturer_mediabank":
        base = min(base + 0.10, 1.0)
    elif candidate.source_type == "manufacturer_website":
        base = min(base + 0.05, 1.0)

    candidate.improvement_score = base
    return base


def _confidence_label(confidence: float) -> str:
    """Return a human-readable confidence label."""
    if confidence >= 0.7:
        return "Høy tillit"
    elif confidence >= 0.45:
        return "Middels tillit"
    elif confidence >= 0.25:
        return "Lav tillit"
    else:
        return "Krever manuell vurdering"


async def search_product_images(
    article_number: str,
    manufacturer_name: str = "",
    manufacturer_artnr: str = "",
    product_description: str = "",
    specification: str = "",
    current_image_status: str = "missing",
    gid: str = "",
) -> list[ImageCandidate]:
    """Search broadly for product images across multiple sources.

    This is the main entry point for the image search module.

    Args:
        article_number: Our internal article number
        manufacturer_name: Producer/supplier name
        manufacturer_artnr: Producer's article number (key identifier)
        product_description: Product description / name
        specification: Product specification (dimensions, materials, etc.)
        current_image_status: Current image status (missing, low_quality, etc.)
        gid: Internal GID identifier

    Returns:
        List of ImageCandidate objects sorted by confidence (highest first).
        Only candidates with identity_score >= 0.25 are included.
    """
    all_candidates: list[ImageCandidate] = []

    logger.info(
        f"Image search started: art={article_number}, mfr={manufacturer_name}, "
        f"mfr_art={manufacturer_artnr}, desc='{(product_description or '')[:50]}...', "
        f"status={current_image_status}"
    )

    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        # Stage 1: Known media banks for this manufacturer
        banks = _find_media_banks(manufacturer_name)
        for bank in banks:
            try:
                raw_results = await _search_media_bank(
                    bank, manufacturer_artnr, product_description, client,
                    specification=specification,
                )
                for img_url, page_text, source_url in raw_results:
                    candidate = ImageCandidate(
                        image_url=img_url,
                        source_url=source_url,
                        source_domain=urlparse(source_url).netloc,
                        source_type=bank.get("source_type", "manufacturer_mediabank"),
                        source_name=bank["name"],
                    )
                    candidate = _verify_candidate(
                        candidate,
                        manufacturer_artnr=manufacturer_artnr,
                        our_artnr=article_number,
                        manufacturer_name=manufacturer_name,
                        product_description=product_description,
                        specification=specification,
                        page_text=page_text,
                    )
                    _compute_improvement_score(current_image_status, candidate)
                    all_candidates.append(candidate)
                    logger.debug(
                        f"  Media bank candidate: {img_url[:80]} "
                        f"identity={candidate.identity_score:.2f} conf={candidate.confidence:.2f}"
                    )
            except Exception as e:
                logger.debug(f"Media bank search error ({bank['name']}): {e}")

        # Stage 2: Manufacturer website direct search (if no media bank hits)
        if not any(c.confidence >= 0.4 for c in all_candidates):
            try:
                raw_results = await _search_manufacturer_site(
                    manufacturer_name, manufacturer_artnr, product_description, client
                )
                for img_url, page_text, source_url in raw_results:
                    candidate = ImageCandidate(
                        image_url=img_url,
                        source_url=source_url,
                        source_domain=urlparse(source_url).netloc,
                        source_type="manufacturer_website",
                        source_name=f"{manufacturer_name} nettside",
                    )
                    candidate = _verify_candidate(
                        candidate,
                        manufacturer_artnr=manufacturer_artnr,
                        our_artnr=article_number,
                        manufacturer_name=manufacturer_name,
                        product_description=product_description,
                        specification=specification,
                        page_text=page_text,
                    )
                    _compute_improvement_score(current_image_status, candidate)
                    all_candidates.append(candidate)
            except Exception as e:
                logger.debug(f"Manufacturer site search error: {e}")

        # Stage 3: Broad web search (if still no good candidates)
        if not any(c.confidence >= 0.35 for c in all_candidates):
            try:
                raw_results = await _search_web_broad(
                    manufacturer_name, manufacturer_artnr,
                    product_description, specification, article_number, client
                )
                for img_url, page_text, source_url, source_type in raw_results:
                    candidate = ImageCandidate(
                        image_url=img_url,
                        source_url=source_url,
                        source_domain=urlparse(source_url).netloc,
                        source_type=source_type,
                        source_name=urlparse(source_url).netloc,
                    )
                    candidate = _verify_candidate(
                        candidate,
                        manufacturer_artnr=manufacturer_artnr,
                        our_artnr=article_number,
                        manufacturer_name=manufacturer_name,
                        product_description=product_description,
                        specification=specification,
                        page_text=page_text,
                    )
                    _compute_improvement_score(current_image_status, candidate)
                    all_candidates.append(candidate)
            except Exception as e:
                logger.debug(f"Broad web search error: {e}")

    # Filter: only include candidates with minimum identity score
    MIN_IDENTITY_SCORE = 0.25
    valid_candidates = [c for c in all_candidates if c.identity_score >= MIN_IDENTITY_SCORE]

    # Deduplicate by image URL (keep highest confidence)
    seen_urls: dict[str, ImageCandidate] = {}
    for c in valid_candidates:
        norm_url = c.image_url.lower().rstrip("/")
        if norm_url not in seen_urls or c.confidence > seen_urls[norm_url].confidence:
            seen_urls[norm_url] = c
    valid_candidates = list(seen_urls.values())

    # Sort by confidence descending
    valid_candidates.sort(key=lambda c: -c.confidence)

    # Log results
    if valid_candidates:
        logger.info(
            f"Image search for {article_number}: {len(valid_candidates)} verified candidates found "
            f"(best confidence: {valid_candidates[0].confidence:.2f}, "
            f"source: {valid_candidates[0].source_name})"
        )
        for i, c in enumerate(valid_candidates[:3]):
            logger.info(
                f"  #{i+1}: {c.image_url[:80]}... "
                f"identity={c.identity_score:.2f} conf={c.confidence:.2f} "
                f"source={c.source_type} signals={c.verification_details}"
            )
    else:
        logger.info(
            f"Image search for {article_number}: no verified candidates found "
            f"(total raw candidates: {len(all_candidates)})"
        )

    return valid_candidates[:5]  # Return top 5
