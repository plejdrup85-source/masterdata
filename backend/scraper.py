"""Scraper module for fetching product data from onemed.no.

Product pages on onemed.no are server-rendered and contain JSON-LD structured data.
However, URLs use internal IDs (e.g. /products/i0016351/slug) NOT article numbers.
The search page is a JavaScript SPA and cannot be scraped with plain HTTP.

Strategy:
1. Build a URL index from the product sitemap (cached)
2. Visit product pages and match article numbers via JSON-LD SKU
3. Verify product existence via CDN image check (fast fallback)
"""

import asyncio
import json
import logging
import os
import re
import ssl
import time
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree

import httpx
from bs4 import BeautifulSoup

from backend.models import ProductData

logger = logging.getLogger(__name__)

# Cache directory - use /tmp on deployed environments
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "/tmp/masterdata_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://www.onemed.no"
PRODUCT_URL_PREFIX = f"{BASE_URL}/nb-no/products"
IMAGE_BASE_URL = "https://res.onemed.com/NO/ARWebBig"
SITEMAP_URL = f"{BASE_URL}/sitemap_b2b_onemed_no_product_0.xml"

# Reasonable timeout and headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "nb-NO,nb;q=0.9,no;q=0.8,en;q=0.5",
}

MAX_RETRIES = 3
RETRY_DELAY_BASE = 2  # seconds

# Sitemap index: maps article_number -> product page URL
# Built by downloading product pages and extracting SKU from JSON-LD
_sitemap_urls: list[str] = []  # All product URLs from sitemap
_sku_to_url: dict[str, str] = {}  # article_number -> product URL (built incrementally)
_sitemap_loaded = False
_sitemap_lock = asyncio.Lock()

# Sitemap index cache file
SITEMAP_INDEX_PATH = CACHE_DIR / "_sitemap_sku_index.json"
SITEMAP_URLS_PATH = CACHE_DIR / "_sitemap_urls.json"
SITEMAP_MAX_AGE = 24 * 60 * 60  # 24 hours


def _get_cache_path(article_number: str) -> Path:
    """Get the cache file path for an article number."""
    safe_name = re.sub(r'[^\w\-]', '_', article_number)
    return CACHE_DIR / f"{safe_name}.json"


def _load_from_cache(article_number: str) -> Optional[ProductData]:
    """Load cached product data if available. Only returns positive (found) results."""
    cache_path = _get_cache_path(article_number)
    if cache_path.exists():
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            product = ProductData(**data)
            # Only use cache for products that were actually found
            if product.found_on_onemed:
                return product
            else:
                # Negative results should not be cached - delete stale cache
                cache_path.unlink(missing_ok=True)
                logger.info(f"Removed stale negative cache for {article_number}")
                return None
        except Exception:
            logger.warning(f"Failed to load cache for {article_number}")
    return None


def _save_to_cache(product: ProductData) -> None:
    """Save product data to cache. Only caches positive (found) results."""
    # Never cache negative results - the product might be added later
    if not product.found_on_onemed:
        return

    cache_path = _get_cache_path(product.article_number)
    try:
        cache_path.write_text(
            product.model_dump_json(indent=2),
            encoding="utf-8"
        )
    except Exception:
        logger.warning(f"Failed to save cache for {product.article_number}")


def _load_sitemap_index() -> tuple[list[str], dict[str, str]]:
    """Load cached sitemap URL list and SKU index from disk."""
    urls = []
    sku_index = {}

    try:
        if SITEMAP_URLS_PATH.exists():
            stat = SITEMAP_URLS_PATH.stat()
            if time.time() - stat.st_mtime < SITEMAP_MAX_AGE:
                urls = json.loads(SITEMAP_URLS_PATH.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Failed to load cached sitemap URLs")

    try:
        if SITEMAP_INDEX_PATH.exists():
            sku_index = json.loads(SITEMAP_INDEX_PATH.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Failed to load cached SKU index")

    return urls, sku_index


def _save_sitemap_index(urls: list[str], sku_index: dict[str, str]) -> None:
    """Persist sitemap URL list and SKU index to disk."""
    try:
        SITEMAP_URLS_PATH.write_text(json.dumps(urls), encoding="utf-8")
    except Exception:
        logger.warning("Failed to save sitemap URLs")
    try:
        SITEMAP_INDEX_PATH.write_text(json.dumps(sku_index), encoding="utf-8")
    except Exception:
        logger.warning("Failed to save SKU index")


def _extract_json_ld(soup: BeautifulSoup) -> list[dict]:
    """Extract all JSON-LD structured data from a page."""
    results = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, list):
                results.extend(data)
            else:
                results.append(data)
        except (json.JSONDecodeError, TypeError):
            continue
    return results


def _extract_product_from_json_ld(json_ld_list: list[dict]) -> dict:
    """Extract product info from JSON-LD data."""
    product_info = {}
    for item in json_ld_list:
        if item.get("@type") == "Product":
            product_info["name"] = item.get("name")
            product_info["description"] = item.get("description")
            product_info["sku"] = item.get("sku")
            product_info["image"] = item.get("image")
            product_info["url"] = item.get("url")
            if "brand" in item:
                brand = item["brand"]
                if isinstance(brand, dict):
                    product_info["brand"] = brand.get("name")
                else:
                    product_info["brand"] = str(brand)
            if "offers" in item:
                offers = item["offers"]
                if isinstance(offers, dict):
                    product_info["price"] = offers.get("price")
        elif item.get("@type") == "BreadcrumbList":
            items = item.get("itemListElement", [])
            product_info["breadcrumbs"] = [
                elem.get("name", "") for elem in sorted(
                    items, key=lambda x: x.get("position", 0)
                )
            ]
    return product_info


def _extract_sku_from_html(html: str) -> Optional[str]:
    """Quick extraction of SKU from page HTML without full parsing."""
    # Look in JSON-LD for "sku": "VALUE"
    match = re.search(r'"sku"\s*:\s*"([^"]+)"', html)
    if match:
        return match.group(1)
    return None


def _parse_product_page(html: str, article_number: str) -> ProductData:
    """Parse a product page HTML into ProductData."""
    soup = BeautifulSoup(html, "lxml")

    # Extract JSON-LD data
    json_ld = _extract_json_ld(soup)
    ld_info = _extract_product_from_json_ld(json_ld)

    product = ProductData(
        article_number=article_number,
        found_on_onemed=True,
    )

    # Product name
    product.product_name = ld_info.get("name")
    if not product.product_name:
        h1 = soup.find("h1")
        if h1:
            product.product_name = h1.get_text(strip=True)

    # Description
    product.description = ld_info.get("description")
    if not product.description:
        desc_el = soup.find("div", class_=re.compile(r"description|product-desc", re.I))
        if desc_el:
            product.description = desc_el.get_text(strip=True)

    # Image
    product.image_url = ld_info.get("image")
    if not product.image_url:
        product.image_url = f"{IMAGE_BASE_URL}/{article_number}.jpg"

    # Category breadcrumbs
    breadcrumbs = ld_info.get("breadcrumbs", [])
    if breadcrumbs:
        product.category_breadcrumb = breadcrumbs
        product.category = breadcrumbs[-1] if breadcrumbs else None

    # Manufacturer / brand
    product.manufacturer = ld_info.get("brand")

    # Product URL
    product.product_url = ld_info.get("url")

    # Try to extract specification from multiple sources
    specs = {}

    # Source 1: HTML tables
    spec_tables = soup.find_all("table")
    for table in spec_tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True)
                val = cells[1].get_text(strip=True)
                if key and val and len(key) < 100:
                    specs[key] = val

    # Source 2: Definition lists (dl/dt/dd)
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = dt.get_text(strip=True)
            val = dd.get_text(strip=True)
            if key and val:
                specs[key] = val

    # Source 3: Key-value divs with common class patterns
    for el in soup.find_all(["div", "section", "ul"], class_=re.compile(
        r"spec|detail|attribute|property|feature|technical|egenskap", re.I
    )):
        # Try list items
        items = el.find_all("li")
        for item in items:
            text = item.get_text(strip=True)
            if ":" in text:
                parts = text.split(":", 1)
                if len(parts) == 2 and parts[0].strip() and parts[1].strip():
                    specs[parts[0].strip()] = parts[1].strip()
        # Try nested key-value spans/divs
        labels = el.find_all(class_=re.compile(r"label|key|name", re.I))
        values = el.find_all(class_=re.compile(r"value|data|content", re.I))
        for label, value in zip(labels, values):
            k = label.get_text(strip=True)
            v = value.get_text(strip=True)
            if k and v:
                specs[k] = v

    if specs:
        product.technical_details = specs
        product.specification = "; ".join(f"{k}: {v}" for k, v in specs.items())

    # Source 4: If no structured specs found, check for spec text in description-like blocks
    if not specs and not product.specification:
        spec_block = soup.find(["div", "section"], class_=re.compile(
            r"spec|technical|egenskap", re.I
        ))
        if spec_block:
            spec_text = spec_block.get_text(strip=True)
            if spec_text and len(spec_text) > 10:
                product.specification = spec_text

    # Extract packaging info from page text
    page_text = soup.get_text()

    # Look for packaging patterns
    pkg_patterns = [
        r"(\d+)\s*(?:stk|st)\s*(?:per|pr|/)\s*(?:forpakning|pakning|frp|pk)",
        r"(?:forpakning|pakning|frp|pk)[\s:]+(\d+)\s*(?:stk|st)?",
        r"Antall\s*(?:per|pr|i)\s*(?:forpakning|pakning)[\s:]*(\d+)",
    ]
    for pattern in pkg_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            product.packaging_info = match.group(0)
            product.packaging_unit = match.group(1) + " stk"
            break

    # Look for "Antall i transportforpakning"
    transport_match = re.search(
        r"(?:transport(?:forpakning|pakning))[\s:]*(\d+)",
        page_text, re.IGNORECASE
    )
    if transport_match:
        product.transport_packaging = transport_match.group(1) + " stk"

    # Check for manufacturer article number patterns
    mfr_patterns = [
        r"(?:Produsentens?\s*(?:art\.?|varenr|artikkel)(?:nummer|nr)?|Lev\.?\s*art\.?\s*nr)[\s.:]*([A-Za-z0-9\-/]+)",
        r"(?:Manufacturer|MFR|Supplier)\s*(?:art|item|part)\s*(?:no|nr|number)?[\s.:]*([A-Za-z0-9\-/]+)",
    ]
    for pattern in mfr_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            product.manufacturer_article_number = match.group(1).strip()
            break

    # Image accessibility check (basic - just check if URL exists)
    product.image_quality_ok = product.image_url is not None

    return product


async def _fetch_with_retry(
    client: httpx.AsyncClient,
    url: str,
    max_retries: int = MAX_RETRIES
) -> Optional[httpx.Response]:
    """Fetch a URL with retry logic."""
    for attempt in range(max_retries):
        try:
            response = await client.get(url, headers=HEADERS, follow_redirects=True, timeout=30)
            if response.status_code == 200:
                return response
            elif response.status_code == 404:
                logger.info(f"404 for {url}")
                return None
            elif response.status_code == 429:
                logger.warning(f"Rate limited by {url}, backing off")
                await asyncio.sleep(RETRY_DELAY_BASE ** (attempt + 2))
            elif response.status_code >= 500:
                logger.warning(f"Server error {response.status_code} for {url}, retry {attempt + 1}")
            else:
                logger.warning(f"HTTP {response.status_code} for {url}")
                return response
        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, ssl.SSLError) as e:
            logger.warning(f"Network error for {url}: {e}, retry {attempt + 1}")
        if attempt < max_retries - 1:
            await asyncio.sleep(RETRY_DELAY_BASE ** (attempt + 1))
    return None


async def _load_sitemap(client: httpx.AsyncClient) -> list[str]:
    """Download and parse the product sitemap XML to get all product URLs."""
    global _sitemap_urls, _sitemap_loaded, _sku_to_url

    async with _sitemap_lock:
        if _sitemap_loaded and _sitemap_urls:
            return _sitemap_urls

        # Try loading from disk cache first
        cached_urls, cached_index = _load_sitemap_index()
        if cached_urls:
            _sitemap_urls = cached_urls
            _sku_to_url.update(cached_index)
            _sitemap_loaded = True
            logger.info(f"Loaded sitemap from cache: {len(cached_urls)} URLs, {len(cached_index)} SKU mappings")
            return _sitemap_urls

        # Download fresh sitemap
        logger.info(f"Downloading product sitemap from {SITEMAP_URL}")
        response = await _fetch_with_retry(client, SITEMAP_URL, max_retries=2)
        if not response or response.status_code != 200:
            logger.warning("Failed to download product sitemap")
            _sitemap_loaded = True  # Don't retry on every request
            return []

        try:
            root = ElementTree.fromstring(response.text)
            # Handle XML namespace
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            urls = []
            for url_elem in root.findall(".//sm:url/sm:loc", ns):
                if url_elem.text:
                    urls.append(url_elem.text.strip())
            # Fallback without namespace
            if not urls:
                for url_elem in root.iter():
                    if url_elem.tag.endswith("loc") and url_elem.text:
                        urls.append(url_elem.text.strip())

            _sitemap_urls = [u for u in urls if "/products/" in u]
            _sitemap_loaded = True
            logger.info(f"Parsed sitemap: {len(_sitemap_urls)} product URLs")

            # Save to disk cache
            _save_sitemap_index(_sitemap_urls, _sku_to_url)
            return _sitemap_urls

        except ElementTree.ParseError as e:
            logger.error(f"Failed to parse sitemap XML: {e}")
            _sitemap_loaded = True
            return []


async def _check_cdn_image_exists(
    client: httpx.AsyncClient,
    article_number: str,
) -> bool:
    """Check if the product image exists on the CDN (fast existence check)."""
    url = f"{IMAGE_BASE_URL}/{article_number}.jpg"
    try:
        response = await client.head(url, headers=HEADERS, follow_redirects=True, timeout=10)
        if response.status_code == 200:
            content_type = response.headers.get("content-type", "")
            content_length = int(response.headers.get("content-length", "0"))
            # Must be an actual image, not an error page
            if "image" in content_type and content_length > 500:
                return True
    except Exception as e:
        logger.debug(f"CDN check failed for {article_number}: {e}")
    return False


async def _find_product_url_via_sitemap(
    client: httpx.AsyncClient,
    article_number: str,
) -> Optional[str]:
    """Find the product page URL by scanning sitemap pages for matching SKU.

    Downloads product pages from the sitemap in batches and checks if
    the JSON-LD SKU matches the target article number.
    Results are cached in the _sku_to_url mapping.
    """
    # Check if we already have a mapping for this article number
    if article_number in _sku_to_url:
        return _sku_to_url[article_number]

    # Load sitemap if not already loaded
    sitemap_urls = await _load_sitemap(client)
    if not sitemap_urls:
        return None

    # Check again after sitemap load (might have loaded cached index)
    if article_number in _sku_to_url:
        return _sku_to_url[article_number]

    # Scan product pages in batches to find the matching SKU
    # Process in batches of 15 concurrent requests
    BATCH_SIZE = 15
    MAX_PAGES_TO_CHECK = 100  # Safety limit per lookup

    # Filter out URLs we've already indexed
    indexed_urls = set(_sku_to_url.values())
    unchecked_urls = [u for u in sitemap_urls if u not in indexed_urls]

    pages_checked = 0
    for batch_start in range(0, len(unchecked_urls), BATCH_SIZE):
        if pages_checked >= MAX_PAGES_TO_CHECK:
            logger.info(f"Reached scan limit ({MAX_PAGES_TO_CHECK}) without finding {article_number}")
            break

        batch = unchecked_urls[batch_start:batch_start + BATCH_SIZE]

        async def check_page(url: str) -> Optional[tuple[str, str]]:
            """Fetch a page and extract its SKU. Returns (sku, url) or None."""
            try:
                resp = await client.get(url, headers=HEADERS, follow_redirects=True, timeout=15)
                if resp.status_code == 200:
                    sku = _extract_sku_from_html(resp.text)
                    if sku:
                        return (sku, url)
            except Exception:
                pass
            return None

        tasks = [check_page(url) for url in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, tuple):
                sku, url = result
                _sku_to_url[sku] = url
                pages_checked += 1
                if sku == article_number:
                    # Found it! Save the updated index
                    _save_sitemap_index(_sitemap_urls, _sku_to_url)
                    logger.info(f"Found {article_number} via sitemap scan at {url}")
                    return url
            elif not isinstance(result, Exception):
                pages_checked += 1

        # Small delay between batches to be polite
        await asyncio.sleep(0.5)

    # Save whatever we've indexed so far
    _save_sitemap_index(_sitemap_urls, _sku_to_url)
    return None


async def scrape_product(
    article_number: str,
    use_cache: bool = True,
    playwright_browser=None,
) -> ProductData:
    """Scrape product data from onemed.no for a given article number.

    Strategy order:
    1. Check cache
    2. Check SKU→URL index (from previous sitemap scans)
    3. Verify product exists via CDN image (fast, <1s)
    4. Try to find full product page via sitemap scan (slower, enriches data)

    The CDN at res.onemed.com uses article numbers directly for images and PDFs,
    so product existence can be confirmed without finding the product page.
    The product page is only needed for metadata (name, description, specs).
    """

    # Check cache first
    if use_cache:
        cached = _load_from_cache(article_number)
        if cached:
            logger.info(f"Cache hit for {article_number}")
            return cached

    clean_num = article_number.strip()

    async with httpx.AsyncClient() as client:
        # Strategy 1: Check if we already know the URL from sitemap index
        if clean_num in _sku_to_url:
            known_url = _sku_to_url[clean_num]
            logger.info(f"SKU index hit for {clean_num}: {known_url}")
            response = await _fetch_with_retry(client, known_url)
            if response and response.status_code == 200:
                product = _parse_product_page(response.text, clean_num)
                product.product_url = str(response.url)
                _save_to_cache(product)
                return product

        # Strategy 2: Verify product exists via CDN image (fast)
        cdn_exists = await _check_cdn_image_exists(client, clean_num)

        # Strategy 3: Try to find product page via sitemap for full metadata
        # Only load sitemap if not already loaded (first call triggers download)
        product_url = await _find_product_url_via_sitemap(client, clean_num)
        if product_url:
            response = await _fetch_with_retry(client, product_url)
            if response and response.status_code == 200:
                product = _parse_product_page(response.text, clean_num)
                product.product_url = str(response.url)
                _save_to_cache(product)
                return product

        # Strategy 4: If CDN image exists, product is in the OneMed system
        # Return partial data (image analysis and PDF check will fill in the rest)
        if cdn_exists:
            logger.info(f"{clean_num}: CDN image confirmed, product page not found in sitemap")
            product = ProductData(
                article_number=clean_num,
                found_on_onemed=True,
                image_url=f"{IMAGE_BASE_URL}/{clean_num}.jpg",
                image_quality_ok=True,
                error=None,
            )
            return product

        # Not found anywhere - do NOT cache this result
        product = ProductData(
            article_number=clean_num,
            found_on_onemed=False,
            error="Produkt ikke funnet på onemed.no"
        )
        return product
