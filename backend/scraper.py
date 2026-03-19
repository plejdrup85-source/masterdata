"""Scraper module for fetching product data from onemed.no."""

import json
import logging
import re
from pathlib import Path
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from backend.models import ProductData

logger = logging.getLogger(__name__)

CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

BASE_URL = "https://www.onemed.no"
SEARCH_URL = f"{BASE_URL}/nb-no/search"
PRODUCT_URL_PREFIX = f"{BASE_URL}/nb-no/products"
IMAGE_BASE_URL = "https://res.onemed.com/NO/ARWebBig"

# Reasonable timeout and headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "nb-NO,nb;q=0.9,no;q=0.8,en;q=0.5",
}

MAX_RETRIES = 3
RETRY_DELAY_BASE = 2  # seconds


def _get_cache_path(article_number: str) -> Path:
    """Get the cache file path for an article number."""
    safe_name = re.sub(r'[^\w\-]', '_', article_number)
    return CACHE_DIR / f"{safe_name}.json"


def _load_from_cache(article_number: str) -> Optional[ProductData]:
    """Load cached product data if available."""
    cache_path = _get_cache_path(article_number)
    if cache_path.exists():
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            return ProductData(**data)
        except Exception:
            logger.warning(f"Failed to load cache for {article_number}")
    return None


def _save_to_cache(product: ProductData) -> None:
    """Save product data to cache."""
    cache_path = _get_cache_path(product.article_number)
    try:
        cache_path.write_text(
            product.model_dump_json(indent=2),
            encoding="utf-8"
        )
    except Exception:
        logger.warning(f"Failed to save cache for {product.article_number}")


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


def _parse_product_page(html: str, article_number: str) -> ProductData:
    """Parse a product page HTML into ProductData."""
    soup = BeautifulSoup(html, "html.parser")

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
        # Last breadcrumb is the most specific category
        product.category = breadcrumbs[-1] if breadcrumbs else None

    # Manufacturer / brand
    product.manufacturer = ld_info.get("brand")

    # Product URL
    product.product_url = ld_info.get("url")

    # Try to extract specification table
    spec_tables = soup.find_all("table")
    specs = {}
    for table in spec_tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True)
                val = cells[1].get_text(strip=True)
                if key and val:
                    specs[key] = val
    if specs:
        product.technical_details = specs
        product.specification = "; ".join(f"{k}: {v}" for k, v in specs.items())

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

    # Assess image quality (basic check - image dimensions/existence)
    product.image_quality_ok = product.image_url is not None

    return product


def _find_product_links_in_search(html: str, article_number: str) -> list[dict]:
    """Extract product links from search results page."""
    soup = BeautifulSoup(html, "html.parser")
    links = []

    # Look for product links in search results
    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href", "")
        if "/products/" in href:
            name = a_tag.get_text(strip=True)
            if not name:
                # Try to get name from child elements
                name_el = a_tag.find(class_=re.compile(r"name|title", re.I))
                if name_el:
                    name = name_el.get_text(strip=True)
            full_url = href if href.startswith("http") else BASE_URL + href
            links.append({"url": full_url, "name": name or "Unknown"})

    return links


async def _fetch_with_retry(
    client: httpx.AsyncClient,
    url: str,
    max_retries: int = MAX_RETRIES
) -> Optional[httpx.Response]:
    """Fetch a URL with retry logic."""
    import asyncio
    for attempt in range(max_retries):
        try:
            response = await client.get(url, headers=HEADERS, follow_redirects=True, timeout=30)
            if response.status_code == 200:
                return response
            elif response.status_code == 404:
                logger.info(f"404 for {url}")
                return None
            elif response.status_code >= 500:
                logger.warning(f"Server error {response.status_code} for {url}, retry {attempt + 1}")
            else:
                logger.warning(f"HTTP {response.status_code} for {url}")
                return response
        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
            logger.warning(f"Network error for {url}: {e}, retry {attempt + 1}")
        if attempt < max_retries - 1:
            await asyncio.sleep(RETRY_DELAY_BASE ** (attempt + 1))
    return None


async def scrape_product(
    article_number: str,
    use_cache: bool = True,
    playwright_browser=None,
) -> ProductData:
    """Scrape product data from onemed.no for a given article number.

    Strategy:
    1. Try direct product URL with article number prefix patterns
    2. If not found, try search
    3. If search gives results, follow the first matching link
    4. If Playwright browser is available, use it for dynamic content
    """
    # Check cache first
    if use_cache:
        cached = _load_from_cache(article_number)
        if cached:
            logger.info(f"Cache hit for {article_number}")
            return cached

    clean_num = article_number.strip()

    async with httpx.AsyncClient() as client:
        # Strategy 1: Try direct product URL patterns
        direct_urls = [
            f"{PRODUCT_URL_PREFIX}/i{clean_num}/",
            f"{PRODUCT_URL_PREFIX}/{clean_num}/",
            f"{BASE_URL}/products/i{clean_num}/",
            f"{BASE_URL}/products/{clean_num}/",
        ]

        for url in direct_urls:
            response = await _fetch_with_retry(client, url)
            if response and response.status_code == 200:
                html = response.text
                # Verify it's actually a product page
                if "application/ld+json" in html and '"@type":"Product"' in html.replace(" ", "").replace("'", '"'):
                    product = _parse_product_page(html, clean_num)
                    product.product_url = str(response.url)
                    _save_to_cache(product)
                    logger.info(f"Found {clean_num} via direct URL: {url}")
                    return product

        # Strategy 2: Try search
        search_url = f"{SEARCH_URL}?q={clean_num}"
        response = await _fetch_with_retry(client, search_url)

        if response and response.status_code == 200:
            html = response.text

            # Check if search page itself is a redirect to a product page
            if '"@type":"Product"' in html.replace(" ", "").replace("'", '"'):
                product = _parse_product_page(html, clean_num)
                product.product_url = str(response.url)
                _save_to_cache(product)
                return product

            # Look for product links in search results
            product_links = _find_product_links_in_search(html, clean_num)

            if len(product_links) > 1:
                logger.info(f"Multiple hits for {clean_num}: {len(product_links)} results")

            if product_links:
                # Try the first link (most relevant)
                first_link = product_links[0]["url"]
                prod_response = await _fetch_with_retry(client, first_link)
                if prod_response and prod_response.status_code == 200:
                    product = _parse_product_page(prod_response.text, clean_num)
                    product.product_url = str(prod_response.url)
                    product.multiple_hits = len(product_links) > 1
                    _save_to_cache(product)
                    return product

        # Strategy 3: Try with Playwright for dynamic content
        if playwright_browser:
            try:
                product = await _scrape_with_playwright(
                    playwright_browser, clean_num
                )
                if product and product.found_on_onemed:
                    _save_to_cache(product)
                    return product
            except Exception as e:
                logger.error(f"Playwright error for {clean_num}: {e}")

        # Not found
        product = ProductData(
            article_number=clean_num,
            found_on_onemed=False,
            error="Produkt ikke funnet på onemed.no"
        )
        _save_to_cache(product)
        return product


async def _scrape_with_playwright(browser, article_number: str) -> ProductData:
    """Use Playwright to scrape dynamically rendered content."""
    page = await browser.new_page()
    try:
        # Try search
        search_url = f"{SEARCH_URL}?q={article_number}"
        await page.goto(search_url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)  # Wait for dynamic content

        # Look for product links
        product_links = await page.query_selector_all('a[href*="/products/"]')

        if product_links:
            first_link = product_links[0]
            href = await first_link.get_attribute("href")
            if href:
                full_url = href if href.startswith("http") else BASE_URL + href
                await page.goto(full_url, wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(2000)

                html = await page.content()
                product = _parse_product_page(html, article_number)
                product.product_url = full_url
                product.multiple_hits = len(product_links) > 1
                return product

        return ProductData(
            article_number=article_number,
            found_on_onemed=False,
            error="Produkt ikke funnet (Playwright)"
        )
    finally:
        await page.close()


async def check_image_quality(image_url: str) -> dict:
    """Check if an image URL is accessible and assess basic quality."""
    result = {"accessible": False, "width": 0, "height": 0, "quality_ok": False}

    if not image_url:
        return result

    try:
        async with httpx.AsyncClient() as client:
            response = await client.head(image_url, headers=HEADERS, timeout=10, follow_redirects=True)
            if response.status_code == 200:
                result["accessible"] = True
                content_length = int(response.headers.get("content-length", 0))
                # Images > 10KB are likely decent quality
                result["quality_ok"] = content_length > 10_000
                result["size_bytes"] = content_length
            else:
                # Try GET as fallback
                response = await client.get(image_url, headers=HEADERS, timeout=10, follow_redirects=True)
                if response.status_code == 200:
                    result["accessible"] = True
                    result["size_bytes"] = len(response.content)
                    result["quality_ok"] = len(response.content) > 10_000
    except Exception as e:
        logger.warning(f"Image check failed for {image_url}: {e}")

    return result
