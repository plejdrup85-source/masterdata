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

from backend.identifiers import normalize_identifier
from backend.models import ProductData, VerificationStatus

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


def _get_structured_text(element) -> str:
    """Extract text from a BeautifulSoup element while preserving paragraph and list structure.

    Unlike .get_text(strip=True) which collapses everything into one line, this:
    - Preserves paragraph breaks (double newline between <p>, <div> blocks)
    - Converts <br> to single newlines
    - Preserves bullet list structure (<ul>/<ol> → newline-separated items with bullet markers)
    - Collapses excessive whitespace within lines (but NOT across paragraphs)
    - Strips leading/trailing whitespace

    Use for: descriptions, web text, specification blocks, accordion content.
    Do NOT use for: product names, category names, single-value fields (use .get_text(strip=True) for those).
    """
    if element is None:
        return ""

    # Replace <br> with newlines before text extraction
    for br in element.find_all("br"):
        br.replace_with("\n")

    parts = []

    # Process block-level children to maintain paragraph structure
    block_tags = {"p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "section", "article", "blockquote"}
    list_tags = {"ul", "ol"}

    children = list(element.children)
    has_block_children = any(
        getattr(child, "name", None) in (block_tags | list_tags)
        for child in children
    )

    if has_block_children:
        for child in children:
            tag_name = getattr(child, "name", None)
            if tag_name in list_tags:
                # Process list items
                for li in child.find_all("li", recursive=False):
                    li_text = li.get_text(strip=True)
                    if li_text:
                        parts.append(f"• {li_text}")
            elif tag_name in block_tags:
                block_text = child.get_text(strip=True)
                if block_text:
                    parts.append(block_text)
            elif tag_name is None:
                # NavigableString (raw text)
                text = str(child).strip()
                if text:
                    parts.append(text)
            else:
                # Inline elements like <span>, <a>, <strong>
                text = child.get_text(strip=True)
                if text:
                    parts.append(text)
    else:
        # No block children — get text but preserve explicit newlines
        raw = element.get_text()
        # Normalize spaces within lines but preserve newlines
        lines = raw.split("\n")
        for line in lines:
            cleaned = re.sub(r"[ \t]+", " ", line).strip()
            if cleaned:
                parts.append(cleaned)

    result = "\n".join(parts).strip()
    # Collapse more than 2 consecutive newlines
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result

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

# Cache version — increment when ProductData schema or extraction logic changes
# in a way that makes old cached data unsafe to reuse. Old cache entries with a
# different (or missing) version are discarded and re-scraped.
_CACHE_VERSION = 2  # v2: added verification_status, structured text extraction


def _get_cache_path(article_number: str) -> Path:
    """Get the cache file path for an article number."""
    safe_name = re.sub(r'[^\w\-]', '_', article_number)
    return CACHE_DIR / f"{safe_name}.json"


def _load_from_cache(article_number: str) -> Optional[ProductData]:
    """Load cached product data if available. Only returns positive (found) results.

    Cache entries are versioned: entries created by an older version of the
    extraction logic are discarded to prevent stale verification_status,
    flattened descriptions, or other outdated data from affecting current runs.
    """
    cache_path = _get_cache_path(article_number)
    if cache_path.exists():
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))

            # Check cache version — reject entries from older code versions
            cached_version = data.pop("_cache_version", None)
            if cached_version != _CACHE_VERSION:
                cache_path.unlink(missing_ok=True)
                logger.info(
                    f"Invalidated stale cache for {article_number} "
                    f"(cache version {cached_version} != current {_CACHE_VERSION})"
                )
                return None

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
    """Save product data to cache. Only caches positive (found) results.

    Includes a _cache_version marker so future code changes can invalidate
    entries that were created with older extraction or verification logic.
    """
    # Never cache negative results - the product might be added later
    if not product.found_on_onemed:
        return

    cache_path = _get_cache_path(product.article_number)
    try:
        data = json.loads(product.model_dump_json())
        data["_cache_version"] = _CACHE_VERSION
        cache_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
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
            raw_breadcrumbs = []
            for elem in sorted(items, key=lambda x: x.get("position", 0)):
                # Handle both top-level name and nested item.name patterns
                name = elem.get("name") or ""
                if not name:
                    # Common JSON-LD pattern: name inside "item" object
                    item_obj = elem.get("item")
                    if isinstance(item_obj, dict):
                        name = item_obj.get("name", "")
                    elif isinstance(item_obj, str):
                        # item is just a URL, try to extract name from URL slug
                        pass
                if name and name.strip():
                    raw_breadcrumbs.append(name.strip())
            if raw_breadcrumbs:
                product_info["breadcrumbs"] = raw_breadcrumbs
    return product_info


def _extract_sku_from_html(html: str) -> Optional[str]:
    """Quick extraction of SKU from page HTML without full parsing."""
    # Look in JSON-LD for "sku": "VALUE"
    match = re.search(r'"sku"\s*:\s*"([^"]+)"', html)
    if match:
        return match.group(1)
    return None


def _verify_sku_match(html: str, expected_article_number: str) -> tuple[VerificationStatus, str]:
    """Verify that a product page actually belongs to the expected article number.

    Checks JSON-LD SKU and page text for the article number to prevent
    cross-contamination (wrong product data attributed to wrong article).

    For medical products, false-positive verification is dangerous.
    If identity cannot be confirmed, we mark as UNVERIFIED (not as confirmed).

    Returns:
        (verification_status, evidence_description)
    """
    clean_expected = normalize_identifier(expected_article_number) or expected_article_number.strip()

    page_sku = _extract_sku_from_html(html)
    if page_sku:
        clean_page = normalize_identifier(page_sku) or page_sku.strip()

        # Exact match (strongest signal)
        if clean_page == clean_expected:
            return (
                VerificationStatus.EXACT_MATCH,
                f"Produktidentitet bekreftet: artikkelnummeret stemmer eksakt med produktsiden.",
            )

        # Normalized match (strip leading N, case-insensitive)
        norm_page = clean_page.lstrip("N").lower()
        norm_expected = clean_expected.lstrip("N").lower()
        if norm_page == norm_expected:
            return (
                VerificationStatus.NORMALIZED_MATCH,
                f"Produktidentitet bekreftet etter normalisering av artikkelnummer ('{page_sku}' ≈ '{expected_article_number}').",
            )

        # SKU present but DIFFERENT — this is a definite mismatch
        return (
            VerificationStatus.MISMATCH,
            f"Mulig feil produkt: artikkelnummeret på produktsiden ('{page_sku}') stemmer ikke med forventet ('{expected_article_number}'). Dataene kan tilhøre feil produkt.",
        )

    # No SKU in JSON-LD — weaker signals only
    if clean_expected in html:
        return (
            VerificationStatus.SKU_IN_PAGE,
            f"Artikkelnummeret ble funnet i sideteksten, men ikke i produktets strukturerte data. Svakere verifisering.",
        )

    # Cannot verify — do NOT assume match. For medical products, unverified = unverified.
    return (
        VerificationStatus.UNVERIFIED,
        f"Produktet kunne ikke verifiseres mot nettstedet. Vurder manuelt om dataene er korrekte.",
    )


def _parse_product_page(html: str, article_number: str) -> ProductData:
    """Parse a product page HTML into ProductData.

    Includes structured debug logging for every extraction step:
    selector matches, raw values, source attribution, and miss reasons.
    """
    tag = f"[parse:{article_number}]"
    soup = BeautifulSoup(html, "lxml")

    # Extract JSON-LD data
    json_ld = _extract_json_ld(soup)
    ld_info = _extract_product_from_json_ld(json_ld)
    logger.debug(f"{tag} JSON-LD keys found: {list(ld_info.keys())}")

    product = ProductData(
        article_number=article_number,
        found_on_onemed=True,
    )

    # ── Product name ──
    product.product_name = ld_info.get("name")
    if product.product_name:
        logger.debug(f"{tag} product_name: JSON-LD → {repr(product.product_name)}")
    else:
        h1 = soup.find("h1")
        if h1:
            product.product_name = h1.get_text(strip=True)
            logger.debug(f"{tag} product_name: <h1> fallback → {repr(product.product_name)}")
        else:
            logger.debug(f"{tag} product_name: MISSING — no JSON-LD name, no <h1>")

    # ── Description ──
    # P0 FIX: Always check the accordion section, even if JSON-LD has a description.
    # JSON-LD descriptions are often short summaries. The accordion contains the full
    # rich description visible to users. Prefer the longer/richer source.
    ld_desc = ld_info.get("description")
    accordion_desc = None

    # Check all known OneMed accordion IDs for description content
    for acc_id in ("accordionItem_descriptionAndDocuments",
                   "accordionItem_description",
                   "accordion-description"):
        desc_accordion = soup.find(id=acc_id)
        if desc_accordion:
            acc_text = _get_structured_text(desc_accordion)
            if acc_text and len(acc_text) > 10:
                accordion_desc = acc_text
                logger.debug(
                    f"{tag} description: #{acc_id} → "
                    f"{len(acc_text)} chars (structured)"
                )
                break
            else:
                logger.debug(
                    f"{tag} description: accordion #{acc_id} found but too short "
                    f"({len(acc_text) if acc_text else 0} chars)"
                )

    # Also check generic accordion containers with description-like content
    if not accordion_desc:
        for acc_el in soup.find_all(class_=re.compile(r"accordion.*desc|desc.*accordion", re.I)):
            acc_text = _get_structured_text(acc_el)
            if acc_text and len(acc_text) > 20:
                accordion_desc = acc_text
                logger.debug(f"{tag} description: accordion class match → {len(acc_text)} chars")
                break

    # Choose the richer source: prefer accordion over JSON-LD when it has more content
    if accordion_desc and ld_desc:
        if len(accordion_desc) > len(ld_desc) * 1.3:
            product.description = accordion_desc
            logger.debug(
                f"{tag} description: accordion preferred over JSON-LD "
                f"({len(accordion_desc)} vs {len(ld_desc)} chars)"
            )
        else:
            product.description = ld_desc
            logger.debug(f"{tag} description: JSON-LD → {len(ld_desc)} chars (accordion not richer)")
    elif accordion_desc:
        product.description = accordion_desc
    elif ld_desc:
        product.description = ld_desc
        logger.debug(f"{tag} description: JSON-LD → {len(ld_desc)} chars (no accordion found)")
    else:
        logger.debug(f"{tag} description: no JSON-LD or accordion found")

    if not product.description:
        # Fallback: generic description class
        desc_el = soup.find("div", class_=re.compile(r"description|product-desc", re.I))
        if desc_el:
            product.description = _get_structured_text(desc_el)
            logger.debug(
                f"{tag} description: div.description fallback → {len(product.description)} chars (structured)"
            )
        else:
            # Fallback: <meta name="description"> tag
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc and meta_desc.get("content", "").strip():
                meta_text = meta_desc["content"].strip()
                if len(meta_text) > 20:
                    product.description = meta_text
                    logger.debug(f"{tag} description: <meta description> fallback → {len(meta_text)} chars")
                else:
                    logger.debug(f"{tag} description: meta description too short ({len(meta_text)} chars)")
            else:
                # Fallback: any section/article with substantial text within main content
                main_content = soup.find("main") or soup.find("article") or soup.find(id=re.compile(r"product|content", re.I))
                if main_content:
                    paragraphs = main_content.find_all("p")
                    long_texts = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 30]
                    if long_texts:
                        product.description = "\n".join(long_texts[:3])  # Take up to 3 substantial paragraphs
                        logger.debug(f"{tag} description: <main> paragraphs fallback → {len(product.description)} chars")
                    else:
                        logger.debug(f"{tag} description: MISSING — no JSON-LD, no accordion, no div, no meta, no paragraphs")
                else:
                    logger.debug(f"{tag} description: MISSING — no JSON-LD, no accordion, no div.description, no meta")

    # ── Image ──
    product.image_url = ld_info.get("image")
    if product.image_url:
        logger.debug(f"{tag} image_url: JSON-LD → {product.image_url}")
    else:
        product.image_url = f"{IMAGE_BASE_URL}/{article_number}.jpg"
        logger.debug(f"{tag} image_url: CDN fallback → {product.image_url}")

    # ── Category breadcrumbs ──
    breadcrumbs = ld_info.get("breadcrumbs", [])
    bc_source = None
    if breadcrumbs:
        bc_source = "JSON-LD BreadcrumbList"
    if not breadcrumbs:
        # Fallback 1: element with class containing "breadcrumb"
        bc_nav = soup.find(class_=re.compile(r"breadcrumb", re.I))
        # Fallback 2: <nav> with aria-label containing "breadcrumb" or "brødsmule"
        if not bc_nav:
            bc_nav = soup.find("nav", attrs={"aria-label": re.compile(r"breadcrumb|brødsmule|sti", re.I)})
        # Fallback 3: <ol> or <ul> with breadcrumb-related attributes
        if not bc_nav:
            bc_nav = soup.find(["ol", "ul"], class_=re.compile(r"breadcrumb|crumb", re.I))
        if bc_nav:
            # Try <a> links first, then <li> items, then <span> items
            bc_links = bc_nav.find_all("a")
            breadcrumbs = [a.get_text(strip=True) for a in bc_links if a.get_text(strip=True)]
            if not breadcrumbs:
                bc_items = bc_nav.find_all("li")
                breadcrumbs = [li.get_text(strip=True) for li in bc_items if li.get_text(strip=True)]
            if not breadcrumbs:
                bc_spans = bc_nav.find_all("span")
                breadcrumbs = [s.get_text(strip=True) for s in bc_spans if s.get_text(strip=True)]
            if breadcrumbs:
                bc_source = f"HTML {bc_nav.name}.{bc_nav.get('class', [])} ({len(breadcrumbs)} items)"
            else:
                logger.debug(f"{tag} breadcrumb: nav element found but no links/items inside")
        else:
            logger.debug(f"{tag} breadcrumb: no JSON-LD BreadcrumbList, no HTML breadcrumb element")
    # Filter out empty/whitespace-only breadcrumb entries and generic separators
    if breadcrumbs:
        breadcrumbs = [b.strip() for b in breadcrumbs if b and b.strip() and b.strip() not in (">", "/", "»", "›")]
    if breadcrumbs:
        product.category_breadcrumb = breadcrumbs
        product.category = breadcrumbs[-1]
        logger.debug(f"{tag} category: {bc_source} → {breadcrumbs}")
    else:
        logger.debug(f"{tag} category: MISSING — no breadcrumb source found")

    # ── Manufacturer / brand ──
    product.manufacturer = ld_info.get("brand")
    if product.manufacturer:
        logger.debug(f"{tag} manufacturer: JSON-LD brand → {repr(product.manufacturer)}")
    else:
        # Fallback: look for manufacturer in spec tables or dedicated elements
        mfr_el = soup.find(class_=re.compile(r"brand|manufacturer|producer|supplier", re.I))
        if mfr_el:
            mfr_text = mfr_el.get_text(strip=True)
            if mfr_text and len(mfr_text) < 100:
                product.manufacturer = mfr_text
                logger.debug(f"{tag} manufacturer: class-based fallback → {repr(mfr_text)}")
        # Note: specs dict is populated later in the function, so manufacturer
        # extraction from specs will be done in a second pass after specs are built.
        if not product.manufacturer:
            logger.debug(f"{tag} manufacturer: MISSING at initial pass — will re-check after specs extraction")

    # ── Product URL ──
    product.product_url = ld_info.get("url")

    # ── Specifications ──
    specs = {}
    spec_sources = {}  # key → source selector

    # P0 FIX: Check specification accordion sections FIRST (OneMed-specific).
    # These contain structured spec content that generic selectors often miss.
    spec_accordion_text = None
    for spec_acc_id in ("accordionItem_specifications",
                        "accordionItem_specification",
                        "accordionItem_spesifikasjon",
                        "accordion-specifications",
                        "accordion-specification"):
        spec_acc = soup.find(id=spec_acc_id)
        if spec_acc:
            # Try structured extraction from tables/dl within the accordion first
            acc_tables = spec_acc.find_all("table")
            for table in acc_tables:
                for row in table.find_all("tr"):
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True)
                        val = cells[1].get_text(strip=True)
                        if key and val and len(key) < 100:
                            specs[key] = val
                            spec_sources[key] = f"spec-accordion#{spec_acc_id}>table"
            for dl in spec_acc.find_all("dl"):
                for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
                    key = dt.get_text(strip=True).rstrip(":")
                    val = dd.get_text(strip=True)
                    if key and val:
                        specs[key] = val
                        spec_sources[key] = f"spec-accordion#{spec_acc_id}>dl"
            # If no structured data, get as text
            if not specs:
                spec_accordion_text = _get_structured_text(spec_acc)
            if specs:
                logger.debug(
                    f"{tag} specs: accordion #{spec_acc_id} → {len(specs)} key-value pairs"
                )
            elif spec_accordion_text and len(spec_accordion_text) > 10:
                logger.debug(
                    f"{tag} specs: accordion #{spec_acc_id} → {len(spec_accordion_text)} chars (text)"
                )
            break

    # Also check generic accordion elements with spec-like classes
    if not specs and not spec_accordion_text:
        for acc_el in soup.find_all(class_=re.compile(r"accordion.*spec|spec.*accordion", re.I)):
            acc_text = _get_structured_text(acc_el)
            if acc_text and len(acc_text) > 20:
                spec_accordion_text = acc_text
                logger.debug(f"{tag} specs: accordion class match → {len(acc_text)} chars")
                break

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
                    spec_sources[key] = "table>tr>td"

    # Source 2: Definition lists (dl/dt/dd)
    dl_count = 0
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            raw_key = dt.get_text(strip=True)
            key = raw_key.rstrip(":")
            val = dd.get_text(strip=True)
            if key and val:
                specs[key] = val
                spec_sources[key] = f"dl>dt/dd (raw_key={repr(raw_key)})"
                dl_count += 1
    if dl_count:
        logger.debug(f"{tag} specs: dl/dt/dd → {dl_count} key-value pairs")

    # Source 3: Key-value divs with common class patterns
    kv_div_count = 0
    for el in soup.find_all(["div", "section", "ul"], class_=re.compile(
        r"spec|detail|attribute|property|feature|technical|egenskap", re.I
    )):
        el_desc = f"{el.name}.{el.get('class', [])}"
        # Try list items
        items = el.find_all("li")
        for item in items:
            text = item.get_text(strip=True)
            if ":" in text:
                parts = text.split(":", 1)
                if len(parts) == 2 and parts[0].strip() and parts[1].strip():
                    specs[parts[0].strip()] = parts[1].strip()
                    spec_sources[parts[0].strip()] = f"{el_desc}>li"
                    kv_div_count += 1
        # Try nested key-value spans/divs
        labels = el.find_all(class_=re.compile(r"label|key|name", re.I))
        values = el.find_all(class_=re.compile(r"value|data|content", re.I))
        for label, value in zip(labels, values):
            k = label.get_text(strip=True)
            v = value.get_text(strip=True)
            if k and v:
                specs[k] = v
                spec_sources[k] = f"{el_desc}>.label+.value"
                kv_div_count += 1
    if kv_div_count:
        logger.debug(f"{tag} specs: kv-div/ul → {kv_div_count} key-value pairs")

    if specs:
        product.technical_details = specs
        product.specification = "\n".join(f"{k}: {v}" for k, v in specs.items())
        logger.debug(
            f"{tag} specification: {len(specs)} attrs from "
            f"{set(spec_sources.values())}"
        )
    else:
        # P0 FIX: Use spec accordion text if available (extracted earlier)
        if spec_accordion_text and len(spec_accordion_text) > 10:
            product.specification = spec_accordion_text
            logger.debug(
                f"{tag} specification: accordion text fallback → {len(spec_accordion_text)} chars"
            )
        else:
            # Source 4: If no structured specs found, check for spec text
            spec_block = soup.find(["div", "section"], class_=re.compile(
                r"spec|technical|egenskap", re.I
            ))
            if spec_block:
                spec_text = _get_structured_text(spec_block)
                if spec_text and len(spec_text) > 10:
                    product.specification = spec_text
                    logger.debug(
                        f"{tag} specification: free-text block → {len(spec_text)} chars"
                    )
                else:
                    logger.debug(f"{tag} specification: MISSING — spec block found but too short")
            else:
                logger.debug(f"{tag} specification: MISSING — no tables, dl, kv-divs, or spec blocks")

    # ── Packaging ──
    pkg_parts = []
    pkg_source = None
    if specs:
        for key, val in specs.items():
            # Normalize key: strip non-breaking spaces, collapse whitespace, lowercase
            key_normalized = key.replace("\xa0", " ").replace("\u200b", "")
            key_normalized = re.sub(r"\s+", " ", key_normalized).strip()
            key_lower = key_normalized.lower().rstrip(":")
            if any(kw in key_lower for kw in [
                "antall i pakn", "antall per pakn", "antall pr pakn",
                "antall i forpakn", "antall pr forpakn",
                "pakningsstørrelse", "pack size", "enheter i pakn",
                "enheter per pakn", "enheter pr pakn",
                "stk i pakn", "stk per pakn", "stk pr pakn",
                "antall i inner",
            ]):
                product.packaging_unit = val
                pkg_parts.append(f"Antall i pakning: {val}")
            elif any(kw in key_lower for kw in [
                "transport", "kolli", "ytterforpakn",
                "antall i trans", "antall pr trans",
            ]):
                product.transport_packaging = val
                pkg_parts.append(f"Antall i transportpakke: {val}")
            elif "pall" in key_lower:
                pkg_parts.append(f"Antall på pall: {val}")

    if pkg_parts:
        product.packaging_info = "; ".join(pkg_parts)
        pkg_source = "structured specs"
        logger.debug(f"{tag} packaging: {pkg_source} → {product.packaging_info}")

    # Priority 2: regex on page text (fallback)
    # NOTE: page_text here is intentionally flattened via get_text() because it is
    # used ONLY for regex pattern matching, NOT stored as a user-facing description.
    # The extracted values (packaging_unit, transport_packaging) are short structured
    # strings like "100 stk", not multi-line text. Do NOT use page_text for display.
    if not product.packaging_info:
        page_text = soup.get_text()
        pkg_patterns = [
            r"(\d+)\s*(?:stk|st)\s*(?:per|pr|/)\s*(?:forpakning|pakning|frp|pk)",
            r"(?:forpakning|pakning|frp|pk)[\s:]+(\d+)\s*(?:stk|st)?",
            r"Antall\s*(?:per|pr|i)\s*(?:forpakning|pakning)[\s:]*(\d+)",
            r"Antall\s+i\s+pakn\w*[\s:]+(\d+)\s*(?:stk|st)?",
        ]
        for pattern in pkg_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                product.packaging_info = match.group(0)
                product.packaging_unit = match.group(1) + " stk"
                pkg_source = f"regex pattern ({pattern[:30]}…)"
                logger.debug(f"{tag} packaging: {pkg_source} → {product.packaging_info}")
                break

        # Look for "Antall i transportforpakning"
        if not product.transport_packaging:
            transport_match = re.search(
                r"(?:transport(?:forpakning|pakning|pakke|-pakke))[\s:]*(\d+)",
                page_text, re.IGNORECASE
            )
            if transport_match:
                product.transport_packaging = transport_match.group(1) + " stk"

    if not product.packaging_info:
        logger.debug(f"{tag} packaging: MISSING — no spec keys matched, no regex matched")

    # ── Manufacturer from specs (second pass after specs are built) ──
    if not product.manufacturer and specs:
        for key, val in specs.items():
            key_lower = key.lower()
            if any(kw in key_lower for kw in ["produsent", "manufacturer", "leverandør", "supplier", "brand", "merke"]):
                product.manufacturer = val
                logger.debug(f"{tag} manufacturer: spec key '{key}' → {repr(val)}")
                break

    # ── Manufacturer article number ──
    # NOTE: page_text is intentionally flattened — used only for regex extraction
    # of short identifier values, not for display. Do NOT assign page_text to any
    # user-facing description or specification field.
    page_text = soup.get_text()
    mfr_patterns = [
        r"(?:Produsentens?\s*(?:art\.?|varenr|artikkel)(?:nummer|nr)?|Lev\.?\s*art\.?\s*nr)[\s.:]*([A-Za-z0-9\-/]+)",
        r"(?:Manufacturer|MFR|Supplier)\s*(?:art|item|part)\s*(?:no|nr|number)?[\s.:]*([A-Za-z0-9\-/]+)",
    ]
    for pattern in mfr_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            product.manufacturer_article_number = match.group(1).strip()
            logger.debug(
                f"{tag} mfr_article_number: regex → {repr(product.manufacturer_article_number)}"
            )
            break
    else:
        logger.debug(f"{tag} mfr_article_number: MISSING — no regex pattern matched")

    # ── Image accessibility check ──
    product.image_quality_ok = product.image_url is not None

    # ── P0 FIX: Normalize all text fields — strip HTML artifacts, excessive whitespace ──
    for field in ("product_name", "description", "specification", "manufacturer",
                  "manufacturer_article_number", "category", "packaging_info"):
        val = getattr(product, field, None)
        if val and isinstance(val, str):
            # Strip common HTML leftovers
            cleaned = val.strip()
            # Collapse runs of spaces/tabs within lines
            cleaned = re.sub(r"[ \t]+", " ", cleaned)
            # Remove zero-width chars and non-breaking spaces
            cleaned = cleaned.replace("\u200b", "").replace("\ufeff", "")
            cleaned = cleaned.replace("\xa0", " ")
            # Collapse excessive newlines
            cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
            # Strip again after cleanup
            cleaned = cleaned.strip()
            if cleaned != val:
                setattr(product, field, cleaned)
                logger.debug(f"{tag} normalized {field}: {len(val)} → {len(cleaned)} chars")
            # Treat whitespace-only as missing
            if not cleaned:
                setattr(product, field, None)

    # ── Final summary ──
    filled = sum(1 for v in [
        product.product_name, product.description, product.specification,
        product.manufacturer, product.manufacturer_article_number,
        product.category, product.packaging_info,
    ] if v)
    logger.info(
        f"{tag} DONE: {filled}/7 fields filled | "
        f"name={'Y' if product.product_name else 'N'} "
        f"desc={'Y' if product.description else 'N'} "
        f"spec={len(specs) if specs else 0} "
        f"cat={'Y' if product.category else 'N'} "
        f"pkg={'Y' if product.packaging_info else 'N'} "
        f"mfr={'Y' if product.manufacturer else 'N'} "
        f"mfr_art={'Y' if product.manufacturer_article_number else 'N'}"
    )

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
    max_pages: int = 2000,
) -> Optional[str]:
    """Find the product page URL by scanning sitemap pages for matching SKU.

    Downloads product pages from the sitemap in batches and checks if
    the JSON-LD SKU matches the target article number.
    Results are cached in the _sku_to_url mapping.

    Args:
        max_pages: Maximum number of pages to scan (default 2000 for full discovery,
                   use lower values like 200 for targeted scans).
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
    BATCH_SIZE = 20
    MAX_PAGES_TO_CHECK = max_pages

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


# ── Index management functions ──


def get_index_stats() -> dict:
    """Return current SKU index statistics (no I/O)."""
    return {
        "sitemap_loaded": _sitemap_loaded,
        "sitemap_url_count": len(_sitemap_urls),
        "sku_index_count": len(_sku_to_url),
        "coverage_pct": round(len(_sku_to_url) / len(_sitemap_urls) * 100, 1) if _sitemap_urls else 0,
    }


async def build_full_index(
    on_progress=None,
) -> dict:
    """Build a complete SKU→URL index by scanning ALL sitemap product pages.

    This is an expensive one-time operation (~9500 pages, ~15-25 minutes).
    Should be run as a background task. Results are persisted to disk and
    reused by all subsequent scrape_product() calls.

    Args:
        on_progress: Optional async callback(indexed, total, new_in_batch)
                     called after each batch completes.

    Returns:
        dict with {total_pages, indexed, skipped, errors, duration_seconds}
    """
    global _sitemap_urls, _sku_to_url, _sitemap_loaded

    start_time = time.time()
    BATCH_SIZE = 20
    BATCH_DELAY = 0.5  # seconds between batches — polite crawling

    async with httpx.AsyncClient() as client:
        # Ensure sitemap is loaded
        sitemap_urls = await _load_sitemap(client)
        if not sitemap_urls:
            return {"error": "Failed to load sitemap", "total_pages": 0, "indexed": 0}

        # Filter out already-indexed URLs
        indexed_urls = set(_sku_to_url.values())
        unchecked_urls = [u for u in sitemap_urls if u not in indexed_urls]

        total_to_check = len(unchecked_urls)
        already_indexed = len(_sku_to_url)
        new_indexed = 0
        errors = 0

        logger.info(
            f"[build-index] Starting full index build: {total_to_check} pages to check, "
            f"{already_indexed} already indexed"
        )

        for batch_start in range(0, total_to_check, BATCH_SIZE):
            batch = unchecked_urls[batch_start:batch_start + BATCH_SIZE]

            async def _check_page(url: str):
                try:
                    resp = await client.get(
                        url, headers=HEADERS, follow_redirects=True, timeout=15
                    )
                    if resp.status_code == 200:
                        sku = _extract_sku_from_html(resp.text)
                        if sku:
                            return (sku, url)
                except Exception:
                    pass
                return None

            tasks = [_check_page(url) for url in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_new = 0
            for result in results:
                if isinstance(result, tuple):
                    sku, url = result
                    if sku not in _sku_to_url:
                        _sku_to_url[sku] = url
                        new_indexed += 1
                        batch_new += 1
                elif isinstance(result, Exception):
                    errors += 1

            # Save progress every 10 batches (200 pages)
            if (batch_start // BATCH_SIZE) % 10 == 9:
                _save_sitemap_index(_sitemap_urls, _sku_to_url)

            # Report progress
            checked_so_far = min(batch_start + BATCH_SIZE, total_to_check)
            if on_progress:
                try:
                    await on_progress(checked_so_far, total_to_check, batch_new)
                except Exception:
                    pass

            if checked_so_far % 200 == 0 or checked_so_far == total_to_check:
                logger.info(
                    f"[build-index] Progress: {checked_so_far}/{total_to_check} pages, "
                    f"{new_indexed} new SKUs indexed"
                )

            await asyncio.sleep(BATCH_DELAY)

        # Final save
        _save_sitemap_index(_sitemap_urls, _sku_to_url)

    duration = time.time() - start_time
    logger.info(
        f"[build-index] DONE: {new_indexed} new SKUs indexed in {duration:.0f}s. "
        f"Total index: {len(_sku_to_url)} SKUs / {len(_sitemap_urls)} sitemap URLs"
    )

    return {
        "total_pages": total_to_check,
        "already_indexed": already_indexed,
        "new_indexed": new_indexed,
        "total_index_size": len(_sku_to_url),
        "sitemap_urls": len(_sitemap_urls),
        "errors": errors,
        "duration_seconds": round(duration, 1),
    }


async def scan_index_incremental(max_pages: int = 50) -> int:
    """Scan a small batch of unindexed sitemap pages to expand the SKU index.

    Designed to be called at the START of each analysis job. Lightweight
    (~5-10 seconds for 50 pages) and incrementally builds coverage over time.

    Args:
        max_pages: Maximum pages to scan (default 50).

    Returns:
        Number of new SKUs indexed in this scan.
    """
    global _sku_to_url

    if not _sitemap_loaded or not _sitemap_urls:
        return 0

    indexed_urls = set(_sku_to_url.values())
    unchecked = [u for u in _sitemap_urls if u not in indexed_urls]

    if not unchecked:
        logger.info("[incremental-scan] Index is complete — all sitemap URLs indexed")
        return 0

    pages_to_scan = min(max_pages, len(unchecked))
    batch_urls = unchecked[:pages_to_scan]
    new_count = 0
    BATCH_SIZE = 15

    logger.info(
        f"[incremental-scan] Scanning {pages_to_scan} unindexed pages "
        f"({len(unchecked)} remaining)"
    )

    async with httpx.AsyncClient() as client:
        for batch_start in range(0, len(batch_urls), BATCH_SIZE):
            batch = batch_urls[batch_start:batch_start + BATCH_SIZE]

            async def _check(url):
                try:
                    resp = await client.get(
                        url, headers=HEADERS, follow_redirects=True, timeout=15
                    )
                    if resp.status_code == 200:
                        sku = _extract_sku_from_html(resp.text)
                        if sku:
                            return (sku, url)
                except Exception:
                    pass
                return None

            tasks = [_check(u) for u in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, tuple):
                    sku, url = result
                    if sku not in _sku_to_url:
                        _sku_to_url[sku] = url
                        new_count += 1

            await asyncio.sleep(0.3)

    if new_count:
        _save_sitemap_index(_sitemap_urls, _sku_to_url)
        logger.info(f"[incremental-scan] Indexed {new_count} new SKUs (total: {len(_sku_to_url)})")
    else:
        logger.info(f"[incremental-scan] No new SKUs found in this batch")

    return new_count


async def scrape_product(
    article_number: str,
    use_cache: bool = True,
    playwright_browser=None,
    enable_discovery: bool = False,
) -> ProductData:
    """Scrape product data from onemed.no for a given article number.

    Normal mode (enable_discovery=False) — strict input-scoped:
    1. Check disk cache
    2. Check SKU→URL index (cached mapping from previous runs)
    3. Verify product exists via CDN image (fast HEAD request)
    No sitemap scan or broad crawl is performed.

    Discovery mode (enable_discovery=True) — explicit opt-in only:
    Also tries to find the product page via sitemap page scanning.
    This fetches unrelated product pages and should NOT be used in
    normal validation runs.

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
        # Strategy 1: Check if we already know the URL from SKU→URL index
        # This uses only the cached index from disk — no network calls to discover new mappings
        if clean_num in _sku_to_url:
            known_url = _sku_to_url[clean_num]
            logger.info(f"SKU index hit for {clean_num}: {known_url}")
            response = await _fetch_with_retry(client, known_url)
            if response and response.status_code == 200:
                product = _parse_product_page(response.text, clean_num)
                product.product_url = str(response.url)
                # Verify SKU matches to prevent cross-contamination
                v_status, v_evidence = _verify_sku_match(response.text, clean_num)
                product.verification_status = v_status
                product.verification_evidence = v_evidence
                if v_status == VerificationStatus.MISMATCH:
                    logger.warning(
                        f"SKU MISMATCH for {clean_num} at {known_url}: {v_evidence}"
                    )
                    product.error = f"SKU-mismatch: {v_evidence}"
                    product.found_on_onemed = False  # Do NOT trust mismatched data
                    product.multiple_hits = True  # Flag for review
                elif v_status == VerificationStatus.UNVERIFIED:
                    logger.warning(
                        f"SKU UNVERIFIED for {clean_num} at {known_url}: {v_evidence}"
                    )
                    product.error = f"Identitet ikke verifisert: {v_evidence}"
                    product.multiple_hits = True  # Flag for manual review
                _save_to_cache(product)
                return product

        # Strategy 2: Verify product exists via CDN image (fast HEAD request)
        cdn_exists = await _check_cdn_image_exists(client, clean_num)

        # Strategy 3 (DISCOVERY MODE ONLY): Sitemap page scan
        # This fetches unrelated product pages to build the SKU→URL index.
        # DISABLED in normal validation runs to prevent scope explosion.
        if enable_discovery:
            logger.warning(
                f"DISCOVERY MODE: sitemap scan triggered for {clean_num} "
                f"(this should NOT happen in normal validation runs)"
            )
            product_url = await _find_product_url_via_sitemap(client, clean_num)
            if product_url:
                response = await _fetch_with_retry(client, product_url)
                if response and response.status_code == 200:
                    product = _parse_product_page(response.text, clean_num)
                    product.product_url = str(response.url)
                    v_status, v_evidence = _verify_sku_match(response.text, clean_num)
                    product.verification_status = v_status
                    product.verification_evidence = v_evidence
                    if v_status == VerificationStatus.MISMATCH:
                        logger.warning(f"SKU MISMATCH for {clean_num} at {product_url}: {v_evidence}")
                        product.error = f"SKU-mismatch: {v_evidence}"
                        product.found_on_onemed = False
                        product.multiple_hits = True
                    elif v_status == VerificationStatus.UNVERIFIED:
                        logger.warning(f"SKU UNVERIFIED for {clean_num}: {v_evidence}")
                        product.error = f"Identitet ikke verifisert: {v_evidence}"
                        product.multiple_hits = True
                    _save_to_cache(product)
                    return product
        else:
            if clean_num not in _sku_to_url:
                logger.info(
                    f"{clean_num}: not in SKU index, skipping sitemap scan (strict input mode)"
                )

        # Strategy 4: If CDN image exists, product MAY be in the OneMed system
        # CDN image alone is WEAK evidence — mark as CDN_ONLY, not as fully verified.
        # found_on_onemed=True allows the pipeline to continue, but verification_status
        # signals that identity is not confirmed.
        if cdn_exists:
            logger.info(f"{clean_num}: CDN image confirmed, product page not found in index")
            product = ProductData(
                article_number=clean_num,
                found_on_onemed=True,
                image_url=f"{IMAGE_BASE_URL}/{clean_num}.jpg",
                image_quality_ok=True,
                verification_status=VerificationStatus.CDN_ONLY,
                verification_evidence=(
                    f"Produktbilde funnet i bildekatalogen for '{clean_num}'. "
                    f"Ingen produktside med detaljer ble funnet — produktidentiteten er usikker."
                ),
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
