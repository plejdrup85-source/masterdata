"""Manufacturer and secondary source lookup module.

Searches for product data from:
1. Known manufacturer websites (primary supplementary source)
2. Norengros (secondary market reference — conservative use only)

Results should be treated as suggestions, not authoritative data.
All competitor-derived information requires manual review.
"""

import logging
import re
from typing import Optional
from urllib.parse import quote_plus

import httpx
from bs4 import BeautifulSoup

from backend.models import ManufacturerLookup, NorengrosLookup, ProductData

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "nb-NO,nb;q=0.9,no;q=0.8,en;q=0.5",
}

# Known manufacturer website configurations
# Only includes manufacturers where we know the site structure works
KNOWN_MANUFACTURERS = {
    "molnlycke": {
        "domains": ["www.molnlycke.com"],
        "search_pattern": "/search?q={query}",
    },
    "m\u00f6lnlycke": {
        "domains": ["www.molnlycke.com"],
        "search_pattern": "/search?q={query}",
    },
    "coloplast": {
        "domains": ["www.coloplast.com", "www.coloplast.no"],
        "search_pattern": "/search?q={query}",
    },
    "essity": {
        "domains": ["www.essity.com"],
        "search_pattern": "/search?q={query}",
    },
    "tena": {
        "domains": ["www.tena.no"],
        "search_pattern": "/search?q={query}",
    },
    "hartmann": {
        "domains": ["www.hartmann.info"],
        "search_pattern": "/search?q={query}",
    },
    "3m": {
        "domains": ["www.3m.com"],
        "search_pattern": "/3M/en_US/search/?Ntt={query}",
    },
    "convatec": {
        "domains": ["www.convatec.com"],
        "search_pattern": "/search?q={query}",
    },
    "b braun": {
        "domains": ["www.bbraun.com"],
        "search_pattern": "/search?q={query}",
    },
    "bbraun": {
        "domains": ["www.bbraun.com"],
        "search_pattern": "/search?q={query}",
    },
    "hollister": {
        "domains": ["www.hollister.com"],
        "search_pattern": "/search?q={query}",
    },
    "medline": {
        "domains": ["www.medline.com"],
        "search_pattern": "/search?q={query}",
    },
    "baxter": {
        "domains": ["www.baxter.com"],
        "search_pattern": "/search?q={query}",
    },
    "fresenius": {
        "domains": ["www.fresenius.com"],
        "search_pattern": "/search?q={query}",
    },
    "nutricia": {
        "domains": ["www.nutricia.no", "www.nutricia.com"],
        "search_pattern": "/search?q={query}",
    },
}


def _find_manufacturer_config(manufacturer_name: str) -> Optional[dict]:
    """Find a known manufacturer configuration by name."""
    if not manufacturer_name:
        return None

    clean = manufacturer_name.lower().strip()

    # Direct match
    if clean in KNOWN_MANUFACTURERS:
        return KNOWN_MANUFACTURERS[clean]

    # Partial match (manufacturer name contains a known key)
    for key, config in KNOWN_MANUFACTURERS.items():
        if key in clean or clean in key:
            return config

    return None


def _build_search_queries(product: ProductData) -> list[str]:
    """Build search queries for finding manufacturer product pages."""
    queries = []

    mfr_article = product.manufacturer_article_number or ""
    name = product.product_name or ""

    # Most specific first
    if mfr_article:
        queries.append(mfr_article)

    if name:
        queries.append(name)

    return queries


def _extract_product_info_from_page(html: str, url: str) -> dict:
    """Extract product information from a manufacturer page."""
    soup = BeautifulSoup(html, "lxml")
    info = {}

    # Try to get product name from h1
    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(strip=True)
        # Avoid picking up generic page titles
        if len(text) > 2 and len(text) < 200:
            info["product_name"] = text

    # Try to get description from meta tags
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc:
        desc = meta_desc.get("content", "")
        if desc and len(desc) > 10:
            info["description"] = desc

    # Look for specification tables
    specs = {}
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True)
                val = cells[1].get_text(strip=True)
                if key and val and len(key) < 100 and len(val) < 500:
                    specs[key] = val

    # Also look for definition lists
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = dt.get_text(strip=True)
            val = dd.get_text(strip=True)
            if key and val:
                specs[key] = val

    if specs:
        info["specifications"] = specs

    # Look for PDF datasheet links
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        text = a.get_text(strip=True).lower()
        if href.endswith(".pdf") or "datasheet" in text or "produktblad" in text:
            pdf_url = href if href.startswith("http") else url.rstrip("/") + "/" + href.lstrip("/")
            info["datasheet_url"] = pdf_url
            break

    # Extract product image URL
    # Look for og:image meta tag first (most reliable)
    og_image = soup.find("meta", attrs={"property": "og:image"})
    if og_image:
        img_url = og_image.get("content", "")
        if img_url and _is_likely_product_image(img_url):
            info["image_url"] = img_url if img_url.startswith("http") else f"https:{img_url}"

    # Fallback: look for large product images in the page
    if "image_url" not in info:
        for img in soup.find_all("img", src=True):
            src = img.get("src", "")
            alt = (img.get("alt", "") or "").lower()
            # Look for product images (skip icons, logos, decorative)
            if _is_likely_product_image(src) and not any(
                skip in alt for skip in ["logo", "icon", "banner", "arrow", "flag"]
            ):
                info["image_url"] = src if src.startswith("http") else f"https:{src}"
                break

    return info


def _is_likely_product_image(url: str) -> bool:
    """Check if a URL is likely a product image (not icon/logo/placeholder)."""
    if not url:
        return False
    url_lower = url.lower()
    # Must be an image
    if not any(ext in url_lower for ext in [".jpg", ".jpeg", ".png", ".webp"]):
        # Could also be a dynamic image URL without extension
        if "image" not in url_lower and "img" not in url_lower and "photo" not in url_lower:
            return False
    # Skip small/icon/placeholder images
    skip_patterns = ["icon", "logo", "favicon", "placeholder", "1x1", "pixel", "spacer", "blank"]
    if any(s in url_lower for s in skip_patterns):
        return False
    return True


async def search_manufacturer_info(product: ProductData) -> ManufacturerLookup:
    """Search for product information from the manufacturer's website.

    Only attempts lookup for known manufacturers with configured websites.
    Returns results with appropriate confidence levels.
    """
    result = ManufacturerLookup(searched=True)

    if not product.manufacturer and not product.product_name:
        result.notes = "Ikke nok informasjon for \u00e5 s\u00f8ke hos produsent"
        return result

    queries = _build_search_queries(product)
    if not queries:
        result.notes = "Kunne ikke bygge søkespørring - mangler både varenummer og produktnavn"
        return result

    # Strategy 1: Known manufacturer direct search (high confidence)
    mfr_config = _find_manufacturer_config(product.manufacturer or "")
    if mfr_config:
        search_pattern = mfr_config.get("search_pattern", "/search?q={query}")
        async with httpx.AsyncClient(timeout=15) as client:
            for domain in mfr_config["domains"]:
                for query in queries[:2]:
                    try:
                        search_url = f"https://{domain}{search_pattern.format(query=quote_plus(query))}"
                        logger.info(f"Manufacturer search: known={domain} query='{query}'")
                        response = await client.get(
                            search_url, headers=HEADERS, follow_redirects=True,
                        )
                        if response.status_code == 200:
                            info = _extract_product_info_from_page(
                                response.text, str(response.url)
                            )
                            if info.get("product_name") or info.get("specifications"):
                                result.found = True
                                result.source_url = str(response.url)
                                result.product_name = info.get("product_name")
                                result.description = info.get("description")
                                result.specifications = info.get("specifications")
                                result.datasheet_url = info.get("datasheet_url")
                                result.image_url = info.get("image_url")
                                result.confidence = 0.5
                                result.notes = (
                                    f"Data funnet via kjent produsentside ({domain}). "
                                    f"Verifiser at det er riktig produkt før bruk."
                                )
                                return result
                    except Exception as e:
                        logger.debug(f"Search error for {domain}: {e}")
                        continue

    # Strategy 2: Generic web search for unknown manufacturers
    # Use manufacturer name + article number to find product pages
    if product.manufacturer and not mfr_config:
        mfr_name = product.manufacturer.strip()
        search_queries = []
        if product.manufacturer_article_number:
            search_queries.append(f"{mfr_name} {product.manufacturer_article_number}")
        if product.product_name:
            search_queries.append(f"{mfr_name} {product.product_name}")

        async with httpx.AsyncClient(timeout=15) as client:
            for sq in search_queries[:2]:
                try:
                    # Try searching on the manufacturer's likely website
                    # by constructing site:manufacturer.com query patterns
                    mfr_domain = _guess_manufacturer_domain(mfr_name)
                    if mfr_domain:
                        search_url = f"https://{mfr_domain}/search?q={quote_plus(sq)}"
                        logger.info(f"Manufacturer search: guessed={mfr_domain} query='{sq}'")
                        response = await client.get(
                            search_url, headers=HEADERS, follow_redirects=True,
                        )
                        if response.status_code == 200:
                            info = _extract_product_info_from_page(
                                response.text, str(response.url)
                            )
                            if info.get("product_name") or info.get("specifications"):
                                result.found = True
                                result.source_url = str(response.url)
                                result.product_name = info.get("product_name")
                                result.description = info.get("description")
                                result.specifications = info.get("specifications")
                                result.datasheet_url = info.get("datasheet_url")
                                result.image_url = info.get("image_url")
                                result.confidence = 0.35
                                result.notes = (
                                    f"Data funnet via antatt produsentside ({mfr_domain}). "
                                    f"Lavere konfidensgrad — verifiser nøye."
                                )
                                return result
                except Exception as e:
                    logger.debug(f"Generic manufacturer search error: {e}")
                    continue

    if mfr_config:
        result.notes = f"Ingen treff hos kjente produsentnettsted for '{product.manufacturer or 'ukjent'}'"
    else:
        result.notes = (
            f"Produsenten '{product.manufacturer or 'ukjent'}' er ikke i listen over kjente "
            f"produsenter. Søkte også generisk uten resultat. Manuelt oppslag anbefales."
        )
    return result


def _guess_manufacturer_domain(manufacturer_name: str) -> Optional[str]:
    """Try to guess a manufacturer's website domain from their name.

    Conservative: only returns a domain if the name maps clearly.
    """
    if not manufacturer_name:
        return None
    clean = re.sub(r'[®™©]', '', manufacturer_name).strip().lower()
    # Remove common suffixes
    for suffix in [" as", " ab", " gmbh", " inc", " ltd", " ag", " sa"]:
        if clean.endswith(suffix):
            clean = clean[:-len(suffix)].strip()

    # Only return if it's a single reasonable word/brand name
    if " " not in clean and len(clean) >= 3 and clean.isalnum():
        return f"www.{clean}.com"

    return None


def generate_improvement_suggestions(
    product: ProductData,
    manufacturer_data: ManufacturerLookup,
) -> list[dict]:
    """Generate specific improvement suggestions based on manufacturer data.

    All suggestions include a note that they should be verified manually.
    """
    suggestions = []

    if not manufacturer_data.found:
        return suggestions

    # Suggest better name if manufacturer has one
    if manufacturer_data.product_name and product.product_name:
        mfr_name = manufacturer_data.product_name
        if len(mfr_name) > len(product.product_name or "") and mfr_name != product.product_name:
            suggestions.append({
                "field": "Produktnavn",
                "current": product.product_name,
                "suggested": mfr_name,
                "source": manufacturer_data.source_url,
                "confidence": manufacturer_data.confidence,
                "reason": "Produsentens produktnavn er mer beskrivende (verifiser manuelt)",
            })

    # Suggest better description
    if manufacturer_data.description and (
        not product.description or len(manufacturer_data.description) > len(product.description or "")
    ):
        suggestions.append({
            "field": "Beskrivelse",
            "current": product.description,
            "suggested": manufacturer_data.description,
            "source": manufacturer_data.source_url,
            "confidence": manufacturer_data.confidence * 0.9,
            "reason": "Produsentens beskrivelse er mer utfyllende (verifiser manuelt)",
        })

    # Suggest specifications
    if manufacturer_data.specifications:
        current_specs = product.technical_details or {}
        for key, value in manufacturer_data.specifications.items():
            if key not in current_specs:
                suggestions.append({
                    "field": f"Spesifikasjon: {key}",
                    "current": None,
                    "suggested": value,
                    "source": manufacturer_data.source_url,
                    "confidence": manufacturer_data.confidence * 0.8,
                    "reason": "Ny spesifikasjon fra produsent (verifiser manuelt)",
                })

    # Suggest datasheet link
    if manufacturer_data.datasheet_url:
        suggestions.append({
            "field": "Datablad",
            "current": None,
            "suggested": manufacturer_data.datasheet_url,
            "source": manufacturer_data.source_url,
            "confidence": manufacturer_data.confidence,
            "reason": "Produktdatablad funnet hos produsent",
        })

    return suggestions


# ── Norengros secondary reference lookup ──

NORENGROS_BASE = "https://www.norengros.no"
NORENGROS_SEARCH = f"{NORENGROS_BASE}/search"


async def search_norengros(product: ProductData) -> NorengrosLookup:
    """Search Norengros as a secondary market reference source.

    Strategy:
    1. Search with manufacturer article number (most specific)
    2. Search with cleaned OneMed article number
    3. Search with product name + manufacturer
    4. For each search: find product links in results, then fetch product page

    Returns results with conservative confidence and review_required=True.
    """
    result = NorengrosLookup(searched=True)

    art_num = product.article_number.strip()
    clean_num = art_num.lstrip("N") if art_num.startswith("N") else art_num

    # Build search queries in order of specificity
    queries = []
    if product.manufacturer_article_number:
        queries.append(("mfr_artnr", product.manufacturer_article_number))
    queries.append(("artnr", clean_num))
    if product.product_name and product.manufacturer:
        queries.append(("name+mfr", f"{product.product_name} {product.manufacturer}"))
    elif product.product_name:
        queries.append(("name", product.product_name))

    strategies_tried = []

    async with httpx.AsyncClient(timeout=15) as client:
        for strategy, query in queries[:3]:
            strategies_tried.append(f"{strategy}={query}")
            try:
                search_url = f"{NORENGROS_SEARCH}?q={quote_plus(query)}"
                logger.info(f"Norengros search: strategy={strategy} query='{query}' url={search_url}")
                response = await client.get(
                    search_url, headers=HEADERS, follow_redirects=True,
                )
                if response.status_code != 200:
                    logger.debug(f"Norengros search HTTP {response.status_code} for '{query}'")
                    continue

                # Step 1: Find product links in search results
                product_urls = _extract_norengros_product_links(response.text)
                if not product_urls:
                    logger.debug(f"Norengros: no product links found for '{query}'")
                    continue

                # Step 2: Fetch the first product page for detail extraction
                for product_url in product_urls[:2]:
                    full_url = product_url if product_url.startswith("http") else f"{NORENGROS_BASE}{product_url}"
                    try:
                        page_resp = await client.get(
                            full_url, headers=HEADERS, follow_redirects=True,
                        )
                        if page_resp.status_code != 200:
                            continue

                        info = _extract_norengros_product(page_resp.text, str(page_resp.url))
                        if info.get("product_name") or info.get("description") or info.get("specifications"):
                            result.found = True
                            result.source_url = str(page_resp.url)
                            result.product_name = info.get("product_name")
                            result.description = info.get("description")
                            result.specifications = info.get("specifications")
                            result.image_url = info.get("image_url")
                            result.confidence = 0.35
                            result.notes = (
                                f"Norengros: data fra produktside ({strategy}). "
                                f"All data krever manuell verifisering. "
                                f"Strategier forsøkt: {', '.join(strategies_tried)}"
                            )
                            logger.info(
                                f"Norengros: found data for {art_num} via {strategy} "
                                f"(name={'yes' if info.get('product_name') else 'no'}, "
                                f"desc={'yes' if info.get('description') else 'no'}, "
                                f"specs={len(info.get('specifications', {}))})"
                            )
                            return result
                    except Exception as e:
                        logger.debug(f"Norengros product page fetch error: {e}")
                        continue

            except Exception as e:
                logger.debug(f"Norengros search error for '{query}': {e}")
                continue

    result.notes = (
        f"Ingen treff på Norengros for {art_num}. "
        f"Strategier forsøkt: {', '.join(strategies_tried)}"
    )
    return result


def _extract_norengros_product_links(html: str) -> list[str]:
    """Extract product page links from Norengros search results page.

    Norengros search results contain product cards with links.
    We extract the href from these cards to then fetch the actual product page.
    """
    soup = BeautifulSoup(html, "lxml")
    links = []

    # Look for product links in search results
    # Common patterns: <a> with href containing /product/ or /p/ or /vare/
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        # Product page patterns on Norwegian e-commerce sites
        if any(seg in href for seg in ["/product/", "/p/", "/vare/", "/produkt/"]):
            if href not in links:
                links.append(href)

    # Fallback: look for links inside product-card-like containers
    if not links:
        for container in soup.find_all(class_=re.compile(
            r"product|item|result|card", re.IGNORECASE
        )):
            a = container.find("a", href=True)
            if a:
                href = a.get("href", "")
                if href and href not in links and not href.startswith("#"):
                    links.append(href)

    logger.debug(f"Norengros: found {len(links)} product links in search results")
    return links[:5]  # Max 5 to avoid excessive fetching


def _extract_norengros_product(html: str, url: str) -> dict:
    """Extract product info from a Norengros page."""
    soup = BeautifulSoup(html, "lxml")
    info = {}

    # Product name from h1
    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(strip=True)
        if 2 < len(text) < 200:
            info["product_name"] = text

    # Description from meta or page content
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc:
        desc = meta_desc.get("content", "")
        if desc and len(desc) > 10:
            info["description"] = desc

    # Image from og:image or product image
    og_image = soup.find("meta", attrs={"property": "og:image"})
    if og_image:
        img_url = og_image.get("content", "")
        if img_url and _is_likely_product_image(img_url):
            info["image_url"] = img_url if img_url.startswith("http") else f"https:{img_url}"

    if "image_url" not in info:
        for img in soup.find_all("img", src=True):
            src = img.get("src", "")
            alt = (img.get("alt", "") or "").lower()
            if _is_likely_product_image(src) and not any(
                skip in alt for skip in ["logo", "icon", "banner"]
            ):
                info["image_url"] = src if src.startswith("http") else f"https:{src}"
                break

    # Specifications from tables
    specs = {}
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True)
                val = cells[1].get_text(strip=True)
                if key and val and len(key) < 100 and len(val) < 500:
                    specs[key] = val
    if specs:
        info["specifications"] = specs

    return info
