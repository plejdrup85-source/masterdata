"""Manufacturer lookup module - searches for product data from manufacturer websites.

This module attempts to find product information from known manufacturer websites.
It uses a curated list of manufacturer domain mappings and search URL patterns.
Results should be treated as suggestions, not authoritative data.
"""

import logging
import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from backend.models import ManufacturerLookup, ProductData

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

    return info


async def search_manufacturer_info(product: ProductData) -> ManufacturerLookup:
    """Search for product information from the manufacturer's website.

    Only attempts lookup for known manufacturers with configured websites.
    Returns results with appropriate confidence levels.
    """
    result = ManufacturerLookup(searched=True)

    if not product.manufacturer and not product.product_name:
        result.notes = "Ikke nok informasjon for \u00e5 s\u00f8ke hos produsent"
        return result

    # Only attempt lookup for known manufacturers
    mfr_config = _find_manufacturer_config(product.manufacturer or "")
    if not mfr_config:
        result.notes = (
            f"Produsenten '{product.manufacturer or 'ukjent'}' er ikke i listen over kjente "
            f"produsenter med s\u00f8kbart nettsted. Manuelt oppslag anbefales."
        )
        return result

    queries = _build_search_queries(product)
    if not queries:
        result.notes = "Kunne ikke bygge s\u00f8kesp\u00f8rring - mangler b\u00e5de varenummer og produktnavn"
        return result

    search_pattern = mfr_config.get("search_pattern", "/search?q={query}")

    async with httpx.AsyncClient(timeout=15) as client:
        for domain in mfr_config["domains"]:
            for query in queries[:2]:  # Max 2 queries per domain
                try:
                    search_url = f"https://{domain}{search_pattern.format(query=query)}"
                    response = await client.get(
                        search_url,
                        headers=HEADERS,
                        follow_redirects=True,
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
                            # Confidence is moderate - this is a search result page,
                            # not necessarily the exact product
                            result.confidence = 0.5
                            result.notes = (
                                f"Data funnet via {domain}. "
                                f"Verifiser at det er riktig produkt f\u00f8r bruk."
                            )
                            return result
                except Exception as e:
                    logger.debug(f"Search error for {domain}: {e}")
                    continue

    result.notes = f"Ingen treff hos kjente produsentnettsted for '{product.manufacturer or 'ukjent'}'"
    return result


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
