"""Manufacturer lookup module - searches for better product data from manufacturer websites."""

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


def _build_search_queries(product: ProductData) -> list[str]:
    """Build search queries for finding manufacturer product pages."""
    queries = []

    manufacturer = product.manufacturer or ""
    mfr_article = product.manufacturer_article_number or ""
    name = product.product_name or ""

    # Priority 1: Manufacturer + manufacturer article number
    if manufacturer and mfr_article:
        queries.append(f"{manufacturer} {mfr_article}")

    # Priority 2: Manufacturer + product name
    if manufacturer and name:
        queries.append(f"{manufacturer} {name}")

    # Priority 3: Product name + "datasheet" or "produktblad"
    if name:
        queries.append(f"{name} datasheet specifications")
        queries.append(f"{name} produktblad spesifikasjoner")

    # Priority 4: Manufacturer article number alone
    if mfr_article:
        queries.append(mfr_article)

    return queries


def _extract_product_info_from_page(html: str, url: str) -> dict:
    """Extract product information from a manufacturer page."""
    soup = BeautifulSoup(html, "lxml")
    info = {}

    # Try to get product name from h1
    h1 = soup.find("h1")
    if h1:
        info["product_name"] = h1.get_text(strip=True)

    # Try to get description from meta tags or content
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc:
        info["description"] = meta_desc.get("content", "")

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


async def search_manufacturer_info(
    product: ProductData,
    search_func=None,
) -> ManufacturerLookup:
    """Search for product information from the manufacturer.

    Args:
        product: The product data with what we know
        search_func: Optional async function for web search (query) -> list of URLs
    """
    result = ManufacturerLookup(searched=True)

    if not product.manufacturer and not product.product_name:
        result.notes = "Ikke nok informasjon for å søke hos produsent"
        return result

    queries = _build_search_queries(product)
    if not queries:
        result.notes = "Kunne ikke bygge søkespørring"
        return result

    # Try to find and fetch manufacturer pages
    async with httpx.AsyncClient(timeout=15) as client:
        for query in queries[:3]:  # Limit to top 3 queries
            try:
                # Use a search engine or direct manufacturer site
                manufacturer = product.manufacturer or ""

                # Try to guess manufacturer domain
                mfr_domains = _guess_manufacturer_domains(manufacturer)

                for domain in mfr_domains:
                    try:
                        # Try searching the manufacturer site
                        search_url = f"https://{domain}/search?q={product.product_name or product.manufacturer_article_number or ''}"
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
                                result.confidence = 0.6
                                result.notes = f"Funnet via produsentens nettside: {domain}"
                                return result
                    except Exception:
                        continue

            except Exception as e:
                logger.debug(f"Search error for query '{query}': {e}")
                continue

    result.notes = "Ingen resultater fra produsentoppslag"
    return result


def _guess_manufacturer_domains(manufacturer_name: str) -> list[str]:
    """Try to guess the manufacturer's website domain."""
    if not manufacturer_name:
        return []

    domains = []
    # Clean the name
    clean = re.sub(r'[^\w\s]', '', manufacturer_name.lower()).strip()
    parts = clean.split()

    if not parts:
        return []

    # Common patterns
    name_joined = "".join(parts)
    name_hyphen = "-".join(parts)

    domains.extend([
        f"www.{name_joined}.com",
        f"www.{name_joined}.no",
        f"www.{name_joined}.se",
        f"www.{name_joined}.dk",
        f"www.{name_hyphen}.com",
        f"{name_joined}.com",
    ])

    # If first word is long enough, try it alone
    if len(parts[0]) > 3:
        domains.append(f"www.{parts[0]}.com")
        domains.append(f"www.{parts[0]}.no")

    # Known manufacturer mappings for medical supplies
    known_manufacturers = {
        "molnlycke": ["www.molnlycke.com"],
        "mölnlycke": ["www.molnlycke.com"],
        "coloplast": ["www.coloplast.com", "www.coloplast.no"],
        "bsn": ["www.bsnmedical.com"],
        "essity": ["www.essity.com"],
        "sca": ["www.essity.com"],
        "tena": ["www.tena.no"],
        "hartmann": ["www.hartmann.info"],
        "paul hartmann": ["www.hartmann.info"],
        "3m": ["www.3m.com"],
        "smith nephew": ["www.smith-nephew.com"],
        "smithnephew": ["www.smith-nephew.com"],
        "convatec": ["www.convatec.com"],
        "medela": ["www.medela.com"],
        "dansac": ["www.dansac.com"],
        "hollister": ["www.hollister.com"],
        "b braun": ["www.bbraun.com", "www.bbraun.no"],
        "bbraun": ["www.bbraun.com"],
        "medline": ["www.medline.com"],
        "cardinal": ["www.cardinalhealth.com"],
        "baxter": ["www.baxter.com"],
        "fresenius": ["www.fresenius.com"],
        "nutricia": ["www.nutricia.no", "www.nutricia.com"],
    }

    for key, urls in known_manufacturers.items():
        if key in clean:
            domains = urls + domains
            break

    return domains[:5]  # Limit attempts


def generate_improvement_suggestions(
    product: ProductData,
    manufacturer_data: ManufacturerLookup,
) -> list[dict]:
    """Generate specific improvement suggestions based on manufacturer data."""
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
                "reason": "Produsentens produktnavn er mer beskrivende",
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
            "reason": "Produsentens beskrivelse er mer utfyllende",
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
                    "reason": f"Ny spesifikasjon fra produsent",
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
