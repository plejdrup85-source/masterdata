"""Webshop readiness evaluation — is a product ready for the online store?

Answers the question: "Can this product be published in the webshop today?"
with a clear ja/nei/delvis verdict and a specific list of blockers.

Criteria are organized into **must-have** (blockers) and **should-have**
(improvements). A product is:

  - **Klar** (ja): all must-haves met, ≤1 should-have missing
  - **Delvis klar** (delvis): all must-haves met OR ≤2 must-haves missing
  - **Ikke klar** (nei): 3+ must-haves missing

Must-have criteria (blockers):
  1. Produktnavn: exists, ≥10 chars, not a placeholder
  2. Beskrivelse: exists, ≥30 chars, in Norwegian, no noise
  3. Kategori: exists
  4. Bilde: at least one usable image
  5. Produsent: known (from any source)

Should-have criteria (improvements):
  6. Spesifikasjon: has ≥1 key-value attribute
  7. Produsentens varenummer: known where manufacturer is known
  8. Beskrivelse: ≥2 sentences, structured
  9. Pakningsinformasjon: has quantity/unit data
"""

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class WebshopStatus(str, Enum):
    """Webshop readiness verdict."""
    READY = "Klar"
    PARTIAL = "Delvis klar"
    NOT_READY = "Ikke klar"


@dataclass
class WebshopBlocker:
    """A single reason why a product is not webshop-ready."""
    field_name: str          # Which field is affected
    criterion: str           # Short description of what's missing
    is_must_have: bool       # True = blocker, False = improvement
    suggestion: str = ""     # What to do about it


@dataclass
class WebshopReadiness:
    """Complete webshop readiness evaluation for a product."""
    status: WebshopStatus = WebshopStatus.NOT_READY
    status_label: str = ""      # Norwegian label
    blockers: list[WebshopBlocker] = field(default_factory=list)
    must_have_met: int = 0      # How many must-haves are satisfied
    must_have_total: int = 5    # Total must-have criteria
    should_have_met: int = 0
    should_have_total: int = 4
    summary: str = ""           # One-line summary for Excel
    missing_list: str = ""      # Comma-separated list of what's missing


# ── Noise patterns (subset — quick check) ──

_NOISE_RE = re.compile(
    r"(?i)(?:tel|telefon|tlf)\s*[.:]?\s*[\+\d\(\)\s\-]{7,}"
    r"|[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
    r"|(?:www\.[a-z0-9\-]+\.[a-z]{2,}|https?://\S+)"
    r"|\bside\s+\d+\b|\bpage\s+\d+\b"
    r"|\bcopyright\b|©"
)

_ENGLISH_HEAVY_RE = re.compile(
    r"(?i)\b(?:the|designed|intended|provides|ensures|available|suitable)\b"
)

_PLACEHOLDER_RE = re.compile(
    r"(?i)^(?:test|placeholder|todo|tbd|n/?a|mangler|\.{3,}|-+|_+)$"
)


# ═══════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════


def evaluate_webshop_readiness(analysis: "ProductAnalysis") -> WebshopReadiness:
    """Evaluate whether a product is ready for the webshop.

    Examines the product data and field analyses to determine readiness.
    Returns a WebshopReadiness with status, blockers, and summary.
    """
    from backend.models import QualityStatus

    result = WebshopReadiness()
    pd = analysis.product_data
    fa_map = {fa.field_name: fa for fa in analysis.field_analyses}
    iq = analysis.image_quality or {}

    # ── Must-have criteria ──

    # 1. Produktnavn
    name = (pd.product_name or "").strip()
    if not name or len(name) < 10 or _PLACEHOLDER_RE.match(name):
        result.blockers.append(WebshopBlocker(
            field_name="Produktnavn",
            criterion="Mangler brukbart produktnavn",
            is_must_have=True,
            suggestion="Legg til et beskrivende produktnavn (minst 10 tegn)",
        ))
    else:
        result.must_have_met += 1

    # 2. Beskrivelse
    desc = (pd.description or "").strip()
    desc_ok = True
    if not desc or len(desc) < 30:
        result.blockers.append(WebshopBlocker(
            field_name="Beskrivelse",
            criterion="Mangler beskrivelse (minst 30 tegn)",
            is_must_have=True,
            suggestion="Legg til en beskrivelse som forklarer produktet for kunden",
        ))
        desc_ok = False
    elif _NOISE_RE.search(desc):
        result.blockers.append(WebshopBlocker(
            field_name="Beskrivelse",
            criterion="Beskrivelsen inneholder støy (kontaktinfo, URL, PDF-metadata)",
            is_must_have=True,
            suggestion="Fjern kontaktinformasjon og PDF-artefakter fra beskrivelsen",
        ))
        desc_ok = False
    elif len(_ENGLISH_HEAVY_RE.findall(desc)) >= 3:
        result.blockers.append(WebshopBlocker(
            field_name="Beskrivelse",
            criterion="Beskrivelsen er på engelsk — nettbutikken krever norsk",
            is_must_have=True,
            suggestion="Oversett beskrivelsen til norsk",
        ))
        desc_ok = False
    else:
        result.must_have_met += 1

    # 3. Kategori
    cat = pd.category or (
        " > ".join(pd.category_breadcrumb) if pd.category_breadcrumb else ""
    )
    if not cat.strip():
        result.blockers.append(WebshopBlocker(
            field_name="Kategori",
            criterion="Mangler produktkategori",
            is_must_have=True,
            suggestion="Tilordne produktet en kategori i kategorihierarkiet",
        ))
    else:
        result.must_have_met += 1

    # 4. Bilde — enhanced with e-commerce suitability
    img_count = iq.get("image_count_found", 0)
    img_status = iq.get("image_quality_status", "MISSING")
    main_is_product = iq.get("main_is_product", True)
    ecom_score = iq.get("ecommerce_suitability_avg", 0)
    if img_count == 0 or img_status == "MISSING":
        result.blockers.append(WebshopBlocker(
            field_name="Bildekvalitet",
            criterion="Mangler produktbilde",
            is_must_have=True,
            suggestion="Last opp minst ett produktbilde",
        ))
    elif img_status == "FAIL":
        result.blockers.append(WebshopBlocker(
            field_name="Bildekvalitet",
            criterion="Produktbildet har for dårlig kvalitet",
            is_must_have=True,
            suggestion="Erstatt bildet med et bilde av høyere kvalitet",
        ))
    elif not main_is_product:
        result.blockers.append(WebshopBlocker(
            field_name="Bildekvalitet",
            criterion="Hovedbildet er ikke et produktbilde (logo/plassholder/ikon)",
            is_must_have=True,
            suggestion="Erstatt med et ekte produktfoto",
        ))
    else:
        result.must_have_met += 1
        # E-commerce suitability as should-have
        if ecom_score > 0 and ecom_score < 50:
            result.blockers.append(WebshopBlocker(
                field_name="Bildekvalitet",
                criterion=f"Bildet har lav e-commerce-egnethet ({round(ecom_score)}/100)",
                is_must_have=False,
                suggestion="Forbedre bakgrunn, utsnitt eller bildekvalitet",
            ))
        else:
            # Count as should-have met if ecom score is decent
            pass

    # 5. Produsent
    mfr = (pd.manufacturer or "").strip()
    # Also check Jeeves supplier as fallback
    jeeves_supplier = ""
    if analysis.jeeves_data and analysis.jeeves_data.supplier:
        jeeves_supplier = analysis.jeeves_data.supplier.strip()
    if not mfr and not jeeves_supplier:
        result.blockers.append(WebshopBlocker(
            field_name="Produsent",
            criterion="Produsent er ukjent",
            is_must_have=True,
            suggestion="Identifiser og registrer produsenten",
        ))
    else:
        result.must_have_met += 1

    # ── Should-have criteria ──

    # 6. Spesifikasjon: ≥1 key-value attribute
    spec = pd.specification or ""
    tech = pd.technical_details or {}
    has_spec = bool(tech) or bool(re.search(r"\w+\s*:\s*\S+", spec))
    if not has_spec:
        result.blockers.append(WebshopBlocker(
            field_name="Spesifikasjon",
            criterion="Mangler tekniske spesifikasjoner",
            is_must_have=False,
            suggestion="Legg til minst ett teknisk attributt (f.eks. materiale, størrelse)",
        ))
    else:
        result.should_have_met += 1

    # 7. Produsentens varenummer (only if manufacturer is known)
    mfr_artno = (pd.manufacturer_article_number or "").strip()
    if (mfr or jeeves_supplier) and not mfr_artno:
        result.blockers.append(WebshopBlocker(
            field_name="Produsentens varenummer",
            criterion="Mangler produsentens varenummer",
            is_must_have=False,
            suggestion="Finn og registrer produsentens artikkelnummer",
        ))
    else:
        result.should_have_met += 1

    # 8. Beskrivelse quality: ≥2 sentences
    if desc_ok and desc:
        sentence_count = len(re.findall(r"[.!?]\s", desc + " "))
        if sentence_count < 2:
            result.blockers.append(WebshopBlocker(
                field_name="Beskrivelse",
                criterion="Beskrivelsen er kort — bør ha minst 2 setninger",
                is_must_have=False,
                suggestion="Utvid beskrivelsen med mer produktinformasjon",
            ))
        else:
            result.should_have_met += 1
    else:
        # Already a must-have blocker, don't double-count
        pass

    # 9. Pakningsinformasjon
    pkg = (pd.packaging_info or pd.packaging_unit or "").strip()
    has_pkg = bool(pkg) and bool(re.search(r"\d+\s*(?:stk|pk|per|pr|x)", pkg, re.I))
    if not has_pkg:
        result.blockers.append(WebshopBlocker(
            field_name="Pakningsinformasjon",
            criterion="Mangler pakningsinformasjon med antall/enhet",
            is_must_have=False,
            suggestion="Legg til pakningsstørrelse (f.eks. '100 stk/eske')",
        ))
    else:
        result.should_have_met += 1

    # ── Determine status ──
    must_missing = result.must_have_total - result.must_have_met
    should_missing = result.should_have_total - result.should_have_met

    if must_missing == 0 and should_missing <= 1:
        result.status = WebshopStatus.READY
    elif must_missing <= 2:
        result.status = WebshopStatus.PARTIAL
    else:
        result.status = WebshopStatus.NOT_READY

    result.status_label = result.status.value

    # ── Build summary ──
    must_blockers = [b for b in result.blockers if b.is_must_have]
    should_blockers = [b for b in result.blockers if not b.is_must_have]

    if result.status == WebshopStatus.READY:
        result.summary = (
            f"Nettbutikkklar ({result.must_have_met}/{result.must_have_total} påkrevde, "
            f"{result.should_have_met}/{result.should_have_total} anbefalte)"
        )
    elif result.status == WebshopStatus.PARTIAL:
        result.summary = (
            f"Delvis klar — {must_missing} påkrevd(e) mangler: "
            + ", ".join(b.field_name for b in must_blockers)
        )
    else:
        result.summary = (
            f"Ikke klar — {must_missing} påkrevd(e) mangler: "
            + ", ".join(b.field_name for b in must_blockers)
        )

    # Missing list (all blockers)
    all_missing = [b.field_name + (" (påkrevd)" if b.is_must_have else "")
                   for b in result.blockers]
    result.missing_list = ", ".join(all_missing) if all_missing else "Ingen mangler"

    return result


def get_missing_for_webshop(analysis: "ProductAnalysis") -> list[str]:
    """Return a list of missing items preventing webshop readiness.

    Convenience function returning just the blocker descriptions.
    """
    readiness = evaluate_webshop_readiness(analysis)
    return [b.criterion for b in readiness.blockers]


def summarize_webshop_blockers(analysis: "ProductAnalysis") -> str:
    """Return a one-line summary of webshop blockers.

    Suitable for an Excel cell or API response.
    """
    readiness = evaluate_webshop_readiness(analysis)
    return readiness.summary
