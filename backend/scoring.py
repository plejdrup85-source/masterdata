"""Centralized scoring framework for masterdata quality assessment.

Provides configurable per-area evaluators with transparent scoring logic.
Each evaluator returns a structured AreaScore with score, status, issues,
and human-readable explanation.

Usage:
    from backend.scoring import score_product_areas, AREA_DESCRIPTION, AREA_SPECIFICATION, ...
    scores = score_product_areas(product_data, jeeves_data, image_quality, areas=None)
    # areas=None → all areas; areas=["description", "images"] → only those

All thresholds and weights are centralized here for easy tuning.
"""

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


# ── Area identifiers ──

AREA_DESCRIPTION = "description"
AREA_SPECIFICATION = "specification"
AREA_IMAGES = "images"
AREA_ATTRIBUTES = "attributes"
AREA_DOCUMENTS = "documents"
AREA_PRODUCT_NAME = "product_name"
AREA_MANUFACTURER = "manufacturer"
AREA_CATEGORY = "category"
AREA_PACKAGING = "packaging"

ALL_AREAS = [
    AREA_PRODUCT_NAME,
    AREA_DESCRIPTION,
    AREA_SPECIFICATION,
    AREA_IMAGES,
    AREA_ATTRIBUTES,
    AREA_DOCUMENTS,
    AREA_MANUFACTURER,
    AREA_CATEGORY,
    AREA_PACKAGING,
]

# Human-readable labels (Norwegian)
AREA_LABELS = {
    AREA_PRODUCT_NAME: "Produktnavn",
    AREA_DESCRIPTION: "Beskrivelse",
    AREA_SPECIFICATION: "Spesifikasjon",
    AREA_IMAGES: "Bilder",
    AREA_ATTRIBUTES: "Attributter",
    AREA_DOCUMENTS: "Dokumentasjon",
    AREA_MANUFACTURER: "Produsent",
    AREA_CATEGORY: "Kategori",
    AREA_PACKAGING: "Pakningsinformasjon",
}

# User-selectable focus areas (subset exposed in UI)
FOCUS_AREAS = [
    AREA_DESCRIPTION,
    AREA_SPECIFICATION,
    AREA_IMAGES,
    AREA_ATTRIBUTES,
    AREA_DOCUMENTS,
]


class AreaStatus(str, Enum):
    """Quality status for a scored area."""
    OK = "OK"
    WEAK = "Svak"
    MISSING = "Mangler"
    NEEDS_IMPROVEMENT = "Bør forbedres"


class Severity(str, Enum):
    """Issue severity for prioritization."""
    CRITICAL = "Kritisk"
    HIGH = "Høy"
    MEDIUM = "Middels"
    LOW = "Lav"


@dataclass
class AreaIssue:
    """A specific quality issue found in an area."""
    description: str
    severity: Severity = Severity.MEDIUM
    metric: Optional[str] = None  # e.g. "length=12" for traceability


@dataclass
class AreaScore:
    """Structured scoring result for one masterdata area."""
    area: str
    area_label: str
    score: float  # 0-100
    status: AreaStatus
    issues: list[AreaIssue] = field(default_factory=list)
    missing_elements: list[str] = field(default_factory=list)
    explanation: str = ""
    recommended_action: str = ""  # e.g. "Needs better description"

    def to_dict(self) -> dict:
        return {
            "area": self.area,
            "area_label": self.area_label,
            "score": round(self.score, 1),
            "status": self.status.value,
            "issues": [
                {"description": i.description, "severity": i.severity.value, "metric": i.metric}
                for i in self.issues
            ],
            "missing_elements": self.missing_elements,
            "explanation": self.explanation,
            "recommended_action": self.recommended_action,
        }


@dataclass
class ProductScoreResult:
    """Complete scoring result for a product across all evaluated areas."""
    article_number: str
    product_name: Optional[str]
    overall_score: float  # 0-100, weighted average
    overall_severity: Severity
    area_scores: list[AreaScore] = field(default_factory=list)
    missing_areas: list[str] = field(default_factory=list)
    issue_summary: str = ""

    def to_dict(self) -> dict:
        return {
            "article_number": self.article_number,
            "product_name": self.product_name,
            "overall_score": round(self.overall_score, 1),
            "overall_severity": self.overall_severity.value,
            "area_scores": [a.to_dict() for a in self.area_scores],
            "missing_areas": self.missing_areas,
            "issue_summary": self.issue_summary,
        }


# ── Configurable thresholds and weights ──

# Weights for overall score calculation (must sum to meaningful relative values)
AREA_WEIGHTS = {
    AREA_PRODUCT_NAME: 2.0,
    AREA_DESCRIPTION: 2.0,
    AREA_SPECIFICATION: 2.0,
    AREA_IMAGES: 1.5,
    AREA_ATTRIBUTES: 1.0,
    AREA_DOCUMENTS: 1.0,
    AREA_MANUFACTURER: 1.5,
    AREA_CATEGORY: 0.75,
    AREA_PACKAGING: 0.75,
}

# Score thresholds for status classification
STATUS_THRESHOLDS = {
    "ok": 75,        # >= 75 → OK
    "weak": 40,      # >= 40 → Weak
    # < 40 → Missing or Needs Improvement
}

# Severity thresholds (overall score)
SEVERITY_THRESHOLDS = {
    "critical": 25,   # < 25
    "high": 50,       # < 50
    "medium": 75,     # < 75
    # >= 75 → Low
}

# Description thresholds
DESC_MIN_LENGTH = 20
DESC_GOOD_LENGTH = 80
DESC_EXCELLENT_LENGTH = 200

# Specification thresholds
SPEC_MIN_ATTRIBUTES = 2
SPEC_GOOD_ATTRIBUTES = 4

# Image thresholds
IMG_MIN_COUNT = 1
IMG_GOOD_COUNT = 2

# Common placeholder values to detect
PLACEHOLDER_VALUES = {
    "ukjent", "unknown", "n/a", "-", ".", "na", "ingen", "test",
    "todo", "tbd", "xxx", "placeholder",
}

# Technical keywords indicating real specification content
SPEC_KEYWORDS = {
    "mm", "cm", "m", "ml", "l", "g", "kg", "stk", "pk", "µm",
    "størrelse", "size", "materiale", "material", "farge", "color",
    "vekt", "weight", "lengde", "length", "bredde", "width",
    "høyde", "height", "tykkelse", "thickness", "diameter",
    "latex", "nitril", "vinyl", "polyester", "bomull", "cotton",
    "steril", "sterile", "usteril", "non-sterile",
    "engangs", "disposable", "flergangs", "reusable",
}


# ── Per-area evaluators ──

def _score_description(product_data, jeeves_data) -> AreaScore:
    """Evaluate description quality."""
    desc = product_data.description
    jeeves_desc = jeeves_data.web_text if jeeves_data else None
    effective = desc or jeeves_desc

    result = AreaScore(
        area=AREA_DESCRIPTION,
        area_label=AREA_LABELS[AREA_DESCRIPTION],
        score=0.0,
        status=AreaStatus.MISSING,
    )

    if not effective:
        result.missing_elements = ["Beskrivelse"]
        result.explanation = "Beskrivelse mangler helt"
        result.recommended_action = "Trenger beskrivelse"
        result.issues.append(AreaIssue("Ingen beskrivelse funnet", Severity.HIGH))
        return result

    score = 0.0
    issues = []

    # Length scoring (0-30 points)
    length = len(effective)
    if length < DESC_MIN_LENGTH:
        score += 5
        issues.append(AreaIssue(
            f"For kort ({length} tegn, minimum {DESC_MIN_LENGTH})",
            Severity.HIGH, f"length={length}",
        ))
    elif length < DESC_GOOD_LENGTH:
        score += 15
        issues.append(AreaIssue(
            f"Kort beskrivelse ({length} tegn)",
            Severity.MEDIUM, f"length={length}",
        ))
    elif length < DESC_EXCELLENT_LENGTH:
        score += 25
    else:
        score += 30

    # Sentence structure (0-25 points)
    sentences = [s.strip() for s in re.split(r'[.!?]', effective) if len(s.strip()) > 10]
    if len(sentences) >= 3:
        score += 25
    elif len(sentences) >= 2:
        score += 20
    elif len(sentences) == 1:
        score += 10
        issues.append(AreaIssue("Kun én setning", Severity.LOW))
    else:
        score += 5
        issues.append(AreaIssue("Ingen fullstendige setninger", Severity.MEDIUM))

    # Content quality (0-25 points)
    if product_data.product_name and effective.strip().lower() == product_data.product_name.strip().lower():
        issues.append(AreaIssue("Identisk med produktnavn", Severity.HIGH))
        score += 0
    elif product_data.article_number and product_data.article_number in effective and length < 50:
        issues.append(AreaIssue("Inneholder hovedsakelig artikkelnummer", Severity.HIGH))
        score += 5
    elif _is_placeholder(effective):
        issues.append(AreaIssue("Ser ut som placeholder-tekst", Severity.HIGH))
        score += 5
    else:
        # Check information density - has technical/descriptive keywords?
        tech_count = sum(1 for kw in SPEC_KEYWORDS if kw in effective.lower())
        if tech_count >= 3:
            score += 25
        elif tech_count >= 1:
            score += 20
        else:
            score += 15

    # Source quality (0-20 points)
    if desc and jeeves_desc:
        score += 20  # Both sources
    elif desc:
        score += 15  # Website only
    elif jeeves_desc:
        score += 10  # Jeeves only
        issues.append(AreaIssue("Kun fra Jeeves, mangler på nettside", Severity.LOW))

    result.score = min(100, score)
    result.issues = issues
    result.status = _status_from_score(result.score)
    result.explanation = _build_explanation("Beskrivelse", result.score, issues)
    if result.status != AreaStatus.OK:
        result.recommended_action = "Trenger bedre beskrivelse"
    return result


def _score_specification(product_data, jeeves_data) -> AreaScore:
    """Evaluate specification quality."""
    spec = product_data.specification
    details = product_data.technical_details
    jeeves_spec = jeeves_data.specification if jeeves_data else None

    result = AreaScore(
        area=AREA_SPECIFICATION,
        area_label=AREA_LABELS[AREA_SPECIFICATION],
        score=0.0,
        status=AreaStatus.MISSING,
    )

    has_any = bool(spec) or bool(details) or bool(jeeves_spec)
    if not has_any:
        result.missing_elements = ["Spesifikasjon", "Tekniske detaljer"]
        result.explanation = "Spesifikasjoner mangler helt"
        result.recommended_action = "Trenger spesifikasjoner"
        result.issues.append(AreaIssue("Ingen spesifikasjoner funnet", Severity.HIGH))
        return result

    score = 0.0
    issues = []

    # Attribute count (0-35 points)
    attr_count = 0
    if details:
        attr_count = len(details)
    elif spec:
        attr_count = spec.count(";") + spec.count("\n") + 1

    if attr_count >= SPEC_GOOD_ATTRIBUTES:
        score += 35
    elif attr_count >= SPEC_MIN_ATTRIBUTES:
        score += 25
    elif attr_count == 1:
        score += 15
        issues.append(AreaIssue(f"Kun {attr_count} attributt", Severity.MEDIUM))
    else:
        score += 5
        issues.append(AreaIssue("Svært få strukturerte attributter", Severity.HIGH))

    # Technical content quality (0-30 points)
    all_text = (spec or "") + " " + " ".join(f"{k}: {v}" for k, v in (details or {}).items())
    tech_count = sum(1 for kw in SPEC_KEYWORDS if kw in all_text.lower())
    unit_matches = len(re.findall(r'\d+[\.,]?\d*\s*(?:mm|cm|m|ml|l|g|kg|stk|pk|µm|%)', all_text.lower()))

    if tech_count >= 4 and unit_matches >= 2:
        score += 30
    elif tech_count >= 2 or unit_matches >= 1:
        score += 20
    elif tech_count >= 1:
        score += 10
    else:
        score += 5
        issues.append(AreaIssue("Mangler teknisk/målbart innhold", Severity.MEDIUM))

    # Key fields presence (0-20 points)
    expected_keys = {"størrelse", "size", "materiale", "material", "farge", "color", "vekt", "weight"}
    if details:
        found_keys = {k.lower() for k in details.keys()} & expected_keys
        if len(found_keys) >= 2:
            score += 20
        elif len(found_keys) >= 1:
            score += 10
        else:
            score += 5
            missing = expected_keys - {k.lower() for k in details.keys()}
            result.missing_elements = [k.title() for k in list(missing)[:3]]
    else:
        score += 5

    # Duplication check (0-15 points)
    if spec and product_data.product_name and spec.strip().lower() == product_data.product_name.strip().lower():
        issues.append(AreaIssue("Identisk med produktnavn", Severity.HIGH))
    elif spec and product_data.description and spec.strip().lower() == product_data.description.strip().lower():
        issues.append(AreaIssue("Identisk med beskrivelse", Severity.MEDIUM))
    else:
        score += 15

    result.score = min(100, score)
    result.issues = issues
    result.status = _status_from_score(result.score)
    result.explanation = _build_explanation("Spesifikasjon", result.score, issues)
    if result.status != AreaStatus.OK:
        result.recommended_action = "Trenger fullstendige spesifikasjoner"
    return result


def _score_images(product_data, image_quality) -> AreaScore:
    """Evaluate image quality and completeness."""
    result = AreaScore(
        area=AREA_IMAGES,
        area_label=AREA_LABELS[AREA_IMAGES],
        score=0.0,
        status=AreaStatus.MISSING,
    )

    if not image_quality:
        # Fallback: check basic image URL presence
        if product_data.image_url:
            result.score = 40.0
            result.status = AreaStatus.WEAK
            result.explanation = "Bilde finnes men er ikke kvalitetsvurdert"
            return result
        result.missing_elements = ["Hovedbilde"]
        result.explanation = "Produktbilde mangler"
        result.recommended_action = "Trenger produktbilde"
        result.issues.append(AreaIssue("Ingen bilder funnet", Severity.HIGH))
        return result

    score = 0.0
    issues = []

    main_exists = image_quality.get("main_image_exists", False)
    img_count = image_quality.get("image_count_found", 0)
    avg_score = image_quality.get("avg_image_score", 0)
    img_status = image_quality.get("image_quality_status", "MISSING")
    issue_summary = image_quality.get("image_issue_summary", "")

    # Main image presence (0-30 points)
    if not main_exists:
        result.missing_elements = ["Hovedbilde"]
        result.issues.append(AreaIssue("Hovedbilde mangler", Severity.CRITICAL))
        result.explanation = "Hovedbilde mangler"
        result.recommended_action = "Trenger produktbilde"
        return result
    score += 30

    # Image count (0-20 points)
    if img_count >= IMG_GOOD_COUNT:
        score += 20
    elif img_count >= IMG_MIN_COUNT:
        score += 10
        issues.append(AreaIssue(f"Kun {img_count} bilde(r), anbefalt minst {IMG_GOOD_COUNT}", Severity.LOW))

    # Image quality score from CV analysis (0-50 points)
    if avg_score >= 80:
        score += 50
    elif avg_score >= 60:
        score += 40
    elif avg_score >= 40:
        score += 25
        issues.append(AreaIssue(f"Middels bildekvalitet (score {avg_score:.0f})", Severity.MEDIUM))
    else:
        score += 10
        issues.append(AreaIssue(f"Lav bildekvalitet (score {avg_score:.0f})", Severity.HIGH))

    if issue_summary:
        issues.append(AreaIssue(issue_summary, Severity.MEDIUM))

    result.score = min(100, score)
    result.issues = issues
    result.status = _status_from_score(result.score)
    result.explanation = _build_explanation("Bilder", result.score, issues)
    if result.status != AreaStatus.OK:
        result.recommended_action = "Trenger bedre bilder"
    return result


def _score_attributes(product_data, jeeves_data) -> AreaScore:
    """Evaluate structured attribute completeness."""
    result = AreaScore(
        area=AREA_ATTRIBUTES,
        area_label=AREA_LABELS[AREA_ATTRIBUTES],
        score=0.0,
        status=AreaStatus.MISSING,
    )

    details = product_data.technical_details or {}
    has_brand = bool(jeeves_data and jeeves_data.product_brand)
    has_packaging = bool(product_data.packaging_info or product_data.packaging_unit)
    has_category = bool(product_data.category or (product_data.category_breadcrumb and len(product_data.category_breadcrumb) > 0))
    has_manufacturer = bool(product_data.manufacturer or (jeeves_data and jeeves_data.supplier))

    score = 0.0
    issues = []
    missing = []

    # Structured technical details (0-40 points)
    attr_count = len(details)
    if attr_count >= 5:
        score += 40
    elif attr_count >= 3:
        score += 30
    elif attr_count >= 1:
        score += 15
        issues.append(AreaIssue(f"Kun {attr_count} tekniske attributter", Severity.MEDIUM))
    else:
        missing.append("Tekniske detaljer")
        issues.append(AreaIssue("Ingen strukturerte tekniske detaljer", Severity.HIGH))

    # Key metadata fields (0-60 points, 15 each)
    if has_brand:
        score += 15
    else:
        missing.append("Merkevare")
        issues.append(AreaIssue("Merkevare mangler", Severity.LOW))

    if has_packaging:
        score += 15
    else:
        missing.append("Pakningsinformasjon")
        issues.append(AreaIssue("Pakningsinformasjon mangler", Severity.MEDIUM))

    if has_category:
        score += 15
        # Check depth
        if product_data.category_breadcrumb and len(product_data.category_breadcrumb) < 2:
            issues.append(AreaIssue("Grunt kategorihierarki (kun 1 nivå)", Severity.LOW))
            score -= 5
    else:
        missing.append("Kategori")
        issues.append(AreaIssue("Kategori mangler", Severity.MEDIUM))

    if has_manufacturer:
        score += 15
        # Check for placeholder
        mfr_val = product_data.manufacturer or (jeeves_data.supplier if jeeves_data else None)
        if mfr_val and _is_placeholder(mfr_val):
            issues.append(AreaIssue("Produsent er en placeholder-verdi", Severity.HIGH))
            score -= 10
    else:
        missing.append("Produsent")
        issues.append(AreaIssue("Produsentinformasjon mangler", Severity.HIGH))

    result.score = max(0, min(100, score))
    result.missing_elements = missing
    result.issues = issues
    result.status = _status_from_score(result.score)
    result.explanation = _build_explanation("Attributter", result.score, issues)
    if result.status != AreaStatus.OK:
        result.recommended_action = "Trenger strukturerte attributter"
    return result


def _score_documents(product_data, pdf_available: bool = False) -> AreaScore:
    """Evaluate documentation/product sheet presence."""
    result = AreaScore(
        area=AREA_DOCUMENTS,
        area_label=AREA_LABELS[AREA_DOCUMENTS],
        score=0.0,
        status=AreaStatus.MISSING,
    )

    issues = []

    if pdf_available:
        result.score = 100.0
        result.status = AreaStatus.OK
        result.explanation = "Produktdatablad tilgjengelig"
        return result

    # No PDF found
    result.missing_elements = ["Produktdatablad"]
    result.explanation = "Produktdatablad mangler"
    result.recommended_action = "Trenger produktdatablad"
    issues.append(AreaIssue("Produktdatablad ikke funnet", Severity.MEDIUM))
    result.issues = issues
    return result


def _score_product_name(product_data, jeeves_data) -> AreaScore:
    """Evaluate product name quality."""
    name = product_data.product_name
    jeeves_name = (jeeves_data.item_description or jeeves_data.web_title) if jeeves_data else None
    effective = name or jeeves_name

    result = AreaScore(
        area=AREA_PRODUCT_NAME,
        area_label=AREA_LABELS[AREA_PRODUCT_NAME],
        score=0.0,
        status=AreaStatus.MISSING,
    )

    if not effective:
        result.missing_elements = ["Produktnavn"]
        result.explanation = "Produktnavn mangler"
        result.recommended_action = "Trenger produktnavn"
        result.issues.append(AreaIssue("Produktnavn mangler", Severity.CRITICAL))
        return result

    score = 0.0
    issues = []

    # Length (0-30)
    if len(effective) < 5:
        score += 10
        issues.append(AreaIssue(f"For kort ({len(effective)} tegn)", Severity.HIGH))
    elif len(effective) < 15:
        score += 20
    else:
        score += 30

    # Not generic (0-25)
    if effective.lower().strip() in PLACEHOLDER_VALUES:
        issues.append(AreaIssue("Generisk/placeholder-navn", Severity.HIGH))
    elif re.match(r"^[\d\s\-/]+$", effective):
        issues.append(AreaIssue("Kun tall/tegn, mangler beskrivende tekst", Severity.HIGH))
    else:
        score += 25

    # Not ALL CAPS (0-15)
    if effective == effective.upper() and len(effective) > 3:
        issues.append(AreaIssue("STORE BOKSTAVER", Severity.LOW))
        score += 5
    else:
        score += 15

    # Source presence (0-30)
    if name and jeeves_name:
        score += 30
    elif name:
        score += 25
    elif jeeves_name:
        score += 15

    result.score = min(100, score)
    result.issues = issues
    result.status = _status_from_score(result.score)
    result.explanation = _build_explanation("Produktnavn", result.score, issues)
    if result.status != AreaStatus.OK:
        result.recommended_action = "Trenger bedre produktnavn"
    return result


def _score_manufacturer_area(product_data, jeeves_data) -> AreaScore:
    """Evaluate manufacturer information."""
    mfr = product_data.manufacturer
    jeeves_supplier = jeeves_data.supplier if jeeves_data else None
    effective = mfr or jeeves_supplier

    result = AreaScore(
        area=AREA_MANUFACTURER,
        area_label=AREA_LABELS[AREA_MANUFACTURER],
        score=0.0,
        status=AreaStatus.MISSING,
    )

    if not effective:
        result.missing_elements = ["Produsent"]
        result.explanation = "Produsentinformasjon mangler"
        result.recommended_action = "Trenger produsentinformasjon"
        result.issues.append(AreaIssue("Produsent mangler", Severity.HIGH))
        return result

    score = 50.0  # Base for having a value
    issues = []

    if _is_placeholder(effective):
        issues.append(AreaIssue("Placeholder-verdi", Severity.HIGH))
        score = 10.0
    elif len(effective) < 2:
        issues.append(AreaIssue("For kort", Severity.MEDIUM))
        score = 20.0
    else:
        score = 70.0

    # Both sources
    if mfr and jeeves_supplier:
        score = min(100, score + 30)
    elif mfr:
        score = min(100, score + 15)

    # Manufacturer article number
    mfr_num = product_data.manufacturer_article_number
    jeeves_num = jeeves_data.supplier_item_no if jeeves_data else None
    if not mfr_num and not jeeves_num:
        result.missing_elements.append("Produsentens varenummer")
        issues.append(AreaIssue("Produsentens varenummer mangler", Severity.MEDIUM))

    result.score = min(100, score)
    result.issues = issues
    result.status = _status_from_score(result.score)
    result.explanation = _build_explanation("Produsent", result.score, issues)
    if result.status != AreaStatus.OK:
        result.recommended_action = "Trenger produsentinformasjon"
    return result


def _score_category_area(product_data) -> AreaScore:
    """Evaluate category information."""
    cat = product_data.category
    breadcrumbs = product_data.category_breadcrumb

    result = AreaScore(
        area=AREA_CATEGORY,
        area_label=AREA_LABELS[AREA_CATEGORY],
        score=0.0,
        status=AreaStatus.MISSING,
    )

    has_any = bool(cat) or bool(breadcrumbs and len(breadcrumbs) > 0)
    if not has_any:
        result.missing_elements = ["Kategori"]
        result.explanation = "Kategori mangler"
        result.recommended_action = "Trenger kategori"
        result.issues.append(AreaIssue("Kategori mangler", Severity.MEDIUM))
        return result

    score = 50.0
    issues = []

    if breadcrumbs and len(breadcrumbs) >= 3:
        score = 100.0
    elif breadcrumbs and len(breadcrumbs) >= 2:
        score = 85.0
    elif breadcrumbs and len(breadcrumbs) == 1:
        score = 60.0
        issues.append(AreaIssue("Grunt kategorihierarki", Severity.LOW))
    elif cat:
        if _is_placeholder(cat):
            score = 15.0
            issues.append(AreaIssue("Placeholder-kategori", Severity.MEDIUM))
        else:
            score = 70.0

    result.score = score
    result.issues = issues
    result.status = _status_from_score(result.score)
    result.explanation = _build_explanation("Kategori", result.score, issues)
    if result.status != AreaStatus.OK:
        result.recommended_action = "Trenger bedre kategorisering"
    return result


def _score_packaging_area(product_data) -> AreaScore:
    """Evaluate packaging information."""
    pkg = product_data.packaging_info or product_data.packaging_unit

    result = AreaScore(
        area=AREA_PACKAGING,
        area_label=AREA_LABELS[AREA_PACKAGING],
        score=0.0,
        status=AreaStatus.MISSING,
    )

    if not pkg:
        result.missing_elements = ["Pakningsinformasjon"]
        result.explanation = "Pakningsinformasjon mangler"
        result.recommended_action = "Trenger pakningsinformasjon"
        result.issues.append(AreaIssue("Pakningsinformasjon mangler", Severity.MEDIUM))
        return result

    result.score = 100.0
    result.status = AreaStatus.OK
    result.explanation = "Pakningsinformasjon tilgjengelig"
    return result


# ── Evaluator registry ──

_EVALUATORS = {
    AREA_PRODUCT_NAME: lambda pd, jd, iq, pdf: _score_product_name(pd, jd),
    AREA_DESCRIPTION: lambda pd, jd, iq, pdf: _score_description(pd, jd),
    AREA_SPECIFICATION: lambda pd, jd, iq, pdf: _score_specification(pd, jd),
    AREA_IMAGES: lambda pd, jd, iq, pdf: _score_images(pd, iq),
    AREA_ATTRIBUTES: lambda pd, jd, iq, pdf: _score_attributes(pd, jd),
    AREA_DOCUMENTS: lambda pd, jd, iq, pdf: _score_documents(pd, pdf),
    AREA_MANUFACTURER: lambda pd, jd, iq, pdf: _score_manufacturer_area(pd, jd),
    AREA_CATEGORY: lambda pd, jd, iq, pdf: _score_category_area(pd),
    AREA_PACKAGING: lambda pd, jd, iq, pdf: _score_packaging_area(pd),
}


# ── Main scoring entry point ──

def score_product_areas(
    product_data,
    jeeves_data=None,
    image_quality: Optional[dict] = None,
    pdf_available: bool = False,
    areas: Optional[list[str]] = None,
) -> ProductScoreResult:
    """Score a product across specified areas (or all areas if None).

    Args:
        product_data: ProductData from scraper
        jeeves_data: Optional JeevesData from ERP
        image_quality: Optional dict from image analyzer
        pdf_available: Whether product datasheet PDF was found
        areas: List of area identifiers to evaluate, or None for all

    Returns:
        ProductScoreResult with per-area scores and weighted overall score.
    """
    target_areas = areas if areas else ALL_AREAS

    area_scores = []
    missing_areas = []

    for area in target_areas:
        evaluator = _EVALUATORS.get(area)
        if not evaluator:
            logger.warning(f"Unknown area '{area}', skipping")
            continue
        area_score = evaluator(product_data, jeeves_data, image_quality, pdf_available)
        area_scores.append(area_score)
        if area_score.status == AreaStatus.MISSING:
            missing_areas.append(area_score.area_label)

    # Calculate weighted overall score
    weighted_sum = 0.0
    total_weight = 0.0
    for a_score in area_scores:
        weight = AREA_WEIGHTS.get(a_score.area, 1.0)
        weighted_sum += a_score.score * weight
        total_weight += weight

    overall_score = round(weighted_sum / total_weight, 1) if total_weight > 0 else 0.0

    # Determine overall severity
    if overall_score < SEVERITY_THRESHOLDS["critical"]:
        severity = Severity.CRITICAL
    elif overall_score < SEVERITY_THRESHOLDS["high"]:
        severity = Severity.HIGH
    elif overall_score < SEVERITY_THRESHOLDS["medium"]:
        severity = Severity.MEDIUM
    else:
        severity = Severity.LOW

    # Build issue summary
    total_issues = sum(len(a.issues) for a in area_scores)
    high_issues = sum(
        1 for a in area_scores
        for i in a.issues
        if i.severity in (Severity.CRITICAL, Severity.HIGH)
    )
    summary_parts = []
    if missing_areas:
        summary_parts.append(f"{len(missing_areas)} områder mangler")
    if high_issues:
        summary_parts.append(f"{high_issues} alvorlige problemer")
    if total_issues - high_issues > 0:
        summary_parts.append(f"{total_issues - high_issues} øvrige problemer")
    if not summary_parts:
        summary_parts.append("Ingen vesentlige problemer funnet")

    return ProductScoreResult(
        article_number=product_data.article_number,
        product_name=product_data.product_name,
        overall_score=overall_score,
        overall_severity=severity,
        area_scores=area_scores,
        missing_areas=missing_areas,
        issue_summary=". ".join(summary_parts),
    )


# ── Helpers ──

def _is_placeholder(text: str) -> bool:
    """Check if text is a placeholder value."""
    if not text:
        return False
    return text.lower().strip() in PLACEHOLDER_VALUES


def _status_from_score(score: float) -> AreaStatus:
    """Derive area status from numeric score."""
    if score >= STATUS_THRESHOLDS["ok"]:
        return AreaStatus.OK
    elif score >= STATUS_THRESHOLDS["weak"]:
        return AreaStatus.WEAK
    elif score > 0:
        return AreaStatus.NEEDS_IMPROVEMENT
    else:
        return AreaStatus.MISSING


def _build_explanation(area_name: str, score: float, issues: list[AreaIssue]) -> str:
    """Build human-readable explanation for a score."""
    if not issues:
        return f"{area_name} er god (score {score:.0f}/100)"
    issue_texts = [i.description for i in issues[:3]]
    return f"{area_name} score {score:.0f}/100: {'; '.join(issue_texts)}"
