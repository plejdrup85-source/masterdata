"""Data models for the masterdata quality check application."""

import time
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class QualityStatus(str, Enum):
    """Field quality classification — drives suggestions and user prioritization.

    Each status answers a distinct question for the user:
      "What is the state of this field, and what action (if any) is needed?"

    Hierarchy (best → worst, for overall scoring):
      STRONG → OK → IMPROVEMENT_READY → WEAK → SOURCE_CONFLICT →
      SHOULD_IMPROVE → PROBABLE_ERROR → MISSING →
      NO_RELIABLE_SOURCE → MANUAL_REVIEW → REQUIRES_MANUFACTURER

    Suggestion policy:
      STRONG/OK: no suggestion
      IMPROVEMENT_READY: suggestion already attached, user just approves/rejects
      WEAK: suggestion only if materially better source found
      SOURCE_CONFLICT: flag conflict, let user choose
      SHOULD_IMPROVE/PROBABLE_ERROR/MISSING: suggestion if any evidence available
      NO_RELIABLE_SOURCE: no suggestion possible, flag for manual action
      MANUAL_REVIEW: ambiguous, human must decide
      REQUIRES_MANUFACTURER: always flag for manufacturer contact
    """
    # ── Good states (no action needed) ──
    STRONG = "Sterk"                            # Present, well-structured, rich content
    OK = "OK"                                   # Acceptable quality, no action needed

    # ── Actionable states (specific action available) ──
    IMPROVEMENT_READY = "Forbedring klar"       # Value exists, but enrichment found a better version
    WEAK = "Svak"                               # Present but thin/short/minimal
    SOURCE_CONFLICT = "Avvik fra kilde"         # Value differs between sources (website vs Jeeves vs PDF)
    SHOULD_IMPROVE = "B\u00f8r forbedres"       # Clear quality issues (formatting, language, structure)

    # ── Problem states (issue needs resolution) ──
    PROBABLE_ERROR = "Sannsynlig feil"          # Likely incorrect data (placeholder, gibberish)
    MISSING = "Mangler"                         # Field is completely empty/absent

    # ── Blocked states (cannot resolve automatically) ──
    NO_RELIABLE_SOURCE = "Ingen sikker kilde"   # No reference data available to evaluate against
    MANUAL_REVIEW = "Manuell vurdering"         # Ambiguous/conflicting signals — human must decide
    REQUIRES_MANUFACTURER = "Krever produsent"  # Cannot resolve without manufacturer contact


class VerificationStatus(str, Enum):
    """How confidently a product's identity was verified on the website.

    Used to prevent false-positive identity matches for medical products.
    False negatives (marking as unverified) are always preferable to
    false positives (confirming wrong product identity).
    """
    EXACT_MATCH = "eksakt_treff"           # SKU in JSON-LD matches exactly
    NORMALIZED_MATCH = "normalisert_treff"  # SKU matches after normalization (e.g., leading N stripped)
    SKU_IN_PAGE = "sku_i_sidetekst"        # Article number found in page text (weaker)
    CDN_ONLY = "kun_cdn"                   # Only CDN image confirmed, no page data
    UNVERIFIED = "ikke_verifisert"         # Cannot verify identity
    MISMATCH = "feil_treff"               # Page SKU contradicts expected article number
    AMBIGUOUS = "tvetydig"                 # Multiple signals conflict

    @staticmethod
    def business_label(status: "VerificationStatus") -> str:
        """Return a business-friendly Norwegian label for the verification status."""
        labels = {
            VerificationStatus.EXACT_MATCH: "Verifisert mot produktside",
            VerificationStatus.NORMALIZED_MATCH: "Verifisert (etter normalisering)",
            VerificationStatus.SKU_IN_PAGE: "Delvis verifisert (artikkelnr funnet i sidetekst)",
            VerificationStatus.CDN_ONLY: "Kun bilde bekreftet — produktside ikke funnet/hentet",
            VerificationStatus.UNVERIFIED: "Ikke verifisert — krever manuell sjekk",
            VerificationStatus.MISMATCH: "Mulig feil produkt — artikkelnr stemmer ikke",
            VerificationStatus.AMBIGUOUS: "Usikker identitet — motstridende signaler",
        }
        return labels.get(status, str(status))

    @staticmethod
    def business_evidence(status: "VerificationStatus", raw_evidence: str | None = None) -> str:
        """Return a business-friendly explanation of the verification evidence.

        Prefers raw_evidence when available (contains product-specific context),
        falls back to standard explanations.
        """
        standard = {
            VerificationStatus.EXACT_MATCH: "Produktidentitet bekreftet: artikkelnummeret stemmer eksakt med produktsiden.",
            VerificationStatus.NORMALIZED_MATCH: "Produktidentitet bekreftet etter normalisering av artikkelnummer.",
            VerificationStatus.SKU_IN_PAGE: "Artikkelnummeret ble funnet i sideteksten, men ikke i produktets strukturerte data. Svakere bevis.",
            VerificationStatus.CDN_ONLY: "Produktbilde ble funnet i bildekatalogen, men ingen produktside med detaljer ble funnet eller hentet. Produktidentiteten er usikker.",
            VerificationStatus.UNVERIFIED: "Produktet kunne ikke verifiseres mot nettstedet. Vurder manuelt om dataene er korrekte.",
            VerificationStatus.MISMATCH: "Artikkelnummeret på produktsiden stemmer IKKE med forventet artikkel. Dataene kan tilhøre feil produkt.",
            VerificationStatus.AMBIGUOUS: "Motstridende signaler gjør det uklart om dette er riktig produkt. Krever manuell vurdering.",
        }
        # Use raw_evidence when available — it contains product-specific context
        # (e.g., which SKUs were compared, which URLs were checked)
        if raw_evidence and raw_evidence.strip():
            return raw_evidence
        return standard.get(status, raw_evidence or "")


class EnrichmentSourceLevel(str, Enum):
    INTERNAL_PRODUCT_SHEET = "internal_product_sheet"
    MANUFACTURER_SOURCE = "manufacturer_source"


class EnrichmentMatchStatus(str, Enum):
    FOUND_IN_INTERNAL_PDF = "FOUND_IN_INTERNAL_PDF"
    FOUND_IN_MANUFACTURER_SOURCE = "FOUND_IN_MANUFACTURER_SOURCE"
    FOUND_IN_BOTH_MATCH = "FOUND_IN_BOTH_MATCH"
    FOUND_IN_BOTH_CONFLICT = "FOUND_IN_BOTH_CONFLICT"
    NOT_FOUND = "NOT_FOUND"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"


class EnrichmentResult(BaseModel):
    """Enrichment result for a single field from the source-priority pipeline."""
    artnr: str
    field_name: str
    current_value: Optional[str] = None
    suggested_value: Optional[str] = None
    source_level: Optional[str] = None  # EnrichmentSourceLevel value
    source_url: Optional[str] = None
    source_type: Optional[str] = None  # "PDF", "website", etc.
    evidence_snippet: Optional[str] = None
    confidence: float = 0.0
    match_status: str = EnrichmentMatchStatus.NOT_FOUND.value
    review_status: str = "auto"  # "auto", "needs_review", "conflict"


class EnrichmentSuggestion(BaseModel):
    """A single field enrichment suggestion with source traceability.

    Produced by the enrichment engine after consolidating all sources.
    """
    field_name: str  # Norwegian field name (e.g., "Produktnavn")
    current_value: Optional[str] = None
    suggested_value: Optional[str] = None
    original_suggested_value: Optional[str] = None  # Pre-AI-review value for diff trail
    source: Optional[str] = None  # Human-readable source label
    source_url: Optional[str] = None
    evidence: Optional[str] = None  # Human-readable explanation (legacy)
    evidence_structured: Optional[dict] = None  # Machine-readable evidence tags
    confidence: float = 0.0  # 0.0-1.0
    review_required: bool = True  # Must a human verify this?
    ai_modified: bool = False  # True if AI review changed the value


class FieldAnalysis(BaseModel):
    field_name: str
    current_value: Optional[str] = None
    suggested_value: Optional[str] = None
    source: Optional[str] = None
    confidence: Optional[float] = None          # 0-100 composite confidence score
    confidence_details: Optional[str] = None    # Human-readable breakdown of score components
    status: QualityStatus = QualityStatus.OK
    comment: Optional[str] = None
    # P1 FIX: Traceability fields — explain WHY each result exists
    website_value: Optional[str] = None         # Raw value from OneMed scraping
    jeeves_value: Optional[str] = None          # Raw value from Jeeves ERP
    value_origin: Optional[str] = None          # Which source provided current_value
    status_reason: Optional[str] = None         # Why this status was assigned
    suggestion_reason: Optional[str] = None     # Why suggestion was/wasn't created
    suggestion_source: Optional[str] = None     # What evidence supports the suggestion
    # Two-dimensional quality scoring
    content_quality: Optional[int] = None       # 0-100: how well-written for webshop
    content_quality_details: Optional[str] = None
    conformity_quality: Optional[int] = None    # 0-100: how well it matches sources
    conformity_quality_details: Optional[str] = None
    quality_label: Optional[str] = None         # Quadrant label (e.g., "Klar for produksjon")


class JeevesData(BaseModel):
    """Structured base data from the Jeeves ERP Excel export.

    Field mapping from Excel columns:
      Item. No        → article_number (primary key)
      GID             → gid (internal OneMed product ID)
      Item description → item_description (ERP product name)
      Specification   → specification (ERP specification)
      Supplier        → supplier (producer / supplier)
      Supplier Item.no → supplier_item_no (producer item number)
      Product Brand   → product_brand (brand)
      Web Title       → web_title (website title candidate)
      Web Text        → web_text (website description candidate)
    """
    article_number: str
    gid: Optional[str] = None
    item_description: Optional[str] = None
    specification: Optional[str] = None
    supplier: Optional[str] = None
    supplier_item_no: Optional[str] = None
    product_brand: Optional[str] = None
    web_title: Optional[str] = None
    web_text: Optional[str] = None


class ProductData(BaseModel):
    """Raw product data scraped from onemed.no."""
    article_number: str
    product_name: Optional[str] = None
    description: Optional[str] = None
    specification: Optional[str] = None
    manufacturer: Optional[str] = None
    manufacturer_article_number: Optional[str] = None
    category: Optional[str] = None
    category_breadcrumb: Optional[list[str]] = None
    technical_details: Optional[dict[str, str]] = None
    packaging_info: Optional[str] = None
    packaging_unit: Optional[str] = None
    transport_packaging: Optional[str] = None
    image_url: Optional[str] = None
    image_quality_ok: Optional[bool] = None
    product_url: Optional[str] = None
    found_on_onemed: bool = False
    multiple_hits: bool = False
    verification_status: VerificationStatus = VerificationStatus.UNVERIFIED
    verification_evidence: Optional[str] = None  # Human-readable explanation of verification
    error: Optional[str] = None


class ManufacturerLookup(BaseModel):
    """Results from manufacturer lookup."""
    searched: bool = False
    found: bool = False
    source_url: Optional[str] = None
    product_name: Optional[str] = None
    description: Optional[str] = None
    specifications: Optional[dict[str, str]] = None
    datasheet_url: Optional[str] = None
    image_url: Optional[str] = None  # Better product image from manufacturer
    confidence: float = 0.0
    notes: Optional[str] = None


class NorengrosLookup(BaseModel):
    """Results from Norengros secondary reference lookup."""
    searched: bool = False
    found: bool = False
    source_url: Optional[str] = None
    product_name: Optional[str] = None
    description: Optional[str] = None
    specifications: Optional[dict[str, str]] = None
    image_url: Optional[str] = None
    confidence: float = 0.0
    notes: Optional[str] = None


class ImageSuggestion(BaseModel):
    """A suggestion for a better product image from an external source."""
    current_image_url: Optional[str] = None
    current_image_status: str = "unknown"  # ok, missing, low_quality, poor_background
    suggested_image_url: Optional[str] = None
    suggested_source: Optional[str] = None  # "manufacturer", "norengros"
    suggested_source_url: Optional[str] = None
    confidence: float = 0.0
    review_required: bool = True
    reason: Optional[str] = None


class ProductAnalysis(BaseModel):
    """Complete analysis result for a single product."""
    article_number: str
    product_data: ProductData
    jeeves_data: Optional[JeevesData] = None
    manufacturer_lookup: ManufacturerLookup = ManufacturerLookup()
    norengros_lookup: Optional[NorengrosLookup] = None
    field_analyses: list[FieldAnalysis] = []
    total_score: float = 0.0
    overall_status: QualityStatus = QualityStatus.OK
    overall_comment: Optional[str] = None
    auto_fix_possible: bool = False
    manual_review_needed: bool = False
    requires_manufacturer_contact: bool = False
    suggested_manufacturer_message: Optional[str] = None
    # Image quality analysis (populated by image_analyzer)
    image_quality: Optional[dict] = None
    # Image suggestion from manufacturer/Norengros
    image_suggestion: Optional[ImageSuggestion] = None
    # Enrichment results (populated by pdf_enricher pipeline)
    enrichment_results: list[EnrichmentResult] = []
    # Enrichment suggestions (populated by enricher engine)
    enrichment_suggestions: list[EnrichmentSuggestion] = []
    pdf_available: bool = False
    pdf_url: Optional[str] = None
    # AI scoring results (populated by ai_scorer)
    ai_score: Optional[dict] = None
    ai_enrichment: Optional[dict] = None


class AIScoreResult(BaseModel):
    """Result from AI-powered product quality scoring."""
    overall_score: float = 0.0
    field_scores: dict[str, float] = {}
    issues: list[str] = []
    improvement_suggestions: list[str] = []


class AIEnrichmentResult(BaseModel):
    """Result from AI-powered product enrichment."""
    improved_description: Optional[str] = None
    missing_specifications: list[str] = []
    suggested_category: Optional[str] = None
    packaging_suggestions: Optional[str] = None


class BatchEvaluationItem(BaseModel):
    """Single item in batch evaluation results."""
    article_number: str
    product_name: Optional[str] = None
    rule_score: float = 0.0
    ai_score: Optional[float] = None
    combined_score: float = 0.0
    flag: str = "Needs review"  # "OK", "Needs review", "Critical"
    issues: list[str] = []
    improvement_suggestions: list[str] = []


class BatchMode(str, Enum):
    FULL = "full"
    RANGE = "range"
    RANDOM = "random"
    SPECIFIC = "specific"


class AnalysisMode(str, Enum):
    """Top-level analysis mode controlling what the pipeline does."""
    FULL_ENRICHMENT = "full_enrichment"  # Existing: full quality + enrichment suggestions
    AUDIT_ONLY = "audit_only"            # Quality audit only, no enrichment
    FOCUSED_SCAN = "focused_scan"        # Focused area audit / improvement scan


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class AnalysisJob(BaseModel):
    """Tracks the status of an analysis job."""
    job_id: str
    status: JobStatus = JobStatus.PENDING
    total_products: int = 0
    processed_products: int = 0
    current_product: Optional[str] = None
    current_step: Optional[str] = None  # e.g. "scraping", "image_analysis", "pdf_enrichment"
    results: list[ProductAnalysis] = []
    errors: list[str] = []
    output_file: Optional[str] = None
    created_at: float = Field(default_factory=time.time)
    cancelled: bool = False
    # Batch selection metadata
    batch_mode: str = BatchMode.FULL.value
    batch_info: Optional[str] = None  # e.g. "Rader 1-200", "Random 100 (seed=42)"
    # Analysis mode metadata
    analysis_mode: str = AnalysisMode.FULL_ENRICHMENT.value
    focus_areas: list[str] = []  # Only used when analysis_mode == "focused_scan"
    source_filename: str = ""  # Original uploaded filename
