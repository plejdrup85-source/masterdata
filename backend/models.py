"""Data models for the masterdata quality check application."""

import time
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class QualityStatus(str, Enum):
    OK = "OK"
    MISSING = "Mangler"
    SHOULD_IMPROVE = "B\u00f8r forbedres"
    PROBABLE_ERROR = "Sannsynlig feil"
    REQUIRES_MANUFACTURER = "Krever produsent"


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
    source: Optional[str] = None  # Human-readable source label
    source_url: Optional[str] = None
    evidence: Optional[str] = None  # Quote / snippet proving the value
    confidence: float = 0.0  # 0.0-1.0
    review_required: bool = True  # Must a human verify this?


class FieldAnalysis(BaseModel):
    field_name: str
    current_value: Optional[str] = None
    suggested_value: Optional[str] = None
    source: Optional[str] = None
    confidence: Optional[float] = None
    status: QualityStatus = QualityStatus.OK
    comment: Optional[str] = None


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
    confidence: float = 0.0
    notes: Optional[str] = None


class ProductAnalysis(BaseModel):
    """Complete analysis result for a single product."""
    article_number: str
    product_data: ProductData
    manufacturer_lookup: ManufacturerLookup = ManufacturerLookup()
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
