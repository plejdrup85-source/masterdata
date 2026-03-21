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
    results: list[ProductAnalysis] = []
    errors: list[str] = []
    output_file: Optional[str] = None
    created_at: float = Field(default_factory=time.time)
    cancelled: bool = False
