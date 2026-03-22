"""FastAPI application for masterdata quality check."""

import asyncio
import logging
import os
import random
import time
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse

from backend.ai_scorer import enrich_product_async, review_suggestions_async, score_product_async
from backend.analyzer import analyze_product
from backend.enricher import (
    apply_ai_review_to_suggestions,
    apply_enrichment_suggestions,
    enrich_product,
    final_quality_gate,
)
from backend.excel_handler import create_output_excel, read_article_numbers
from backend.image_analyzer import analyze_product_images
from backend.jeeves_loader import JeevesIndex, load_jeeves
from backend.manufacturer import (
    generate_improvement_suggestions,
    search_manufacturer_info,
    search_norengros,
)
from backend.models import (
    AnalysisJob, BatchMode, ImageSuggestion, JobStatus,
    ProductAnalysis, ProductData, QualityStatus, VerificationStatus,
)
from backend.pdf_enricher import run_enrichment_pipeline
from backend.scraper import scrape_product, _load_sitemap, _sitemap_loaded, _sku_to_url

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Masterdata Kvalitetssjekk",
    description="Kvalitetsanalyse av produktkatalog mot onemed.no",
    version="2.0.0",
)

# CORS - allow Render domain and localhost for dev
ALLOWED_ORIGINS = os.environ.get(
    "ALLOWED_ORIGINS",
    "http://localhost:8000,http://localhost:3000,http://127.0.0.1:8000"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)


def _find_jeeves_file() -> Optional[str]:
    """Find Jeeves Excel file from env var or well-known paths."""
    if JEEVES_FILE_PATH and Path(JEEVES_FILE_PATH).exists():
        return JEEVES_FILE_PATH
    # Auto-detect from common locations
    candidates = [
        Path("Masterdata 2103.xlsx"),
        Path("masterdata/Masterdata 2103.xlsx"),
        Path("/tmp/masterdata_cache/Masterdata_2103.xlsx"),
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    return None


@app.on_event("startup")
async def preload_data():
    """Pre-load sitemap and Jeeves data on startup."""
    global _jeeves_index

    # Load Jeeves ERP data
    jeeves_path = _find_jeeves_file()
    if jeeves_path:
        try:
            _jeeves_index = load_jeeves(jeeves_path)
            logger.info(f"Jeeves data loaded: {_jeeves_index.count} products from {jeeves_path}")
        except Exception as e:
            logger.warning(f"Failed to load Jeeves data from {jeeves_path}: {e}")
    else:
        logger.warning(
            "Jeeves Excel file not found. Set JEEVES_FILE_PATH env var or place "
            "'Masterdata 2103.xlsx' in the project root. Two-source comparison disabled."
        )

    # Pre-load sitemap XML and cached SKU→URL index on startup.
    # This downloads ONE sitemap XML file and loads the cached SKU index from disk.
    # It does NOT scan/crawl product pages — that only happens in discovery mode.
    try:
        import httpx
        async with httpx.AsyncClient() as client:
            await _load_sitemap(client)
        logger.info("Sitemap index pre-loaded on startup (no page scanning)")
    except Exception as e:
        logger.warning(f"Failed to pre-load sitemap on startup: {e}")

# In-memory job storage
jobs: dict[str, AnalysisJob] = {}

# Keep references to background tasks so they don't get GC-ed
_background_tasks: set[asyncio.Task] = set()

# Output directory - use /tmp on deployed environments for reliability
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "/tmp/masterdata_output"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Jeeves ERP data file path — auto-detected from repo or configured via env
JEEVES_FILE_PATH = os.environ.get("JEEVES_FILE_PATH", "")
_jeeves_index: Optional[JeevesIndex] = None

# Concurrency control - be polite to onemed.no
SCRAPE_CONCURRENCY = 3
SCRAPE_DELAY = 1.0  # seconds between requests

# Limits
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 MB
MAX_ARTICLES = 1000
MAX_CONCURRENT_JOBS = 3
JOB_TTL_SECONDS = 2 * 60 * 60  # 2 hours

# Rate limiting (simple per-IP)
_rate_limit: dict[str, list[float]] = defaultdict(list)
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX = 5  # max uploads per window


def _cleanup_old_jobs() -> None:
    """Remove jobs older than JOB_TTL_SECONDS."""
    now = time.time()
    expired = [
        jid for jid, job in jobs.items()
        if now - job.created_at > JOB_TTL_SECONDS
    ]
    for jid in expired:
        job = jobs.pop(jid, None)
        if job and job.output_file:
            try:
                Path(job.output_file).unlink(missing_ok=True)
            except Exception:
                pass
        logger.info(f"Cleaned up expired job {jid}")


def _check_rate_limit(client_ip: str) -> bool:
    """Return True if request is within rate limits."""
    now = time.time()
    timestamps = _rate_limit[client_ip]
    # Remove old entries
    _rate_limit[client_ip] = [t for t in timestamps if now - t < RATE_LIMIT_WINDOW]
    if len(_rate_limit[client_ip]) >= RATE_LIMIT_MAX:
        return False
    _rate_limit[client_ip].append(now)
    return True


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the frontend."""
    frontend_path = Path("frontend/index.html")
    if frontend_path.exists():
        return HTMLResponse(content=frontend_path.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>Frontend not found</h1>", status_code=404)


@app.get("/api/health")
async def health_check():
    """Health check endpoint with feature status."""
    return {
        "status": "ok",
        "version": "2.0.0",
        "active_jobs": len(jobs),
        "jeeves_loaded": _jeeves_index is not None and _jeeves_index.loaded,
        "jeeves_product_count": _jeeves_index.count if _jeeves_index else 0,
        "ai_scoring_available": bool(os.environ.get("ANTHROPIC_API_KEY")),
    }


def _apply_batch_selection(
    articles: list[str],
    batch_mode: str,
    range_start: Optional[int],
    range_end: Optional[int],
    sample_size: Optional[int],
    sample_seed: Optional[int],
    specific_articles: Optional[str],
) -> tuple[list[str], str]:
    """Apply batch selection to article list.

    Returns (selected_articles, batch_info_description).
    """
    total = len(articles)

    if batch_mode == BatchMode.RANGE.value:
        # 1-based row indices
        start = max(1, range_start or 1) - 1  # convert to 0-based
        end = min(total, range_end or total)
        selected = articles[start:end]
        info = f"Rader {start + 1}\u2013{end} av {total} (utvalg: {len(selected)})"
        return selected, info

    elif batch_mode == BatchMode.RANDOM.value:
        n = min(sample_size or 100, total)
        seed = sample_seed if sample_seed is not None else int(time.time())
        rng = random.Random(seed)
        selected = rng.sample(articles, n)
        info = f"Tilfeldig utvalg: {n} av {total} (seed={seed})"
        return selected, info

    elif batch_mode == BatchMode.SPECIFIC.value:
        if specific_articles:
            # Parse comma or newline separated list
            requested = {
                a.strip()
                for a in specific_articles.replace("\n", ",").split(",")
                if a.strip()
            }
            # Keep only articles that exist in the uploaded file
            selected = [a for a in articles if a in requested]
            # Also add any specified articles not in the file (user might want to check them anyway)
            in_file = set(articles)
            extra = [a for a in requested if a not in in_file]
            selected.extend(extra)
            info = f"Spesifikke artikler: {len(selected)} valgt ({len(extra)} ikke i fil)"
            return selected, info
        return articles, f"Alle {total} artikler (ingen spesifikke angitt)"

    # FULL mode (default)
    return articles, f"Alle {total} artikler"


@app.post("/api/upload")
async def upload_excel(
    request: Request,
    file: UploadFile = File(...),
    skip_cache: bool = Query(False, description="Skip cache and re-scrape all products"),
    batch_mode: str = Query(BatchMode.FULL.value, description="Batch mode: full, range, random, specific"),
    range_start: Optional[int] = Query(None, description="Start row (1-based) for range mode"),
    range_end: Optional[int] = Query(None, description="End row (inclusive) for range mode"),
    sample_size: Optional[int] = Query(None, description="Number of random samples"),
    sample_seed: Optional[int] = Query(None, description="Random seed for reproducible sampling"),
    specific_articles: Optional[str] = Query(None, description="Comma-separated article numbers for specific mode"),
):
    """Upload an Excel file and start analysis with batch selection support."""
    # Cleanup old jobs first
    _cleanup_old_jobs()

    # Rate limiting per client IP
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(
            429,
            f"For mange opplastinger. Maks {RATE_LIMIT_MAX} per {RATE_LIMIT_WINDOW} sekunder."
        )

    if not file.filename:
        raise HTTPException(400, "Ingen fil valgt")

    ext = Path(file.filename).suffix.lower()
    if ext != ".xlsx":
        raise HTTPException(
            400,
            f"Filformatet {ext} st\u00f8ttes ikke. Bruk .xlsx (Excel 2007+)."
        )

    # Check file size before reading
    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(
            400,
            f"Filen er for stor ({len(content) / 1024 / 1024:.1f} MB). Maks {MAX_FILE_SIZE / 1024 / 1024:.0f} MB."
        )

    # Check concurrent job limit
    active_jobs = sum(
        1 for j in jobs.values()
        if j.status in (JobStatus.PENDING, JobStatus.RUNNING)
    )
    if active_jobs >= MAX_CONCURRENT_JOBS:
        raise HTTPException(
            429,
            f"For mange samtidige analyser ({active_jobs}). Vent til en er ferdig."
        )

    try:
        article_numbers, detected_column = read_article_numbers(content, file.filename)
    except Exception as e:
        logger.error(f"Failed to read Excel: {e}")
        raise HTTPException(400, f"Kunne ikke lese Excel-filen: {e}")

    if not article_numbers:
        raise HTTPException(400, "Ingen artikkelnumre funnet i filen")

    if len(article_numbers) > MAX_ARTICLES:
        raise HTTPException(
            400,
            f"For mange artikkelnumre ({len(article_numbers)}). Maks {MAX_ARTICLES} per analyse."
        )

    # Remove duplicates while preserving order
    seen = set()
    unique_articles = []
    for a in article_numbers:
        if a not in seen:
            seen.add(a)
            unique_articles.append(a)

    # Apply batch selection
    selected_articles, batch_info = _apply_batch_selection(
        unique_articles, batch_mode, range_start, range_end,
        sample_size, sample_seed, specific_articles,
    )

    if not selected_articles:
        raise HTTPException(400, "Ingen artikler valgt etter batch-filtrering")

    # Create job
    job_id = str(uuid.uuid4())[:8]
    job = AnalysisJob(
        job_id=job_id,
        status=JobStatus.PENDING,
        total_products=len(selected_articles),
        created_at=time.time(),
        batch_mode=batch_mode,
        batch_info=batch_info,
    )
    jobs[job_id] = job

    # Start analysis in background - store task reference to prevent GC
    task = asyncio.create_task(_run_analysis(job_id, selected_articles, skip_cache))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)

    # Estimate ~5 seconds per product (scrape + image + PDF + analysis)
    n = len(selected_articles)
    est_seconds = n * 5 // SCRAPE_CONCURRENCY
    if est_seconds < 60:
        est_str = f"~{est_seconds} sekunder"
    elif est_seconds < 3600:
        est_str = f"~{est_seconds // 60} minutter"
    else:
        est_str = f"~{est_seconds // 3600} timer {(est_seconds % 3600) // 60} minutter"

    jeeves_status = f", Jeeves: {_jeeves_index.count} produkter" if _jeeves_index else ", Jeeves: ikke lastet"

    return {
        "job_id": job_id,
        "total_products": len(selected_articles),
        "detected_column": detected_column,
        "batch_mode": batch_mode,
        "batch_info": batch_info,
        "estimated_time": est_str,
        "jeeves_loaded": _jeeves_index is not None and _jeeves_index.loaded,
        "message": f"Analyse startet for {n} produkter ({batch_info}). Estimert tid: {est_str}{jeeves_status}",
    }


@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    """Get the status of an analysis job."""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Jobb ikke funnet. Den kan ha utl\u00f8pt.")

    return {
        "job_id": job.job_id,
        "status": job.status.value,
        "total_products": job.total_products,
        "processed_products": job.processed_products,
        "current_product": job.current_product,
        "current_step": job.current_step,
        "progress_percent": round(
            job.processed_products / job.total_products * 100, 1
        ) if job.total_products > 0 else 0,
        "errors": job.errors[-10:],
        "output_file": job.output_file,
        "batch_mode": job.batch_mode,
        "batch_info": job.batch_info,
    }


@app.delete("/api/cancel/{job_id}")
async def cancel_job(job_id: str):
    """Cancel a running analysis job."""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Jobb ikke funnet")

    if job.status not in (JobStatus.PENDING, JobStatus.RUNNING):
        raise HTTPException(400, "Jobben er allerede ferdig")

    job.cancelled = True
    job.status = JobStatus.FAILED
    job.errors.append("Analyse avbrutt av bruker")
    job.current_product = None
    logger.info(f"[{job_id}] Job cancelled by user")

    return {"message": "Analyse avbrutt", "job_id": job_id}


@app.get("/api/download/{job_id}")
async def download_result(job_id: str):
    """Download the analysis result Excel file."""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Jobb ikke funnet")

    if job.status != JobStatus.COMPLETED:
        raise HTTPException(400, "Analysen er ikke fullf\u00f8rt enda")

    if not job.output_file or not Path(job.output_file).exists():
        raise HTTPException(404, "Resultatfilen finnes ikke lenger. Den kan ha blitt slettet.")

    return FileResponse(
        job.output_file,
        filename=f"masterdata_kvalitetssjekk_{job_id}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.get("/api/results/{job_id}")
async def get_results(job_id: str):
    """Get analysis results as JSON."""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Jobb ikke funnet")

    if job.status not in (JobStatus.COMPLETED, JobStatus.RUNNING):
        raise HTTPException(400, "Ingen resultater tilgjengelig enda")

    return {
        "job_id": job.job_id,
        "status": job.status.value,
        "total_products": job.total_products,
        "processed_products": job.processed_products,
        "results": [
            {
                "article_number": r.article_number,
                "product_name": r.product_data.product_name,
                "found": r.product_data.found_on_onemed,
                "score": r.total_score,
                "status": r.overall_status.value,
                "comment": r.overall_comment,
                "manufacturer": r.product_data.manufacturer,
                "category": r.product_data.category,
                "requires_followup": r.requires_manufacturer_contact,
                "manual_review": r.manual_review_needed,
                "auto_fix": r.auto_fix_possible,
                "image_quality": _format_image_quality(r.image_quality),
                "pdf_available": r.pdf_available,
                "pdf_url": r.pdf_url,
                "enrichment_count": len([
                    e for e in r.enrichment_results
                    if e.match_status != "NOT_FOUND"
                ]),
                "enrichment_conflicts": len([
                    e for e in r.enrichment_results
                    if e.match_status == "FOUND_IN_BOTH_CONFLICT"
                ]),
                "ai_score": r.ai_score,
                "ai_enrichment": r.ai_enrichment,
                "enrichment_suggestions": [
                    {
                        "field_name": es.field_name,
                        "current_value": es.current_value,
                        "suggested_value": es.suggested_value,
                        "source": es.source,
                        "source_url": es.source_url,
                        "evidence": es.evidence,
                        "confidence": es.confidence,
                        "review_required": es.review_required,
                    }
                    for es in r.enrichment_suggestions
                ],
                "field_analyses": [
                    {
                        "field_name": fa.field_name,
                        "current_value": fa.current_value,
                        "status": fa.status.value,
                        "comment": fa.comment,
                        "suggested_value": fa.suggested_value,
                        "source": fa.source,
                        "confidence": fa.confidence,
                    }
                    for fa in r.field_analyses
                ],
            }
            for r in job.results
        ],
    }


@app.get("/api/families/{job_id}")
async def get_families(job_id: str):
    """Run family/variant detection on completed analysis results.

    Returns product families with Mother/Child structure, variant dimensions,
    and confidence scoring for Inriver/webshop review.
    """
    from backend.family_detector import detect_families, products_from_analyses

    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Jobb ikke funnet")
    if job.status != JobStatus.COMPLETED:
        raise HTTPException(400, "Analyse ikke fullført ennå")
    if not job.results:
        raise HTTPException(400, "Ingen resultater tilgjengelig")

    # Build product dicts from analysis results and run family detection
    product_dicts = products_from_analyses(job.results)
    families, all_members = detect_families(product_dicts)

    # Serialize families for JSON response
    family_list = []
    for f in sorted(families, key=lambda x: (-x.confidence, -len(x.members))):
        family_list.append({
            "family_id": f.family_id,
            "family_name": f.family_name,
            "member_count": len(f.members),
            "variant_dimensions": f.variant_dimension_names,
            "mother_article": f.mother_article,
            "confidence": f.confidence,
            "review_required": f.review_required,
            "grouping_reason": f.grouping_reason,
            "members": [
                {
                    "article_number": m.article_number,
                    "role": m.role,
                    "product_name": m.product_name,
                    "specification": m.specification,
                    "child_specific_title": m.child_specific_title,
                    "variant_dimensions": [
                        {"name": d.dimension_name, "value": d.value, "source": d.source}
                        for d in m.variant_dimensions
                    ],
                }
                for m in f.members
            ],
        })

    # Summary stats
    families_with_dims = sum(1 for f in families if f.variant_dimension_names)
    strong = sum(1 for f in families if f.confidence >= 0.65)
    standalone_count = sum(1 for m in all_members if m.role == "standalone")

    return {
        "job_id": job_id,
        "total_products": len(all_members),
        "total_families": len(families),
        "strong_families": strong,
        "families_with_dimensions": families_with_dims,
        "families_without_dimensions": len(families) - families_with_dims,
        "standalone_products": standalone_count,
        "families": family_list,
    }


@app.post("/api/score-product")
async def score_product_endpoint(data: dict):
    """Score a single product using Claude AI.

    Input: {product_name, description, specification, category, packaging}
    Returns: {overall_score, field_scores, issues, improvement_suggestions}
    """
    result = await score_product_async(
        product_name=data.get("product_name"),
        description=data.get("description"),
        specification=data.get("specification"),
        category=data.get("category"),
        packaging=data.get("packaging"),
    )
    if result is None:
        raise HTTPException(
            503,
            "AI-scoring er ikke tilgjengelig. Sjekk at ANTHROPIC_API_KEY er satt."
        )
    return result


@app.post("/api/enrich-product")
async def enrich_product_endpoint(data: dict):
    """Get AI-powered enrichment suggestions for a product.

    Input: {product_name, description, specification, category, packaging}
    Returns: {improved_description, missing_specifications, suggested_category, packaging_suggestions}
    """
    result = await enrich_product_async(
        product_name=data.get("product_name"),
        description=data.get("description"),
        specification=data.get("specification"),
        category=data.get("category"),
        packaging=data.get("packaging"),
    )
    if result is None:
        raise HTTPException(
            503,
            "AI-berikelse er ikke tilgjengelig. Sjekk at ANTHROPIC_API_KEY er satt."
        )
    return result


@app.post("/api/batch-evaluate")
async def batch_evaluate_endpoint(data: dict):
    """Evaluate a batch of products and return scores + flags.

    Input: {products: [{product_name, description, specification, category, packaging}, ...]}
    Returns: {results: [{article_number, score, flag, issues, ...}, ...]}
    """
    products = data.get("products", [])
    if not products:
        raise HTTPException(400, "Ingen produkter å evaluere")
    if len(products) > 50:
        raise HTTPException(400, "Maks 50 produkter per batch-evaluering")

    results = []
    for product in products:
        # Rule-based score (from analyzer)
        rule_score = 0.0
        issues = []

        # Quick rule-based checks
        if not product.get("product_name"):
            issues.append("Produktnavn mangler")
        if not product.get("description"):
            issues.append("Beskrivelse mangler")
        if not product.get("specification"):
            issues.append("Spesifikasjon mangler")
        if not product.get("category"):
            issues.append("Kategori mangler")
        if not product.get("packaging"):
            issues.append("Pakningsinfo mangler")

        fields_present = 5 - len(issues)
        rule_score = fields_present / 5 * 100

        # AI score (if available)
        ai_result = await score_product_async(
            product_name=product.get("product_name"),
            description=product.get("description"),
            specification=product.get("specification"),
            category=product.get("category"),
            packaging=product.get("packaging"),
        )

        ai_score = ai_result["overall_score"] if ai_result else None
        ai_issues = ai_result.get("issues", []) if ai_result else []
        ai_suggestions = ai_result.get("improvement_suggestions", []) if ai_result else []

        # Combined score: weight rule-based 40%, AI 60% (if available)
        if ai_score is not None:
            combined = rule_score * 0.4 + ai_score * 0.6
        else:
            combined = rule_score

        # Flag
        if combined >= 70:
            flag = "OK"
        elif combined >= 40:
            flag = "Needs review"
        else:
            flag = "Critical"

        results.append({
            "article_number": product.get("article_number", ""),
            "product_name": product.get("product_name"),
            "rule_score": round(rule_score, 1),
            "ai_score": round(ai_score, 1) if ai_score is not None else None,
            "combined_score": round(combined, 1),
            "flag": flag,
            "issues": issues + ai_issues,
            "improvement_suggestions": ai_suggestions,
        })

    return {"results": results}


def _build_image_suggestion(
    product_data: ProductData,
    image_summary,
    mfr_data,
    norengros_data,
) -> Optional[ImageSuggestion]:
    """Build an image improvement suggestion if the current image is weak/missing.

    Priority: manufacturer image > Norengros image.
    Manufacturer images can be auto-suggested; Norengros always requires review.
    """
    current_url = product_data.image_url
    current_status = "ok"

    # Determine current image status
    if not image_summary or not image_summary.main_image_exists:
        current_status = "missing"
    elif image_summary.image_analyses:
        main = image_summary.image_analyses[0]
        score = main.overall_score
        if score < 40:
            current_status = "low_quality"
        elif score < 70:
            issues = main.issues or []
            if any("background" in str(i).lower() for i in issues):
                current_status = "poor_background"
            elif any("resolution" in str(i).lower() for i in issues):
                current_status = "low_quality"
            else:
                current_status = "review"
        # OK images don't need suggestions
        else:
            return None

    if current_status == "ok":
        return None

    # Check manufacturer image first (preferred)
    if mfr_data and mfr_data.found and mfr_data.image_url:
        return ImageSuggestion(
            current_image_url=current_url,
            current_image_status=current_status,
            suggested_image_url=mfr_data.image_url,
            suggested_source="manufacturer",
            suggested_source_url=mfr_data.source_url,
            confidence=mfr_data.confidence * 0.8,
            review_required=True,
            reason=f"Produsentbilde funnet ({current_status}). Verifiser produktmatch.",
        )

    # Fallback: Norengros image (always conservative)
    if norengros_data and norengros_data.found and norengros_data.image_url:
        return ImageSuggestion(
            current_image_url=current_url,
            current_image_status=current_status,
            suggested_image_url=norengros_data.image_url,
            suggested_source="norengros",
            suggested_source_url=norengros_data.source_url,
            confidence=0.25,
            review_required=True,
            reason=f"Norengros-bilde funnet ({current_status}). Konkurrentkilde — krever manuell godkjenning.",
        )

    # No better image available — just report the issue
    if current_status != "ok":
        return ImageSuggestion(
            current_image_url=current_url,
            current_image_status=current_status,
            confidence=0.0,
            review_required=True,
            reason=f"Bildestatus: {current_status}. Ingen bedre bildekilde funnet automatisk.",
        )

    return None


def _apply_enrichment_to_analysis(analysis: ProductAnalysis) -> None:
    """Apply enrichment results to field analyses.

    Enrichment from internal PDF takes priority over manufacturer sources.
    Only applies suggestions where confidence is sufficient and no conflict.
    """
    if not analysis.enrichment_results:
        return

    # Map enrichment field names to FieldAnalysis field names
    field_map = {
        "product_name": "Produktnavn",
        "description": "Beskrivelse",
        "manufacturer": "Produsent",
        "manufacturer_article_number": "Produsentens varenummer",
        "packaging_info": "Pakningsinformasjon",
        "specifications": "Spesifikasjon",
        "category": "Kategori",
    }

    for er in analysis.enrichment_results:
        if not er.suggested_value or er.match_status == "NOT_FOUND":
            continue

        fa_name = field_map.get(er.field_name)
        if not fa_name:
            continue

        # Only apply if confidence >= 0.6 and not a conflict requiring review
        if er.confidence < 0.6 or er.review_status == "conflict":
            continue

        # Find matching field analysis
        for fa in analysis.field_analyses:
            if fa.field_name == fa_name:
                # Only suggest if field is currently missing/poor and enrichment has value
                if fa.status in (QualityStatus.MISSING, QualityStatus.SHOULD_IMPROVE, QualityStatus.PROBABLE_ERROR):
                    # Don't overwrite existing manufacturer suggestion with lower-confidence PDF
                    if fa.suggested_value and fa.confidence and fa.confidence > er.confidence:
                        continue
                    # PDF source takes priority
                    if er.source_level == "internal_product_sheet":
                        fa.suggested_value = er.suggested_value
                        fa.source = f"Produktdatablad ({er.source_url})"
                        fa.confidence = er.confidence
                    elif not fa.suggested_value:
                        fa.suggested_value = er.suggested_value
                        fa.source = f"Produsent ({er.source_url})"
                        fa.confidence = er.confidence
                break


def _format_image_quality(iq: Optional[dict]) -> dict:
    """Format image quality data for API response."""
    if not iq:
        return {"score": 0, "status": "MISSING", "count": 0, "main_exists": False, "issues": ""}
    return {
        "score": iq.get("avg_image_score", 0),
        "status": iq.get("image_quality_status", "MISSING"),
        "count": iq.get("image_count_found", 0),
        "main_exists": iq.get("main_image_exists", False),
        "issues": iq.get("image_issue_summary", ""),
    }


async def _run_analysis(
    job_id: str,
    article_numbers: list[str],
    skip_cache: bool = False,
) -> None:
    """Run the full analysis pipeline for all products."""
    job = jobs[job_id]
    job.status = JobStatus.RUNNING

    # --- Deduplicate article numbers to avoid duplicate HTTP requests ---
    # Preserve original order and count for output mapping
    unique_articles: list[str] = []
    seen: set[str] = set()
    duplicate_count = 0
    for art in article_numbers:
        if art not in seen:
            seen.add(art)
            unique_articles.append(art)
        else:
            duplicate_count += 1

    if duplicate_count:
        logger.info(
            f"[{job_id}] Dedup: {len(article_numbers)} rows → "
            f"{len(unique_articles)} unique articles "
            f"({duplicate_count} duplicate fetches prevented)"
        )

    # Scope logging — verify strict input processing
    # Defensive: _sku_to_url is an optional metrics aid; never crash the job if unavailable
    try:
        in_index = sum(1 for a in unique_articles if a.strip() in _sku_to_url)
    except Exception:
        logger.warning(f"[{job_id}] SKU index unavailable for scope metrics, defaulting to 0")
        in_index = 0
    logger.info(
        f"[{job_id}] SCOPE: mode=strict_input | "
        f"input_rows={len(article_numbers)} | "
        f"unique_articles={len(unique_articles)} | "
        f"in_sku_index={in_index} | "
        f"not_in_index={len(unique_articles) - in_index} | "
        f"discovery=OFF"
    )

    semaphore = asyncio.Semaphore(SCRAPE_CONCURRENCY)
    _fetch_count = 0  # Track actual URL fetches for scope verification

    async def process_product(article_number: str) -> Optional[ProductAnalysis]:
        nonlocal _fetch_count
        # Check if job was cancelled
        if job.cancelled:
            return None

        async with semaphore:
            if job.cancelled:
                return None

            job.current_product = article_number
            try:
                # Step 1: Scrape from onemed.no (strict input mode — no sitemap scan)
                job.current_step = "scraping"
                logger.info(f"[{job_id}] Scraping {article_number}")
                product_data = await scrape_product(
                    article_number, use_cache=not skip_cache,
                    enable_discovery=False,
                )
                _fetch_count += 1

                # Step 2: Analyze product images with CV
                # Always run - CDN URLs use article numbers directly, no scraping needed
                image_quality_dict = None
                job.current_step = "image_analysis"
                logger.info(f"[{job_id}] Image analysis for {article_number}")
                image_summary = await analyze_product_images(article_number)
                image_quality_dict = image_summary.to_dict()

                # Update product_data with image info from CV analysis
                product_data.image_quality_ok = image_summary.main_image_exists
                if image_summary.main_image_exists and image_summary.image_analyses:
                    product_data.image_url = image_summary.image_analyses[0].image_url

                # If scraper failed but images exist on CDN, mark as CDN-only (weak verification)
                # This allows the enrichment pipeline to continue, but does NOT confirm identity.
                if not product_data.found_on_onemed and image_summary.main_image_exists:
                    product_data.found_on_onemed = True
                    product_data.error = None
                    # Only downgrade verification — never upgrade from a confirmed mismatch
                    if product_data.verification_status not in (
                        VerificationStatus.MISMATCH,
                        VerificationStatus.EXACT_MATCH,
                        VerificationStatus.NORMALIZED_MATCH,
                    ):
                        product_data.verification_status = VerificationStatus.CDN_ONLY
                        product_data.verification_evidence = (
                            f"Produkt bekreftet kun via CDN-bilde. "
                            f"Ingen produktside tilgjengelig for identitetsverifisering."
                        )
                    logger.info(
                        f"[{job_id}] {article_number} found via CDN image "
                        f"(verification: {product_data.verification_status.value})"
                    )

                # Step 3-5: enrichment pipeline
                mfr_data = None
                norengros_data = None
                enrichment_results = []
                pdf_exists = False
                pdf_url = None

                # Step 3: Manufacturer lookup (only if product found and data incomplete)
                # Check Jeeves first — if Jeeves has supplier info, skip mfr lookup for that
                jeeves_check = _jeeves_index.get(article_number) if _jeeves_index else None
                if product_data.found_on_onemed:
                    has_mfr = product_data.manufacturer or (jeeves_check and jeeves_check.supplier)
                    has_mfr_num = product_data.manufacturer_article_number or (jeeves_check and jeeves_check.supplier_item_no)
                    has_spec = product_data.specification or product_data.technical_details
                    needs_mfr = not has_mfr or not has_mfr_num or not has_spec
                    if needs_mfr:
                        job.current_step = "manufacturer_lookup"
                        logger.info(f"[{job_id}] Manufacturer lookup for {article_number}")
                        mfr_data = await search_manufacturer_info(product_data)

                # Step 4: PDF enrichment - always try (PDF CDN uses article numbers directly)
                job.current_step = "pdf_enrichment"
                logger.info(f"[{job_id}] Enrichment pipeline for {article_number}")
                pdf_exists, pdf_url, enrichment_results = await run_enrichment_pipeline(
                    article_number, product_data, manufacturer_data=mfr_data
                )

                # Step 4b: Norengros secondary lookup
                # Only when primary sources are weak (no PDF, no manufacturer data)
                data_is_weak = (
                    not pdf_exists
                    and (not mfr_data or not mfr_data.found)
                    and (not product_data.description or len(product_data.description or "") < 20)
                )
                if data_is_weak and product_data.found_on_onemed:
                    job.current_step = "norengros_lookup"
                    logger.info(f"[{job_id}] Norengros secondary lookup for {article_number}")
                    try:
                        norengros_data = await search_norengros(product_data)
                    except Exception as e:
                        logger.debug(f"[{job_id}] Norengros lookup failed for {article_number}: {e}")

                # Step 5: Quality analysis (with all collected data + Jeeves)
                job.current_step = "quality_analysis"
                jeeves_data = _jeeves_index.get(article_number) if _jeeves_index else None
                analysis = analyze_product(
                    product_data, image_quality=image_quality_dict, jeeves=jeeves_data
                )
                analysis.image_quality = image_quality_dict
                analysis.enrichment_results = enrichment_results
                analysis.pdf_available = pdf_exists
                analysis.pdf_url = pdf_url

                # Apply manufacturer data if found
                if mfr_data:
                    analysis.manufacturer_lookup = mfr_data
                    if mfr_data.found:
                        suggestions = generate_improvement_suggestions(
                            product_data, mfr_data
                        )
                        for suggestion in suggestions:
                            for fa in analysis.field_analyses:
                                if fa.field_name == suggestion["field"]:
                                    fa.suggested_value = suggestion["suggested"]
                                    fa.source = suggestion["source"]
                                    fa.confidence = suggestion["confidence"]
                                    break

                # Store Norengros data if found
                if norengros_data:
                    analysis.norengros_lookup = norengros_data

                # Step 5b: Image suggestion logic
                # Check if current image is weak/missing and suggest better alternatives
                analysis.image_suggestion = _build_image_suggestion(
                    product_data, image_summary, mfr_data, norengros_data,
                )

                # Apply enrichment suggestions to field_analyses (PDF takes priority)
                _apply_enrichment_to_analysis(analysis)

                # Run source-priority enrichment engine
                enrichment_suggestions = enrich_product(
                    analysis, enrichment_results, manufacturer_data=mfr_data
                )

                # Step 6: AI quality layer (if API key configured)
                job.current_step = "ai_scoring"
                try:
                    # 6a: AI scoring (quality evaluation)
                    ai_score_result = await score_product_async(
                        product_name=product_data.product_name,
                        description=product_data.description,
                        specification=product_data.specification,
                        category=product_data.category,
                        packaging=product_data.packaging_info or product_data.packaging_unit,
                    )
                    if ai_score_result:
                        analysis.ai_score = ai_score_result
                        logger.info(
                            f"[{job_id}] AI score for {article_number}: "
                            f"{ai_score_result.get('overall_score', 'N/A')}"
                        )

                    # 6b: AI editorial review of enrichment suggestions
                    # Pass source-grounded suggestions through AI for polish/rejection
                    if enrichment_suggestions:
                        suggestions_for_review = [
                            {
                                "field_name": s.field_name,
                                "current_value": s.current_value,
                                "suggested_value": s.suggested_value,
                                "source": s.source,
                                "evidence": s.evidence,
                            }
                            for s in enrichment_suggestions
                        ]
                        ai_reviews = await review_suggestions_async(
                            article_number=article_number,
                            product_name=product_data.product_name,
                            suggestions=suggestions_for_review,
                        )
                        if ai_reviews:
                            enrichment_suggestions = apply_ai_review_to_suggestions(
                                enrichment_suggestions, ai_reviews
                            )

                    # 6c: Legacy AI enrichment (generates independent suggestions)
                    # Kept for fields not covered by source-grounded enrichment
                    ai_enrich_result = await enrich_product_async(
                        product_name=product_data.product_name,
                        description=product_data.description,
                        specification=product_data.specification,
                        category=product_data.category,
                        packaging=product_data.packaging_info or product_data.packaging_unit,
                    )
                    if ai_enrich_result:
                        analysis.ai_enrichment = ai_enrich_result

                except Exception as e:
                    logger.warning(f"[{job_id}] AI scoring/review failed for {article_number}: {e}")

                # Step 7: Final quality gate — rule-based rejection of remaining bad suggestions
                if enrichment_suggestions:
                    enrichment_suggestions = final_quality_gate(enrichment_suggestions)

                # Apply surviving suggestions
                if enrichment_suggestions:
                    analysis.enrichment_suggestions = enrichment_suggestions
                    apply_enrichment_suggestions(analysis, enrichment_suggestions)
                    logger.info(
                        f"[{job_id}] {article_number}: "
                        f"{len(enrichment_suggestions)} enrichment suggestion(s) after quality gate"
                    )

                return analysis

            except Exception as e:
                logger.error(f"[{job_id}] Error processing {article_number}: {e}")
                job.errors.append(f"{article_number}: {str(e)}")
                fallback_data = ProductData(
                    article_number=article_number,
                    error=str(e),
                )
                return ProductAnalysis(
                    article_number=article_number,
                    product_data=fallback_data,
                    overall_comment=f"Feil under analyse: {str(e)}",
                )
            finally:
                await asyncio.sleep(SCRAPE_DELAY)

    try:
        # Process only unique articles — duplicate rows reuse the same result
        tasks = [process_product(art) for art in unique_articles]
        result_map: dict[str, ProductAnalysis] = {}
        cache_hits = 0

        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result is None:
                continue  # Job was cancelled
            result_map[result.article_number] = result
            job.processed_products += 1
            logger.info(
                f"[{job_id}] Progress: {job.processed_products}/{len(unique_articles)} "
                f"({result.article_number}: {result.overall_status.value})"
            )

        if job.cancelled:
            logger.info(f"[{job_id}] Analysis cancelled by user")
            return

        # Map results back to all original rows (including duplicates)
        for art in article_numbers:
            if art in result_map:
                job.results.append(result_map[art])

        # Update processed count to include duplicates for progress reporting
        job.processed_products = len(job.results)

        logger.info(
            f"[{job_id}] Fetch stats: {len(article_numbers)} input_rows, "
            f"{len(unique_articles)} unique_articles, "
            f"{_fetch_count} product_urls_fetched, "
            f"{duplicate_count} duplicates_prevented, "
            f"discovery=OFF"
        )
        # Scope safety check: fetched URLs should never exceed unique articles
        if _fetch_count > len(unique_articles) * 2:
            logger.error(
                f"[{job_id}] SCOPE VIOLATION: fetched {_fetch_count} URLs "
                f"for {len(unique_articles)} unique articles — possible scope leak"
            )

        # Results are already in input order from the article_numbers loop above.
        # No sort needed — duplicate rows preserve their original positions.

        # Selector health check — warn if key website fields missing in >50% of found products
        found_results = [r for r in job.results if r.product_data.found_on_onemed]
        if len(found_results) >= 3:
            no_desc = sum(1 for r in found_results if not r.product_data.description)
            no_spec = sum(1 for r in found_results if not r.product_data.specification and not r.product_data.technical_details)
            no_cat = sum(1 for r in found_results if not r.product_data.category and not r.product_data.category_breadcrumb)
            threshold = len(found_results) * 0.5
            if no_desc > threshold:
                msg = f"SELECTOR WARNING: {no_desc}/{len(found_results)} products missing description — website structure may have changed"
                logger.error(f"[{job_id}] {msg}")
                job.errors.append(msg)
            if no_spec > threshold:
                msg = f"SELECTOR WARNING: {no_spec}/{len(found_results)} products missing specification — website structure may have changed"
                logger.error(f"[{job_id}] {msg}")
                job.errors.append(msg)
            if no_cat > threshold:
                msg = f"SELECTOR WARNING: {no_cat}/{len(found_results)} products missing category — website structure may have changed"
                logger.error(f"[{job_id}] {msg}")
                job.errors.append(msg)

        # Generate output Excel
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"masterdata_kvalitetssjekk_{job_id}_{timestamp}.xlsx"
        output_path = str(OUTPUT_DIR / output_filename)

        create_output_excel(job.results, output_path)
        job.output_file = output_path
        job.status = JobStatus.COMPLETED
        job.current_product = None

        logger.info(f"[{job_id}] Analysis completed. Output: {output_path}")

    except Exception as e:
        logger.error(f"[{job_id}] Analysis failed: {e}")
        job.status = JobStatus.FAILED
        job.errors.append(f"Fatal error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
