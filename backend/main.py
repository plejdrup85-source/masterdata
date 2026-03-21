"""FastAPI application for masterdata quality check."""

import asyncio
import logging
import os
import time
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse

from backend.analyzer import analyze_product
from backend.excel_handler import create_output_excel, read_article_numbers
from backend.manufacturer import (
    generate_improvement_suggestions,
    search_manufacturer_info,
)
from backend.image_analyzer import analyze_product_images
from backend.models import AnalysisJob, JobStatus, ProductAnalysis, ProductData
from backend.scraper import scrape_product

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Masterdata Kvalitetssjekk",
    description="Kvalitetsanalyse av produktkatalog mot onemed.no",
    version="1.1.0",
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

# In-memory job storage
jobs: dict[str, AnalysisJob] = {}

# Keep references to background tasks so they don't get GC-ed
_background_tasks: set[asyncio.Task] = set()

# Output directory - use /tmp on deployed environments for reliability
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "/tmp/masterdata_output"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
    """Health check endpoint."""
    return {"status": "ok", "version": "1.1.0", "active_jobs": len(jobs)}


@app.post("/api/upload")
async def upload_excel(
    file: UploadFile = File(...),
    skip_cache: bool = Query(False, description="Skip cache and re-scrape all products"),
):
    """Upload an Excel file and start analysis."""
    # Cleanup old jobs first
    _cleanup_old_jobs()

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

    # Create job
    job_id = str(uuid.uuid4())[:8]
    job = AnalysisJob(
        job_id=job_id,
        status=JobStatus.PENDING,
        total_products=len(unique_articles),
        created_at=time.time(),
    )
    jobs[job_id] = job

    # Start analysis in background - store task reference to prevent GC
    task = asyncio.create_task(_run_analysis(job_id, unique_articles, skip_cache))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)

    return {
        "job_id": job_id,
        "total_products": len(unique_articles),
        "detected_column": detected_column,
        "message": f"Analyse startet for {len(unique_articles)} produkter",
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
        "progress_percent": round(
            job.processed_products / job.total_products * 100, 1
        ) if job.total_products > 0 else 0,
        "errors": job.errors[-10:],
        "output_file": job.output_file,
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

    semaphore = asyncio.Semaphore(SCRAPE_CONCURRENCY)

    async def process_product(article_number: str) -> Optional[ProductAnalysis]:
        # Check if job was cancelled
        if job.cancelled:
            return None

        async with semaphore:
            if job.cancelled:
                return None

            job.current_product = article_number
            try:
                # Step 1: Scrape from onemed.no
                logger.info(f"[{job_id}] Scraping {article_number}")
                product_data = await scrape_product(
                    article_number, use_cache=not skip_cache
                )

                # Step 1b: Analyze product images with CV
                logger.info(f"[{job_id}] Image analysis for {article_number}")
                image_summary = await analyze_product_images(article_number)
                image_quality_dict = image_summary.to_dict()

                # Update product_data with image info from CV analysis
                product_data.image_quality_ok = image_summary.main_image_exists
                if image_summary.main_image_exists and image_summary.image_analyses:
                    product_data.image_url = image_summary.image_analyses[0].image_url

                # Step 2: Analyze quality (with image CV data)
                analysis = analyze_product(product_data, image_quality=image_quality_dict)
                analysis.image_quality = image_quality_dict

                # Step 3: Search manufacturer if data is insufficient
                if analysis.requires_manufacturer_contact or analysis.total_score < 60:
                    logger.info(f"[{job_id}] Manufacturer lookup for {article_number}")
                    mfr_data = await search_manufacturer_info(product_data)
                    analysis.manufacturer_lookup = mfr_data

                    # Generate improvement suggestions from manufacturer data
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
        tasks = [process_product(art) for art in article_numbers]

        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result is None:
                continue  # Job was cancelled
            job.results.append(result)
            job.processed_products += 1
            logger.info(
                f"[{job_id}] Progress: {job.processed_products}/{job.total_products} "
                f"({result.article_number}: {result.overall_status.value})"
            )

        if job.cancelled:
            logger.info(f"[{job_id}] Analysis cancelled by user")
            return

        # Sort results to match input order
        order_map = {art: idx for idx, art in enumerate(article_numbers)}
        job.results.sort(key=lambda r: order_map.get(r.article_number, 999999))

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
