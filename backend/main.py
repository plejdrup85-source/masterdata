"""FastAPI application for masterdata quality check."""

import asyncio
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from backend.analyzer import analyze_product
from backend.excel_handler import create_output_excel, read_article_numbers
from backend.manufacturer import (
    generate_improvement_suggestions,
    search_manufacturer_info,
)
from backend.models import AnalysisJob, JobStatus, ProductAnalysis
from backend.scraper import check_image_quality, scrape_product

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Masterdata Kvalitetssjekk",
    description="Kvalitetsanalyse av produktkatalog mot onemed.no",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job storage
jobs: dict[str, AnalysisJob] = {}

# Output directory
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Concurrency control - be polite to onemed.no
SCRAPE_CONCURRENCY = 3
SCRAPE_DELAY = 1.0  # seconds between requests


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the frontend."""
    frontend_path = Path("frontend/index.html")
    if frontend_path.exists():
        return HTMLResponse(content=frontend_path.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>Frontend not found</h1>", status_code=404)


@app.post("/api/upload")
async def upload_excel(file: UploadFile = File(...)):
    """Upload an Excel file and start analysis."""
    if not file.filename:
        raise HTTPException(400, "No file provided")

    ext = Path(file.filename).suffix.lower()
    if ext not in (".xlsx", ".xls"):
        raise HTTPException(400, f"Unsupported file format: {ext}. Use .xlsx or .xls")

    content = await file.read()

    try:
        article_numbers = read_article_numbers(content, file.filename)
    except Exception as e:
        logger.error(f"Failed to read Excel: {e}")
        raise HTTPException(400, f"Could not read Excel file: {e}")

    if not article_numbers:
        raise HTTPException(400, "No article numbers found in file")

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
    )
    jobs[job_id] = job

    # Start analysis in background
    asyncio.create_task(_run_analysis(job_id, unique_articles))

    return {
        "job_id": job_id,
        "total_products": len(unique_articles),
        "message": f"Analyse startet for {len(unique_articles)} produkter",
    }


@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    """Get the status of an analysis job."""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    return {
        "job_id": job.job_id,
        "status": job.status.value,
        "total_products": job.total_products,
        "processed_products": job.processed_products,
        "current_product": job.current_product,
        "progress_percent": round(
            job.processed_products / job.total_products * 100, 1
        ) if job.total_products > 0 else 0,
        "errors": job.errors[-10:],  # Last 10 errors
        "output_file": job.output_file,
    }


@app.get("/api/download/{job_id}")
async def download_result(job_id: str):
    """Download the analysis result Excel file."""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    if job.status != JobStatus.COMPLETED:
        raise HTTPException(400, "Analysis not completed yet")

    if not job.output_file or not Path(job.output_file).exists():
        raise HTTPException(404, "Output file not found")

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
        raise HTTPException(404, "Job not found")

    if job.status not in (JobStatus.COMPLETED, JobStatus.RUNNING):
        raise HTTPException(400, "No results available yet")

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
                "requires_followup": r.requires_manufacturer_contact,
                "manual_review": r.manual_review_needed,
            }
            for r in job.results
        ],
    }


async def _run_analysis(job_id: str, article_numbers: list[str]) -> None:
    """Run the full analysis pipeline for all products."""
    job = jobs[job_id]
    job.status = JobStatus.RUNNING

    semaphore = asyncio.Semaphore(SCRAPE_CONCURRENCY)

    async def process_product(article_number: str) -> ProductAnalysis:
        async with semaphore:
            job.current_product = article_number
            try:
                # Step 1: Scrape from onemed.no
                logger.info(f"[{job_id}] Scraping {article_number}")
                product_data = await scrape_product(article_number)

                # Step 1b: Check image quality if we have an image
                if product_data.image_url:
                    img_result = await check_image_quality(product_data.image_url)
                    product_data.image_quality_ok = img_result.get("quality_ok", False)

                # Step 2: Analyze quality
                analysis = analyze_product(product_data)

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
                            # Update field analyses with suggestions
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
                from backend.models import ProductData as PD
                fallback_data = PD(
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
        # Process products with controlled concurrency
        tasks = [process_product(art) for art in article_numbers]

        for coro in asyncio.as_completed(tasks):
            result = await coro
            job.results.append(result)
            job.processed_products += 1
            logger.info(
                f"[{job_id}] Progress: {job.processed_products}/{job.total_products} "
                f"({result.article_number}: {result.overall_status.value})"
            )

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
    uvicorn.run(app, host="0.0.0.0", port=8000)
