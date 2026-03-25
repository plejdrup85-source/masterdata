"""Bildeanalyse service — top-level image analysis module.

Provides the business logic for the standalone image analysis workflow:
1. Select products (full catalog, random, manual, Excel upload)
2. Run image quality analysis (reuses image_analyzer.py)
3. Search for better images from external sources
4. Review (approve/reject) image suggestions
5. Export approved images as JPEG ZIP

Persistence: JSON files on disk + in-memory cache, same pattern as family_detector.
"""

import asyncio
import hashlib
import json
import logging
import os
import random
import time
import zipfile
from dataclasses import dataclass, field, asdict
from enum import Enum
from io import BytesIO
from pathlib import Path
from typing import Optional

import httpx
from PIL import Image

from backend.image_analyzer import (
    IMAGE_BASE_URL,
    IMAGE_HEADERS,
    IMAGE_TIMEOUT,
    analyze_product_images,
    ProductImageSummary,
)

logger = logging.getLogger(__name__)

# ── Persistence directories ──
IMAGE_ANALYSIS_DIR = Path(os.environ.get("IMAGE_ANALYSIS_DIR", "/tmp/masterdata_output/image_analysis"))
IMAGE_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

# In-memory cache of analysis sessions
_sessions: dict[str, dict] = {}


class ImageReviewStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class ImageAnalysisStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


def _generate_session_id(prefix: str = "img") -> str:
    """Generate a short unique session ID."""
    h = hashlib.sha256(f"{prefix}-{time.time()}-{random.random()}".encode()).hexdigest()[:10]
    return f"{prefix}-{h}"


def _build_cdn_url(article_number: str, index: int = 0) -> str:
    """Build CDN image URL for an article number."""
    clean = article_number.strip()
    if index == 0:
        return f"{IMAGE_BASE_URL}/{clean}.jpg"
    return f"{IMAGE_BASE_URL}/{clean}-{index + 1}.jpg"


async def run_image_analysis(
    article_numbers: list[str],
    session_id: str,
    jeeves_index=None,
    on_progress=None,
) -> dict:
    """Run image analysis for a list of article numbers.

    Returns the full session result dict with analysis results and review items.
    """
    session = {
        "session_id": session_id,
        "status": ImageAnalysisStatus.RUNNING.value,
        "created_at": time.time(),
        "total_products": len(article_numbers),
        "processed_products": 0,
        "current_product": None,
        "items": [],
        "summary": {},
        "review_log": [],
    }
    _sessions[session_id] = session

    semaphore = asyncio.Semaphore(5)
    items = []

    async def analyze_one(artnr: str):
        async with semaphore:
            session["current_product"] = artnr
            try:
                # Run CV-based image analysis
                summary = await analyze_product_images(artnr)
                summary_dict = summary.to_dict()

                # Look up Jeeves data for product info
                product_name = ""
                supplier = ""
                supplier_item_no = ""
                specification = ""
                gid = ""
                if jeeves_index and jeeves_index.loaded:
                    j = jeeves_index.get(artnr)
                    if j:
                        product_name = j.item_description or j.web_title or ""
                        supplier = j.supplier or ""
                        supplier_item_no = j.supplier_item_no or ""
                        specification = j.specification or ""
                        gid = j.gid or ""

                # Determine current image URL
                current_image_url = _build_cdn_url(artnr)
                current_image_exists = summary.main_image_exists

                # Check if image needs improvement
                needs_review = False
                suggestion_reason = None
                current_status = "ok"
                if not current_image_exists:
                    needs_review = True
                    suggestion_reason = "Hovedbilde mangler"
                    current_status = "missing"
                elif summary.main_image_score < 70:
                    needs_review = True
                    suggestion_reason = f"Lav bildescore ({summary.main_image_score:.0f}/100)"
                    current_status = "low_quality"
                elif summary.image_quality_status in ("FAIL", "REVIEW"):
                    needs_review = True
                    suggestion_reason = f"Bildekvalitet: {summary.image_quality_status}"
                    current_status = "review"

                # Search for better images if product needs review
                candidates = []
                if needs_review:
                    try:
                        from backend.image_search import search_product_images
                        raw_candidates = await search_product_images(
                            article_number=artnr,
                            manufacturer_name=supplier,
                            manufacturer_artnr=supplier_item_no,
                            product_description=product_name,
                            specification=specification,
                            current_image_status=current_status,
                            gid=gid,
                        )
                        for c in raw_candidates:
                            candidates.append({
                                "image_url": c.image_url,
                                "source_url": c.source_url,
                                "source_domain": c.source_domain,
                                "source_type": c.source_type,
                                "source_name": c.source_name,
                                "identity_score": round(c.identity_score, 2),
                                "improvement_score": round(c.improvement_score, 2),
                                "confidence": round(c.confidence, 2),
                                "reason": c.reason,
                                "verification_details": c.verification_details,
                                "is_manufacturer": c.source_type in (
                                    "manufacturer_mediabank", "manufacturer_website"
                                ),
                            })
                    except Exception as e:
                        logger.warning(f"Image search failed for {artnr}: {e}")

                # Build the item
                best_candidate = candidates[0] if candidates else None
                item = {
                    "article_number": artnr,
                    "product_name": product_name,
                    "supplier": supplier,
                    "supplier_item_no": supplier_item_no,
                    "specification": specification,
                    "current_image_url": current_image_url if current_image_exists else None,
                    "current_image_exists": current_image_exists,
                    "image_score": round(summary.main_image_score, 1),
                    "image_status": summary.image_quality_status,
                    "image_issues": summary.image_issue_summary,
                    "image_count": summary.image_count_found,
                    "secondary_images": summary.secondary_images_found,
                    "needs_review": needs_review,
                    "suggestion_reason": suggestion_reason,
                    "suggested_image_url": best_candidate["image_url"] if best_candidate else None,
                    "suggested_source": best_candidate["source_name"] if best_candidate else None,
                    "suggested_source_url": best_candidate["source_url"] if best_candidate else None,
                    "candidates": candidates,
                    "selected_candidate_index": 0 if candidates else None,
                    "review_status": ImageReviewStatus.PENDING.value,
                    "review_timestamp": None,
                    "image_analyses": summary_dict.get("image_analyses", []),
                }
                items.append(item)
            except Exception as e:
                logger.error(f"Image analysis failed for {artnr}: {e}")
                items.append({
                    "article_number": artnr,
                    "product_name": "",
                    "supplier": "",
                    "supplier_item_no": "",
                    "specification": "",
                    "current_image_url": None,
                    "current_image_exists": False,
                    "image_score": 0,
                    "image_status": "ERROR",
                    "image_issues": str(e),
                    "image_count": 0,
                    "secondary_images": 0,
                    "needs_review": True,
                    "suggestion_reason": f"Analysefeil: {e}",
                    "suggested_image_url": None,
                    "suggested_source": None,
                    "suggested_source_url": None,
                    "candidates": [],
                    "selected_candidate_index": None,
                    "review_status": ImageReviewStatus.PENDING.value,
                    "review_timestamp": None,
                    "image_analyses": [],
                })
            finally:
                session["processed_products"] += 1
                if on_progress:
                    await on_progress(session["processed_products"], session["total_products"])

    # Run all analyses concurrently
    tasks = [analyze_one(artnr) for artnr in article_numbers]
    await asyncio.gather(*tasks)

    # Sort items: products needing review first, then by score ascending
    items.sort(key=lambda x: (not x["needs_review"], x["image_score"]))

    # Build summary
    total = len(items)
    missing = sum(1 for i in items if not i["current_image_exists"])
    low_quality = sum(1 for i in items if i["current_image_exists"] and i["image_score"] < 40)
    needs_review_count = sum(1 for i in items if i["needs_review"])
    good = sum(1 for i in items if i["current_image_exists"] and i["image_score"] >= 70)

    session["items"] = items
    session["status"] = ImageAnalysisStatus.COMPLETED.value
    session["current_product"] = None
    session["summary"] = {
        "total": total,
        "missing_images": missing,
        "low_quality": low_quality,
        "needs_review": needs_review_count,
        "good_quality": good,
        "avg_score": round(sum(i["image_score"] for i in items) / total, 1) if total > 0 else 0,
    }

    # Persist to disk
    _persist_session(session_id)
    return session


def _persist_session(session_id: str) -> None:
    """Write session data to disk."""
    if session_id not in _sessions:
        return
    try:
        path = IMAGE_ANALYSIS_DIR / f"{session_id}.json"
        data = _sessions[session_id].copy()
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning(f"Failed to persist image analysis session: {e}")


def load_session(session_id: str) -> dict:
    """Load session from memory or disk. Raises ValueError if not found."""
    if session_id in _sessions:
        return _sessions[session_id]

    path = IMAGE_ANALYSIS_DIR / f"{session_id}.json"
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            _sessions[session_id] = data
            return data
        except Exception as e:
            logger.error(f"Failed to load session {session_id}: {e}")

    raise ValueError(f"Session {session_id} not found")


def get_session_status(session_id: str) -> dict:
    """Get status of a session without loading full items."""
    session = load_session(session_id)
    return {
        "session_id": session["session_id"],
        "status": session["status"],
        "total_products": session["total_products"],
        "processed_products": session["processed_products"],
        "current_product": session.get("current_product"),
        "summary": session.get("summary", {}),
    }


def update_review_status(
    session_id: str,
    article_number: str,
    status: str,
    suggested_image_url: Optional[str] = None,
    selected_candidate_index: Optional[int] = None,
) -> dict:
    """Update review status for a single item."""
    session = load_session(session_id)
    valid = {s.value for s in ImageReviewStatus}
    if status not in valid:
        raise ValueError(f"Invalid status: {status}. Valid: {valid}")

    item = next((i for i in session["items"] if i["article_number"] == article_number), None)
    if not item:
        raise ValueError(f"Article {article_number} not found in session")

    old_status = item["review_status"]
    item["review_status"] = status
    item["review_timestamp"] = time.time()

    # If selecting a specific candidate, update the selected image
    if selected_candidate_index is not None:
        candidates = item.get("candidates", [])
        if 0 <= selected_candidate_index < len(candidates):
            chosen = candidates[selected_candidate_index]
            item["selected_candidate_index"] = selected_candidate_index
            item["suggested_image_url"] = chosen["image_url"]
            item["suggested_source"] = chosen["source_name"]
            item["suggested_source_url"] = chosen["source_url"]

    # If approving with a custom suggested image URL, store it
    if suggested_image_url:
        item["suggested_image_url"] = suggested_image_url

    # Log the decision
    session.setdefault("review_log", []).append({
        "article_number": article_number,
        "old_status": old_status,
        "new_status": status,
        "timestamp": time.time(),
    })

    _persist_session(session_id)
    return {"article_number": article_number, "review_status": status}


def bulk_update_review(
    session_id: str,
    article_numbers: list[str],
    status: str,
) -> dict:
    """Bulk update review status for multiple items."""
    session = load_session(session_id)
    valid = {s.value for s in ImageReviewStatus}
    if status not in valid:
        raise ValueError(f"Invalid status: {status}. Valid: {valid}")

    updated = 0
    article_set = set(article_numbers)
    timestamp = time.time()

    for item in session["items"]:
        if item["article_number"] in article_set:
            old_status = item["review_status"]
            item["review_status"] = status
            item["review_timestamp"] = timestamp
            session.setdefault("review_log", []).append({
                "article_number": item["article_number"],
                "old_status": old_status,
                "new_status": status,
                "timestamp": timestamp,
            })
            updated += 1

    _persist_session(session_id)
    return {"updated": updated, "review_status": status}


async def export_approved_images_zip(session_id: str) -> BytesIO:
    """Export all approved images as a ZIP file with JPEG format.

    Filenames follow the pattern:
    - Single image: ARTIKKELNUMMER.jpg
    - Multiple images: ARTIKKELNUMMER-1.jpg, ARTIKKELNUMMER-2.jpg, etc.
    """
    session = load_session(session_id)

    approved_items = [
        i for i in session["items"]
        if i["review_status"] == ImageReviewStatus.APPROVED.value
    ]

    if not approved_items:
        raise ValueError("Ingen godkjente bilder å eksportere")

    zip_buffer = BytesIO()
    errors = []

    async with httpx.AsyncClient(timeout=IMAGE_TIMEOUT) as client:
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for item in approved_items:
                artnr = item["article_number"].strip()
                if not artnr:
                    errors.append("Tomt artikkelnummer hoppet over")
                    continue

                # Collect all image URLs to download for this product
                image_urls = []

                # Use suggested image if available, otherwise use current
                if item.get("suggested_image_url"):
                    image_urls.append(item["suggested_image_url"])
                elif item.get("current_image_url"):
                    image_urls.append(item["current_image_url"])

                # Also include any approved secondary images from analyses
                for analysis in item.get("image_analyses", []):
                    if analysis.get("exists") and analysis.get("image_index", 0) > 0:
                        image_urls.append(analysis["image_url"])

                if not image_urls:
                    errors.append(f"{artnr}: Ingen bilde-URL tilgjengelig")
                    continue

                for idx, url in enumerate(image_urls):
                    try:
                        response = await client.get(url, headers=IMAGE_HEADERS, follow_redirects=True)
                        if response.status_code != 200 or len(response.content) < 500:
                            errors.append(f"{artnr}: Kunne ikke laste ned bilde fra {url}")
                            continue

                        # Convert to JPEG
                        try:
                            img = Image.open(BytesIO(response.content))
                            if img.mode in ("RGBA", "P", "LA"):
                                # Convert transparency to white background
                                background = Image.new("RGB", img.size, (255, 255, 255))
                                if img.mode == "P":
                                    img = img.convert("RGBA")
                                background.paste(img, mask=img.split()[-1] if "A" in img.mode else None)
                                img = background
                            elif img.mode != "RGB":
                                img = img.convert("RGB")

                            jpeg_buffer = BytesIO()
                            img.save(jpeg_buffer, format="JPEG", quality=90)
                            jpeg_bytes = jpeg_buffer.getvalue()
                        except Exception as e:
                            errors.append(f"{artnr}: Konverteringsfeil: {e}")
                            continue

                        # Build filename
                        if len(image_urls) == 1:
                            filename = f"{artnr}.jpg"
                        else:
                            filename = f"{artnr}-{idx + 1}.jpg"

                        zf.writestr(filename, jpeg_bytes)

                    except Exception as e:
                        errors.append(f"{artnr}: Nedlastingsfeil: {e}")

    zip_buffer.seek(0)
    return zip_buffer


def list_sessions() -> list[dict]:
    """List all available sessions (from disk)."""
    sessions = []
    for path in sorted(IMAGE_ANALYSIS_DIR.glob("img-*.json"), reverse=True):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            sessions.append({
                "session_id": data.get("session_id", path.stem),
                "status": data.get("status", "unknown"),
                "total_products": data.get("total_products", 0),
                "created_at": data.get("created_at", 0),
                "summary": data.get("summary", {}),
            })
        except Exception:
            pass
    return sessions[:20]  # Last 20 sessions


def get_suppliers_from_jeeves(jeeves_index) -> list[str]:
    """Extract unique supplier names from Jeeves index."""
    if not jeeves_index or not jeeves_index.loaded:
        return []

    suppliers = set()
    for artnr in jeeves_index.all_article_numbers():
        j = jeeves_index.get(artnr)
        if j and j.supplier:
            suppliers.add(j.supplier)

    return sorted(suppliers)


def get_articles_by_supplier(jeeves_index, supplier: str) -> list[str]:
    """Get all article numbers for a given supplier."""
    if not jeeves_index or not jeeves_index.loaded:
        return []

    articles = []
    for artnr in jeeves_index.all_article_numbers():
        j = jeeves_index.get(artnr)
        if j and j.supplier and j.supplier.lower() == supplier.lower():
            articles.append(artnr)

    return articles
