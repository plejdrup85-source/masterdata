"""Image quality analysis engine using classical computer vision.

Downloads product images from the OneMed CDN and analyzes them for:
- Resolution adequacy
- Sharpness (blur detection via Laplacian variance)
- Brightness (mean luminance)
- Contrast (standard deviation of luminance)
- Background cleanliness (white pixel ratio)
- Visual detail presence (edge density via Canny)
- Product framing (how much of the frame the product fills)

All analysis is deterministic, fast, and explainable.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum
from io import BytesIO
from typing import Optional

import cv2
import httpx
import numpy as np
from PIL import Image

logger = logging.getLogger(__name__)

# ── Image source URL pattern ──
IMAGE_BASE_URL = "https://res.onemed.com/NO/ARWebBig"

# ── Configurable thresholds ──
# Resolution
MIN_WIDTH = 400
MIN_HEIGHT = 400
IDEAL_WIDTH = 800
IDEAL_HEIGHT = 800

# Blur (Laplacian variance) - higher = sharper
BLUR_THRESHOLD_FAIL = 30.0
BLUR_THRESHOLD_GOOD = 100.0

# Brightness (0-255 mean)
BRIGHTNESS_MIN = 40
BRIGHTNESS_MAX = 240
BRIGHTNESS_IDEAL_MIN = 80
BRIGHTNESS_IDEAL_MAX = 220

# Contrast (std dev of grayscale)
CONTRAST_MIN = 15.0
CONTRAST_GOOD = 40.0

# Background whiteness (ratio of near-white pixels)
BG_WHITE_THRESHOLD = 230  # pixel value considered "white"
BG_CLEAN_MIN = 0.15       # minimum white ratio for "clean" background
BG_CLEAN_IDEAL = 0.30     # ideal white ratio

# Edge density (ratio of edge pixels from Canny)
EDGE_MIN = 0.005          # below this = visually empty
EDGE_GOOD = 0.02

# Product fill (ratio of non-background pixels)
FILL_MIN = 0.05           # product too small
FILL_MAX = 0.90           # product too cropped / no background visible
FILL_IDEAL_MIN = 0.10
FILL_IDEAL_MAX = 0.75

# Score weights for overall image score
SCORE_WEIGHTS = {
    "resolution": 0.15,
    "blur": 0.20,
    "brightness": 0.10,
    "contrast": 0.10,
    "background": 0.15,
    "edge": 0.10,
    "fill": 0.20,
}

# Max secondary images to check
MAX_SECONDARY_IMAGES = 10
# Stop after this many consecutive misses
MAX_CONSECUTIVE_MISSES = 2

# HTTP settings
IMAGE_TIMEOUT = 15  # seconds
IMAGE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "image/*,*/*;q=0.8",
}


class ImageStatus(str, Enum):
    PASS = "PASS"
    PASS_WITH_NOTES = "PASS_WITH_NOTES"
    REVIEW = "REVIEW"
    FAIL = "FAIL"
    MISSING = "MISSING"


class ImageIssue(str, Enum):
    LOW_RESOLUTION = "LOW_RESOLUTION"
    BLURRY = "BLURRY"
    BRIGHTNESS_OFF = "BRIGHTNESS_OFF"
    LOW_CONTRAST = "LOW_CONTRAST"
    BACKGROUND_NOT_CLEAN = "BACKGROUND_NOT_CLEAN"
    TOO_FEW_VISUAL_DETAILS = "TOO_FEW_VISUAL_DETAILS"
    PRODUCT_TOO_SMALL = "PRODUCT_TOO_SMALL"
    PRODUCT_TOO_CROPPED = "PRODUCT_TOO_CROPPED"
    IMAGE_NOT_FOUND = "IMAGE_NOT_FOUND"
    ANALYSIS_ERROR = "ANALYSIS_ERROR"
    # E-commerce specific issues
    LIKELY_NOT_PRODUCT = "LIKELY_NOT_PRODUCT"      # Logo, icon, illustration, placeholder
    UNPROFESSIONAL = "UNPROFESSIONAL"              # Low technical quality overall
    BAD_ASPECT_RATIO = "BAD_ASPECT_RATIO"          # Too wide/tall for product listing
    PLACEHOLDER_IMAGE = "PLACEHOLDER_IMAGE"         # Solid color or near-empty image


@dataclass
class SingleImageAnalysis:
    """Analysis result for one image."""
    artnr: str
    image_index: int  # 0 = main, 1 = -2.jpg, etc.
    image_name: str
    image_url: str
    exists: bool = False
    http_status: int = 0
    file_size_kb: float = 0.0
    width: int = 0
    height: int = 0
    aspect_ratio: float = 0.0
    resolution_score: float = 0.0
    blur_score_raw: float = 0.0
    blur_score: float = 0.0
    brightness_mean: float = 0.0
    brightness_score: float = 0.0
    contrast_std: float = 0.0
    contrast_score: float = 0.0
    white_bg_ratio: float = 0.0
    background_score: float = 0.0
    edge_density: float = 0.0
    edge_score: float = 0.0
    product_fill_ratio: float = 0.0
    fill_score: float = 0.0
    overall_score: float = 0.0
    # E-commerce composite scores
    technical_quality: float = 0.0      # Resolution + blur + brightness + contrast
    ecommerce_suitability: float = 0.0  # Background + fill + aspect + professional
    color_uniformity: float = 0.0       # 0=varied, 1=solid color (placeholder indicator)
    aspect_ratio_score: float = 0.0     # How suitable the aspect ratio is for listings
    is_likely_product: bool = True       # False if detected as logo/icon/placeholder
    image_type: str = "product"          # "product", "logo", "placeholder", "unknown"
    status: ImageStatus = ImageStatus.MISSING
    issues: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "artnr": self.artnr,
            "image_index": self.image_index,
            "image_name": self.image_name,
            "image_url": self.image_url,
            "exists": self.exists,
            "http_status": self.http_status,
            "file_size_kb": round(self.file_size_kb, 1),
            "width": self.width,
            "height": self.height,
            "aspect_ratio": round(self.aspect_ratio, 2),
            "resolution_score": round(self.resolution_score, 1),
            "blur_score_raw": round(self.blur_score_raw, 1),
            "blur_score": round(self.blur_score, 1),
            "brightness_mean": round(self.brightness_mean, 1),
            "brightness_score": round(self.brightness_score, 1),
            "contrast_std": round(self.contrast_std, 1),
            "contrast_score": round(self.contrast_score, 1),
            "white_bg_ratio": round(self.white_bg_ratio, 3),
            "background_score": round(self.background_score, 1),
            "edge_density": round(self.edge_density, 4),
            "edge_score": round(self.edge_score, 1),
            "product_fill_ratio": round(self.product_fill_ratio, 3),
            "fill_score": round(self.fill_score, 1),
            "overall_score": round(self.overall_score, 1),
            "technical_quality": round(self.technical_quality, 1),
            "ecommerce_suitability": round(self.ecommerce_suitability, 1),
            "color_uniformity": round(self.color_uniformity, 3),
            "aspect_ratio_score": round(self.aspect_ratio_score, 1),
            "is_likely_product": self.is_likely_product,
            "image_type": self.image_type,
            "status": self.status.value,
            "issues": self.issues,
        }


@dataclass
class ProductImageSummary:
    """Aggregated image quality summary for one product."""
    artnr: str
    image_count_found: int = 0
    main_image_exists: bool = False
    main_image_score: float = 0.0
    main_image_status: str = "MISSING"
    avg_image_score: float = 0.0
    best_image_score: float = 0.0
    secondary_images_found: int = 0
    image_issue_summary: str = ""
    image_quality_status: str = "MISSING"
    image_quality_priority: str = "none"
    # E-commerce aggregate scores
    technical_quality_avg: float = 0.0      # Average technical quality
    ecommerce_suitability_avg: float = 0.0  # Average e-commerce suitability
    main_is_product: bool = True             # Is main image a product photo?
    main_image_type: str = "unknown"         # Type of main image
    image_analyses: list[SingleImageAnalysis] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "artnr": self.artnr,
            "image_count_found": self.image_count_found,
            "main_image_exists": self.main_image_exists,
            "main_image_score": round(self.main_image_score, 1),
            "main_image_status": self.main_image_status,
            "avg_image_score": round(self.avg_image_score, 1),
            "best_image_score": round(self.best_image_score, 1),
            "secondary_images_found": self.secondary_images_found,
            "image_issue_summary": self.image_issue_summary,
            "image_quality_status": self.image_quality_status,
            "image_quality_priority": self.image_quality_priority,
            "technical_quality_avg": round(self.technical_quality_avg, 1),
            "ecommerce_suitability_avg": round(self.ecommerce_suitability_avg, 1),
            "main_is_product": self.main_is_product,
            "main_image_type": self.main_image_type,
            "image_analyses": [a.to_dict() for a in self.image_analyses],
        }


def _build_image_urls(article_number: str) -> list[tuple[int, str, str]]:
    """Build list of (index, name, url) for an article number."""
    clean = article_number.strip()
    urls = [(0, f"{clean}.jpg", f"{IMAGE_BASE_URL}/{clean}.jpg")]
    for i in range(2, MAX_SECONDARY_IMAGES + 2):
        urls.append((i - 1, f"{clean}-{i}.jpg", f"{IMAGE_BASE_URL}/{clean}-{i}.jpg"))
    return urls


def _score_resolution(width: int, height: int) -> float:
    """Score 0-100 based on image dimensions."""
    if width == 0 or height == 0:
        return 0.0
    min_dim = min(width, height)
    max_dim = max(width, height)
    ideal_min = min(IDEAL_WIDTH, IDEAL_HEIGHT)

    if min_dim >= ideal_min:
        return 100.0
    elif min_dim >= MIN_WIDTH:
        return 50.0 + 50.0 * (min_dim - MIN_WIDTH) / (ideal_min - MIN_WIDTH)
    else:
        return max(0.0, 50.0 * min_dim / MIN_WIDTH)


def _score_blur(laplacian_var: float) -> float:
    """Score 0-100 based on Laplacian variance (sharpness)."""
    if laplacian_var >= BLUR_THRESHOLD_GOOD:
        return 100.0
    elif laplacian_var >= BLUR_THRESHOLD_FAIL:
        return 40.0 + 60.0 * (laplacian_var - BLUR_THRESHOLD_FAIL) / (BLUR_THRESHOLD_GOOD - BLUR_THRESHOLD_FAIL)
    else:
        return max(0.0, 40.0 * laplacian_var / BLUR_THRESHOLD_FAIL)


def _score_brightness(mean_brightness: float) -> float:
    """Score 0-100 based on mean brightness."""
    if BRIGHTNESS_IDEAL_MIN <= mean_brightness <= BRIGHTNESS_IDEAL_MAX:
        return 100.0
    elif BRIGHTNESS_MIN <= mean_brightness <= BRIGHTNESS_MAX:
        if mean_brightness < BRIGHTNESS_IDEAL_MIN:
            return 60.0 + 40.0 * (mean_brightness - BRIGHTNESS_MIN) / (BRIGHTNESS_IDEAL_MIN - BRIGHTNESS_MIN)
        else:
            return 60.0 + 40.0 * (BRIGHTNESS_MAX - mean_brightness) / (BRIGHTNESS_MAX - BRIGHTNESS_IDEAL_MAX)
    else:
        return 20.0


def _score_contrast(std_dev: float) -> float:
    """Score 0-100 based on contrast (std dev of grayscale)."""
    if std_dev >= CONTRAST_GOOD:
        return 100.0
    elif std_dev >= CONTRAST_MIN:
        return 40.0 + 60.0 * (std_dev - CONTRAST_MIN) / (CONTRAST_GOOD - CONTRAST_MIN)
    else:
        return max(0.0, 40.0 * std_dev / CONTRAST_MIN)


def _score_background(white_ratio: float) -> float:
    """Score 0-100 based on background cleanliness."""
    if white_ratio >= BG_CLEAN_IDEAL:
        return 100.0
    elif white_ratio >= BG_CLEAN_MIN:
        return 50.0 + 50.0 * (white_ratio - BG_CLEAN_MIN) / (BG_CLEAN_IDEAL - BG_CLEAN_MIN)
    else:
        return max(0.0, 50.0 * white_ratio / BG_CLEAN_MIN)


def _score_edges(edge_density: float) -> float:
    """Score 0-100 based on edge/detail presence."""
    if edge_density >= EDGE_GOOD:
        return 100.0
    elif edge_density >= EDGE_MIN:
        return 40.0 + 60.0 * (edge_density - EDGE_MIN) / (EDGE_GOOD - EDGE_MIN)
    else:
        return max(0.0, 40.0 * edge_density / EDGE_MIN)


def _score_fill(fill_ratio: float) -> float:
    """Score 0-100 based on product fill ratio."""
    if FILL_IDEAL_MIN <= fill_ratio <= FILL_IDEAL_MAX:
        return 100.0
    elif FILL_MIN <= fill_ratio <= FILL_MAX:
        if fill_ratio < FILL_IDEAL_MIN:
            return 50.0 + 50.0 * (fill_ratio - FILL_MIN) / (FILL_IDEAL_MIN - FILL_MIN)
        else:
            return 50.0 + 50.0 * (FILL_MAX - fill_ratio) / (FILL_MAX - FILL_IDEAL_MAX)
    elif fill_ratio < FILL_MIN:
        return max(0.0, 50.0 * fill_ratio / FILL_MIN)
    else:
        return 30.0  # Extremely cropped


def _score_aspect_ratio(aspect: float) -> float:
    """Score 0-100 based on aspect ratio suitability for e-commerce.

    Ideal: 0.75-1.33 (roughly square, standard product listing format).
    Penalize very wide (banner) or very tall (strip) images.
    """
    if aspect <= 0:
        return 0.0
    if 0.75 <= aspect <= 1.33:
        return 100.0
    elif 0.5 <= aspect <= 2.0:
        # Moderate penalty
        if aspect < 0.75:
            return 60.0 + 40.0 * (aspect - 0.5) / 0.25
        else:
            return 60.0 + 40.0 * (2.0 - aspect) / 0.67
    else:
        return max(10.0, 30.0)  # Extreme aspect ratio


def _detect_image_type(result: "SingleImageAnalysis") -> str:
    """Classify the image type using heuristics.

    Returns: "product", "logo", "placeholder", "unknown"
    """
    # Placeholder: very high color uniformity + very low edge density
    if result.color_uniformity > 0.90 and result.edge_density < 0.005:
        return "placeholder"

    # Logo/icon: very small file + low fill + high contrast often
    if (result.file_size_kb < 15
            and min(result.width, result.height) < 300
            and result.product_fill_ratio < 0.15):
        return "logo"

    # Logo: very few visual details + very small area of interest
    if result.edge_density < 0.008 and result.product_fill_ratio < 0.08:
        return "logo"

    return "product"


def _compute_color_uniformity(gray: "np.ndarray") -> float:
    """Compute color uniformity (0=varied, 1=solid color).

    High uniformity suggests a placeholder or solid-color background without a product.
    Uses the ratio of the most common color bin to total pixels.
    """
    # Quantize to 16 bins for efficiency
    hist = cv2.calcHist([gray], [0], None, [16], [0, 256])
    total = gray.size
    if total == 0:
        return 0.0
    dominant_ratio = float(hist.max()) / total
    return dominant_ratio


def analyze_image_data(image_bytes: bytes, artnr: str, index: int, name: str, url: str) -> SingleImageAnalysis:
    """Analyze a single image using classical CV techniques."""
    result = SingleImageAnalysis(
        artnr=artnr,
        image_index=index,
        image_name=name,
        image_url=url,
        exists=True,
        file_size_kb=len(image_bytes) / 1024.0,
    )

    try:
        # Load with Pillow for dimensions
        pil_img = Image.open(BytesIO(image_bytes))
        result.width = pil_img.width
        result.height = pil_img.height
        result.aspect_ratio = pil_img.width / pil_img.height if pil_img.height > 0 else 0

        # Convert to numpy/OpenCV
        img_rgb = np.array(pil_img.convert("RGB"))
        gray = cv2.cvtColor(img_rgb, cv2.COLOR_RGB2GRAY)

        # 1. Resolution score
        result.resolution_score = _score_resolution(result.width, result.height)
        if min(result.width, result.height) < MIN_WIDTH:
            result.issues.append(ImageIssue.LOW_RESOLUTION.value)

        # 2. Blur / sharpness (Laplacian variance)
        laplacian = cv2.Laplacian(gray, cv2.CV_64F)
        result.blur_score_raw = float(laplacian.var())
        result.blur_score = _score_blur(result.blur_score_raw)
        if result.blur_score_raw < BLUR_THRESHOLD_FAIL:
            result.issues.append(ImageIssue.BLURRY.value)

        # 3. Brightness (mean of grayscale)
        result.brightness_mean = float(gray.mean())
        result.brightness_score = _score_brightness(result.brightness_mean)
        if result.brightness_mean < BRIGHTNESS_MIN or result.brightness_mean > BRIGHTNESS_MAX:
            result.issues.append(ImageIssue.BRIGHTNESS_OFF.value)

        # 4. Contrast (std dev of grayscale)
        result.contrast_std = float(gray.std())
        result.contrast_score = _score_contrast(result.contrast_std)
        if result.contrast_std < CONTRAST_MIN:
            result.issues.append(ImageIssue.LOW_CONTRAST.value)

        # 5. Background cleanliness (ratio of near-white pixels)
        white_mask = gray >= BG_WHITE_THRESHOLD
        total_pixels = gray.size
        result.white_bg_ratio = float(white_mask.sum()) / total_pixels if total_pixels > 0 else 0
        result.background_score = _score_background(result.white_bg_ratio)
        if result.white_bg_ratio < BG_CLEAN_MIN:
            result.issues.append(ImageIssue.BACKGROUND_NOT_CLEAN.value)

        # 6. Edge density (Canny edge detection)
        edges = cv2.Canny(gray, 50, 150)
        edge_pixels = (edges > 0).sum()
        result.edge_density = float(edge_pixels) / total_pixels if total_pixels > 0 else 0
        result.edge_score = _score_edges(result.edge_density)
        if result.edge_density < EDGE_MIN:
            result.issues.append(ImageIssue.TOO_FEW_VISUAL_DETAILS.value)

        # 7. Product fill ratio (non-white pixels / total)
        # Use Otsu threshold to separate foreground from background
        _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        foreground_pixels = (binary > 0).sum()
        result.product_fill_ratio = float(foreground_pixels) / total_pixels if total_pixels > 0 else 0
        result.fill_score = _score_fill(result.product_fill_ratio)
        if result.product_fill_ratio < FILL_MIN:
            result.issues.append(ImageIssue.PRODUCT_TOO_SMALL.value)
        elif result.product_fill_ratio > FILL_MAX:
            result.issues.append(ImageIssue.PRODUCT_TOO_CROPPED.value)

        # ── E-commerce enhancements ──

        # Color uniformity (placeholder detection)
        result.color_uniformity = _compute_color_uniformity(gray)
        if result.color_uniformity > 0.90 and result.edge_density < 0.005:
            result.issues.append(ImageIssue.PLACEHOLDER_IMAGE.value)

        # Aspect ratio suitability
        result.aspect_ratio_score = _score_aspect_ratio(result.aspect_ratio)
        if result.aspect_ratio > 0 and (result.aspect_ratio < 0.5 or result.aspect_ratio > 2.0):
            result.issues.append(ImageIssue.BAD_ASPECT_RATIO.value)

        # Image type detection
        result.image_type = _detect_image_type(result)
        result.is_likely_product = result.image_type == "product"
        if not result.is_likely_product:
            result.issues.append(ImageIssue.LIKELY_NOT_PRODUCT.value)

        # Technical quality: resolution + sharpness + brightness + contrast
        result.technical_quality = (
            result.resolution_score * 0.25
            + result.blur_score * 0.35
            + result.brightness_score * 0.20
            + result.contrast_score * 0.20
        )

        # E-commerce suitability: background + fill + aspect ratio + product type
        product_type_score = 100.0 if result.is_likely_product else 20.0
        result.ecommerce_suitability = (
            result.background_score * 0.30
            + result.fill_score * 0.25
            + result.aspect_ratio_score * 0.20
            + product_type_score * 0.25
        )

        # Professional quality flag
        if result.technical_quality < 40 and result.ecommerce_suitability < 40:
            result.issues.append(ImageIssue.UNPROFESSIONAL.value)

        # Calculate weighted overall score
        result.overall_score = (
            result.resolution_score * SCORE_WEIGHTS["resolution"]
            + result.blur_score * SCORE_WEIGHTS["blur"]
            + result.brightness_score * SCORE_WEIGHTS["brightness"]
            + result.contrast_score * SCORE_WEIGHTS["contrast"]
            + result.background_score * SCORE_WEIGHTS["background"]
            + result.edge_score * SCORE_WEIGHTS["edge"]
            + result.fill_score * SCORE_WEIGHTS["fill"]
        )

        # Penalize non-product images in overall score
        if not result.is_likely_product:
            result.overall_score = min(result.overall_score, 30.0)

        # Determine status
        if not result.issues:
            result.status = ImageStatus.PASS
        elif result.overall_score >= 70:
            result.status = ImageStatus.PASS_WITH_NOTES
        elif result.overall_score >= 40:
            result.status = ImageStatus.REVIEW
        else:
            result.status = ImageStatus.FAIL

    except Exception as e:
        logger.error(f"Image analysis error for {name}: {e}")
        result.issues.append(ImageIssue.ANALYSIS_ERROR.value)
        result.status = ImageStatus.FAIL
        result.overall_score = 0.0

    return result


async def analyze_product_images(
    article_number: str,
    client: Optional[httpx.AsyncClient] = None,
) -> ProductImageSummary:
    """Download and analyze all images for a product.

    Tries main image first, then secondary images (-2, -3, etc.).
    Stops checking secondary images after MAX_CONSECUTIVE_MISSES misses.
    """
    summary = ProductImageSummary(artnr=article_number)
    image_urls = _build_image_urls(article_number)

    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=IMAGE_TIMEOUT)

    try:
        consecutive_misses = 0

        for index, name, url in image_urls:
            # Stop checking secondary images after consecutive misses
            if index > 0 and consecutive_misses >= MAX_CONSECUTIVE_MISSES:
                break

            try:
                response = await client.get(url, headers=IMAGE_HEADERS, follow_redirects=True)

                if response.status_code == 200 and len(response.content) > 500:
                    # Check content type
                    content_type = response.headers.get("content-type", "")
                    if "image" not in content_type and len(response.content) < 2000:
                        # Probably an error page, not an image
                        consecutive_misses += 1
                        if index == 0:
                            result = SingleImageAnalysis(
                                artnr=article_number,
                                image_index=index,
                                image_name=name,
                                image_url=url,
                                exists=False,
                                http_status=response.status_code,
                                status=ImageStatus.MISSING,
                                issues=[ImageIssue.IMAGE_NOT_FOUND.value],
                            )
                            summary.image_analyses.append(result)
                        continue

                    # Analyze the image
                    result = await asyncio.get_running_loop().run_in_executor(
                        None,
                        analyze_image_data,
                        response.content,
                        article_number,
                        index,
                        name,
                        url,
                    )
                    result.http_status = response.status_code
                    summary.image_analyses.append(result)
                    consecutive_misses = 0

                else:
                    consecutive_misses += 1
                    if index == 0:
                        result = SingleImageAnalysis(
                            artnr=article_number,
                            image_index=index,
                            image_name=name,
                            image_url=url,
                            exists=False,
                            http_status=response.status_code,
                            status=ImageStatus.MISSING,
                            issues=[ImageIssue.IMAGE_NOT_FOUND.value],
                        )
                        summary.image_analyses.append(result)

            except Exception as e:
                logger.warning(f"Failed to fetch image {url}: {e}")
                consecutive_misses += 1
                if index == 0:
                    result = SingleImageAnalysis(
                        artnr=article_number,
                        image_index=index,
                        image_name=name,
                        image_url=url,
                        exists=False,
                        status=ImageStatus.MISSING,
                        issues=[ImageIssue.IMAGE_NOT_FOUND.value],
                    )
                    summary.image_analyses.append(result)

    finally:
        if own_client:
            await client.aclose()

    # Aggregate results
    found_analyses = [a for a in summary.image_analyses if a.exists]
    summary.image_count_found = len(found_analyses)
    summary.secondary_images_found = max(0, summary.image_count_found - 1)

    if summary.image_analyses:
        main = summary.image_analyses[0]
        summary.main_image_exists = main.exists
        summary.main_image_score = main.overall_score if main.exists else 0.0
        summary.main_image_status = main.status.value

    if found_analyses:
        scores = [a.overall_score for a in found_analyses]
        summary.avg_image_score = sum(scores) / len(scores)
        summary.best_image_score = max(scores)

        # E-commerce aggregate scores
        tech_scores = [a.technical_quality for a in found_analyses]
        ecom_scores = [a.ecommerce_suitability for a in found_analyses]
        summary.technical_quality_avg = sum(tech_scores) / len(tech_scores)
        summary.ecommerce_suitability_avg = sum(ecom_scores) / len(ecom_scores)

        # Main image type info
        if summary.image_analyses and summary.image_analyses[0].exists:
            main_analysis = summary.image_analyses[0]
            summary.main_is_product = main_analysis.is_likely_product
            summary.main_image_type = main_analysis.image_type

        # Collect all unique issues
        all_issues = set()
        for a in found_analyses:
            all_issues.update(a.issues)
        # Also add MISSING for main image if not found
        if not summary.main_image_exists:
            all_issues.add(ImageIssue.IMAGE_NOT_FOUND.value)
        summary.image_issue_summary = ", ".join(sorted(all_issues)) if all_issues else "Ingen problemer"
    else:
        summary.image_issue_summary = ImageIssue.IMAGE_NOT_FOUND.value

    # Determine overall image quality status
    if not summary.main_image_exists:
        summary.image_quality_status = "MISSING"
        summary.image_quality_priority = "high"
    elif summary.avg_image_score >= 70:
        summary.image_quality_status = "PASS" if not any(
            a.status in (ImageStatus.REVIEW, ImageStatus.FAIL) for a in found_analyses
        ) else "PASS_WITH_NOTES"
        summary.image_quality_priority = "none" if summary.image_quality_status == "PASS" else "low"
    elif summary.avg_image_score >= 40:
        summary.image_quality_status = "REVIEW"
        summary.image_quality_priority = "medium"
    else:
        summary.image_quality_status = "FAIL"
        summary.image_quality_priority = "high"

    return summary
