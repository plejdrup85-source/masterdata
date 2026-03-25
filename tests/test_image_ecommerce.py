"""Tests for enhanced e-commerce image scoring."""

import io
import pytest
import numpy as np
from PIL import Image as PILImage

from backend.image_analyzer import (
    analyze_image_data,
    _score_aspect_ratio,
    _detect_image_type,
    _compute_color_uniformity,
    SingleImageAnalysis,
    ImageIssue,
    ImageStatus,
)


def _create_test_image(
    width=800, height=800,
    bg_color=(255, 255, 255),
    product_color=(50, 80, 120),
    product_fill=0.3,
    add_noise=True,
) -> bytes:
    """Create a synthetic test image as JPEG bytes."""
    img = PILImage.new("RGB", (width, height), bg_color)
    pixels = np.array(img)

    if product_fill > 0:
        # Draw a centered rectangle as "product"
        fill_area = product_fill
        product_h = int(height * fill_area ** 0.5)
        product_w = int(width * fill_area ** 0.5)
        y1 = (height - product_h) // 2
        x1 = (width - product_w) // 2
        pixels[y1:y1+product_h, x1:x1+product_w] = product_color

        if add_noise:
            # Add some edge detail to the product area
            for i in range(0, product_h, 10):
                pixels[y1+i, x1:x1+product_w] = (
                    min(255, product_color[0] + 50),
                    min(255, product_color[1] + 50),
                    min(255, product_color[2] + 50),
                )

    img = PILImage.fromarray(pixels)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def _create_solid_color_image(width=200, height=200, color=(200, 200, 200)) -> bytes:
    """Create a solid-color image (placeholder)."""
    img = PILImage.new("RGB", (width, height), color)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def _create_tiny_logo(width=100, height=100) -> bytes:
    """Create a tiny image that resembles a logo."""
    img = PILImage.new("RGB", (width, height), (255, 255, 255))
    pixels = np.array(img)
    # Small mark in center
    pixels[45:55, 45:55] = (0, 0, 0)
    img = PILImage.fromarray(pixels)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=50)
    return buf.getvalue()


class TestAspectRatioScore:
    def test_square_is_ideal(self):
        assert _score_aspect_ratio(1.0) == 100.0

    def test_nearly_square_is_ideal(self):
        assert _score_aspect_ratio(0.9) == 100.0
        assert _score_aspect_ratio(1.2) == 100.0

    def test_moderately_wide_penalized(self):
        score = _score_aspect_ratio(1.8)
        assert 60 <= score < 100

    def test_extreme_aspect_heavily_penalized(self):
        score = _score_aspect_ratio(3.0)
        assert score <= 30

    def test_zero_returns_zero(self):
        assert _score_aspect_ratio(0) == 0.0


class TestColorUniformity:
    def test_solid_color_high_uniformity(self):
        img = PILImage.new("L", (100, 100), 200)
        gray = np.array(img)
        uniformity = _compute_color_uniformity(gray)
        assert uniformity > 0.8

    def test_varied_image_low_uniformity(self):
        gray = np.random.randint(0, 256, (100, 100), dtype=np.uint8)
        uniformity = _compute_color_uniformity(gray)
        assert uniformity < 0.3


class TestImageTypeDetection:
    def test_good_product_detected(self):
        img_bytes = _create_test_image(width=800, height=800, product_fill=0.3)
        result = analyze_image_data(img_bytes, "TEST1", 0, "test.jpg", "http://test/test.jpg")
        assert result.is_likely_product
        assert result.image_type == "product"

    def test_solid_placeholder_detected(self):
        img_bytes = _create_solid_color_image(200, 200)
        result = analyze_image_data(img_bytes, "TEST2", 0, "test.jpg", "http://test/test.jpg")
        assert result.image_type == "placeholder"
        assert not result.is_likely_product
        assert ImageIssue.PLACEHOLDER_IMAGE.value in result.issues

    def test_tiny_logo_detected(self):
        img_bytes = _create_tiny_logo()
        result = analyze_image_data(img_bytes, "TEST3", 0, "test.jpg", "http://test/test.jpg")
        # Small + minimal content → logo or placeholder
        assert result.image_type in ("logo", "placeholder")
        assert not result.is_likely_product


class TestEcommerceScores:
    def test_good_product_image_scores(self):
        img_bytes = _create_test_image(800, 800, product_fill=0.3, bg_color=(255, 255, 255))
        result = analyze_image_data(img_bytes, "GOOD", 0, "good.jpg", "http://test/good.jpg")
        assert result.technical_quality > 50
        assert result.ecommerce_suitability > 50
        assert result.overall_score > 50

    def test_low_res_gets_lower_technical_quality(self):
        """Low resolution penalizes technical quality vs high resolution."""
        low = _create_test_image(200, 200, product_fill=0.3)
        high = _create_test_image(800, 800, product_fill=0.3)
        low_result = analyze_image_data(low, "LOWRES", 0, "low.jpg", "http://test/low.jpg")
        high_result = analyze_image_data(high, "HIGHRES", 0, "high.jpg", "http://test/high.jpg")
        assert low_result.technical_quality < high_result.technical_quality
        assert low_result.resolution_score < high_result.resolution_score

    def test_non_product_capped_overall(self):
        """Non-product images should have capped overall score."""
        img_bytes = _create_solid_color_image(400, 400)
        result = analyze_image_data(img_bytes, "PLACEHOLDER", 0, "ph.jpg", "http://test/ph.jpg")
        if not result.is_likely_product:
            assert result.overall_score <= 30

    def test_bad_aspect_ratio_flagged(self):
        img_bytes = _create_test_image(1200, 300, product_fill=0.3)
        result = analyze_image_data(img_bytes, "WIDE", 0, "wide.jpg", "http://test/wide.jpg")
        assert result.aspect_ratio_score < 60
        assert ImageIssue.BAD_ASPECT_RATIO.value in result.issues

    def test_good_aspect_ratio_not_flagged(self):
        img_bytes = _create_test_image(800, 800)
        result = analyze_image_data(img_bytes, "SQUARE", 0, "sq.jpg", "http://test/sq.jpg")
        assert result.aspect_ratio_score == 100.0
        assert ImageIssue.BAD_ASPECT_RATIO.value not in result.issues


class TestNewFieldsInOutput:
    def test_to_dict_has_new_fields(self):
        img_bytes = _create_test_image()
        result = analyze_image_data(img_bytes, "DICT", 0, "dict.jpg", "http://test/dict.jpg")
        d = result.to_dict()
        assert "technical_quality" in d
        assert "ecommerce_suitability" in d
        assert "color_uniformity" in d
        assert "aspect_ratio_score" in d
        assert "is_likely_product" in d
        assert "image_type" in d

    def test_scores_are_0_to_100(self):
        img_bytes = _create_test_image()
        result = analyze_image_data(img_bytes, "RANGE", 0, "range.jpg", "http://test/range.jpg")
        assert 0 <= result.technical_quality <= 100
        assert 0 <= result.ecommerce_suitability <= 100
        assert 0 <= result.aspect_ratio_score <= 100


class TestIssueFlags:
    def test_placeholder_issue(self):
        img_bytes = _create_solid_color_image()
        result = analyze_image_data(img_bytes, "PH", 0, "ph.jpg", "http://test/ph.jpg")
        assert ImageIssue.PLACEHOLDER_IMAGE.value in result.issues or ImageIssue.LIKELY_NOT_PRODUCT.value in result.issues

    def test_good_image_no_ecommerce_issues(self):
        img_bytes = _create_test_image(800, 800, product_fill=0.3)
        result = analyze_image_data(img_bytes, "GOOD2", 0, "good.jpg", "http://test/good.jpg")
        ecom_issues = {
            ImageIssue.LIKELY_NOT_PRODUCT.value,
            ImageIssue.PLACEHOLDER_IMAGE.value,
            ImageIssue.BAD_ASPECT_RATIO.value,
            ImageIssue.UNPROFESSIONAL.value,
        }
        found_ecom = set(result.issues) & ecom_issues
        assert len(found_ecom) == 0, f"Unexpected e-commerce issues: {found_ecom}"
