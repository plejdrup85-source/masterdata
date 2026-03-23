"""Tests for index fingerprint and cache validation logic."""

import json
import pytest
from pathlib import Path

from backend.index_fingerprint import (
    compute_sitemap_fingerprint,
    compute_index_signature,
    save_index_fingerprint,
    load_index_fingerprint,
    should_rebuild_index,
    load_cached_index_if_valid,
)


@pytest.fixture
def cache_dir(tmp_path):
    """Create a temp cache directory for tests."""
    d = tmp_path / "cache"
    d.mkdir()
    return d


SAMPLE_URLS = [
    "https://example.com/products/p1",
    "https://example.com/products/p2",
    "https://example.com/products/p3",
]


class TestComputeSitemapFingerprint:
    def test_deterministic(self):
        fp1 = compute_sitemap_fingerprint(SAMPLE_URLS)
        fp2 = compute_sitemap_fingerprint(SAMPLE_URLS)
        assert fp1 == fp2

    def test_order_independent(self):
        """Same URLs in different order should produce same fingerprint."""
        fp1 = compute_sitemap_fingerprint(["a", "c", "b"])
        fp2 = compute_sitemap_fingerprint(["b", "a", "c"])
        assert fp1 == fp2

    def test_different_urls_different_fingerprint(self):
        fp1 = compute_sitemap_fingerprint(SAMPLE_URLS)
        fp2 = compute_sitemap_fingerprint(SAMPLE_URLS + ["https://example.com/products/p4"])
        assert fp1 != fp2

    def test_hex_string(self):
        fp = compute_sitemap_fingerprint(SAMPLE_URLS)
        assert isinstance(fp, str)
        assert len(fp) == 32
        int(fp, 16)  # Should be valid hex


class TestComputeIndexSignature:
    def test_deterministic(self):
        sig1 = compute_index_signature("abc", 100, 50)
        sig2 = compute_index_signature("abc", 100, 50)
        assert sig1 == sig2

    def test_changes_with_inputs(self):
        base = compute_index_signature("abc", 100, 50)
        diff_fp = compute_index_signature("xyz", 100, 50)
        diff_count = compute_index_signature("abc", 200, 50)
        assert base != diff_fp
        assert base != diff_count


class TestSaveLoadFingerprint:
    def test_save_and_load(self, cache_dir):
        save_index_fingerprint(cache_dir, "fp123", 100, 50, 200)
        loaded = load_index_fingerprint(cache_dir)
        assert loaded is not None
        assert loaded["sitemap_fingerprint"] == "fp123"
        assert loaded["sku_count"] == 100
        assert loaded["checked_no_sku_count"] == 50
        assert loaded["sitemap_url_count"] == 200
        assert "signature" in loaded

    def test_load_missing(self, cache_dir):
        loaded = load_index_fingerprint(cache_dir)
        assert loaded is None

    def test_load_corrupted(self, cache_dir):
        fp_path = cache_dir / "_index_fingerprint.json"
        fp_path.write_text("not json", encoding="utf-8")
        loaded = load_index_fingerprint(cache_dir)
        assert loaded is None

    def test_load_missing_fields(self, cache_dir):
        fp_path = cache_dir / "_index_fingerprint.json"
        fp_path.write_text('{"foo": "bar"}', encoding="utf-8")
        loaded = load_index_fingerprint(cache_dir)
        assert loaded is None


class TestShouldRebuildIndex:
    def test_no_fingerprint_no_index_needs_rebuild(self, cache_dir):
        needs, reason = should_rebuild_index(cache_dir, SAMPLE_URLS, 0, 0)
        assert needs is True
        assert "nødvendig" in reason.lower()

    def test_no_fingerprint_with_existing_index_reuses(self, cache_dir):
        """If we have an index but no fingerprint, save fingerprint and reuse."""
        needs, reason = should_rebuild_index(cache_dir, SAMPLE_URLS, 500, 100)
        assert needs is False
        assert "gjenbruker" in reason.lower()
        # Should have saved fingerprint
        loaded = load_index_fingerprint(cache_dir)
        assert loaded is not None

    def test_same_sitemap_no_rebuild(self, cache_dir):
        fp = compute_sitemap_fingerprint(SAMPLE_URLS)
        save_index_fingerprint(cache_dir, fp, 100, 50, len(SAMPLE_URLS))
        needs, reason = should_rebuild_index(cache_dir, SAMPLE_URLS, 100, 50)
        assert needs is False
        assert "matcher" in reason.lower()

    def test_changed_sitemap_triggers_rebuild(self, cache_dir):
        fp = compute_sitemap_fingerprint(SAMPLE_URLS)
        save_index_fingerprint(cache_dir, fp, 100, 50, len(SAMPLE_URLS))
        new_urls = SAMPLE_URLS + ["https://example.com/products/new"]
        needs, reason = should_rebuild_index(cache_dir, new_urls, 100, 50)
        assert needs is True
        assert "endret" in reason.lower()

    def test_empty_index_with_urls_triggers_rebuild(self, cache_dir):
        fp = compute_sitemap_fingerprint(SAMPLE_URLS)
        save_index_fingerprint(cache_dir, fp, 0, 0, len(SAMPLE_URLS))
        needs, reason = should_rebuild_index(cache_dir, SAMPLE_URLS, 0, 0)
        assert needs is True

    def test_unchanged_sitemap_after_deploy(self, cache_dir):
        """Simulates a deploy where the cache dir is persistent and data unchanged."""
        fp = compute_sitemap_fingerprint(SAMPLE_URLS)
        save_index_fingerprint(cache_dir, fp, 500, 200, len(SAMPLE_URLS))

        # After deploy, load and check — should NOT rebuild
        needs, reason = should_rebuild_index(cache_dir, SAMPLE_URLS, 500, 200)
        assert needs is False
        assert "rebuild ikke nødvendig" in reason.lower()


class TestLoadCachedIndexIfValid:
    def test_no_files_returns_none(self, cache_dir, monkeypatch):
        monkeypatch.setattr("backend.scraper.SITEMAP_INDEX_PATH", cache_dir / "nonexistent.json")
        result = load_cached_index_if_valid(cache_dir)
        assert result is None

    def test_valid_index_returns_data(self, cache_dir, monkeypatch):
        # Create mock index files
        idx_path = cache_dir / "_sitemap_sku_index.json"
        urls_path = cache_dir / "_sitemap_urls.json"
        no_sku_path = cache_dir / "_checked_no_sku.json"

        idx_path.write_text('{"12345": "https://example.com/p/12345"}', encoding="utf-8")
        urls_path.write_text('["https://example.com/p/12345"]', encoding="utf-8")
        no_sku_path.write_text('["https://example.com/p/unknown"]', encoding="utf-8")

        monkeypatch.setattr("backend.scraper.SITEMAP_INDEX_PATH", idx_path)
        monkeypatch.setattr("backend.scraper.SITEMAP_URLS_PATH", urls_path)
        monkeypatch.setattr("backend.scraper.CHECKED_NO_SKU_PATH", no_sku_path)

        result = load_cached_index_if_valid(cache_dir)
        assert result is not None
        assert len(result["sku_index"]) == 1
        assert len(result["urls"]) == 1
        assert len(result["no_sku"]) == 1

    def test_empty_index_returns_none(self, cache_dir, monkeypatch):
        idx_path = cache_dir / "_sitemap_sku_index.json"
        idx_path.write_text("{}", encoding="utf-8")
        monkeypatch.setattr("backend.scraper.SITEMAP_INDEX_PATH", idx_path)

        result = load_cached_index_if_valid(cache_dir)
        assert result is None


class TestDeterministicStability:
    """Verify that fingerprints are stable across multiple calls and orderings."""

    def test_multiple_runs_same_result(self):
        """Running 10 times should give identical results."""
        results = [compute_sitemap_fingerprint(SAMPLE_URLS) for _ in range(10)]
        assert len(set(results)) == 1

    def test_large_url_list_stability(self):
        """Large list should still be deterministic."""
        urls = [f"https://example.com/products/{i}" for i in range(10000)]
        fp1 = compute_sitemap_fingerprint(urls)
        fp2 = compute_sitemap_fingerprint(list(reversed(urls)))
        assert fp1 == fp2

    def test_no_timestamp_dependency(self, cache_dir):
        """Fingerprint should not change based on when it's computed."""
        import time
        fp1 = compute_sitemap_fingerprint(SAMPLE_URLS)
        time.sleep(0.01)
        fp2 = compute_sitemap_fingerprint(SAMPLE_URLS)
        assert fp1 == fp2
