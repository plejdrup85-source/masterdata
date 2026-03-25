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
    RebuildDecision,
    INDEX_FORMAT_VERSION,
    SIGNIFICANT_CHANGE_THRESHOLD,
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

    def test_includes_format_version(self):
        """Signature should change if format version changes."""
        sig_v2 = compute_index_signature("abc", 100, 50, index_format_version=2)
        sig_v3 = compute_index_signature("abc", 100, 50, index_format_version=3)
        assert sig_v2 != sig_v3


class TestSaveLoadFingerprint:
    def test_save_and_load(self, cache_dir):
        save_index_fingerprint(cache_dir, "fp123", 100, 50, 200)
        loaded = load_index_fingerprint(cache_dir)
        assert loaded is not None
        assert loaded["sitemap_fingerprint"] == "fp123"
        assert loaded["sku_count"] == 100
        assert loaded["checked_no_sku_count"] == 50
        assert loaded["sitemap_url_count"] == 200
        assert loaded["index_format_version"] == INDEX_FORMAT_VERSION
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
    """Test the rebuild decision logic — the core of preventing unnecessary rebuilds."""

    def test_no_fingerprint_no_index_needs_full_rebuild(self, cache_dir):
        """First-time startup with no cache should trigger full rebuild."""
        decision = should_rebuild_index(cache_dir, SAMPLE_URLS, 0, 0)
        assert decision.needs_rebuild
        assert decision.action == RebuildDecision.FULL

    def test_no_fingerprint_with_existing_index_reuses(self, cache_dir):
        """If we have an index but no fingerprint, save fingerprint and reuse."""
        decision = should_rebuild_index(cache_dir, SAMPLE_URLS, 500, 100)
        assert decision.can_reuse
        assert decision.action == RebuildDecision.NONE
        # Should have saved fingerprint
        loaded = load_index_fingerprint(cache_dir)
        assert loaded is not None

    def test_same_sitemap_no_rebuild(self, cache_dir):
        """Unchanged sitemap = no rebuild. This is the deploy scenario."""
        fp = compute_sitemap_fingerprint(SAMPLE_URLS)
        save_index_fingerprint(cache_dir, fp, 100, 50, len(SAMPLE_URLS))
        decision = should_rebuild_index(cache_dir, SAMPLE_URLS, 100, 50)
        assert decision.can_reuse
        assert "ikke nødvendig" in decision.reason.lower()

    def test_minor_sitemap_change_incremental(self, cache_dir):
        """Small sitemap change (<5%) should trigger incremental, not full rebuild."""
        # Create a large URL list (100 URLs)
        base_urls = [f"https://example.com/products/p{i}" for i in range(100)]
        fp = compute_sitemap_fingerprint(base_urls)
        save_index_fingerprint(cache_dir, fp, 80, 20, len(base_urls))

        # Add 2 new URLs (2% change)
        new_urls = base_urls + [
            "https://example.com/products/new1",
            "https://example.com/products/new2",
        ]
        decision = should_rebuild_index(cache_dir, new_urls, 80, 20)
        assert decision.needs_incremental
        assert decision.action == RebuildDecision.INCREMENTAL

    def test_major_sitemap_change_full_rebuild(self, cache_dir):
        """Large sitemap change (>5%) should trigger full rebuild."""
        base_urls = [f"https://example.com/products/p{i}" for i in range(100)]
        fp = compute_sitemap_fingerprint(base_urls)
        save_index_fingerprint(cache_dir, fp, 80, 20, len(base_urls))

        # Add 10 new URLs (10% change)
        new_urls = base_urls + [
            f"https://example.com/products/new{i}" for i in range(10)
        ]
        decision = should_rebuild_index(cache_dir, new_urls, 80, 20)
        assert decision.needs_rebuild
        assert decision.action == RebuildDecision.FULL

    def test_empty_index_with_urls_triggers_rebuild(self, cache_dir):
        fp = compute_sitemap_fingerprint(SAMPLE_URLS)
        save_index_fingerprint(cache_dir, fp, 0, 0, len(SAMPLE_URLS))
        decision = should_rebuild_index(cache_dir, SAMPLE_URLS, 0, 0)
        assert decision.needs_rebuild

    def test_format_version_change_triggers_rebuild(self, cache_dir):
        """Changing index format version should trigger full rebuild."""
        fp = compute_sitemap_fingerprint(SAMPLE_URLS)
        save_index_fingerprint(cache_dir, fp, 100, 50, len(SAMPLE_URLS))

        # Simulate old format version in saved fingerprint
        fp_path = cache_dir / "_index_fingerprint.json"
        data = json.loads(fp_path.read_text())
        data["index_format_version"] = INDEX_FORMAT_VERSION - 1
        fp_path.write_text(json.dumps(data))

        decision = should_rebuild_index(cache_dir, SAMPLE_URLS, 100, 50)
        assert decision.needs_rebuild
        assert "format" in decision.reason.lower()

    def test_unchanged_sitemap_after_deploy(self, cache_dir):
        """Simulates a deploy where cache dir is persistent and data unchanged."""
        fp = compute_sitemap_fingerprint(SAMPLE_URLS)
        save_index_fingerprint(cache_dir, fp, 500, 200, len(SAMPLE_URLS))

        # After deploy, load and check — should NOT rebuild
        decision = should_rebuild_index(cache_dir, SAMPLE_URLS, 500, 200)
        assert decision.can_reuse
        assert not decision.needs_rebuild
        assert not decision.needs_incremental


class TestDeployScenarios:
    """End-to-end deploy scenarios to ensure deterministic behavior."""

    def test_scenario_a_code_deploy_no_catalog_change(self, cache_dir):
        """Deploy with code change, catalog unchanged → NO rebuild."""
        urls = [f"https://example.com/products/{i}" for i in range(1000)]
        fp = compute_sitemap_fingerprint(urls)
        save_index_fingerprint(cache_dir, fp, 800, 150, 1000)

        # Simulate restart with same sitemap
        decision = should_rebuild_index(cache_dir, urls, 800, 150)
        assert decision.can_reuse
        assert decision.action == RebuildDecision.NONE

    def test_scenario_b_container_restart(self, cache_dir):
        """Container restart, catalog unchanged → NO rebuild."""
        urls = [f"https://example.com/products/{i}" for i in range(500)]
        fp = compute_sitemap_fingerprint(urls)
        save_index_fingerprint(cache_dir, fp, 400, 80, 500)

        decision = should_rebuild_index(cache_dir, urls, 400, 80)
        assert decision.can_reuse

    def test_scenario_c_user_job_unchanged(self, cache_dir):
        """New user job, catalog unchanged → NO rebuild."""
        urls = SAMPLE_URLS
        fp = compute_sitemap_fingerprint(urls)
        save_index_fingerprint(cache_dir, fp, 2, 1, 3)

        decision = should_rebuild_index(cache_dir, urls, 2, 1)
        assert decision.can_reuse

    def test_scenario_d_new_catalog(self, cache_dir):
        """New catalog uploaded → YES rebuild."""
        old_urls = [f"https://example.com/products/{i}" for i in range(100)]
        fp = compute_sitemap_fingerprint(old_urls)
        save_index_fingerprint(cache_dir, fp, 80, 20, 100)

        # Completely different catalog
        new_urls = [f"https://newsite.com/products/{i}" for i in range(120)]
        decision = should_rebuild_index(cache_dir, new_urls, 80, 20)
        assert decision.needs_rebuild

    def test_scenario_e_cache_missing(self, cache_dir):
        """Cache wiped (no fingerprint, no data) → YES rebuild."""
        decision = should_rebuild_index(cache_dir, SAMPLE_URLS, 0, 0)
        assert decision.needs_rebuild


class TestLoadCachedIndexIfValid:
    def test_no_files_returns_none(self, cache_dir, monkeypatch):
        monkeypatch.setattr("backend.scraper.SITEMAP_INDEX_PATH", cache_dir / "nonexistent.json")
        result = load_cached_index_if_valid(cache_dir)
        assert result is None

    def test_valid_index_returns_data(self, cache_dir, monkeypatch):
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

    def test_empty_index_returns_none(self, cache_dir, monkeypatch):
        idx_path = cache_dir / "_sitemap_sku_index.json"
        idx_path.write_text("{}", encoding="utf-8")
        monkeypatch.setattr("backend.scraper.SITEMAP_INDEX_PATH", idx_path)

        result = load_cached_index_if_valid(cache_dir)
        assert result is None


class TestDeterministicStability:
    """Verify that fingerprints are stable across multiple calls and orderings."""

    def test_multiple_runs_same_result(self):
        results = [compute_sitemap_fingerprint(SAMPLE_URLS) for _ in range(10)]
        assert len(set(results)) == 1

    def test_large_url_list_stability(self):
        urls = [f"https://example.com/products/{i}" for i in range(10000)]
        fp1 = compute_sitemap_fingerprint(urls)
        fp2 = compute_sitemap_fingerprint(list(reversed(urls)))
        assert fp1 == fp2

    def test_no_timestamp_dependency(self, cache_dir):
        import time
        fp1 = compute_sitemap_fingerprint(SAMPLE_URLS)
        time.sleep(0.01)
        fp2 = compute_sitemap_fingerprint(SAMPLE_URLS)
        assert fp1 == fp2


class TestRebuildDecision:
    """Test the RebuildDecision data class."""

    def test_none_decision(self):
        d = RebuildDecision(RebuildDecision.NONE, "all good")
        assert d.can_reuse
        assert not d.needs_rebuild
        assert not d.needs_incremental

    def test_incremental_decision(self):
        d = RebuildDecision(RebuildDecision.INCREMENTAL, "small change")
        assert not d.can_reuse
        assert not d.needs_rebuild
        assert d.needs_incremental

    def test_full_decision(self):
        d = RebuildDecision(RebuildDecision.FULL, "major change")
        assert not d.can_reuse
        assert d.needs_rebuild
        assert not d.needs_incremental
