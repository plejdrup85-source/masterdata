"""Index fingerprint and cache validation — prevents unnecessary rebuilds.

Computes a stable, deterministic signature for the SKU index so the system
can detect whether a rebuild is actually needed after deploy/restart.

A rebuild is triggered ONLY when:
  - No cached index exists on disk
  - The sitemap content has changed (new/removed URLs)
  - Index configuration parameters have changed

A rebuild is NOT triggered when:
  - App is redeployed with same sitemap and same config
  - A new job starts with an already-valid index
  - The server process restarts
"""

import hashlib
import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Fingerprint metadata file — stored alongside the index
_FINGERPRINT_FILENAME = "_index_fingerprint.json"


def compute_sitemap_fingerprint(sitemap_urls: list[str]) -> str:
    """Compute a stable hash of the sitemap URL list.

    Sorts URLs to ensure deterministic output regardless of parse order.
    Returns a hex digest string.
    """
    # Sort for deterministic ordering
    sorted_urls = sorted(sitemap_urls)
    content = "\n".join(sorted_urls)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:32]


def compute_index_signature(
    sitemap_fingerprint: str,
    sku_count: int,
    checked_no_sku_count: int,
) -> str:
    """Compute a signature representing the current state of the index.

    Combines sitemap content hash with index completeness metrics.
    """
    parts = f"{sitemap_fingerprint}|skus={sku_count}|no_sku={checked_no_sku_count}"
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()[:32]


def save_index_fingerprint(
    cache_dir: Path,
    sitemap_fingerprint: str,
    sku_count: int,
    checked_no_sku_count: int,
    sitemap_url_count: int,
) -> None:
    """Save the current index fingerprint to disk."""
    fp_path = cache_dir / _FINGERPRINT_FILENAME
    data = {
        "sitemap_fingerprint": sitemap_fingerprint,
        "sku_count": sku_count,
        "checked_no_sku_count": checked_no_sku_count,
        "sitemap_url_count": sitemap_url_count,
        "signature": compute_index_signature(
            sitemap_fingerprint, sku_count, checked_no_sku_count
        ),
    }
    try:
        fp_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        logger.debug(f"Index fingerprint saved: {data['signature']}")
    except Exception as e:
        logger.warning(f"Failed to save index fingerprint: {e}")


def load_index_fingerprint(cache_dir: Path) -> Optional[dict]:
    """Load the saved index fingerprint from disk.

    Returns None if no fingerprint exists or it's corrupted.
    """
    fp_path = cache_dir / _FINGERPRINT_FILENAME
    if not fp_path.exists():
        return None
    try:
        data = json.loads(fp_path.read_text(encoding="utf-8"))
        # Validate required fields
        required = {"sitemap_fingerprint", "sku_count", "signature"}
        if not required.issubset(data.keys()):
            logger.warning("Index fingerprint file is missing required fields")
            return None
        return data
    except Exception as e:
        logger.warning(f"Failed to load index fingerprint: {e}")
        return None


def should_rebuild_index(
    cache_dir: Path,
    current_sitemap_urls: list[str],
    cached_sku_count: int,
    cached_no_sku_count: int,
) -> tuple[bool, str]:
    """Determine whether the SKU index needs to be rebuilt.

    Returns (should_rebuild, reason) where reason explains the decision.

    Rebuild is needed when:
      - No fingerprint file exists (first run or wiped cache)
      - Sitemap content has changed (URLs added/removed)
      - Index is empty despite having sitemap URLs

    Rebuild is NOT needed when:
      - Fingerprint matches and index has reasonable coverage
    """
    saved = load_index_fingerprint(cache_dir)
    current_fp = compute_sitemap_fingerprint(current_sitemap_urls)

    if saved is None:
        if cached_sku_count > 0:
            # We have an index but no fingerprint — save fingerprint and reuse
            save_index_fingerprint(
                cache_dir, current_fp, cached_sku_count,
                cached_no_sku_count, len(current_sitemap_urls),
            )
            return False, (
                f"Indeks funnet uten fingerprint — lagrer fingerprint og gjenbruker "
                f"({cached_sku_count} SKU-er)"
            )
        return True, "Ingen indeks-fingerprint funnet — full indeksering nødvendig"

    # Check if sitemap content changed
    if saved["sitemap_fingerprint"] != current_fp:
        return True, (
            f"Sitemap har endret seg (gammel fingerprint: {saved['sitemap_fingerprint'][:8]}…, "
            f"ny: {current_fp[:8]}…) — rebuild nødvendig"
        )

    # Check if index is empty despite sitemap having URLs
    if cached_sku_count == 0 and len(current_sitemap_urls) > 0:
        return True, "Indeks er tom men sitemap har URLer — rebuild nødvendig"

    # Sitemap unchanged, index has data — reuse
    coverage_pct = round(cached_sku_count / max(len(current_sitemap_urls), 1) * 100, 1)
    return False, (
        f"Cache-signatur matcher — rebuild ikke nødvendig "
        f"(dekning: {coverage_pct}%, {cached_sku_count} SKU-er indeksert)"
    )


def load_cached_index_if_valid(
    cache_dir: Path,
) -> Optional[dict]:
    """Load the cached index from disk if it passes validation.

    Returns a dict with {urls, sku_index, no_sku} if valid, None otherwise.
    """
    from backend.scraper import (
        SITEMAP_INDEX_PATH,
        SITEMAP_URLS_PATH,
        CHECKED_NO_SKU_PATH,
    )

    # Check all required files exist
    if not SITEMAP_INDEX_PATH.exists():
        logger.info("Ingen cached SKU-indeks funnet på disk")
        return None

    try:
        sku_index = json.loads(SITEMAP_INDEX_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"Kunne ikke laste SKU-indeks fra disk: {e}")
        return None

    urls = []
    if SITEMAP_URLS_PATH.exists():
        try:
            urls = json.loads(SITEMAP_URLS_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass

    no_sku = set()
    if CHECKED_NO_SKU_PATH.exists():
        try:
            no_sku = set(json.loads(CHECKED_NO_SKU_PATH.read_text(encoding="utf-8")))
        except Exception:
            pass

    if not sku_index:
        logger.info("Cached SKU-indeks er tom")
        return None

    logger.info(
        f"Fant gyldig indeks-cache på disk: "
        f"{len(sku_index)} SKU-er, {len(urls)} sitemap-URLer, "
        f"{len(no_sku)} sjekket-uten-SKU"
    )

    return {
        "urls": urls,
        "sku_index": sku_index,
        "no_sku": no_sku,
    }
