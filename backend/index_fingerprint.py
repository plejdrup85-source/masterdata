"""Index fingerprint and cache validation — prevents unnecessary rebuilds.

Computes a stable, deterministic signature for the SKU index so the system
can detect whether a rebuild is actually needed after deploy/restart.

DECISION MATRIX:

  ┌─────────────────────────────────┬───────────────────────────────────┐
  │ Condition                       │ Action                            │
  ├─────────────────────────────────┼───────────────────────────────────┤
  │ No cache exists at all          │ FULL REBUILD                      │
  │ Cache exists, fingerprint match │ REUSE (no rebuild)                │
  │ Sitemap changed slightly (<5%)  │ INCREMENTAL SCAN (fast)           │
  │ Sitemap changed heavily (>=5%)  │ FULL REBUILD                      │
  │ Index format version changed    │ FULL REBUILD                      │
  │ Cache corrupt / invalid         │ FULL REBUILD                      │
  │ App code changed, same catalog  │ REUSE (no rebuild)                │
  │ Container restarted             │ REUSE (no rebuild)                │
  │ User job starts                 │ REUSE (no rebuild)                │
  └─────────────────────────────────┴───────────────────────────────────┘

Index format version: Increment INDEX_FORMAT_VERSION when the index
schema, scraping logic, or cache structure changes in an incompatible way.
This is SEPARATE from catalog content changes.
"""

import hashlib
import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── Configuration ──

# Fingerprint metadata file — stored alongside the index
_FINGERPRINT_FILENAME = "_index_fingerprint.json"

# Index format version — increment ONLY when index structure/schema changes.
# This does NOT change when app code changes that don't affect the index.
# Examples of when to bump:
#   - SKU extraction logic changes
#   - Cache file format changes
#   - New fields added to cached product data
INDEX_FORMAT_VERSION = 2

# Threshold for "significant" sitemap change (triggers full rebuild)
# Below this: use incremental scan. Above this: full rebuild.
SIGNIFICANT_CHANGE_THRESHOLD = 0.05  # 5% of URLs added/removed


class RebuildDecision:
    """Result of should_rebuild_index with structured metadata."""

    NONE = "none"           # No rebuild needed — reuse cache
    INCREMENTAL = "incremental"  # Minor change — incremental scan sufficient
    FULL = "full"           # Major change — full rebuild needed

    def __init__(self, action: str, reason: str, details: Optional[dict] = None):
        self.action = action
        self.reason = reason
        self.details = details or {}

    @property
    def needs_rebuild(self) -> bool:
        return self.action == self.FULL

    @property
    def needs_incremental(self) -> bool:
        return self.action == self.INCREMENTAL

    @property
    def can_reuse(self) -> bool:
        return self.action == self.NONE

    def __repr__(self):
        return f"RebuildDecision(action={self.action!r}, reason={self.reason!r})"


def compute_sitemap_fingerprint(sitemap_urls: list[str]) -> str:
    """Compute a stable hash of the sitemap URL list.

    Sorts URLs to ensure deterministic output regardless of parse order.
    Returns a hex digest string.
    """
    sorted_urls = sorted(sitemap_urls)
    content = "\n".join(sorted_urls)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:32]


def compute_index_signature(
    sitemap_fingerprint: str,
    sku_count: int,
    checked_no_sku_count: int,
    index_format_version: int = INDEX_FORMAT_VERSION,
) -> str:
    """Compute a signature representing the current state of the index.

    Combines sitemap content hash with index completeness metrics AND
    the index format version for change detection.
    """
    parts = (
        f"{sitemap_fingerprint}"
        f"|skus={sku_count}"
        f"|no_sku={checked_no_sku_count}"
        f"|fmt={index_format_version}"
    )
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
        "index_format_version": INDEX_FORMAT_VERSION,
        "signature": compute_index_signature(
            sitemap_fingerprint, sku_count, checked_no_sku_count
        ),
    }
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        fp_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        logger.info(
            f"[fingerprint] Lagret: signature={data['signature'][:12]}… "
            f"format_v={INDEX_FORMAT_VERSION} "
            f"skus={sku_count} urls={sitemap_url_count}"
        )
    except Exception as e:
        logger.warning(f"[fingerprint] Kunne ikke lagre fingerprint: {e}")


def load_index_fingerprint(cache_dir: Path) -> Optional[dict]:
    """Load the saved index fingerprint from disk.

    Returns None if no fingerprint exists or it's corrupted.
    """
    fp_path = cache_dir / _FINGERPRINT_FILENAME
    if not fp_path.exists():
        logger.info("[fingerprint] Ingen fingerprint-fil funnet på disk")
        return None
    try:
        data = json.loads(fp_path.read_text(encoding="utf-8"))
        required = {"sitemap_fingerprint", "sku_count", "signature"}
        if not required.issubset(data.keys()):
            logger.warning("[fingerprint] Fingerprint-fil mangler påkrevde felt")
            return None
        return data
    except Exception as e:
        logger.warning(f"[fingerprint] Kunne ikke laste fingerprint: {e}")
        return None


def should_rebuild_index(
    cache_dir: Path,
    current_sitemap_urls: list[str],
    cached_sku_count: int,
    cached_no_sku_count: int,
) -> RebuildDecision:
    """Determine whether and how the SKU index needs to be rebuilt.

    Returns a RebuildDecision with action (none/incremental/full) and reason.

    FULL rebuild when:
      - No fingerprint exists AND no cache
      - Index format version changed (incompatible cache)
      - Sitemap changed significantly (>5% URLs added/removed)
      - Index is completely empty

    INCREMENTAL scan when:
      - Sitemap changed slightly (<5% URLs added/removed)
      - Some new URLs need to be checked

    NO rebuild when:
      - Fingerprint matches exactly
      - Index has data and format is compatible
    """
    saved = load_index_fingerprint(cache_dir)
    current_fp = compute_sitemap_fingerprint(current_sitemap_urls)
    current_url_count = len(current_sitemap_urls)

    # ── Case 1: No fingerprint on disk ──
    if saved is None:
        if cached_sku_count > 0:
            # Index exists but no fingerprint — create fingerprint, reuse index
            save_index_fingerprint(
                cache_dir, current_fp, cached_sku_count,
                cached_no_sku_count, current_url_count,
            )
            logger.info(
                f"[fingerprint] Indeks funnet ({cached_sku_count} SKU-er) "
                f"men ingen fingerprint — oppretter fingerprint og gjenbruker cache"
            )
            return RebuildDecision(
                RebuildDecision.NONE,
                f"Indeks funnet uten fingerprint — lagrer fingerprint og gjenbruker "
                f"({cached_sku_count} SKU-er)",
            )
        return RebuildDecision(
            RebuildDecision.FULL,
            "Ingen indeks-fingerprint og ingen cache — full indeksering nødvendig",
        )

    # ── Case 2: Index format version changed ──
    saved_format = saved.get("index_format_version", 1)
    if saved_format != INDEX_FORMAT_VERSION:
        logger.info(
            f"[fingerprint] Index-format endret: v{saved_format} → v{INDEX_FORMAT_VERSION}. "
            f"Full rebuild nødvendig."
        )
        return RebuildDecision(
            RebuildDecision.FULL,
            f"Index-format endret (v{saved_format} → v{INDEX_FORMAT_VERSION}) "
            f"— full rebuild for kompatibilitet",
            {"old_format": saved_format, "new_format": INDEX_FORMAT_VERSION},
        )

    # ── Case 3: Sitemap fingerprint matches exactly ──
    if saved["sitemap_fingerprint"] == current_fp:
        # Index is empty but shouldn't be
        if cached_sku_count == 0 and current_url_count > 0:
            return RebuildDecision(
                RebuildDecision.FULL,
                "Indeks er tom men sitemap har URLer — full rebuild nødvendig",
            )

        coverage_pct = round(
            cached_sku_count / max(current_url_count, 1) * 100, 1
        )
        logger.info(
            f"[fingerprint] ✓ Katalog uendret — gjenbruker cache. "
            f"Fingerprint: {current_fp[:12]}… "
            f"Dekning: {coverage_pct}% ({cached_sku_count} SKU-er)"
        )
        return RebuildDecision(
            RebuildDecision.NONE,
            f"Katalog-fingerprint matcher — rebuild IKKE nødvendig "
            f"(dekning: {coverage_pct}%, {cached_sku_count} SKU-er indeksert)",
            {"coverage_pct": coverage_pct, "fingerprint": current_fp[:12]},
        )

    # ── Case 4: Sitemap changed — determine severity ──
    saved_url_count = saved.get("sitemap_url_count", 0) or cached_sku_count
    if saved_url_count > 0:
        change_ratio = abs(current_url_count - saved_url_count) / saved_url_count
    else:
        change_ratio = 1.0  # No prior data, treat as major

    logger.info(
        f"[fingerprint] Sitemap endret: "
        f"gammel={saved['sitemap_fingerprint'][:12]}… ({saved_url_count} URLer) "
        f"ny={current_fp[:12]}… ({current_url_count} URLer) "
        f"endring={change_ratio:.1%}"
    )

    if change_ratio >= SIGNIFICANT_CHANGE_THRESHOLD:
        return RebuildDecision(
            RebuildDecision.FULL,
            f"Sitemap har endret seg vesentlig ({change_ratio:.1%} endring, "
            f"terskel={SIGNIFICANT_CHANGE_THRESHOLD:.0%}) — full rebuild nødvendig",
            {
                "old_fingerprint": saved["sitemap_fingerprint"][:12],
                "new_fingerprint": current_fp[:12],
                "change_ratio": change_ratio,
            },
        )
    else:
        # Small change — incremental scan is sufficient
        new_urls = current_url_count - saved_url_count
        return RebuildDecision(
            RebuildDecision.INCREMENTAL,
            f"Sitemap har endret seg minimalt ({change_ratio:.1%}, "
            f"~{abs(new_urls)} URLer) — inkrementell skanning tilstrekkelig",
            {
                "old_fingerprint": saved["sitemap_fingerprint"][:12],
                "new_fingerprint": current_fp[:12],
                "new_urls_approx": abs(new_urls),
            },
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

    if not SITEMAP_INDEX_PATH.exists():
        logger.info("[cache] Ingen cached SKU-indeks funnet på disk")
        return None

    try:
        sku_index = json.loads(SITEMAP_INDEX_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"[cache] Kunne ikke laste SKU-indeks fra disk: {e}")
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
        logger.info("[cache] Cached SKU-indeks er tom")
        return None

    logger.info(
        f"[cache] Gyldig indeks-cache funnet: "
        f"{len(sku_index)} SKU-er, {len(urls)} sitemap-URLer, "
        f"{len(no_sku)} sjekket-uten-SKU"
    )

    return {
        "urls": urls,
        "sku_index": sku_index,
        "no_sku": no_sku,
    }
