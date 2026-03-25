"""Feedback learning loop — improves suggestions based on approval history.

Tracks which suggestions are approved, rejected, or modified, and uses
this data to adjust confidence, detect low-quality patterns, and improve
auto-approval over time.

Storage: JSON file in data/feedback_store.json (persistent across sessions).

How feedback improves the system:
  1. Confidence adjustment: fields/sources with high rejection rates get
     lower confidence multipliers; fields with high approval rates get boosts.
  2. Source quality tracking: sources that consistently produce rejected
     suggestions are penalized in future runs.
  3. Pattern detection: identifies field+source combinations that often fail.
  4. Auto-approval tuning: raises/lowers thresholds based on actual accuracy.

No ML required — uses running statistics (acceptance rates, field-level
accuracy) to produce simple multipliers.
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════

FEEDBACK_DIR = Path(__file__).parent.parent / "data"
FEEDBACK_FILE = FEEDBACK_DIR / "feedback_store.json"

# Minimum observations before adjusting confidence
MIN_OBSERVATIONS = 5

# Maximum confidence adjustment (prevent wild swings)
MAX_CONFIDENCE_BOOST = 0.10
MAX_CONFIDENCE_PENALTY = 0.15

# Auto-approval accuracy threshold — if below this, tighten auto-approval
AUTO_APPROVAL_ACCURACY_FLOOR = 0.85


# ═══════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════


@dataclass
class FeedbackEntry:
    """A single feedback event."""
    timestamp: str = ""
    article_number: str = ""
    field_name: str = ""
    source: str = ""
    confidence: float = 0.0
    outcome: str = ""       # "approved", "auto_approved", "rejected", "modified"
    comment: str = ""
    was_auto_approved: bool = False
    reviewer: str = ""


@dataclass
class FieldSourceStats:
    """Running statistics for a field+source combination."""
    approved: int = 0
    auto_approved: int = 0
    rejected: int = 0
    modified: int = 0
    total_confidence: float = 0.0
    count: int = 0

    @property
    def total(self) -> int:
        return self.approved + self.auto_approved + self.rejected + self.modified

    @property
    def acceptance_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return (self.approved + self.auto_approved) / self.total

    @property
    def rejection_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.rejected / self.total

    @property
    def avg_confidence(self) -> float:
        if self.count == 0:
            return 0.0
        return self.total_confidence / self.count


@dataclass
class FeedbackStatistics:
    """Aggregate feedback statistics."""
    total_feedback: int = 0
    by_field: dict[str, FieldSourceStats] = field(default_factory=dict)
    by_source: dict[str, FieldSourceStats] = field(default_factory=dict)
    by_field_source: dict[str, FieldSourceStats] = field(default_factory=dict)
    auto_approval_accuracy: float = 1.0
    low_quality_patterns: list[dict] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════
# FEEDBACK STORE — file-based persistence
# ═══════════════════════════════════════════════════════════


def _ensure_store() -> dict:
    """Load or create feedback store."""
    if FEEDBACK_FILE.exists():
        try:
            with open(FEEDBACK_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            logger.warning("Korrupt feedback-fil, oppretter ny")
    return {"entries": [], "stats": {}, "version": 1}


def _save_store(store: dict) -> None:
    """Save feedback store to disk."""
    FEEDBACK_DIR.mkdir(parents=True, exist_ok=True)
    tmp = FEEDBACK_FILE.with_suffix(".tmp")
    try:
        with open(tmp, "w") as f:
            json.dump(store, f, ensure_ascii=False, indent=2)
        tmp.replace(FEEDBACK_FILE)
    except OSError as e:
        logger.error(f"Kunne ikke lagre feedback: {e}")


# ═══════════════════════════════════════════════════════════
# CORE FUNCTIONS
# ═══════════════════════════════════════════════════════════


def log_suggestion_feedback(
    article_number: str,
    field_name: str,
    source: str,
    confidence: float,
    outcome: str,
    comment: str = "",
    was_auto_approved: bool = False,
    reviewer: str = "",
) -> None:
    """Log a feedback event when a suggestion is approved/rejected/modified.

    Args:
        outcome: One of "approved", "auto_approved", "rejected", "modified"
    """
    store = _ensure_store()

    entry = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "article_number": article_number,
        "field_name": field_name,
        "source": source or "ukjent",
        "confidence": round(confidence, 3),
        "outcome": outcome,
        "comment": comment,
        "was_auto_approved": was_auto_approved,
        "reviewer": reviewer,
    }
    store["entries"].append(entry)

    # Update running stats
    _update_stats_incremental(store, entry)
    _save_store(store)

    logger.info(
        f"Feedback logget: {article_number} {field_name} "
        f"[{source}] conf={confidence:.2f} → {outcome}"
    )


def _update_stats_incremental(store: dict, entry: dict) -> None:
    """Update running statistics with a new entry."""
    stats = store.setdefault("stats", {})
    outcome = entry["outcome"]
    field_name = entry["field_name"]
    source = entry["source"]
    confidence = entry["confidence"]

    for key_type, key in [
        ("by_field", field_name),
        ("by_source", source),
        ("by_field_source", f"{field_name}|{source}"),
    ]:
        section = stats.setdefault(key_type, {})
        s = section.setdefault(key, {
            "approved": 0, "auto_approved": 0,
            "rejected": 0, "modified": 0,
            "total_confidence": 0.0, "count": 0,
        })
        if outcome in s:
            s[outcome] = s.get(outcome, 0) + 1
        s["total_confidence"] = s.get("total_confidence", 0.0) + confidence
        s["count"] = s.get("count", 0) + 1


def update_feedback_statistics(store: Optional[dict] = None) -> FeedbackStatistics:
    """Recompute full statistics from all entries.

    Call this for a fresh recalculation (e.g., after fixing corrupted stats).
    """
    if store is None:
        store = _ensure_store()

    result = FeedbackStatistics()
    entries = store.get("entries", [])
    result.total_feedback = len(entries)

    for entry in entries:
        outcome = entry.get("outcome", "")
        field_name = entry.get("field_name", "")
        source = entry.get("source", "ukjent")
        confidence = entry.get("confidence", 0.0)

        for bucket_name, key in [
            ("by_field", field_name),
            ("by_source", source),
            ("by_field_source", f"{field_name}|{source}"),
        ]:
            bucket = getattr(result, bucket_name)
            if key not in bucket:
                bucket[key] = FieldSourceStats()
            s = bucket[key]
            if outcome == "approved":
                s.approved += 1
            elif outcome == "auto_approved":
                s.auto_approved += 1
            elif outcome == "rejected":
                s.rejected += 1
            elif outcome == "modified":
                s.modified += 1
            s.total_confidence += confidence
            s.count += 1

    # Auto-approval accuracy
    auto_approved = sum(
        1 for e in entries if e.get("was_auto_approved")
    )
    auto_then_rejected = sum(
        1 for e in entries
        if e.get("was_auto_approved") and e.get("outcome") == "rejected"
    )
    if auto_approved > 0:
        result.auto_approval_accuracy = 1.0 - (auto_then_rejected / auto_approved)

    # Detect low quality patterns
    result.low_quality_patterns = identify_low_quality_patterns(result)

    return result


def adjust_confidence_from_feedback(
    confidence: float,
    field_name: str,
    source: str,
) -> float:
    """Adjust a suggestion's confidence based on historical feedback.

    Increases confidence for field+source combos with high acceptance rate,
    decreases for those with high rejection rate.

    Returns adjusted confidence (still clamped to 0.0-1.0).
    """
    store = _ensure_store()
    stats = store.get("stats", {})

    # Look up field+source combo first (most specific), then field, then source
    adjustments = []

    for key_type, key in [
        ("by_field_source", f"{field_name}|{source}"),
        ("by_field", field_name),
        ("by_source", source),
    ]:
        section = stats.get(key_type, {})
        s = section.get(key)
        if not s:
            continue

        total = s.get("approved", 0) + s.get("auto_approved", 0) + s.get("rejected", 0) + s.get("modified", 0)
        if total < MIN_OBSERVATIONS:
            continue

        accepted = s.get("approved", 0) + s.get("auto_approved", 0)
        rejected = s.get("rejected", 0)
        acceptance_rate = accepted / total if total > 0 else 0.5

        if acceptance_rate >= 0.9:
            adjustments.append(MAX_CONFIDENCE_BOOST * 0.5)
        elif acceptance_rate >= 0.8:
            adjustments.append(MAX_CONFIDENCE_BOOST * 0.25)
        elif acceptance_rate < 0.5:
            adjustments.append(-MAX_CONFIDENCE_PENALTY)
        elif acceptance_rate < 0.7:
            adjustments.append(-MAX_CONFIDENCE_PENALTY * 0.5)
        break  # Use most specific match only

    if not adjustments:
        return confidence

    adjustment = adjustments[0]
    adjusted = max(0.0, min(1.0, confidence + adjustment))

    if abs(adjustment) > 0.01:
        logger.debug(
            f"Confidence justert: {field_name}|{source} "
            f"{confidence:.2f} → {adjusted:.2f} (adj={adjustment:+.2f})"
        )

    return adjusted


def identify_low_quality_patterns(
    stats: Optional[FeedbackStatistics] = None,
) -> list[dict]:
    """Identify field+source combinations that consistently underperform.

    Returns a list of patterns with high rejection rates that should
    trigger stricter review or lower confidence.
    """
    if stats is None:
        store = _ensure_store()
        stats = update_feedback_statistics(store)

    patterns = []

    for key, s in stats.by_field_source.items():
        if s.total < MIN_OBSERVATIONS:
            continue
        if s.rejection_rate > 0.3:
            field_name, source = key.split("|", 1) if "|" in key else (key, "ukjent")
            patterns.append({
                "field": field_name,
                "source": source,
                "rejection_rate": round(s.rejection_rate, 2),
                "acceptance_rate": round(s.acceptance_rate, 2),
                "total_observations": s.total,
                "avg_confidence": round(s.avg_confidence, 2),
                "recommendation": _recommend_action(s),
            })

    # Also check source-level patterns
    for source, s in stats.by_source.items():
        if s.total < MIN_OBSERVATIONS and s.rejection_rate > 0.4:
            continue
        if s.total >= MIN_OBSERVATIONS and s.rejection_rate > 0.3:
            patterns.append({
                "field": "(alle felt)",
                "source": source,
                "rejection_rate": round(s.rejection_rate, 2),
                "acceptance_rate": round(s.acceptance_rate, 2),
                "total_observations": s.total,
                "avg_confidence": round(s.avg_confidence, 2),
                "recommendation": _recommend_action(s),
            })

    patterns.sort(key=lambda p: -p["rejection_rate"])
    return patterns


def get_feedback_summary() -> dict:
    """Get a human-readable summary of feedback statistics.

    Suitable for API response or Excel summary.
    """
    store = _ensure_store()
    stats = update_feedback_statistics(store)

    # Field-level summary
    field_summaries = {}
    for field_name, s in stats.by_field.items():
        field_summaries[field_name] = {
            "total": s.total,
            "acceptance_rate": round(s.acceptance_rate, 2),
            "rejection_rate": round(s.rejection_rate, 2),
            "avg_confidence": round(s.avg_confidence, 2),
        }

    # Source-level summary
    source_summaries = {}
    for source, s in stats.by_source.items():
        source_summaries[source] = {
            "total": s.total,
            "acceptance_rate": round(s.acceptance_rate, 2),
            "rejection_rate": round(s.rejection_rate, 2),
            "avg_confidence": round(s.avg_confidence, 2),
        }

    return {
        "total_feedback_entries": stats.total_feedback,
        "auto_approval_accuracy": round(stats.auto_approval_accuracy, 2),
        "by_field": field_summaries,
        "by_source": source_summaries,
        "low_quality_patterns": stats.low_quality_patterns,
        "confidence_adjustment_active": stats.total_feedback >= MIN_OBSERVATIONS,
    }


def get_auto_approval_threshold(field_name: str) -> float:
    """Get the current auto-approval confidence threshold for a field.

    Adjusts the base threshold (0.75) based on historical accuracy.
    If auto-approved suggestions for this field are often rejected,
    the threshold increases (making it harder to auto-approve).
    """
    base_threshold = 0.75
    store = _ensure_store()
    stats_data = store.get("stats", {})

    field_stats = stats_data.get("by_field", {}).get(field_name)
    if not field_stats:
        return base_threshold

    total = (
        field_stats.get("approved", 0) + field_stats.get("auto_approved", 0)
        + field_stats.get("rejected", 0) + field_stats.get("modified", 0)
    )
    if total < MIN_OBSERVATIONS:
        return base_threshold

    rejected = field_stats.get("rejected", 0)
    rejection_rate = rejected / total

    if rejection_rate > 0.3:
        return min(0.90, base_threshold + 0.10)  # Tighten significantly
    elif rejection_rate > 0.2:
        return min(0.85, base_threshold + 0.05)  # Tighten moderately
    elif rejection_rate < 0.1 and total >= 10:
        return max(0.65, base_threshold - 0.05)  # Loosen slightly (proven reliable)

    return base_threshold


# ═══════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════


def _recommend_action(s: FieldSourceStats) -> str:
    """Generate a Norwegian recommendation based on stats."""
    if s.rejection_rate > 0.5:
        return "Deaktiver eller reduser confidence kraftig for denne kilden"
    elif s.rejection_rate > 0.3:
        return "Øk review-terskel, ikke auto-godkjenn fra denne kilden"
    elif s.acceptance_rate > 0.9:
        return "Pålitelig kilde — kan vurderes for lavere review-terskel"
    return "Overvåk videre"
