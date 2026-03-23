"""Approval workflow for enrichment suggestions.

Manages the lifecycle of suggestions from generation through review to export:

  NOT_REVIEWED → APPROVED / REJECTED / NEEDS_REVIEW
  NOT_REVIEWED → AUTO_APPROVED (system, if criteria met)

Auto-approval criteria (same as quick_wins — high confidence, low risk):
  - Confidence ≥ 0.75
  - Not medically sensitive
  - No source conflicts
  - Not AI-only source
  - Variant securely identified

Usage:
    from backend.approval import mark_auto_approved, set_approval, get_approval_summary
    mark_auto_approved(results)              # Auto-approve safe suggestions
    set_approval(job, "12345", 0, "Godkjent") # Manual approval
"""

import logging
from datetime import datetime
from typing import Optional

from backend.models import (
    ApprovalStatus,
    EnrichmentSuggestion,
    ProductAnalysis,
)

logger = logging.getLogger(__name__)


def mark_auto_approved(results: list[ProductAnalysis]) -> int:
    """Auto-approve suggestions that meet quick win criteria.

    Sets approval_status to AUTO_APPROVED for suggestions that are
    high-confidence, non-medical, source-grounded, and conflict-free.

    Returns the number of auto-approved suggestions.
    """
    from backend.quick_wins import is_quick_win

    count = 0
    for result in results:
        fa_map = {fa.field_name: fa for fa in result.field_analyses}
        for es in result.enrichment_suggestions:
            if es.approval_status != ApprovalStatus.NOT_REVIEWED:
                continue  # Don't override manual decisions
            if not es.suggested_value:
                continue

            fa = fa_map.get(es.field_name)
            if is_quick_win(es, fa):
                es.approval_status = ApprovalStatus.AUTO_APPROVED
                es.approved_at = datetime.now().isoformat()
                es.approval_comment = "Auto-godkjent: høy confidence, lav risiko"
                count += 1
            elif es.review_required:
                es.approval_status = ApprovalStatus.NEEDS_REVIEW
    return count


def set_approval(
    results: list[ProductAnalysis],
    article_number: str,
    suggestion_index: int,
    status: str,
    comment: str = "",
    reviewer: str = "",
) -> bool:
    """Set approval status for a specific suggestion.

    Args:
        results: Job results list
        article_number: Product article number
        suggestion_index: Index into enrichment_suggestions list
        status: One of ApprovalStatus values
        comment: Optional reviewer comment
        reviewer: Optional reviewer name

    Returns True if the suggestion was found and updated.
    """
    # Validate status
    try:
        approval = ApprovalStatus(status)
    except ValueError:
        logger.warning(f"Ugyldig godkjenningsstatus: {status}")
        return False

    for result in results:
        if result.article_number != article_number:
            continue
        if suggestion_index < 0 or suggestion_index >= len(result.enrichment_suggestions):
            logger.warning(f"Ugyldig forslagsindeks {suggestion_index} for {article_number}")
            return False

        es = result.enrichment_suggestions[suggestion_index]
        es.approval_status = approval
        es.approval_comment = comment or None
        es.approved_by = reviewer or None
        es.approved_at = datetime.now().isoformat()

        logger.info(
            f"Godkjenning: {article_number} felt={es.field_name} "
            f"status={approval.value} av={reviewer or 'ukjent'}"
        )
        return True

    logger.warning(f"Produkt {article_number} ikke funnet i resultater")
    return False


def bulk_set_approval(
    results: list[ProductAnalysis],
    article_number: str,
    status: str,
    comment: str = "",
    reviewer: str = "",
) -> int:
    """Set approval status for ALL suggestions of a product.

    Returns the number of suggestions updated.
    """
    try:
        approval = ApprovalStatus(status)
    except ValueError:
        return 0

    count = 0
    now = datetime.now().isoformat()
    for result in results:
        if result.article_number != article_number:
            continue
        for es in result.enrichment_suggestions:
            if not es.suggested_value:
                continue
            es.approval_status = approval
            es.approval_comment = comment or None
            es.approved_by = reviewer or None
            es.approved_at = now
            count += 1
        break

    return count


def get_approval_summary(results: list[ProductAnalysis]) -> dict:
    """Get aggregate approval statistics across all results."""
    counts = {s.value: 0 for s in ApprovalStatus}
    total = 0

    for result in results:
        for es in result.enrichment_suggestions:
            if not es.suggested_value:
                continue
            total += 1
            counts[es.approval_status.value] = counts.get(es.approval_status.value, 0) + 1

    return {
        "total_suggestions": total,
        "counts": counts,
        "approved": counts.get(ApprovalStatus.APPROVED.value, 0),
        "auto_approved": counts.get(ApprovalStatus.AUTO_APPROVED.value, 0),
        "rejected": counts.get(ApprovalStatus.REJECTED.value, 0),
        "needs_review": counts.get(ApprovalStatus.NEEDS_REVIEW.value, 0),
        "not_reviewed": counts.get(ApprovalStatus.NOT_REVIEWED.value, 0),
        "approved_total": (
            counts.get(ApprovalStatus.APPROVED.value, 0)
            + counts.get(ApprovalStatus.AUTO_APPROVED.value, 0)
        ),
    }


def filter_by_approval(
    results: list[ProductAnalysis],
    status: Optional[str] = None,
    approved_only: bool = False,
    exclude_rejected: bool = False,
) -> list[ProductAnalysis]:
    """Filter results to only include products with matching approval status.

    Args:
        status: Specific ApprovalStatus value to filter on
        approved_only: Only include products with ≥1 approved/auto-approved suggestion
        exclude_rejected: Remove rejected suggestions from results

    Returns filtered copy of results (does not modify originals).
    """
    filtered = []
    for result in results:
        matching_suggestions = []
        for es in result.enrichment_suggestions:
            if not es.suggested_value:
                continue

            if exclude_rejected and es.approval_status == ApprovalStatus.REJECTED:
                continue

            if status and es.approval_status.value != status:
                continue

            if approved_only and es.approval_status not in (
                ApprovalStatus.APPROVED, ApprovalStatus.AUTO_APPROVED
            ):
                continue

            matching_suggestions.append(es)

        if matching_suggestions or not (status or approved_only):
            # Create a shallow copy with filtered suggestions
            result_copy = result.model_copy()
            if status or approved_only or exclude_rejected:
                result_copy.enrichment_suggestions = matching_suggestions
            filtered.append(result_copy)

    return filtered
