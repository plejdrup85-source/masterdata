"""Human-readable diff display for field-level changes.

Produces compact, reviewer-friendly summaries showing what changed
between a current value and a proposed value. Designed for Excel
output where readability matters more than machine precision.

Three main outputs per field change:

  1. **Diff text** — a compact representation of added/removed content
  2. **Change type** — classification (language fix, new info, restructuring, etc.)
  3. **Change scope** — how much changed (minor / moderate / major)

Design principles:
  - Readable by non-technical reviewers (warehouse/PIM staff)
  - Norwegian labels throughout
  - Compact enough for an Excel cell
  - Never longer than ~300 chars for the diff text
"""

import re
from enum import Enum
from typing import Optional


class ChangeType(str, Enum):
    """Classification of what kind of change a suggestion represents."""
    NEW_VALUE = "Ny verdi"                    # Field was empty, now has content
    LANGUAGE_FIX = "Språkvask"                # Same info, better Norwegian
    NEW_INFORMATION = "Ny informasjon"        # Adds facts not in current
    RESTRUCTURING = "Omstrukturering"         # Same info, better format
    REPLACEMENT = "Erstatning"                # Substantially different content
    EXTENSION = "Utvidelse"                   # Adds to existing without removing
    CORRECTION = "Korreksjon"                 # Fixes likely error
    MINOR_EDIT = "Mindre justering"           # Small tweaks


class ChangeScope(str, Enum):
    """How much of the content changed."""
    MINOR = "Liten"       # <20% of content changed
    MODERATE = "Moderat"  # 20-60% changed
    MAJOR = "Stor"        # >60% changed
    FULL = "Komplett"     # Entirely new or entirely different


# ── Word-level diff helpers ──

def _tokenize(text: str) -> list[str]:
    """Split text into words, preserving punctuation as separate tokens."""
    if not text:
        return []
    return re.findall(r"\S+", text)


def _normalize_token(token: str) -> str:
    """Normalize a token for comparison: lowercase, strip trailing punctuation."""
    return token.lower().rstrip(".,;:!?")


def _longest_common_subsequence(a: list[str], b: list[str]) -> list[str]:
    """Find longest common subsequence of two token lists.

    Used for identifying what's shared between current and proposed.
    Optimized for short-to-medium texts (typical product fields).
    """
    # Cap at 200 tokens to avoid O(n²) on very long texts
    a = a[:200]
    b = b[:200]

    m, n = len(a), len(b)
    dp = [[0] * (n + 1) for _ in range(m + 1)]

    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if _normalize_token(a[i - 1]) == _normalize_token(b[j - 1]):
                dp[i][j] = dp[i - 1][j - 1] + 1
            else:
                dp[i][j] = max(dp[i - 1][j], dp[i][j - 1])

    # Backtrack to find LCS
    lcs = []
    i, j = m, n
    while i > 0 and j > 0:
        if _normalize_token(a[i - 1]) == _normalize_token(b[j - 1]):
            lcs.append(a[i - 1])
            i -= 1
            j -= 1
        elif dp[i - 1][j] >= dp[i][j - 1]:
            i -= 1
        else:
            j -= 1
    lcs.reverse()
    return lcs


def _compute_added_removed(
    current_tokens: list[str],
    proposed_tokens: list[str],
) -> tuple[list[str], list[str]]:
    """Compute which tokens were added and which were removed.

    Returns (added_tokens, removed_tokens).
    """
    lcs = _longest_common_subsequence(current_tokens, proposed_tokens)
    lcs_normalized = {}
    for token in lcs:
        key = _normalize_token(token)
        lcs_normalized[key] = lcs_normalized.get(key, 0) + 1

    # Removed = in current but not in LCS
    removed = []
    lcs_remaining = dict(lcs_normalized)
    for token in current_tokens:
        key = _normalize_token(token)
        if lcs_remaining.get(key, 0) > 0:
            lcs_remaining[key] -= 1
        else:
            removed.append(token)

    # Added = in proposed but not in LCS
    added = []
    lcs_remaining = dict(lcs_normalized)
    for token in proposed_tokens:
        key = _normalize_token(token)
        if lcs_remaining.get(key, 0) > 0:
            lcs_remaining[key] -= 1
        else:
            added.append(token)

    return added, removed


# ═══════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════


def build_field_diff(
    current_value: Optional[str],
    proposed_value: Optional[str],
    max_length: int = 300,
) -> str:
    """Build a compact, human-readable diff between current and proposed values.

    Output format:
      - "Ny verdi: <proposed>" if current is empty
      - "Lagt til: <words>  |  Fjernet: <words>" for partial changes
      - "Helt ny tekst" if completely different

    Truncated to max_length for Excel readability.
    """
    current = (current_value or "").strip()
    proposed = (proposed_value or "").strip()

    if not current and not proposed:
        return ""

    if not current and proposed:
        preview = proposed[:120] + ("…" if len(proposed) > 120 else "")
        return f"Ny verdi: {preview}"

    if current and not proposed:
        return "Foreslått verdi er tom"

    if current.strip().lower() == proposed.strip().lower():
        return "Ingen endring (kun formatering)"

    current_tokens = _tokenize(current)
    proposed_tokens = _tokenize(proposed)

    added, removed = _compute_added_removed(current_tokens, proposed_tokens)

    # If everything changed, it's a full replacement
    if len(removed) >= len(current_tokens) * 0.8 and len(added) >= len(proposed_tokens) * 0.8:
        preview = proposed[:120] + ("…" if len(proposed) > 120 else "")
        return f"Helt ny tekst: {preview}"

    parts = []
    if added:
        added_text = " ".join(added[:20])
        if len(added) > 20:
            added_text += f" (+{len(added) - 20} ord til)"
        parts.append(f"Lagt til: {added_text}")

    if removed:
        removed_text = " ".join(removed[:15])
        if len(removed) > 15:
            removed_text += f" (+{len(removed) - 15} ord til)"
        parts.append(f"Fjernet: {removed_text}")

    if not parts:
        return "Kun ordstilling/formatering endret"

    result = "  |  ".join(parts)

    if len(result) > max_length:
        result = result[: max_length - 1] + "…"

    return result


def summarize_change_type(
    current_value: Optional[str],
    proposed_value: Optional[str],
) -> str:
    """Classify the type of change between current and proposed value.

    Returns a Norwegian label from ChangeType.
    """
    current = (current_value or "").strip()
    proposed = (proposed_value or "").strip()

    if not current and proposed:
        return ChangeType.NEW_VALUE.value

    if not proposed:
        return ""

    if current.lower() == proposed.lower():
        return ChangeType.MINOR_EDIT.value

    current_tokens = set(_tokenize(current.lower()))
    proposed_tokens = set(_tokenize(proposed.lower()))

    if not current_tokens:
        return ChangeType.NEW_VALUE.value

    overlap = current_tokens & proposed_tokens
    overlap_ratio = len(overlap) / len(current_tokens) if current_tokens else 0
    new_ratio = len(proposed_tokens - current_tokens) / len(proposed_tokens) if proposed_tokens else 0

    # Check language improvement FIRST — translation replaces most words
    # but the intent is language-fixing, not content replacement
    if _is_language_improvement(current, proposed):
        return ChangeType.LANGUAGE_FIX.value

    # High overlap: minor edit or restructuring
    if overlap_ratio > 0.6 and new_ratio < 0.4:
        # Check if it's a structural change (same words, different format)
        current_sorted = sorted(current_tokens)
        proposed_sorted = sorted(proposed_tokens)
        if current_sorted == proposed_sorted:
            return ChangeType.RESTRUCTURING.value

        return ChangeType.MINOR_EDIT.value

    # Extension: proposed contains most of current + new content
    if overlap_ratio > 0.7 and len(proposed_tokens) > len(current_tokens) * 1.3:
        return ChangeType.EXTENSION.value

    # New information: mostly new content with some overlap
    if new_ratio > 0.5 and overlap_ratio > 0.3:
        return ChangeType.NEW_INFORMATION.value

    # Correction: short values that are substantially different
    if len(current) < 50 and len(proposed) < 50:
        return ChangeType.CORRECTION.value

    # Full replacement
    if overlap_ratio < 0.3:
        return ChangeType.REPLACEMENT.value

    return ChangeType.NEW_INFORMATION.value


def detect_change_scope(
    current_value: Optional[str],
    proposed_value: Optional[str],
) -> str:
    """Detect how much of the content changed.

    Returns a Norwegian label from ChangeScope.
    """
    current = (current_value or "").strip()
    proposed = (proposed_value or "").strip()

    if not current and proposed:
        return ChangeScope.FULL.value

    if not proposed or not current:
        return ""

    if current.lower() == proposed.lower():
        return ChangeScope.MINOR.value

    current_tokens = _tokenize(current)
    proposed_tokens = _tokenize(proposed)

    added, removed = _compute_added_removed(current_tokens, proposed_tokens)

    total_tokens = len(current_tokens) + len(proposed_tokens)
    if total_tokens == 0:
        return ChangeScope.MINOR.value
    change_ratio = (len(added) + len(removed)) / total_tokens

    if change_ratio < 0.20:
        return ChangeScope.MINOR.value
    elif change_ratio < 0.45:
        return ChangeScope.MODERATE.value
    elif change_ratio < 0.75:
        return ChangeScope.MAJOR.value
    else:
        return ChangeScope.FULL.value


# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════


def _is_language_improvement(current: str, proposed: str) -> bool:
    """Check if the change is primarily a language/grammar improvement.

    Looks for indicators like:
    - English → Norwegian translation
    - Better Norwegian grammar (common word replacements)
    - Capitalization/punctuation fixes
    """
    # English → Norwegian translation markers
    english_words = re.findall(
        r"\b(?:the|and|for|with|this|designed|intended|provides|ensures|"
        r"available|suitable|glove|used|made|features)\b",
        current.lower(),
    )
    norwegian_words = re.findall(
        r"\b(?:og|for|med|denne|designet|beregnet|gir|sikrer|"
        r"tilgjengelig|egnet|hanske|brukes|laget|egenskaper)\b",
        proposed.lower(),
    )

    if len(english_words) >= 2 and len(norwegian_words) >= 2:
        return True

    # Swedish → Norwegian markers
    swedish_markers = re.findall(
        r"\b(?:och|för|inte|användas|engångs|storlek|handske)\b",
        current.lower(),
    )
    if len(swedish_markers) >= 2:
        return True

    return False
