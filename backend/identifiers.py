"""Identifier normalization for article numbers, SKUs, and other product identifiers.

Medical product masterdata requires exact, deterministic identifier handling.
Excel/openpyxl can auto-type numeric-looking identifiers as float (e.g., 12345 → 12345.0),
which silently breaks lookups, comparisons, and cross-source matching.

This module provides a single normalization function used everywhere identifiers
are read, stored, compared, or exported.

Rules:
- Identifiers are ALWAYS strings
- Float-formatted integers (12345.0) → "12345"
- Leading zeros are preserved ("007890" stays "007890")
- Whitespace (including non-breaking spaces) is stripped
- None / NaN / empty → None
- No silent truncation or rounding
"""

import math
import re
from typing import Optional, Union


# Non-breaking spaces, zero-width chars, and other Unicode whitespace noise
_WHITESPACE_NOISE = re.compile(r"[\s\u00a0\u200b\u200c\u200d\ufeff]+")


def normalize_identifier(value: Union[str, int, float, None]) -> Optional[str]:
    """Normalize a product identifier (article number, SKU, GID, supplier item no, etc.)
    to a clean, deterministic string.

    Handles all common Excel/openpyxl type coercion issues:
    - float 12345.0 → "12345"
    - int 12345 → "12345"
    - str "12345.0" → "12345" (if it looks like a float-formatted integer)
    - str " 12345 " → "12345" (whitespace stripped)
    - str "007890" → "007890" (leading zeros preserved)
    - None / NaN / "nan" / "none" / "" → None

    Args:
        value: Raw cell value from openpyxl, pandas, or user input.

    Returns:
        Normalized string identifier, or None if the value is empty/invalid.
    """
    if value is None:
        return None

    # Handle numeric types directly (common with openpyxl)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        # Float that represents an integer: 12345.0 → "12345"
        if value == int(value):
            return str(int(value))
        # True decimal float (unusual for identifiers, but preserve exactly)
        return str(value)

    if isinstance(value, int):
        return str(value)

    # String handling
    s = str(value)

    # Strip all whitespace noise (including non-breaking spaces)
    s = _WHITESPACE_NOISE.sub("", s).strip()

    # Reject empty/sentinel values
    if not s or s.lower() in ("none", "nan", "null", "na", "n/a", ""):
        return None

    # Handle string that looks like a float-formatted integer: "12345.0" → "12345"
    # But preserve genuine alphanumeric identifiers like "ABC-123.5"
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".")[0]

    return s


def normalize_identifier_strict(value: Union[str, int, float, None]) -> str:
    """Like normalize_identifier, but returns empty string instead of None.

    Use this when writing to Excel cells or contexts that don't accept None.
    """
    return normalize_identifier(value) or ""
