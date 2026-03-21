"""Loader for Jeeves ERP Excel export (Masterdata 2103.xlsx).

Reads the Excel file once, builds a lookup dict keyed by article number,
and returns JeevesData objects for individual products.
"""

import logging
from pathlib import Path
from typing import Optional

from openpyxl import load_workbook

from backend.models import JeevesData

logger = logging.getLogger(__name__)

# Column name → JeevesData field mapping
# The Excel has these exact headers (note trailing space on 'Web Title '):
_COLUMN_MAP = {
    "Item. No": "article_number",
    "GID": "gid",
    "Item description": "item_description",
    "Specification": "specification",
    "Supplier": "supplier",
    "Supplier Item.no": "supplier_item_no",
    "Product Brand": "product_brand",
    "Web Title": "web_title",
    "Web Title ": "web_title",  # handle trailing space variant
    "Web Text": "web_text",
}


class JeevesIndex:
    """In-memory index of Jeeves product data, keyed by article number."""

    def __init__(self) -> None:
        self._data: dict[str, JeevesData] = {}
        self._loaded = False
        self._source_path: Optional[str] = None

    @property
    def loaded(self) -> bool:
        return self._loaded

    @property
    def count(self) -> int:
        return len(self._data)

    def load(self, excel_path: str) -> int:
        """Load the Jeeves Excel file and build the article number index.

        Returns the number of products loaded.
        """
        path = Path(excel_path)
        if not path.exists():
            raise FileNotFoundError(f"Jeeves Excel file not found: {excel_path}")

        logger.info(f"Loading Jeeves data from {excel_path}")
        wb = load_workbook(excel_path, read_only=True, data_only=True)
        ws = wb.active

        # Read header row to build column index
        rows = ws.iter_rows()
        header_row = next(rows)
        headers = [cell.value for cell in header_row]

        # Map column positions to JeevesData field names
        col_to_field: dict[int, str] = {}
        for col_idx, header in enumerate(headers):
            if header and header in _COLUMN_MAP:
                col_to_field[col_idx] = _COLUMN_MAP[header]

        if "article_number" not in col_to_field.values():
            wb.close()
            raise ValueError(
                f"Could not find 'Item. No' column in {excel_path}. "
                f"Headers found: {headers}"
            )

        logger.debug(f"Jeeves column mapping: {col_to_field}")

        # Read data rows
        count = 0
        for row in rows:
            values: dict[str, Optional[str]] = {}
            for col_idx, field_name in col_to_field.items():
                cell_val = row[col_idx].value
                if cell_val is not None:
                    values[field_name] = str(cell_val).strip()

            artnr = values.get("article_number")
            if not artnr:
                continue

            self._data[artnr] = JeevesData(**values)
            count += 1

        wb.close()
        self._loaded = True
        self._source_path = str(excel_path)
        logger.info(f"Jeeves index: {count} products loaded from {excel_path}")
        return count

    def get(self, article_number: str) -> Optional[JeevesData]:
        """Look up Jeeves data for a given article number."""
        return self._data.get(article_number)

    def has(self, article_number: str) -> bool:
        """Check if an article number exists in Jeeves."""
        return article_number in self._data

    def all_article_numbers(self) -> list[str]:
        """Return all article numbers in the index."""
        return list(self._data.keys())


# Module-level singleton for convenience
_default_index: Optional[JeevesIndex] = None


def load_jeeves(excel_path: str) -> JeevesIndex:
    """Load Jeeves data and return the index. Caches the result."""
    global _default_index
    if _default_index and _default_index.loaded:
        return _default_index
    _default_index = JeevesIndex()
    _default_index.load(excel_path)
    return _default_index


def get_jeeves_data(article_number: str) -> Optional[JeevesData]:
    """Look up Jeeves data for an article number using the default index."""
    if _default_index:
        return _default_index.get(article_number)
    return None
