"""Excel import/export handler for masterdata quality check."""

import logging
from io import BytesIO
from pathlib import Path
from typing import Optional

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from backend.models import ProductAnalysis, QualityStatus

logger = logging.getLogger(__name__)

# Color scheme for statuses
STATUS_COLORS = {
    QualityStatus.OK: "C6EFCE",          # Green
    QualityStatus.SHOULD_IMPROVE: "FFEB9C",  # Yellow
    QualityStatus.MISSING: "FFC7CE",      # Red
    QualityStatus.PROBABLE_ERROR: "FF6B6B",  # Dark red
    QualityStatus.REQUIRES_MANUFACTURER: "B4C7E7",  # Blue
}

STATUS_FONT_COLORS = {
    QualityStatus.OK: "006100",
    QualityStatus.SHOULD_IMPROVE: "9C6500",
    QualityStatus.MISSING: "9C0006",
    QualityStatus.PROBABLE_ERROR: "FFFFFF",
    QualityStatus.REQUIRES_MANUFACTURER: "003380",
}


def read_article_numbers(file_content: bytes, filename: str) -> list[str]:
    """Read article numbers from an uploaded Excel file.

    Supports .xlsx and .xls formats.
    Looks for article numbers in the first column, or a column named
    'artikkelnummer', 'artikkel', 'varenummer', 'article', etc.
    """
    wb = load_workbook(BytesIO(file_content), read_only=True, data_only=True)
    ws = wb.active

    article_numbers = []
    article_col = 0  # Default to first column (0-indexed in our logic)

    # Check header row for article number column
    header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
    if header_row:
        search_terms = {
            "artikkelnummer", "artikkel", "artikkelnr", "art.nr", "art nr",
            "varenummer", "varenr", "article", "articlenumber", "article_number",
            "sku", "item", "itemnumber", "produktnummer", "artnr",
        }
        for idx, cell_value in enumerate(header_row):
            if cell_value and str(cell_value).strip().lower().replace(".", "").replace(" ", "") in {
                t.replace(".", "").replace(" ", "") for t in search_terms
            }:
                article_col = idx
                logger.info(f"Found article number column: '{cell_value}' at index {idx}")
                break

    # Read article numbers
    start_row = 2 if header_row else 1  # Skip header if present
    for row in ws.iter_rows(min_row=start_row, values_only=True):
        if row and len(row) > article_col:
            value = row[article_col]
            if value is not None:
                article_num = str(value).strip()
                if article_num and article_num.lower() not in ("", "none", "nan"):
                    article_numbers.append(article_num)

    wb.close()
    logger.info(f"Read {len(article_numbers)} article numbers from {filename}")
    return article_numbers


def create_output_excel(
    results: list[ProductAnalysis],
    output_path: Optional[str] = None,
) -> bytes:
    """Create a structured Excel output file with analysis results.

    Returns the Excel file as bytes.
    """
    wb = Workbook()

    # ── Sheet 1: Overview ──
    ws_overview = wb.active
    ws_overview.title = "Oversikt"
    _create_overview_sheet(ws_overview, results)

    # ── Sheet 2: Field Analysis Detail ──
    ws_detail = wb.create_sheet("Feltanalyse")
    _create_detail_sheet(ws_detail, results)

    # ── Sheet 3: Improvement Suggestions ──
    ws_improvements = wb.create_sheet("Forbedringsforslag")
    _create_improvements_sheet(ws_improvements, results)

    # ── Sheet 4: Manufacturer Follow-up ──
    ws_manufacturer = wb.create_sheet("Produsentoppfølging")
    _create_manufacturer_sheet(ws_manufacturer, results)

    # ── Sheet 5: Summary Statistics ──
    ws_stats = wb.create_sheet("Statistikk")
    _create_stats_sheet(ws_stats, results)

    # Save to bytes
    output = BytesIO()
    wb.save(output)
    output.seek(0)
    content = output.read()

    if output_path:
        Path(output_path).write_bytes(content)
        logger.info(f"Excel output saved to {output_path}")

    return content


def _style_header(ws, row: int, num_cols: int) -> None:
    """Apply header styling to a row."""
    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF", size=11)
    thin_border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )
    for col in range(1, num_cols + 1):
        cell = ws.cell(row=row, column=col)
        cell.fill = header_fill
        cell.font = header_font
        cell.border = thin_border
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)


def _apply_status_style(cell, status: QualityStatus) -> None:
    """Apply color styling based on quality status."""
    fill_color = STATUS_COLORS.get(status, "FFFFFF")
    font_color = STATUS_FONT_COLORS.get(status, "000000")
    cell.fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")
    cell.font = Font(color=font_color)


def _create_overview_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create the overview sheet with one row per product."""
    headers = [
        "Artikkelnummer",
        "Produktnavn",
        "Funnet på OneMed",
        "Total score (%)",
        "Status",
        "Kommentar",
        "Produsent",
        "Kategori",
        "Auto-fix",
        "Manuell vurdering",
        "Krever produsentkontakt",
        "Produkt-URL",
    ]

    for col, header in enumerate(headers, 1):
        ws.cell(row=1, column=col, value=header)
    _style_header(ws, 1, len(headers))

    for row_idx, result in enumerate(results, 2):
        pd = result.product_data
        ws.cell(row=row_idx, column=1, value=result.article_number)
        ws.cell(row=row_idx, column=2, value=pd.product_name or "")
        ws.cell(row=row_idx, column=3, value="Ja" if pd.found_on_onemed else "Nei")
        ws.cell(row=row_idx, column=4, value=result.total_score)
        status_cell = ws.cell(row=row_idx, column=5, value=result.overall_status.value)
        _apply_status_style(status_cell, result.overall_status)
        ws.cell(row=row_idx, column=6, value=result.overall_comment or "")
        ws.cell(row=row_idx, column=7, value=pd.manufacturer or "")
        ws.cell(row=row_idx, column=8, value=pd.category or "")
        ws.cell(row=row_idx, column=9, value="Ja" if result.auto_fix_possible else "Nei")
        ws.cell(row=row_idx, column=10, value="Ja" if result.manual_review_needed else "Nei")
        ws.cell(row=row_idx, column=11, value="Ja" if result.requires_manufacturer_contact else "Nei")
        ws.cell(row=row_idx, column=12, value=pd.product_url or "")

    # Auto-width
    for col in range(1, len(headers) + 1):
        ws.column_dimensions[get_column_letter(col)].width = max(15, len(headers[col - 1]) + 5)

    # Freeze header
    ws.freeze_panes = "A2"

    # Auto-filter
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{len(results) + 1}"


def _create_detail_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create detailed field analysis sheet."""
    headers = [
        "Artikkelnummer",
        "Produktnavn",
        "Felt",
        "Nåværende verdi",
        "Status",
        "Kommentar",
        "Foreslått verdi",
        "Kilde",
        "Confidence",
    ]

    for col, header in enumerate(headers, 1):
        ws.cell(row=1, column=col, value=header)
    _style_header(ws, 1, len(headers))

    row_idx = 2
    for result in results:
        for fa in result.field_analyses:
            ws.cell(row=row_idx, column=1, value=result.article_number)
            ws.cell(row=row_idx, column=2, value=result.product_data.product_name or "")
            ws.cell(row=row_idx, column=3, value=fa.field_name)
            ws.cell(row=row_idx, column=4, value=fa.current_value or "")
            status_cell = ws.cell(row=row_idx, column=5, value=fa.status.value)
            _apply_status_style(status_cell, fa.status)
            ws.cell(row=row_idx, column=6, value=fa.comment or "")
            ws.cell(row=row_idx, column=7, value=fa.suggested_value or "")
            ws.cell(row=row_idx, column=8, value=fa.source or "")
            ws.cell(row=row_idx, column=9, value=fa.confidence if fa.confidence else "")
            row_idx += 1

    for col in range(1, len(headers) + 1):
        ws.column_dimensions[get_column_letter(col)].width = max(15, len(headers[col - 1]) + 5)

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{row_idx - 1}"


def _create_improvements_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create improvements suggestion sheet."""
    headers = [
        "Artikkelnummer",
        "Produktnavn",
        "Felt",
        "Nåværende verdi",
        "Foreslått verdi",
        "Kilde",
        "Confidence",
        "Begrunnelse",
    ]

    for col, header in enumerate(headers, 1):
        ws.cell(row=1, column=col, value=header)
    _style_header(ws, 1, len(headers))

    row_idx = 2
    for result in results:
        for fa in result.field_analyses:
            if fa.suggested_value:
                ws.cell(row=row_idx, column=1, value=result.article_number)
                ws.cell(row=row_idx, column=2, value=result.product_data.product_name or "")
                ws.cell(row=row_idx, column=3, value=fa.field_name)
                ws.cell(row=row_idx, column=4, value=fa.current_value or "")
                ws.cell(row=row_idx, column=5, value=fa.suggested_value)
                ws.cell(row=row_idx, column=6, value=fa.source or "")
                ws.cell(row=row_idx, column=7, value=fa.confidence if fa.confidence else "")
                ws.cell(row=row_idx, column=8, value=fa.comment or "")
                row_idx += 1

    if row_idx == 2:
        ws.cell(row=2, column=1, value="Ingen forbedringsforslag generert")

    for col in range(1, len(headers) + 1):
        ws.column_dimensions[get_column_letter(col)].width = max(15, len(headers[col - 1]) + 5)

    ws.freeze_panes = "A2"


def _create_manufacturer_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create manufacturer follow-up sheet, grouped by manufacturer."""
    headers = [
        "Produsent",
        "Artikkelnummer",
        "Produktnavn",
        "Manglende felt",
        "Status",
        "Foreslått melding",
    ]

    for col, header in enumerate(headers, 1):
        ws.cell(row=1, column=col, value=header)
    _style_header(ws, 1, len(headers))

    # Group by manufacturer
    by_manufacturer: dict[str, list[ProductAnalysis]] = {}
    for result in results:
        if result.requires_manufacturer_contact:
            mfr = result.product_data.manufacturer or "Ukjent produsent"
            if mfr not in by_manufacturer:
                by_manufacturer[mfr] = []
            by_manufacturer[mfr].append(result)

    row_idx = 2
    for manufacturer in sorted(by_manufacturer.keys()):
        products = by_manufacturer[manufacturer]

        # Manufacturer group header
        group_fill = PatternFill(start_color="D9E2F3", end_color="D9E2F3", fill_type="solid")
        group_font = Font(bold=True, size=11)
        cell = ws.cell(row=row_idx, column=1, value=f"── {manufacturer} ({len(products)} produkter) ──")
        cell.fill = group_fill
        cell.font = group_font
        for col in range(2, len(headers) + 1):
            ws.cell(row=row_idx, column=col).fill = group_fill
        row_idx += 1

        for result in products:
            missing_fields = [
                fa.field_name for fa in result.field_analyses
                if fa.status in (QualityStatus.MISSING, QualityStatus.REQUIRES_MANUFACTURER)
            ]
            ws.cell(row=row_idx, column=1, value=manufacturer)
            ws.cell(row=row_idx, column=2, value=result.article_number)
            ws.cell(row=row_idx, column=3, value=result.product_data.product_name or "")
            ws.cell(row=row_idx, column=4, value=", ".join(missing_fields))
            status_cell = ws.cell(row=row_idx, column=5, value=result.overall_status.value)
            _apply_status_style(status_cell, result.overall_status)
            ws.cell(row=row_idx, column=6, value=result.suggested_manufacturer_message or "")
            row_idx += 1

    if row_idx == 2:
        ws.cell(row=2, column=1, value="Ingen produkter krever produsentkontakt")

    for col in range(1, len(headers) + 1):
        ws.column_dimensions[get_column_letter(col)].width = max(18, len(headers[col - 1]) + 5)

    ws.freeze_panes = "A2"


def _create_stats_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create summary statistics sheet."""
    title_font = Font(bold=True, size=14)
    header_font = Font(bold=True, size=11)

    ws.cell(row=1, column=1, value="Masterdata Kvalitetsrapport - Sammendrag").font = title_font

    total = len(results)
    found = sum(1 for r in results if r.product_data.found_on_onemed)
    not_found = total - found

    status_counts = {}
    for r in results:
        status = r.overall_status.value
        status_counts[status] = status_counts.get(status, 0) + 1

    avg_score = sum(r.total_score for r in results) / total if total else 0
    auto_fix = sum(1 for r in results if r.auto_fix_possible)
    manual = sum(1 for r in results if r.manual_review_needed)
    mfr_contact = sum(1 for r in results if r.requires_manufacturer_contact)

    rows = [
        ("", ""),
        ("Totalt antall produkter:", total),
        ("Funnet på OneMed:", found),
        ("Ikke funnet:", not_found),
        ("", ""),
        ("Gjennomsnittlig kvalitetsscore:", f"{avg_score:.1f}%"),
        ("", ""),
        ("Statusfordeling:", ""),
    ]

    for status_name, count in sorted(status_counts.items()):
        rows.append((f"  {status_name}:", count))

    rows.extend([
        ("", ""),
        ("Oppfølging:", ""),
        ("  Auto-fix mulig:", auto_fix),
        ("  Manuell vurdering:", manual),
        ("  Krever produsentkontakt:", mfr_contact),
    ])

    for idx, (label, value) in enumerate(rows, 3):
        cell_label = ws.cell(row=idx, column=1, value=label)
        cell_value = ws.cell(row=idx, column=2, value=value)
        if label and not label.startswith("  "):
            cell_label.font = header_font

    ws.column_dimensions["A"].width = 35
    ws.column_dimensions["B"].width = 20
