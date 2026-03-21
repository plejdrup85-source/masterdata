"""Excel import/export handler for masterdata quality check."""

import logging
from datetime import datetime
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
    QualityStatus.OK: "C6EFCE",
    QualityStatus.SHOULD_IMPROVE: "FFEB9C",
    QualityStatus.MISSING: "FFC7CE",
    QualityStatus.PROBABLE_ERROR: "FF6B6B",
    QualityStatus.REQUIRES_MANUFACTURER: "B4C7E7",
}

STATUS_FONT_COLORS = {
    QualityStatus.OK: "006100",
    QualityStatus.SHOULD_IMPROVE: "9C6500",
    QualityStatus.MISSING: "9C0006",
    QualityStatus.PROBABLE_ERROR: "FFFFFF",
    QualityStatus.REQUIRES_MANUFACTURER: "003380",
}


def read_article_numbers(file_content: bytes, filename: str) -> tuple[list[str], str]:
    """Read article numbers from an uploaded Excel file.

    Returns a tuple of (article_numbers, detected_column_name).
    """
    wb = load_workbook(BytesIO(file_content), read_only=True, data_only=True)
    ws = wb.active

    article_numbers = []
    article_col = 0
    detected_column = "Kolonne A (standard)"

    # Check header row for article number column
    header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
    if header_row:
        search_terms = {
            "artikkelnummer", "artikkel", "artikkelnr", "artnr", "artnummer",
            "varenummer", "varenr",
            "article", "articlenumber", "article_number",
            "sku", "item", "itemnumber", "produktnummer",
        }
        normalized_search = {
            t.replace(".", "").replace(" ", "").replace("_", "")
            for t in search_terms
        }
        for idx, cell_value in enumerate(header_row):
            if cell_value:
                normalized = str(cell_value).strip().lower().replace(".", "").replace(" ", "").replace("_", "")
                if normalized in normalized_search:
                    article_col = idx
                    detected_column = str(cell_value).strip()
                    logger.info(f"Found article number column: '{cell_value}' at index {idx}")
                    break

    # Read article numbers
    start_row = 2 if header_row else 1
    for row in ws.iter_rows(min_row=start_row, values_only=True):
        if row and len(row) > article_col:
            value = row[article_col]
            if value is not None:
                article_num = str(value).strip()
                if article_num and article_num.lower() not in ("", "none", "nan"):
                    article_numbers.append(article_num)

    wb.close()
    logger.info(f"Read {len(article_numbers)} article numbers from {filename} (column: {detected_column})")
    return article_numbers, detected_column


def create_output_excel(
    results: list[ProductAnalysis],
    output_path: str,
) -> None:
    """Create a structured Excel output file with analysis results.

    Writes directly to file to avoid holding entire workbook in memory twice.
    """
    wb = Workbook()

    # Sheet 1: Overview
    ws_overview = wb.active
    ws_overview.title = "Oversikt"
    _create_overview_sheet(ws_overview, results)

    # Sheet 2: Field Analysis Detail
    ws_detail = wb.create_sheet("Feltanalyse")
    _create_detail_sheet(ws_detail, results)

    # Sheet 3: Improvement Suggestions
    ws_improvements = wb.create_sheet("Forbedringsforslag")
    _create_improvements_sheet(ws_improvements, results)

    # Sheet 4: Manufacturer Follow-up
    ws_manufacturer = wb.create_sheet("Produsentoppf\u00f8lging")
    _create_manufacturer_sheet(ws_manufacturer, results)

    # Sheet 5: Image Details
    ws_images = wb.create_sheet("Bildeanalyse")
    _create_image_detail_sheet(ws_images, results)

    # Sheet 6: Image Issues Priority
    ws_img_issues = wb.create_sheet("Bildeproblemer")
    _create_image_issues_sheet(ws_img_issues, results)

    # Sheet 7: Enrichment Details
    ws_enrichment = wb.create_sheet("Berikelse")
    _create_enrichment_sheet(ws_enrichment, results)

    # Sheet 8: Source Conflicts
    ws_conflicts = wb.create_sheet("Kildekonflikter")
    _create_conflicts_sheet(ws_conflicts, results)

    # Sheet 9: Summary Statistics
    ws_stats = wb.create_sheet("Statistikk")
    _create_stats_sheet(ws_stats, results)

    # Sheet 10: Inriver Import staging
    ws_inriver = wb.create_sheet("Inriver Import")
    _create_inriver_import_sheet(ws_inriver, results)

    # Save directly to file
    wb.save(output_path)
    logger.info(f"Excel output saved to {output_path}")


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


IMAGE_STATUS_COLORS = {
    "PASS": "C6EFCE",
    "PASS_WITH_NOTES": "FFEB9C",
    "REVIEW": "FFEB9C",
    "FAIL": "FF6B6B",
    "MISSING": "FFC7CE",
}

IMAGE_STATUS_FONT_COLORS = {
    "PASS": "006100",
    "PASS_WITH_NOTES": "9C6500",
    "REVIEW": "9C6500",
    "FAIL": "FFFFFF",
    "MISSING": "9C0006",
}


def _apply_image_status_style(cell, status: str) -> None:
    """Apply color styling based on image quality status."""
    fill_color = IMAGE_STATUS_COLORS.get(status, "FFFFFF")
    font_color = IMAGE_STATUS_FONT_COLORS.get(status, "000000")
    cell.fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")
    cell.font = Font(color=font_color)


def _create_overview_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create the overview sheet with one row per product."""
    headers = [
        "Artikkelnummer",
        "Produktnavn",
        "Funnet p\u00e5 OneMed",
        "Total score (%)",
        "Status",
        "Kommentar",
        "Produsent",
        "Kategori",
        "Bildescore",
        "Bildestatus",
        "Antall bilder",
        "PDF tilgjengelig",
        "Berikelser funnet",
        "Kildekonflikter",
        "Auto-fix",
        "Manuell vurdering",
        "Krever produsentkontakt",
        "Produkt-URL",
        "PDF-URL",
    ]

    for col, header in enumerate(headers, 1):
        ws.cell(row=1, column=col, value=header)
    _style_header(ws, 1, len(headers))

    for row_idx, result in enumerate(results, 2):
        pd = result.product_data
        iq = result.image_quality or {}
        enriched = [e for e in result.enrichment_results if e.match_status != "NOT_FOUND"]
        conflicts = [e for e in result.enrichment_results if e.match_status == "FOUND_IN_BOTH_CONFLICT"]
        ws.cell(row=row_idx, column=1, value=result.article_number)
        ws.cell(row=row_idx, column=2, value=pd.product_name or "")
        ws.cell(row=row_idx, column=3, value="Ja" if pd.found_on_onemed else "Nei")
        ws.cell(row=row_idx, column=4, value=result.total_score)
        status_cell = ws.cell(row=row_idx, column=5, value=result.overall_status.value)
        _apply_status_style(status_cell, result.overall_status)
        ws.cell(row=row_idx, column=6, value=result.overall_comment or "")
        ws.cell(row=row_idx, column=7, value=pd.manufacturer or "")
        ws.cell(row=row_idx, column=8, value=pd.category or "")
        # Image quality columns
        img_score = iq.get("avg_image_score", 0)
        img_status = iq.get("image_quality_status", "MISSING")
        img_count = iq.get("image_count_found", 0)
        ws.cell(row=row_idx, column=9, value=round(img_score, 1) if img_score else 0)
        img_status_cell = ws.cell(row=row_idx, column=10, value=img_status)
        _apply_image_status_style(img_status_cell, img_status)
        ws.cell(row=row_idx, column=11, value=img_count)
        # Enrichment columns
        ws.cell(row=row_idx, column=12, value="Ja" if result.pdf_available else "Nei")
        ws.cell(row=row_idx, column=13, value=len(enriched))
        conflict_cell = ws.cell(row=row_idx, column=14, value=len(conflicts))
        if conflicts:
            conflict_cell.fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
            conflict_cell.font = Font(color="9C6500")
        ws.cell(row=row_idx, column=15, value="Ja" if result.auto_fix_possible else "Nei")
        ws.cell(row=row_idx, column=16, value="Ja" if result.manual_review_needed else "Nei")
        ws.cell(row=row_idx, column=17, value="Ja" if result.requires_manufacturer_contact else "Nei")
        ws.cell(row=row_idx, column=18, value=pd.product_url or "")
        ws.cell(row=row_idx, column=19, value=result.pdf_url or "")

    for col in range(1, len(headers) + 1):
        ws.column_dimensions[get_column_letter(col)].width = max(15, len(headers[col - 1]) + 5)

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{len(results) + 1}"


def _create_detail_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create detailed field analysis sheet."""
    headers = [
        "Artikkelnummer",
        "Produktnavn",
        "Felt",
        "N\u00e5v\u00e6rende verdi",
        "Status",
        "Kommentar",
        "Foresl\u00e5tt verdi",
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
    if row_idx > 2:
        ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{row_idx - 1}"


def _create_improvements_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create improvements suggestion sheet."""
    headers = [
        "Artikkelnummer",
        "Produktnavn",
        "Felt",
        "N\u00e5v\u00e6rende verdi",
        "Foresl\u00e5tt verdi",
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
        "Foresl\u00e5tt melding",
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

        group_fill = PatternFill(start_color="D9E2F3", end_color="D9E2F3", fill_type="solid")
        group_font = Font(bold=True, size=11)
        cell = ws.cell(row=row_idx, column=1, value=f"{manufacturer} ({len(products)} produkter)")
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


def _create_image_detail_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create detailed image analysis sheet with one row per image."""
    headers = [
        "Artikkelnummer",
        "Bilde",
        "URL",
        "Finnes",
        "Filstr. (KB)",
        "Bredde",
        "H\u00f8yde",
        "Ratio",
        "Oppl\u00f8sning",
        "Skarphet (r\u00e5)",
        "Skarphet",
        "Lysstyrke",
        "Lysstyrke score",
        "Kontrast",
        "Kontrast score",
        "Hvit bakgr.",
        "Bakgrunn score",
        "Kantdeteksjon",
        "Kant score",
        "Produktfyll",
        "Fyll score",
        "Total score",
        "Status",
        "Problemer",
    ]

    for col, header in enumerate(headers, 1):
        ws.cell(row=1, column=col, value=header)
    _style_header(ws, 1, len(headers))

    row_idx = 2
    for result in results:
        iq = result.image_quality
        if not iq:
            continue
        for img in iq.get("image_analyses", []):
            ws.cell(row=row_idx, column=1, value=result.article_number)
            ws.cell(row=row_idx, column=2, value=img.get("image_name", ""))
            ws.cell(row=row_idx, column=3, value=img.get("image_url", ""))
            ws.cell(row=row_idx, column=4, value="Ja" if img.get("exists") else "Nei")
            ws.cell(row=row_idx, column=5, value=img.get("file_size_kb", 0))
            ws.cell(row=row_idx, column=6, value=img.get("width", 0))
            ws.cell(row=row_idx, column=7, value=img.get("height", 0))
            ws.cell(row=row_idx, column=8, value=img.get("aspect_ratio", 0))
            ws.cell(row=row_idx, column=9, value=img.get("resolution_score", 0))
            ws.cell(row=row_idx, column=10, value=img.get("blur_score_raw", 0))
            ws.cell(row=row_idx, column=11, value=img.get("blur_score", 0))
            ws.cell(row=row_idx, column=12, value=img.get("brightness_mean", 0))
            ws.cell(row=row_idx, column=13, value=img.get("brightness_score", 0))
            ws.cell(row=row_idx, column=14, value=img.get("contrast_std", 0))
            ws.cell(row=row_idx, column=15, value=img.get("contrast_score", 0))
            ws.cell(row=row_idx, column=16, value=img.get("white_bg_ratio", 0))
            ws.cell(row=row_idx, column=17, value=img.get("background_score", 0))
            ws.cell(row=row_idx, column=18, value=img.get("edge_density", 0))
            ws.cell(row=row_idx, column=19, value=img.get("edge_score", 0))
            ws.cell(row=row_idx, column=20, value=img.get("product_fill_ratio", 0))
            ws.cell(row=row_idx, column=21, value=img.get("fill_score", 0))
            ws.cell(row=row_idx, column=22, value=img.get("overall_score", 0))
            status = img.get("status", "MISSING")
            status_cell = ws.cell(row=row_idx, column=23, value=status)
            _apply_image_status_style(status_cell, status)
            issues = img.get("issues", [])
            ws.cell(row=row_idx, column=24, value=", ".join(issues) if issues else "")
            row_idx += 1

    if row_idx == 2:
        ws.cell(row=2, column=1, value="Ingen bildedata tilgjengelig")

    for col in range(1, len(headers) + 1):
        ws.column_dimensions[get_column_letter(col)].width = max(12, len(headers[col - 1]) + 3)

    ws.freeze_panes = "A2"
    if row_idx > 2:
        ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{row_idx - 1}"


def _create_image_issues_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create prioritized image issues sheet."""
    headers = [
        "Prioritet",
        "Artikkelnummer",
        "Produktnavn",
        "Bildestatus",
        "Bildescore",
        "Antall bilder",
        "Problemer",
    ]

    for col, header in enumerate(headers, 1):
        ws.cell(row=1, column=col, value=header)
    _style_header(ws, 1, len(headers))

    # Collect products with image issues, sorted by priority
    issue_rows = []
    for result in results:
        iq = result.image_quality or {}
        status = iq.get("image_quality_status", "MISSING")
        priority = iq.get("image_quality_priority", "none")
        if priority == "none":
            continue
        priority_order = {"high": 0, "medium": 1, "low": 2}
        issue_rows.append((
            priority_order.get(priority, 3),
            priority,
            result,
            iq,
        ))

    issue_rows.sort(key=lambda x: (x[0], -x[3].get("avg_image_score", 0)))

    row_idx = 2
    priority_labels = {"high": "H\u00f8y", "medium": "Medium", "low": "Lav"}
    for _, priority, result, iq in issue_rows:
        label = priority_labels.get(priority, priority)
        ws.cell(row=row_idx, column=1, value=label)
        ws.cell(row=row_idx, column=2, value=result.article_number)
        ws.cell(row=row_idx, column=3, value=result.product_data.product_name or "")
        status = iq.get("image_quality_status", "MISSING")
        status_cell = ws.cell(row=row_idx, column=4, value=status)
        _apply_image_status_style(status_cell, status)
        ws.cell(row=row_idx, column=5, value=round(iq.get("avg_image_score", 0), 1))
        ws.cell(row=row_idx, column=6, value=iq.get("image_count_found", 0))
        ws.cell(row=row_idx, column=7, value=iq.get("image_issue_summary", ""))
        row_idx += 1

    if row_idx == 2:
        ws.cell(row=2, column=1, value="Ingen bildeproblemer funnet")

    for col in range(1, len(headers) + 1):
        ws.column_dimensions[get_column_letter(col)].width = max(15, len(headers[col - 1]) + 5)

    ws.freeze_panes = "A2"


ENRICHMENT_STATUS_COLORS = {
    "FOUND_IN_INTERNAL_PDF": "C6EFCE",
    "FOUND_IN_MANUFACTURER_SOURCE": "D9E2F3",
    "FOUND_IN_BOTH_MATCH": "C6EFCE",
    "FOUND_IN_BOTH_CONFLICT": "FFEB9C",
    "NOT_FOUND": "F2F2F2",
    "REVIEW_REQUIRED": "FFC7CE",
}


def _apply_enrichment_status_style(cell, status: str) -> None:
    fill_color = ENRICHMENT_STATUS_COLORS.get(status, "FFFFFF")
    cell.fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")


def _create_enrichment_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create enrichment details sheet with one row per enriched field."""
    headers = [
        "Artikkelnummer",
        "Felt",
        "N\u00e5v\u00e6rende verdi",
        "Foresl\u00e5tt verdi",
        "Kildeniv\u00e5",
        "Kildetype",
        "Kilde-URL",
        "Evidens",
        "Confidence",
        "Match-status",
        "Gjennomgang",
    ]

    for col, header in enumerate(headers, 1):
        ws.cell(row=1, column=col, value=header)
    _style_header(ws, 1, len(headers))

    row_idx = 2
    for result in results:
        for er in result.enrichment_results:
            if er.match_status == "NOT_FOUND":
                continue  # Skip empty results to keep sheet actionable
            ws.cell(row=row_idx, column=1, value=result.article_number)
            ws.cell(row=row_idx, column=2, value=er.field_name)
            ws.cell(row=row_idx, column=3, value=er.current_value or "")
            ws.cell(row=row_idx, column=4, value=er.suggested_value or "")
            ws.cell(row=row_idx, column=5, value=er.source_level or "")
            ws.cell(row=row_idx, column=6, value=er.source_type or "")
            ws.cell(row=row_idx, column=7, value=er.source_url or "")
            ws.cell(row=row_idx, column=8, value=er.evidence_snippet or "")
            ws.cell(row=row_idx, column=9, value=round(er.confidence, 2) if er.confidence else 0)
            status_cell = ws.cell(row=row_idx, column=10, value=er.match_status)
            _apply_enrichment_status_style(status_cell, er.match_status)
            ws.cell(row=row_idx, column=11, value=er.review_status)
            row_idx += 1

    if row_idx == 2:
        ws.cell(row=2, column=1, value="Ingen berikelser funnet")

    for col in range(1, len(headers) + 1):
        ws.column_dimensions[get_column_letter(col)].width = max(14, len(headers[col - 1]) + 3)

    ws.freeze_panes = "A2"
    if row_idx > 2:
        ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{row_idx - 1}"


def _create_conflicts_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create source conflict sheet - shows fields where PDF and manufacturer disagree."""
    headers = [
        "Artikkelnummer",
        "Produktnavn",
        "Felt",
        "N\u00e5v\u00e6rende verdi",
        "PDF-verdi",
        "Produsent-verdi",
        "Evidens",
        "Anbefaling",
    ]

    for col, header in enumerate(headers, 1):
        ws.cell(row=1, column=col, value=header)
    _style_header(ws, 1, len(headers))

    row_idx = 2
    for result in results:
        for er in result.enrichment_results:
            if er.match_status != "FOUND_IN_BOTH_CONFLICT":
                continue
            ws.cell(row=row_idx, column=1, value=result.article_number)
            ws.cell(row=row_idx, column=2, value=result.product_data.product_name or "")
            ws.cell(row=row_idx, column=3, value=er.field_name)
            ws.cell(row=row_idx, column=4, value=er.current_value or "")
            # Parse the evidence snippet to extract both values
            evidence = er.evidence_snippet or ""
            pdf_val = ""
            mfr_val = ""
            if "KONFLIKT" in evidence:
                parts = evidence.split(" vs ")
                if len(parts) == 2:
                    pdf_val = parts[0].replace("KONFLIKT - PDF: ", "").strip("'")
                    mfr_val = parts[1].replace("Produsent: ", "").strip("'")
            ws.cell(row=row_idx, column=5, value=pdf_val)
            ws.cell(row=row_idx, column=6, value=mfr_val)
            ws.cell(row=row_idx, column=7, value=evidence)
            ws.cell(row=row_idx, column=8, value="Manuell vurdering p\u00e5krevd")
            # Color the row
            for col in range(1, len(headers) + 1):
                ws.cell(row=row_idx, column=col).fill = PatternFill(
                    start_color="FFF2CC", end_color="FFF2CC", fill_type="solid"
                )
            row_idx += 1

    if row_idx == 2:
        ws.cell(row=2, column=1, value="Ingen kildekonflikter funnet")

    for col in range(1, len(headers) + 1):
        ws.column_dimensions[get_column_letter(col)].width = max(16, len(headers[col - 1]) + 4)

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
        ("Funnet p\u00e5 OneMed:", found),
        ("Ikke funnet:", not_found),
        ("", ""),
        ("Gjennomsnittlig kvalitetsscore:", f"{avg_score:.1f}%"),
        ("", ""),
        ("Statusfordeling:", ""),
    ]

    for status_name, count in sorted(status_counts.items()):
        rows.append((f"  {status_name}:", count))

    # Image quality stats
    img_scores = []
    img_missing = 0
    img_fail = 0
    img_review = 0
    img_pass = 0
    for r in results:
        iq = r.image_quality or {}
        s = iq.get("image_quality_status", "MISSING")
        if s == "MISSING":
            img_missing += 1
        elif s == "FAIL":
            img_fail += 1
        elif s in ("REVIEW", "PASS_WITH_NOTES"):
            img_review += 1
        else:
            img_pass += 1
        if iq.get("avg_image_score", 0) > 0:
            img_scores.append(iq["avg_image_score"])

    avg_img_score = sum(img_scores) / len(img_scores) if img_scores else 0

    rows.extend([
        ("", ""),
        ("Bildekvalitet:", ""),
        ("  Gjennomsnittlig bildescore:", f"{avg_img_score:.1f}"),
        ("  Bilder OK:", img_pass),
        ("  Trenger gjennomgang:", img_review),
        ("  Feiler:", img_fail),
        ("  Mangler bilde:", img_missing),
    ])

    # Enrichment stats
    pdf_found = sum(1 for r in results if r.pdf_available)
    enriched_products = sum(1 for r in results if any(
        e.match_status != "NOT_FOUND" for e in r.enrichment_results
    ))
    total_enrichments = sum(
        1 for r in results for e in r.enrichment_results
        if e.match_status != "NOT_FOUND"
    )
    from_pdf = sum(
        1 for r in results for e in r.enrichment_results
        if e.match_status in ("FOUND_IN_INTERNAL_PDF", "FOUND_IN_BOTH_MATCH")
    )
    from_mfr = sum(
        1 for r in results for e in r.enrichment_results
        if e.match_status == "FOUND_IN_MANUFACTURER_SOURCE"
    )
    conflict_count = sum(
        1 for r in results for e in r.enrichment_results
        if e.match_status == "FOUND_IN_BOTH_CONFLICT"
    )

    rows.extend([
        ("", ""),
        ("Berikelse:", ""),
        ("  Produktdatablad funnet:", pdf_found),
        ("  Produkter med berikelse:", enriched_products),
        ("  Totalt berikede felt:", total_enrichments),
        ("  Fra internt datablad:", from_pdf),
        ("  Fra produsent:", from_mfr),
        ("  Kildekonflikter:", conflict_count),
        ("", ""),
        ("Oppf\u00f8lging:", ""),
        ("  Auto-fix mulig:", auto_fix),
        ("  Manuell vurdering:", manual),
        ("  Krever produsentkontakt:", mfr_contact),
    ])

    for idx, (label, value) in enumerate(rows, 3):
        cell_label = ws.cell(row=idx, column=1, value=label)
        ws.cell(row=idx, column=2, value=value)
        if label and not label.startswith("  "):
            cell_label.font = header_font

    ws.column_dimensions["A"].width = 35
    ws.column_dimensions["B"].width = 20


# --- Inriver Import colors ---
INRIVER_STATUS_COLORS = {
    "Ready for Inriver": PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid"),
    "Needs Review": PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid"),
    "No Change": PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid"),
    "Missing Source Data": PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid"),
}

INRIVER_STATUS_FONTS = {
    "Ready for Inriver": Font(color="006100"),
    "Needs Review": Font(color="9C6500"),
    "No Change": Font(color="808080"),
    "Missing Source Data": Font(color="9C0006"),
}


def _get_suggestion_for_field(result: ProductAnalysis, field_name: str) -> Optional[str]:
    """Get the best suggestion for a field from all enrichment sources."""
    # Priority 1: AI enrichment
    ai = result.ai_enrichment or {}
    ai_map = {
        "Beskrivelse": ai.get("improved_description"),
        "Kategori": ai.get("suggested_category"),
        "Pakningsinformasjon": ai.get("packaging_suggestions"),
    }
    if field_name in ai_map and ai_map[field_name]:
        return ai_map[field_name]

    # Priority 2: Field analysis suggestions (from PDF/manufacturer enrichment)
    for fa in result.field_analyses:
        if fa.field_name == field_name and fa.suggested_value:
            return fa.suggested_value

    # Priority 3: Enrichment results
    enrichment_field_map = {
        "Produktnavn": "product_name",
        "Beskrivelse": "description",
        "Produsent": "manufacturer",
        "Produsentens varenummer": "manufacturer_article_number",
        "Pakningsinformasjon": "packaging_info",
    }
    er_key = enrichment_field_map.get(field_name)
    if er_key:
        for er in result.enrichment_results:
            if er.field_name == er_key and er.suggested_value and er.match_status != "NOT_FOUND":
                return er.suggested_value

    return None


def _get_suggestion_source(result: ProductAnalysis, field_name: str) -> str:
    """Determine the source of the suggestion for a field."""
    sources = []

    # Check AI enrichment
    ai = result.ai_enrichment or {}
    ai_map = {
        "Beskrivelse": ai.get("improved_description"),
        "Kategori": ai.get("suggested_category"),
        "Pakningsinformasjon": ai.get("packaging_suggestions"),
    }
    if field_name in ai_map and ai_map[field_name]:
        sources.append("AI suggestion")

    # Check field analysis suggestions
    for fa in result.field_analyses:
        if fa.field_name == field_name and fa.suggested_value and fa.source:
            sources.append(fa.source)

    # Check enrichment results
    enrichment_field_map = {
        "Produktnavn": "product_name",
        "Beskrivelse": "description",
        "Produsent": "manufacturer",
        "Produsentens varenummer": "manufacturer_article_number",
        "Pakningsinformasjon": "packaging_info",
    }
    er_key = enrichment_field_map.get(field_name)
    if er_key:
        for er in result.enrichment_results:
            if er.field_name == er_key and er.suggested_value and er.match_status != "NOT_FOUND":
                if er.source_level == "internal_product_sheet":
                    sources.append("product datasheet")
                elif er.source_level == "manufacturer_source":
                    sources.append("manufacturer website")

    if not sources:
        if result.product_data.found_on_onemed:
            return "onemed.no"
        return "existing catalog"

    return "; ".join(dict.fromkeys(sources))  # deduplicate preserving order


def _determine_enrichment_status(result: ProductAnalysis) -> tuple[str, bool, str]:
    """Determine Enrichment_Status, Review_Required, and comment for a product.

    Returns (enrichment_status, review_required, comment).
    """
    pd = result.product_data
    comments = []

    if not pd.found_on_onemed:
        return "Missing Source Data", True, "Produkt ikke funnet i kilde"

    # Count how many fields have suggestions
    suggestion_fields = []
    ai_fields = []
    for field_name in ["Produktnavn", "Beskrivelse", "Spesifikasjon", "Kategori", "Pakningsinformasjon"]:
        suggestion = _get_suggestion_for_field(result, field_name)
        if suggestion:
            suggestion_fields.append(field_name)
            source = _get_suggestion_source(result, field_name)
            if "AI" in source:
                ai_fields.append(field_name)

    # Count missing/poor fields
    missing_fields = [
        fa.field_name for fa in result.field_analyses
        if fa.status in (QualityStatus.MISSING, QualityStatus.PROBABLE_ERROR)
        and fa.field_name not in ("Bildekvalitet", "Konsistens mellom felter")
    ]
    improve_fields = [
        fa.field_name for fa in result.field_analyses
        if fa.status == QualityStatus.SHOULD_IMPROVE
        and fa.field_name not in ("Bildekvalitet", "Konsistens mellom felter")
    ]

    # No suggestions and no problems = No Change
    if not suggestion_fields and not missing_fields:
        return "No Change", False, "Produkt har akseptabel kvalitet"

    # Missing source data if too many critical fields are missing with no suggestions
    critical_missing = [f for f in missing_fields if f in ("Produktnavn", "Beskrivelse", "Spesifikasjon")]
    if len(critical_missing) >= 2 and not suggestion_fields:
        comments.append(f"Mangler: {', '.join(critical_missing)}")
        return "Missing Source Data", True, "; ".join(comments)

    # If AI generated content or low-confidence suggestions → Needs Review
    if ai_fields:
        comments.append(f"AI-generert innhold for: {', '.join(ai_fields)}")

    # Check for conflicts in enrichment
    conflicts = [
        er for er in result.enrichment_results
        if er.match_status == "FOUND_IN_BOTH_CONFLICT"
    ]
    if conflicts:
        comments.append(f"{len(conflicts)} kildekonflikt(er)")

    # Check confidence levels
    low_confidence_fields = []
    for fa in result.field_analyses:
        if fa.suggested_value and fa.confidence and fa.confidence < 0.7:
            low_confidence_fields.append(fa.field_name)
    if low_confidence_fields:
        comments.append(f"Lav confidence: {', '.join(low_confidence_fields)}")

    needs_review = bool(ai_fields or conflicts or low_confidence_fields or missing_fields)

    if suggestion_fields and not needs_review:
        # High confidence, structured data, no AI guessing
        comments.append(f"Forslag for: {', '.join(suggestion_fields)}")
        return "Ready for Inriver", False, "; ".join(comments)

    if suggestion_fields:
        if missing_fields:
            comments.append(f"Mangler fortsatt: {', '.join(missing_fields)}")
        return "Needs Review", True, "; ".join(comments)

    if missing_fields:
        comments.append(f"Mangler: {', '.join(missing_fields)}")
        return "Needs Review", True, "; ".join(comments)

    if improve_fields:
        comments.append(f"Bør forbedres: {', '.join(improve_fields)}")
        return "Needs Review", True, "; ".join(comments)

    return "No Change", False, "Ingen endringer nødvendig"


def _create_inriver_import_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create the Inriver Import staging sheet.

    One row per product with existing values, suggested values, and import workflow columns.
    Designed as a real staging layer for business users before Inriver import.
    """
    headers = [
        "Artikkelnummer",
        "Produktnavn_eksisterende",
        "Produktnavn_forslag",
        "Beskrivelse_eksisterende",
        "Beskrivelse_forslag",
        "Spesifikasjon_eksisterende",
        "Spesifikasjon_forslag",
        "Kategori_eksisterende",
        "Kategori_forslag",
        "Pakningsinformasjon_eksisterende",
        "Pakningsinformasjon_forslag",
        "Produsent_eksisterende",
        "Produsent_forslag",
        "Produsent_artnr_eksisterende",
        "Produsent_artnr_forslag",
        "Datablad_URL",
        "Bilde_URL",
        "Quality_Score",
        "Enrichment_Status",
        "Review_Required",
        "Import_Approved",
        "Import_Batch",
        "Kommentar",
        "Kilde",
        "Sist_oppdatert",
    ]

    # Write headers
    for col, header in enumerate(headers, 1):
        ws.cell(row=1, column=col, value=header)
    _style_header(ws, 1, len(headers))

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    batch_id = f"Batch_{datetime.now().strftime('%Y_%m_%d')}"

    wrap_alignment = Alignment(wrap_text=True, vertical="top")
    top_alignment = Alignment(vertical="top")

    for row_idx, result in enumerate(results, 2):
        pd = result.product_data

        # Determine enrichment status
        enrichment_status, review_required, comment = _determine_enrichment_status(result)

        # Collect all sources for this product
        sources = set()
        if pd.found_on_onemed:
            sources.add("onemed.no")
        if result.pdf_available:
            sources.add("product datasheet")
        if result.ai_enrichment or result.ai_score:
            sources.add("AI suggestion")
        for er in result.enrichment_results:
            if er.match_status != "NOT_FOUND":
                if er.source_level == "internal_product_sheet":
                    sources.add("product datasheet")
                elif er.source_level == "manufacturer_source":
                    sources.add("manufacturer website")
        if not sources:
            sources.add("existing catalog")

        # Col 1: Artikkelnummer
        ws.cell(row=row_idx, column=1, value=result.article_number).alignment = top_alignment

        # Col 2-3: Produktnavn
        ws.cell(row=row_idx, column=2, value=pd.product_name or "").alignment = top_alignment
        ws.cell(row=row_idx, column=3, value=_get_suggestion_for_field(result, "Produktnavn") or "").alignment = top_alignment

        # Col 4-5: Beskrivelse
        ws.cell(row=row_idx, column=4, value=pd.description or "").alignment = wrap_alignment
        ws.cell(row=row_idx, column=5, value=_get_suggestion_for_field(result, "Beskrivelse") or "").alignment = wrap_alignment

        # Col 6-7: Spesifikasjon
        spec_existing = pd.specification or ""
        if not spec_existing and pd.technical_details:
            spec_existing = "; ".join(f"{k}: {v}" for k, v in pd.technical_details.items())
        ws.cell(row=row_idx, column=6, value=spec_existing).alignment = wrap_alignment

        # Spec suggestion: combine AI missing_specifications with enrichment
        spec_suggestion = _get_suggestion_for_field(result, "Spesifikasjon") or ""
        ai = result.ai_enrichment or {}
        missing_specs = ai.get("missing_specifications", [])
        if missing_specs and not spec_suggestion:
            spec_suggestion = "Manglende: " + ", ".join(missing_specs)
        ws.cell(row=row_idx, column=7, value=spec_suggestion).alignment = wrap_alignment

        # Col 8-9: Kategori
        cat_existing = pd.category or ""
        if not cat_existing and pd.category_breadcrumb:
            cat_existing = " > ".join(pd.category_breadcrumb)
        ws.cell(row=row_idx, column=8, value=cat_existing).alignment = top_alignment
        ws.cell(row=row_idx, column=9, value=_get_suggestion_for_field(result, "Kategori") or "").alignment = top_alignment

        # Col 10-11: Pakningsinformasjon
        pkg_existing = pd.packaging_info or pd.packaging_unit or ""
        ws.cell(row=row_idx, column=10, value=pkg_existing).alignment = top_alignment
        ws.cell(row=row_idx, column=11, value=_get_suggestion_for_field(result, "Pakningsinformasjon") or "").alignment = top_alignment

        # Col 12-13: Produsent
        ws.cell(row=row_idx, column=12, value=pd.manufacturer or "").alignment = top_alignment
        ws.cell(row=row_idx, column=13, value=_get_suggestion_for_field(result, "Produsent") or "").alignment = top_alignment

        # Col 14-15: Produsent artnr
        ws.cell(row=row_idx, column=14, value=pd.manufacturer_article_number or "").alignment = top_alignment
        ws.cell(row=row_idx, column=15, value=_get_suggestion_for_field(result, "Produsentens varenummer") or "").alignment = top_alignment

        # Col 16: Datablad URL
        ws.cell(row=row_idx, column=16, value=result.pdf_url or "").alignment = top_alignment

        # Col 17: Bilde URL
        ws.cell(row=row_idx, column=17, value=pd.image_url or "").alignment = top_alignment

        # Col 18: Quality Score
        score = result.total_score
        # Incorporate AI score if available
        if result.ai_score and "overall_score" in result.ai_score:
            ai_score = result.ai_score["overall_score"]
            score = round(score * 0.4 + ai_score * 0.6, 1)
        ws.cell(row=row_idx, column=18, value=score).alignment = top_alignment

        # Col 19: Enrichment_Status
        status_cell = ws.cell(row=row_idx, column=19, value=enrichment_status)
        status_cell.alignment = top_alignment
        if enrichment_status in INRIVER_STATUS_COLORS:
            status_cell.fill = INRIVER_STATUS_COLORS[enrichment_status]
            status_cell.font = INRIVER_STATUS_FONTS.get(enrichment_status, Font())

        # Col 20: Review_Required
        ws.cell(row=row_idx, column=20, value="Yes" if review_required else "No").alignment = top_alignment

        # Col 21: Import_Approved (always No by default)
        ws.cell(row=row_idx, column=21, value="No").alignment = top_alignment

        # Col 22: Import_Batch
        ws.cell(row=row_idx, column=22, value=batch_id).alignment = top_alignment

        # Col 23: Kommentar
        ws.cell(row=row_idx, column=23, value=comment).alignment = wrap_alignment

        # Col 24: Kilde
        ws.cell(row=row_idx, column=24, value="; ".join(sorted(sources))).alignment = top_alignment

        # Col 25: Sist_oppdatert
        ws.cell(row=row_idx, column=25, value=now_str).alignment = top_alignment

    # Column widths - readable for business users
    col_widths = {
        1: 16,   # Artikkelnummer
        2: 30,   # Produktnavn_eksisterende
        3: 30,   # Produktnavn_forslag
        4: 40,   # Beskrivelse_eksisterende
        5: 40,   # Beskrivelse_forslag
        6: 35,   # Spesifikasjon_eksisterende
        7: 35,   # Spesifikasjon_forslag
        8: 25,   # Kategori_eksisterende
        9: 25,   # Kategori_forslag
        10: 22,  # Pakningsinformasjon_eksisterende
        11: 22,  # Pakningsinformasjon_forslag
        12: 20,  # Produsent_eksisterende
        13: 20,  # Produsent_forslag
        14: 20,  # Produsent_artnr_eksisterende
        15: 20,  # Produsent_artnr_forslag
        16: 30,  # Datablad_URL
        17: 30,  # Bilde_URL
        18: 14,  # Quality_Score
        19: 20,  # Enrichment_Status
        20: 16,  # Review_Required
        21: 16,  # Import_Approved
        22: 20,  # Import_Batch
        23: 40,  # Kommentar
        24: 25,  # Kilde
        25: 18,  # Sist_oppdatert
    }
    for col, width in col_widths.items():
        ws.column_dimensions[get_column_letter(col)].width = width

    # Freeze header row
    ws.freeze_panes = "A2"

    # Enable auto-filter on all columns
    if len(results) > 0:
        ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{len(results) + 1}"
