"""Excel import/export handler for masterdata quality check."""

import logging
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from backend.models import EnrichmentSuggestion, JeevesData, ProductAnalysis, QualityStatus

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

    # Sheet 1: Summary (high-level metrics)
    ws_summary = wb.active
    ws_summary.title = "Summary"
    _create_summary_sheet(ws_summary, results)

    # Sheet 2: Comparison_And_Enrichment (main review sheet)
    ws_comparison = wb.create_sheet("Comparison_And_Enrichment")
    _create_comparison_and_enrichment_sheet(ws_comparison, results)

    # Sheet 3: Debug_Log (traceability)
    ws_debug = wb.create_sheet("Debug_Log")
    _create_debug_log_sheet(ws_debug, results)

    # Sheet 4: Overview (one row per product, legacy)
    ws_overview = wb.create_sheet("Oversikt")
    _create_overview_sheet(ws_overview, results)

    # Sheet 5: Field Analysis Detail
    ws_detail = wb.create_sheet("Feltanalyse")
    _create_detail_sheet(ws_detail, results)

    # Sheet 6: Improvement Suggestions
    ws_improvements = wb.create_sheet("Forbedringsforslag")
    _create_improvements_sheet(ws_improvements, results)

    # Sheet 7: Manufacturer Follow-up
    ws_manufacturer = wb.create_sheet("Produsentoppf\u00f8lging")
    _create_manufacturer_sheet(ws_manufacturer, results)

    # Sheet 8: Image Details
    ws_images = wb.create_sheet("Bildeanalyse")
    _create_image_detail_sheet(ws_images, results)

    # Sheet 9: Image Issues Priority
    ws_img_issues = wb.create_sheet("Bildeproblemer")
    _create_image_issues_sheet(ws_img_issues, results)

    # Sheet 10: Source Conflicts
    ws_conflicts = wb.create_sheet("Kildekonflikter")
    _create_conflicts_sheet(ws_conflicts, results)

    # Sheet 11: Inriver Import staging
    ws_inriver = wb.create_sheet("Inriver Import")
    _create_inriver_import_sheet(ws_inriver, results)

    # Sheet 12: Product Families / Variant Structure
    ws_families = wb.create_sheet("Produktfamilier")
    _create_family_sheet(ws_families, results)

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
    """Create improvements suggestion sheet.

    Combines field analysis suggestions with enrichment engine suggestions.
    Enrichment suggestions include source evidence and review-required flag.
    """
    headers = [
        "Artikkelnummer",
        "Produktnavn",
        "Felt",
        "Nåværende verdi",
        "Foreslått verdi",
        "Kilde",
        "Confidence",
        "Evidens / Begrunnelse",
        "Krever gjennomgang",
    ]

    for col, header in enumerate(headers, 1):
        ws.cell(row=1, column=col, value=header)
    _style_header(ws, 1, len(headers))

    row_idx = 2
    for result in results:
        # Track which fields already have enrichment suggestions (to avoid duplicates)
        enriched_fields = set()

        # First: enrichment suggestions (higher priority, with evidence)
        for es in result.enrichment_suggestions:
            if es.suggested_value:
                ws.cell(row=row_idx, column=1, value=result.article_number)
                ws.cell(row=row_idx, column=2, value=result.product_data.product_name or "")
                ws.cell(row=row_idx, column=3, value=es.field_name)
                ws.cell(row=row_idx, column=4, value=es.current_value or "")
                ws.cell(row=row_idx, column=5, value=es.suggested_value)
                source_display = es.source or ""
                if es.source_url:
                    source_display = f"{source_display} ({es.source_url})"
                ws.cell(row=row_idx, column=6, value=source_display)
                ws.cell(row=row_idx, column=7, value=es.confidence if es.confidence else "")
                ws.cell(row=row_idx, column=8, value=es.evidence or "")
                ws.cell(row=row_idx, column=9, value="Ja" if es.review_required else "Nei")
                enriched_fields.add(es.field_name)
                row_idx += 1

        # Then: field analysis suggestions (for fields not covered by enricher)
        for fa in result.field_analyses:
            if fa.suggested_value and fa.field_name not in enriched_fields:
                ws.cell(row=row_idx, column=1, value=result.article_number)
                ws.cell(row=row_idx, column=2, value=result.product_data.product_name or "")
                ws.cell(row=row_idx, column=3, value=fa.field_name)
                ws.cell(row=row_idx, column=4, value=fa.current_value or "")
                ws.cell(row=row_idx, column=5, value=fa.suggested_value)
                ws.cell(row=row_idx, column=6, value=fa.source or "")
                ws.cell(row=row_idx, column=7, value=fa.confidence if fa.confidence else "")
                ws.cell(row=row_idx, column=8, value=fa.comment or "")
                ws.cell(row=row_idx, column=9, value="Ja")
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


def _create_comparison_and_enrichment_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create the main comparison + enrichment sheet.

    One row per product. Shows Jeeves source values, website source values,
    field statuses, enrichment suggestions, and traceability columns.
    Every product is included — not just those with suggestions.
    """
    headers = [
        # IDENTIFIERS (1-2)
        "Artikkelnummer",
        "GID",
        # JEEVES SOURCE VALUES (3-9)
        "Jeeves_Item_description",
        "Jeeves_Specification",
        "Jeeves_Supplier",
        "Jeeves_Supplier_Item_no",
        "Jeeves_Product_Brand",
        "Jeeves_Web_Title",
        "Jeeves_Web_Text",
        # WEBSITE SOURCE VALUES (10-17)
        "Website_URL",
        "Website_Title",
        "Website_Breadcrumb",
        "Website_Description",
        "Website_Specification",
        "Website_Packaging",
        "Website_Image_Present",
        "Website_Datasheet_URL",
        # STATUS COLUMNS (18-26)
        "Status_Produktnavn",
        "Status_Beskrivelse",
        "Status_Spesifikasjon",
        "Status_Produsent",
        "Status_Produsent_Artnr",
        "Status_Merkevare",
        "Status_Kategori",
        "Status_Pakningsinformasjon",
        "Status_Bilde",
        # SUGGESTION COLUMNS (27-34)
        "Forslag_Produktnavn",
        "Forslag_Beskrivelse",
        "Forslag_Spesifikasjon",
        "Forslag_Produsent",
        "Forslag_Produsent_Artnr",
        "Forslag_Merkevare",
        "Forslag_Kategori",
        "Forslag_Pakningsinformasjon",
        # TRACEABILITY (35-38)
        "Kilde_for_forslag",
        "Confidence",
        "Review_Required",
        "Kommentar",
    ]

    for col, header in enumerate(headers, 1):
        ws.cell(row=1, column=col, value=header)

    # Section-based header colors
    section_colors = {
        range(1, 3): "4472C4",     # Identifiers: blue
        range(3, 10): "548235",    # Jeeves: green
        range(10, 18): "BF8F00",   # Website: gold
        range(18, 27): "7030A0",   # Status: purple
        range(27, 35): "C55A11",   # Suggestions: orange
        range(35, 39): "404040",   # Traceability: dark gray
    }
    header_font = Font(bold=True, color="FFFFFF", size=10)
    thin_border = Border(
        left=Side(style="thin"), right=Side(style="thin"),
        top=Side(style="thin"), bottom=Side(style="thin"),
    )
    for col_range, color in section_colors.items():
        fill = PatternFill(start_color=color, end_color=color, fill_type="solid")
        for col in col_range:
            cell = ws.cell(row=1, column=col)
            cell.fill = fill
            cell.font = header_font
            cell.border = thin_border
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    # Fills for suggestion / status cells
    suggestion_fill = PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid")
    review_fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")
    wrap_align = Alignment(wrap_text=True, vertical="top")
    top_align = Alignment(vertical="top")

    row_idx = 2
    for result in results:
        product = result.product_data
        jeeves = result.jeeves_data

        # Build field status lookup
        fa_by_name = {fa.field_name: fa for fa in result.field_analyses}

        # Build suggestion lookup from enrichment_suggestions + field_analyses
        sugg_by_field = {}
        for es in result.enrichment_suggestions:
            sugg_by_field[es.field_name] = es
        for fa in result.field_analyses:
            if fa.suggested_value and fa.field_name not in sugg_by_field:
                sugg_by_field[fa.field_name] = EnrichmentSuggestion(
                    field_name=fa.field_name,
                    current_value=fa.current_value,
                    suggested_value=fa.suggested_value,
                    source=fa.source,
                    confidence=fa.confidence or 0.0,
                    review_required=True,
                )

        # Website-derived values
        web_spec = product.specification or ""
        if not web_spec and product.technical_details:
            web_spec = "; ".join(f"{k}: {v}" for k, v in product.technical_details.items())
        web_breadcrumb = " > ".join(product.category_breadcrumb) if product.category_breadcrumb else ""
        web_packaging = product.packaging_info or product.packaging_unit or ""
        web_image_present = "Ja" if product.image_url else "Nei"

        def _status_val(field_name):
            fa = fa_by_name.get(field_name)
            if not fa:
                return ""
            return fa.status.value

        def _sugg_val(field_name):
            es = sugg_by_field.get(field_name)
            return es.suggested_value if es else ""

        # Build comment and sources for traceability
        sources = set()
        comments = []
        min_confidence = 1.0
        any_review = False

        for es in sugg_by_field.values():
            if es.source:
                sources.add(es.source)
            if es.confidence is not None:
                min_confidence = min(min_confidence, es.confidence)
            if es.review_required:
                any_review = True

        # Generate per-field comments
        field_comment_parts = []
        for fa in result.field_analyses:
            if fa.field_name in ("Konsistens mellom felter",):
                continue
            src = fa.source or ""
            if fa.status == QualityStatus.OK and "Jeeves kun" in src:
                field_comment_parts.append(f"{fa.field_name}: present in Jeeves only")
            elif fa.status == QualityStatus.OK and "nettside kun" in src:
                field_comment_parts.append(f"{fa.field_name}: present on website only")
            elif fa.status == QualityStatus.MISSING:
                field_comment_parts.append(f"{fa.field_name}: missing in both sources")
            elif fa.status == QualityStatus.SHOULD_IMPROVE:
                field_comment_parts.append(f"{fa.field_name}: {fa.comment}")

        # Check for Jeeves vs website conflicts
        if jeeves and product.product_name and jeeves.item_description:
            if product.product_name.lower().strip() != jeeves.item_description.lower().strip():
                if jeeves.web_title and product.product_name.lower().strip() != jeeves.web_title.lower().strip():
                    field_comment_parts.append(
                        "Produktnavn: website and Jeeves differ"
                    )
                    any_review = True

        comment_text = "; ".join(field_comment_parts) if field_comment_parts else ""

        # --- Write row ---
        c = 1
        # IDENTIFIERS
        ws.cell(row=row_idx, column=c, value=result.article_number).alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=jeeves.gid if jeeves else "").alignment = top_align; c += 1
        # JEEVES SOURCE VALUES
        ws.cell(row=row_idx, column=c, value=jeeves.item_description if jeeves else "").alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=jeeves.specification if jeeves else "").alignment = wrap_align; c += 1
        ws.cell(row=row_idx, column=c, value=jeeves.supplier if jeeves else "").alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=jeeves.supplier_item_no if jeeves else "").alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=jeeves.product_brand if jeeves else "").alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=jeeves.web_title if jeeves else "").alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=jeeves.web_text if jeeves else "").alignment = wrap_align; c += 1
        # WEBSITE SOURCE VALUES
        ws.cell(row=row_idx, column=c, value=product.product_url or "").alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=product.product_name or "").alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=web_breadcrumb).alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=product.description or "").alignment = wrap_align; c += 1
        ws.cell(row=row_idx, column=c, value=web_spec).alignment = wrap_align; c += 1
        ws.cell(row=row_idx, column=c, value=web_packaging).alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=web_image_present).alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=result.pdf_url or "").alignment = top_align; c += 1
        # STATUS COLUMNS
        status_fields = [
            "Produktnavn", "Beskrivelse", "Spesifikasjon", "Produsent",
            "Produsentens varenummer", "Merkevare", "Kategori",
            "Pakningsinformasjon", "Bildekvalitet",
        ]
        for sf in status_fields:
            status_text = _status_val(sf)
            cell = ws.cell(row=row_idx, column=c, value=status_text)
            cell.alignment = top_align
            fa = fa_by_name.get(sf)
            if fa:
                _apply_status_style(cell, fa.status)
            c += 1
        # SUGGESTION COLUMNS
        suggestion_fields = [
            "Produktnavn", "Beskrivelse", "Spesifikasjon", "Produsent",
            "Produsentens varenummer", "Merkevare", "Kategori", "Pakningsinformasjon",
        ]
        for sf in suggestion_fields:
            val = _sugg_val(sf) or ""
            cell = ws.cell(row=row_idx, column=c, value=val)
            cell.alignment = wrap_align
            if val:
                cell.fill = review_fill if any_review else suggestion_fill
            c += 1
        # TRACEABILITY
        ws.cell(row=row_idx, column=c, value="; ".join(sorted(sources)) if sources else "").alignment = top_align; c += 1
        conf_val = round(min_confidence, 2) if sugg_by_field else ""
        ws.cell(row=row_idx, column=c, value=conf_val).alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value="Ja" if any_review else "Nei").alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=comment_text).alignment = wrap_align; c += 1

        # Verify column count matches header count
        assert c == len(headers) + 1, (
            f"Column count mismatch in Comparison_And_Enrichment: "
            f"wrote {c - 1} columns, expected {len(headers)}"
        )

        row_idx += 1

    # Column widths
    col_widths = {
        1: 16, 2: 12,                                    # identifiers
        3: 25, 4: 20, 5: 22, 6: 18, 7: 16, 8: 25, 9: 35,  # Jeeves
        10: 35, 11: 25, 12: 30, 13: 35, 14: 35, 15: 25, 16: 14, 17: 35,  # website
        18: 14, 19: 14, 20: 14, 21: 14, 22: 14, 23: 14, 24: 14, 25: 14, 26: 14,  # status
        27: 25, 28: 35, 29: 30, 30: 20, 31: 18, 32: 16, 33: 20, 34: 25,  # suggestions
        35: 25, 36: 12, 37: 14, 38: 50,  # traceability
    }
    for col, width in col_widths.items():
        ws.column_dimensions[get_column_letter(col)].width = width

    ws.freeze_panes = "C2"
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


def _create_summary_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create the Summary sheet with high-level two-source metrics."""
    title_font = Font(bold=True, size=14)
    section_font = Font(bold=True, size=12, color="4472C4")
    label_font = Font(bold=True, size=11)
    value_font = Font(size=11)
    indent_font = Font(size=11, color="404040")

    ws.cell(row=1, column=1, value="Masterdata Kvalitetsrapport").font = title_font
    ws.cell(row=2, column=1, value=f"Generert: {datetime.now().strftime('%Y-%m-%d %H:%M')}").font = indent_font

    total = len(results)
    found = sum(1 for r in results if r.product_data.found_on_onemed)
    not_found = total - found
    has_jeeves = sum(1 for r in results if r.jeeves_data)
    avg_score = sum(r.total_score for r in results) / total if total else 0

    # Per-field: count how many have website data
    web_desc = sum(1 for r in results if r.product_data.description)
    web_spec = sum(1 for r in results if r.product_data.specification or r.product_data.technical_details)
    web_pkg = sum(1 for r in results if r.product_data.packaging_info or r.product_data.packaging_unit)
    web_img = sum(1 for r in results if r.product_data.image_url)
    web_cat = sum(1 for r in results if r.product_data.category or r.product_data.category_breadcrumb)

    # Suggestions
    products_with_suggestions = sum(1 for r in results if r.enrichment_suggestions)
    total_suggestions = sum(len(r.enrichment_suggestions) for r in results)
    manual_review = sum(
        1 for r in results
        if r.manual_review_needed or any(es.review_required for es in r.enrichment_suggestions)
    )

    # Status distribution
    status_counts = {}
    for r in results:
        status = r.overall_status.value
        status_counts[status] = status_counts.get(status, 0) + 1

    # Per-field status distribution
    field_status_counts: dict[str, dict[str, int]] = {}
    for r in results:
        for fa in r.field_analyses:
            if fa.field_name == "Konsistens mellom felter":
                continue
            if fa.field_name not in field_status_counts:
                field_status_counts[fa.field_name] = {}
            s = fa.status.value
            field_status_counts[fa.field_name][s] = field_status_counts[fa.field_name].get(s, 0) + 1

    # Build summary rows as (label, value, indent_level)
    rows: list[tuple[str, str | int | float, int]] = [
        # General
        ("GENERELT", "", 0),
        ("Totalt antall produkter", total, 1),
        ("Produkter i Jeeves", has_jeeves, 1),
        ("Gjennomsnittlig kvalitetsscore", f"{avg_score:.1f}%", 1),
        ("", "", 0),
        # Website coverage
        ("NETTSIDE-DEKNING", "", 0),
        ("Funnet på onemed.no", found, 1),
        ("Ikke funnet på onemed.no", not_found, 1),
        ("Med nettside-beskrivelse", web_desc, 1),
        ("Med nettside-spesifikasjon", web_spec, 1),
        ("Med pakningsinformasjon", web_pkg, 1),
        ("Med bilde", web_img, 1),
        ("Med kategori/breadcrumb", web_cat, 1),
        ("", "", 0),
        # Enrichment
        ("BERIKELSE OG FORSLAG", "", 0),
        ("Produkter med forslag", products_with_suggestions, 1),
        ("Totalt antall forslag", total_suggestions, 1),
        ("Krever manuell gjennomgang", manual_review, 1),
        ("", "", 0),
        # Status distribution
        ("STATUSFORDELING (OVERORDNET)", "", 0),
    ]
    for status_name, count in sorted(status_counts.items()):
        rows.append((status_name, count, 1))

    rows.append(("", "", 0))
    rows.append(("STATUSFORDELING PER FELT", "", 0))
    for field_name in [
        "Produktnavn", "Beskrivelse", "Spesifikasjon", "Produsent",
        "Produsentens varenummer", "Merkevare", "Kategori",
        "Pakningsinformasjon", "Bildekvalitet",
    ]:
        counts = field_status_counts.get(field_name, {})
        if not counts:
            continue
        ok = counts.get("OK", 0)
        missing = counts.get("Mangler", 0)
        improve = counts.get("Bør forbedres", 0)
        error = counts.get("Sannsynlig feil", 0)
        rows.append((field_name, f"OK: {ok} | Mangler: {missing} | Forbedres: {improve} | Feil: {error}", 1))

    # Write rows
    row_num = 4
    for label, value, indent in rows:
        if not label and not value:
            row_num += 1
            continue
        cell_label = ws.cell(row=row_num, column=1, value=label)
        cell_value = ws.cell(row=row_num, column=2, value=value)
        if indent == 0:
            cell_label.font = section_font
        elif indent == 1:
            cell_label.font = label_font
            cell_value.font = value_font
        row_num += 1

    ws.column_dimensions["A"].width = 40
    ws.column_dimensions["B"].width = 55


def _create_debug_log_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create the Debug_Log sheet for traceability.

    One row per product. Shows raw extraction details so a reviewer can
    understand why the product got its status and suggestions.
    """
    headers = [
        "Artikkelnummer",
        "Website_URL",
        "Product_Found_On_Website",
        "Accordion_Expanded",
        "Extracted_Title_Raw",
        "Extracted_Breadcrumb_Raw",
        "Extracted_Description_Raw",
        "Extracted_Specification_Raw",
        "Extracted_Packaging_Raw",
        "Image_Found",
        "Datasheet_URL_Found",
        "Jeeves_Fields_Used",
        "Website_Fields_Used",
        "Suggestion_Source",
        "Review_Required",
        "Quality_Score",
        "Debug_Comment",
    ]

    for col, header in enumerate(headers, 1):
        ws.cell(row=1, column=col, value=header)
    _style_header(ws, 1, len(headers))

    wrap_align = Alignment(wrap_text=True, vertical="top")
    top_align = Alignment(vertical="top")

    for row_idx, result in enumerate(results, 2):
        product = result.product_data
        jeeves = result.jeeves_data

        # Determine which Jeeves fields are populated
        jeeves_fields_used = []
        if jeeves:
            if jeeves.item_description:
                jeeves_fields_used.append("Item description")
            if jeeves.specification:
                jeeves_fields_used.append("Specification")
            if jeeves.supplier:
                jeeves_fields_used.append("Supplier")
            if jeeves.supplier_item_no:
                jeeves_fields_used.append("Supplier Item.no")
            if jeeves.product_brand:
                jeeves_fields_used.append("Product Brand")
            if jeeves.web_title:
                jeeves_fields_used.append("Web Title")
            if jeeves.web_text:
                jeeves_fields_used.append("Web Text")

        # Determine which website fields are populated
        website_fields_used = []
        if product.product_name:
            website_fields_used.append("Title")
        if product.description:
            website_fields_used.append("Description")
        if product.specification or product.technical_details:
            website_fields_used.append("Specification")
        if product.category or product.category_breadcrumb:
            website_fields_used.append("Category")
        if product.packaging_info or product.packaging_unit:
            website_fields_used.append("Packaging")
        if product.image_url:
            website_fields_used.append("Image")
        if product.manufacturer:
            website_fields_used.append("Manufacturer")

        # Accordion: website description from accordion selector
        accordion_status = "N/A"
        if product.found_on_onemed:
            if product.description and len(product.description) > 10:
                accordion_status = "Yes"
            else:
                accordion_status = "No content found"

        # Build suggestion source summary
        suggestion_sources = set()
        any_review = False
        for es in result.enrichment_suggestions:
            if es.source:
                suggestion_sources.add(es.source)
            if es.review_required:
                any_review = True
        for fa in result.field_analyses:
            if fa.source and fa.suggested_value:
                suggestion_sources.add(fa.source)

        # Debug comment: explain notable findings
        debug_parts = []
        if not product.found_on_onemed:
            debug_parts.append("Not found on website")
        if not jeeves:
            debug_parts.append("Not in Jeeves")

        # Note fields where Jeeves fills gaps
        for fa in result.field_analyses:
            if fa.source and "Jeeves kun" in fa.source:
                debug_parts.append(f"{fa.field_name} from Jeeves only")
            elif fa.status == QualityStatus.MISSING:
                debug_parts.append(f"{fa.field_name} missing in both")

        # Note weak fields
        for fa in result.field_analyses:
            if fa.status == QualityStatus.SHOULD_IMPROVE:
                debug_parts.append(f"{fa.field_name}: {fa.comment}")

        # Raw website extraction values
        raw_spec = product.specification or ""
        if not raw_spec and product.technical_details:
            raw_spec = "; ".join(f"{k}: {v}" for k, v in product.technical_details.items())
        raw_breadcrumb = " > ".join(product.category_breadcrumb) if product.category_breadcrumb else ""
        raw_packaging = product.packaging_info or product.packaging_unit or ""

        c = 1
        ws.cell(row=row_idx, column=c, value=result.article_number).alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=product.product_url or "").alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value="Ja" if product.found_on_onemed else "Nei").alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=accordion_status).alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=product.product_name or "").alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=raw_breadcrumb).alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=product.description or "").alignment = wrap_align; c += 1
        ws.cell(row=row_idx, column=c, value=raw_spec).alignment = wrap_align; c += 1
        ws.cell(row=row_idx, column=c, value=raw_packaging).alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value="Ja" if product.image_url else "Nei").alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=result.pdf_url or "").alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=", ".join(jeeves_fields_used) if jeeves_fields_used else "None").alignment = wrap_align; c += 1
        ws.cell(row=row_idx, column=c, value=", ".join(website_fields_used) if website_fields_used else "None").alignment = wrap_align; c += 1
        ws.cell(row=row_idx, column=c, value=", ".join(sorted(suggestion_sources)) if suggestion_sources else "None").alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value="Ja" if any_review else "Nei").alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value=result.total_score).alignment = top_align; c += 1
        ws.cell(row=row_idx, column=c, value="; ".join(debug_parts) if debug_parts else "No issues").alignment = wrap_align; c += 1

    # Column widths
    col_widths = {
        1: 16, 2: 35, 3: 14, 4: 14, 5: 25, 6: 30, 7: 35,
        8: 35, 9: 25, 10: 12, 11: 35, 12: 30, 13: 30,
        14: 25, 15: 14, 16: 12, 17: 55,
    }
    for col, width in col_widths.items():
        ws.column_dimensions[get_column_letter(col)].width = width

    ws.freeze_panes = "B2"
    if len(results) > 0:
        ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{len(results) + 1}"


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
    """Get the best suggestion for a field from all enrichment sources.

    Priority order (source-grounded first, AI as fallback):
    1. Enrichment suggestions (AI-reviewed, source-grounded, quality-gated)
    2. Field analysis suggestions (from PDF/manufacturer enrichment)
    3. Enrichment results (raw source data)
    4. AI enrichment (independent AI suggestions — used only as last resort)

    AI enrichment is demoted to Priority 4 because it operates on raw product
    data without source evidence, making it less reliable than source-grounded
    suggestions that have been through the quality gate.
    """
    # Priority 1: Enrichment suggestions (AI-reviewed, quality-gated)
    for es in result.enrichment_suggestions:
        if es.field_name == field_name and es.suggested_value:
            return es.suggested_value

    # Priority 2: Field analysis suggestions (from PDF/manufacturer enrichment)
    for fa in result.field_analyses:
        if fa.field_name == field_name and fa.suggested_value:
            return fa.suggested_value

    # Priority 3: Enrichment results (raw source extractions)
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
                # Only use if confidence is sufficient
                if er.confidence >= 0.5:
                    return er.suggested_value

    # Priority 4 (LAST RESORT): AI enrichment — independent AI suggestions
    # These are not source-grounded so require more caution
    ai = result.ai_enrichment or {}
    ai_map = {
        "Beskrivelse": ai.get("improved_description"),
        "Kategori": ai.get("suggested_category"),
        "Pakningsinformasjon": ai.get("packaging_suggestions"),
    }
    ai_val = ai_map.get(field_name)
    if ai_val and isinstance(ai_val, str) and len(ai_val.strip()) > 5:
        return ai_val

    return None


def _get_suggestion_source(result: ProductAnalysis, field_name: str) -> str:
    """Determine the source of the suggestion for a field.

    Matches the priority order in _get_suggestion_for_field().
    Returns the source of whichever suggestion would actually be used.
    """
    # Priority 1: Enrichment suggestions (AI-reviewed, quality-gated)
    for es in result.enrichment_suggestions:
        if es.field_name == field_name and es.suggested_value:
            return es.source or "enrichment"

    # Priority 2: Field analysis suggestions
    for fa in result.field_analyses:
        if fa.field_name == field_name and fa.suggested_value and fa.source:
            return fa.source

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
                if er.confidence >= 0.5:
                    if er.source_level == "internal_product_sheet":
                        return "product datasheet"
                    elif er.source_level == "manufacturer_source":
                        return "manufacturer website"

    # Priority 4: AI enrichment
    ai = result.ai_enrichment or {}
    ai_map = {
        "Beskrivelse": ai.get("improved_description"),
        "Kategori": ai.get("suggested_category"),
        "Pakningsinformasjon": ai.get("packaging_suggestions"),
    }
    if field_name in ai_map and ai_map[field_name]:
        return "AI suggestion (verifiser manuelt)"

    if result.product_data.found_on_onemed:
        return "onemed.no"
    return "existing catalog"


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


def _create_family_sheet(ws, results: list[ProductAnalysis]) -> None:
    """Create Sheet 12: Product Family / Variant Structure.

    Runs family detection on the analyzed products and outputs a reviewable
    table with Mother/Child/Standalone assignments and variant dimensions.
    """
    from backend.family_detector import detect_families, products_from_analyses

    # Build product dicts from analysis results
    product_dicts = products_from_analyses(results)

    # Run family detection
    families, all_members = detect_families(product_dicts)

    # Sort: families first (by family_id, then role), standalone last
    def sort_key(m):
        if m.family_id:
            role_order = 0 if m.role == "mother" else 1
            return (0, m.family_id, role_order, m.article_number)
        return (1, "", 0, m.article_number)

    sorted_members = sorted(all_members, key=sort_key)

    # Headers
    headers = [
        "Artikkelnummer",
        "Produktnavn",
        "Familie_ID",
        "Familienavn",
        "Rolle",
        "Mor_Artikkelnummer",
        "Familiestr.",
        "Variantdimensjon_1",
        "Variantverdi_1",
        "Variantdimensjon_2",
        "Variantverdi_2",
        "Variantdimensjon_3",
        "Variantverdi_3",
        "Felles_basetittel",
        "Barnespesifikt",
        "Konfidensgrad",
        "Gjennomgang_påkrevd",
        "Grupperingsgrunn",
        "Søsken",
        "Kilde_signaler",
        "Merknader",
    ]
    top_align = Alignment(vertical="top", wrap_text=False)
    wrap_align = Alignment(vertical="top", wrap_text=True)

    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF", size=10)
    family_fill = PatternFill(start_color="D9E2F3", end_color="D9E2F3", fill_type="solid")
    mother_fill = PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid")
    review_fill = PatternFill(start_color="FCE4D6", end_color="FCE4D6", fill_type="solid")

    for col_idx, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = top_align

    row_idx = 2
    prev_family_id = None

    for member in sorted_members:
        c = 1
        # Alternate family background for readability
        is_new_family = member.family_id and member.family_id != prev_family_id
        prev_family_id = member.family_id

        # Get up to 3 variant dimensions
        dims = member.variant_dimensions[:3] if member.variant_dimensions else []

        row_data = [
            member.article_number,
            member.product_name,
            member.family_id or "",
            member.family_name or "",
            {"mother": "Mor", "child": "Barn", "standalone": "Frittstående"}.get(member.role, member.role),
            member.mother_article_number or "(abstrakt)",
            member.family_size if member.family_id else "",
            dims[0].dimension_name if len(dims) > 0 else "",
            dims[0].value if len(dims) > 0 else "",
            dims[1].dimension_name if len(dims) > 1 else "",
            dims[1].value if len(dims) > 1 else "",
            dims[2].dimension_name if len(dims) > 2 else "",
            dims[2].value if len(dims) > 2 else "",
            member.shared_base_title or "",
            member.child_specific_title or "",
            member.confidence,
            "Ja" if member.review_required else "Nei",
            member.grouping_reason,
            "; ".join(member.candidate_siblings[:5]) if member.candidate_siblings else "",
            "; ".join(member.source_signals) if member.source_signals else "",
            member.notes,
        ]

        for val in row_data:
            cell = ws.cell(row=row_idx, column=c, value=val)
            cell.alignment = wrap_align if c in (2, 14, 18, 19, 20, 21) else top_align
            # Apply role-based styling
            if member.role == "mother":
                cell.fill = mother_fill
            elif member.family_id and member.review_required:
                cell.fill = review_fill
            elif member.family_id:
                cell.fill = family_fill
            c += 1

        row_idx += 1

    # Column widths
    col_widths = {
        1: 18, 2: 35, 3: 12, 4: 30, 5: 14, 6: 18, 7: 10,
        8: 18, 9: 14, 10: 18, 11: 14, 12: 18, 13: 14,
        14: 30, 15: 25, 16: 14, 17: 14, 18: 40, 19: 35, 20: 40, 21: 30,
    }
    for col, width in col_widths.items():
        ws.column_dimensions[get_column_letter(col)].width = width

    ws.freeze_panes = "C2"
    if row_idx > 2:
        ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{row_idx - 1}"

    logger.info(
        f"Family sheet: {len(families)} families, "
        f"{sum(1 for m in sorted_members if m.role != 'standalone')} in families, "
        f"{sum(1 for m in sorted_members if m.role == 'standalone')} standalone"
    )
