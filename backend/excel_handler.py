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

    # Sheet 7: Summary Statistics
    ws_stats = wb.create_sheet("Statistikk")
    _create_stats_sheet(ws_stats, results)

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
        iq = result.image_quality or {}
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
        ws.cell(row=row_idx, column=12, value="Ja" if result.auto_fix_possible else "Nei")
        ws.cell(row=row_idx, column=13, value="Ja" if result.manual_review_needed else "Nei")
        ws.cell(row=row_idx, column=14, value="Ja" if result.requires_manufacturer_contact else "Nei")
        ws.cell(row=row_idx, column=15, value=pd.product_url or "")

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
