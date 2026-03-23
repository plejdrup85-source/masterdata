"""Tests for run comparison and snapshot logic."""

import json
import pytest
from pathlib import Path

from backend.models import (
    FieldAnalysis,
    ProductAnalysis,
    ProductData,
    QualityStatus,
)
from backend.run_comparison import (
    save_snapshot,
    load_snapshot,
    list_snapshots,
    delete_snapshot,
    compare_runs,
    _get_snapshot_dir,
)


def _make(
    artno="12345",
    name="Test produkt",
    score=70.0,
    webshop="Klar",
    priority="Lav",
    field_statuses=None,
    suggestions=None,
) -> ProductAnalysis:
    pd = ProductData(
        article_number=artno,
        product_name=name,
        found_on_onemed=True,
    )
    fas = []
    if field_statuses:
        for fname, status in field_statuses.items():
            fas.append(FieldAnalysis(field_name=fname, status=status))
    else:
        fas = [
            FieldAnalysis(field_name="Produktnavn", status=QualityStatus.OK),
            FieldAnalysis(field_name="Beskrivelse", status=QualityStatus.OK),
        ]
    return ProductAnalysis(
        article_number=artno,
        product_data=pd,
        total_score=score,
        field_analyses=fas,
        webshop_status=webshop,
        priority_label=priority,
        enrichment_suggestions=suggestions or [],
    )


@pytest.fixture(autouse=True)
def temp_snapshot_dir(tmp_path, monkeypatch):
    """Use temp dir for snapshots in all tests."""
    snap_dir = tmp_path / "history" / "snapshots"
    snap_dir.mkdir(parents=True)
    monkeypatch.setenv("HISTORY_DIR", str(tmp_path / "history"))
    return snap_dir


class TestSnapshotSaveLoad:
    def test_save_and_load(self):
        results = [_make(artno="A1"), _make(artno="A2")]
        save_snapshot("job1", results, timestamp="2026-01-01T12:00:00")
        loaded = load_snapshot("job1")
        assert loaded is not None
        assert loaded["job_id"] == "job1"
        assert loaded["product_count"] == 2
        assert "A1" in loaded["products"]
        assert "A2" in loaded["products"]

    def test_snapshot_content(self):
        results = [_make(artno="X1", score=75.0, webshop="Delvis klar")]
        save_snapshot("job2", results)
        loaded = load_snapshot("job2")
        p = loaded["products"]["X1"]
        assert p["total_score"] == 75.0
        assert p["webshop_status"] == "Delvis klar"
        assert "Produktnavn" in p["field_statuses"]

    def test_load_nonexistent(self):
        assert load_snapshot("nonexistent") is None

    def test_list_snapshots(self):
        save_snapshot("j1", [_make()])
        save_snapshot("j2", [_make(), _make(artno="B")])
        snaps = list_snapshots()
        assert len(snaps) == 2
        job_ids = {s["job_id"] for s in snaps}
        assert "j1" in job_ids
        assert "j2" in job_ids

    def test_delete_snapshot(self):
        save_snapshot("j1", [_make()])
        assert load_snapshot("j1") is not None
        assert delete_snapshot("j1") is True
        assert load_snapshot("j1") is None
        assert delete_snapshot("j1") is False


class TestCompareRuns:
    def _make_snapshot(self, products_data):
        """Build a snapshot dict directly."""
        products = {}
        for artno, score, webshop, fields in products_data:
            field_statuses = {}
            for fname, status in (fields or {}).items():
                field_statuses[fname] = {"status": status, "has_suggestion": False}
            products[artno] = {
                "product_name": f"Product {artno}",
                "total_score": score,
                "overall_status": "OK",
                "webshop_status": webshop,
                "priority_label": "Lav",
                "priority_score": 20,
                "field_statuses": field_statuses,
                "suggestion_count": 0,
                "auto_fix_possible": False,
                "manual_review_needed": False,
            }
        return {
            "job_id": "prev",
            "timestamp": "2026-01-01T12:00:00",
            "product_count": len(products),
            "products": products,
        }

    def test_no_changes(self):
        prev = self._make_snapshot([("A1", 70.0, "Klar", {"Produktnavn": "OK"})])
        current = [_make(artno="A1", score=70.0, webshop="Klar",
                         field_statuses={"Produktnavn": QualityStatus.OK})]
        comp = compare_runs(current, prev)
        assert comp.improved_count == 0
        assert comp.regressed_count == 0
        assert comp.unchanged_count == 1
        assert len(comp.deltas) == 0  # No changes = no delta entry

    def test_score_improved(self):
        prev = self._make_snapshot([("A1", 50.0, "Ikke klar", {})])
        current = [_make(artno="A1", score=80.0, webshop="Klar")]
        comp = compare_runs(current, prev)
        assert comp.improved_count == 1
        assert comp.avg_score_change == 30.0

    def test_score_regressed(self):
        prev = self._make_snapshot([("A1", 80.0, "Klar", {})])
        current = [_make(artno="A1", score=40.0, webshop="Ikke klar")]
        comp = compare_runs(current, prev)
        assert comp.regressed_count == 1
        assert any(d.score_change < 0 for d in comp.deltas)

    def test_new_product(self):
        prev = self._make_snapshot([("A1", 70.0, "Klar", {})])
        current = [_make(artno="A1", score=70.0), _make(artno="A2", score=50.0)]
        comp = compare_runs(current, prev)
        assert comp.new_products == 1
        new_deltas = [d for d in comp.deltas if d.is_new]
        assert len(new_deltas) == 1
        assert new_deltas[0].article_number == "A2"

    def test_removed_product(self):
        prev = self._make_snapshot([("A1", 70.0, "Klar", {}), ("A2", 50.0, "Ikke klar", {})])
        current = [_make(artno="A1", score=70.0)]
        comp = compare_runs(current, prev)
        assert comp.removed_products == 1
        removed = [d for d in comp.deltas if d.is_removed]
        assert len(removed) == 1
        assert removed[0].article_number == "A2"

    def test_webshop_status_change(self):
        prev = self._make_snapshot([("A1", 70.0, "Ikke klar", {})])
        current = [_make(artno="A1", score=70.0, webshop="Klar")]
        comp = compare_runs(current, prev)
        assert comp.webshop_ready_after == 1
        assert comp.webshop_ready_before == 0
        ws_deltas = [d for d in comp.deltas if d.webshop_changed]
        assert len(ws_deltas) == 1

    def test_field_improved(self):
        prev = self._make_snapshot([
            ("A1", 50.0, "Ikke klar", {"Beskrivelse": "Mangler", "Produktnavn": "OK"})
        ])
        current = [_make(artno="A1", score=70.0, field_statuses={
            "Beskrivelse": QualityStatus.OK,
            "Produktnavn": QualityStatus.OK,
        })]
        comp = compare_runs(current, prev)
        improved_deltas = [d for d in comp.deltas if d.fields_improved]
        assert len(improved_deltas) == 1
        assert "Beskrivelse" in improved_deltas[0].fields_improved

    def test_field_regressed(self):
        prev = self._make_snapshot([
            ("A1", 70.0, "Klar", {"Beskrivelse": "OK"})
        ])
        current = [_make(artno="A1", score=40.0, field_statuses={
            "Beskrivelse": QualityStatus.MISSING,
        })]
        comp = compare_runs(current, prev)
        regressed = [d for d in comp.deltas if d.fields_regressed]
        assert len(regressed) == 1
        assert "Beskrivelse" in regressed[0].fields_regressed

    def test_summary_text(self):
        prev = self._make_snapshot([("A1", 50.0, "Ikke klar", {})])
        current = [_make(artno="A1", score=80.0, webshop="Klar")]
        comp = compare_runs(current, prev)
        assert len(comp.summary) > 0
        assert "forbedret" in comp.summary.lower() or "økte" in comp.summary.lower()

    def test_empty_comparison(self):
        prev = self._make_snapshot([])
        current = []
        comp = compare_runs(current, prev)
        assert comp.total_current == 0
        assert comp.total_previous == 0


class TestProductDelta:
    def test_delta_summary_for_improvement(self):
        prev_snap = {
            "job_id": "prev",
            "timestamp": "2026-01-01T12:00:00",
            "product_count": 1,
            "products": {
                "A1": {
                    "product_name": "Test",
                    "total_score": 40.0,
                    "overall_status": "Bør forbedres",
                    "webshop_status": "Ikke klar",
                    "priority_label": "Høy",
                    "priority_score": 70,
                    "field_statuses": {
                        "Beskrivelse": {"status": "Mangler", "has_suggestion": True},
                    },
                    "suggestion_count": 2,
                    "auto_fix_possible": False,
                    "manual_review_needed": True,
                },
            },
        }
        current = [_make(artno="A1", score=80.0, webshop="Klar", priority="Lav",
                         field_statuses={"Beskrivelse": QualityStatus.OK})]
        comp = compare_runs(current, prev_snap)
        delta = comp.deltas[0]
        assert delta.score_change == 40.0
        assert delta.webshop_changed
        assert "Beskrivelse" in delta.fields_improved
        assert "Score +40.0" in delta.summary
