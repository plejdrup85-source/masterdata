"""Tests for feedback learning loop."""

import json
import os
import tempfile
from unittest.mock import patch

import pytest

import backend.feedback_learning as fl
from backend.feedback_learning import (
    FieldSourceStats,
    adjust_confidence_from_feedback,
    get_auto_approval_threshold,
    get_feedback_summary,
    identify_low_quality_patterns,
    log_suggestion_feedback,
    update_feedback_statistics,
)


@pytest.fixture(autouse=True)
def _isolated_feedback_store(tmp_path):
    """Use a temp file for feedback store in all tests."""
    test_file = tmp_path / "feedback_store.json"
    with patch.object(fl, "FEEDBACK_FILE", test_file), \
         patch.object(fl, "FEEDBACK_DIR", tmp_path):
        yield test_file


class TestLogFeedback:
    def test_basic_logging(self, _isolated_feedback_store):
        log_suggestion_feedback(
            article_number="12345",
            field_name="Produktnavn",
            source="datablad (PDF)",
            confidence=0.85,
            outcome="approved",
        )
        store = json.loads(_isolated_feedback_store.read_text())
        assert len(store["entries"]) == 1
        assert store["entries"][0]["outcome"] == "approved"
        assert store["entries"][0]["confidence"] == 0.85

    def test_multiple_entries(self, _isolated_feedback_store):
        for i in range(3):
            log_suggestion_feedback(
                article_number=f"A{i}",
                field_name="Beskrivelse",
                source="produsent",
                confidence=0.7,
                outcome="rejected",
            )
        store = json.loads(_isolated_feedback_store.read_text())
        assert len(store["entries"]) == 3

    def test_incremental_stats_updated(self, _isolated_feedback_store):
        log_suggestion_feedback(
            article_number="X1",
            field_name="Produktnavn",
            source="PDF",
            confidence=0.8,
            outcome="approved",
        )
        log_suggestion_feedback(
            article_number="X2",
            field_name="Produktnavn",
            source="PDF",
            confidence=0.7,
            outcome="rejected",
        )
        store = json.loads(_isolated_feedback_store.read_text())
        stats = store["stats"]["by_field"]["Produktnavn"]
        assert stats["approved"] == 1
        assert stats["rejected"] == 1
        assert stats["count"] == 2

    def test_corrupted_file_handled(self, _isolated_feedback_store):
        _isolated_feedback_store.write_text("not json!!")
        # Should not crash — creates fresh store
        log_suggestion_feedback(
            article_number="A1",
            field_name="Test",
            source="test",
            confidence=0.5,
            outcome="approved",
        )
        store = json.loads(_isolated_feedback_store.read_text())
        assert len(store["entries"]) == 1


class TestUpdateStatistics:
    def test_full_recalculation(self, _isolated_feedback_store):
        for outcome in ["approved", "approved", "rejected", "modified", "auto_approved"]:
            log_suggestion_feedback(
                article_number="A1",
                field_name="Beskrivelse",
                source="PDF",
                confidence=0.8,
                outcome=outcome,
            )
        stats = update_feedback_statistics()
        assert stats.total_feedback == 5
        assert stats.by_field["Beskrivelse"].approved == 2
        assert stats.by_field["Beskrivelse"].rejected == 1
        assert stats.by_field["Beskrivelse"].auto_approved == 1
        assert stats.by_field["Beskrivelse"].modified == 1

    def test_acceptance_rate(self, _isolated_feedback_store):
        for _ in range(8):
            log_suggestion_feedback("A", "F", "S", 0.8, "approved")
        for _ in range(2):
            log_suggestion_feedback("A", "F", "S", 0.8, "rejected")
        stats = update_feedback_statistics()
        assert abs(stats.by_field["F"].acceptance_rate - 0.8) < 0.01

    def test_empty_store(self, _isolated_feedback_store):
        stats = update_feedback_statistics()
        assert stats.total_feedback == 0
        assert len(stats.by_field) == 0


class TestConfidenceAdjustment:
    def test_no_data_no_change(self, _isolated_feedback_store):
        """Without feedback data, confidence stays the same."""
        result = adjust_confidence_from_feedback(0.80, "Produktnavn", "PDF")
        assert result == 0.80

    def test_insufficient_data_no_change(self, _isolated_feedback_store):
        """With fewer than MIN_OBSERVATIONS, no adjustment."""
        for _ in range(3):  # Less than MIN_OBSERVATIONS (5)
            log_suggestion_feedback("A", "Produktnavn", "PDF", 0.8, "rejected")
        result = adjust_confidence_from_feedback(0.80, "Produktnavn", "PDF")
        assert result == 0.80

    def test_high_rejection_lowers_confidence(self, _isolated_feedback_store):
        """High rejection rate → confidence penalty."""
        for _ in range(7):
            log_suggestion_feedback("A", "Beskrivelse", "Produsent", 0.8, "rejected")
        for _ in range(3):
            log_suggestion_feedback("A", "Beskrivelse", "Produsent", 0.8, "approved")
        # 70% rejection rate → should lower
        result = adjust_confidence_from_feedback(0.80, "Beskrivelse", "Produsent")
        assert result < 0.80

    def test_high_acceptance_boosts_confidence(self, _isolated_feedback_store):
        """High acceptance rate → slight confidence boost."""
        for _ in range(9):
            log_suggestion_feedback("A", "Produktnavn", "PDF", 0.75, "approved")
        for _ in range(1):
            log_suggestion_feedback("A", "Produktnavn", "PDF", 0.75, "rejected")
        # 90% acceptance → should boost
        result = adjust_confidence_from_feedback(0.75, "Produktnavn", "PDF")
        assert result > 0.75

    def test_confidence_clamped_to_range(self, _isolated_feedback_store):
        """Adjusted confidence stays in [0.0, 1.0]."""
        for _ in range(10):
            log_suggestion_feedback("A", "F", "S", 0.98, "approved")
        result = adjust_confidence_from_feedback(0.98, "F", "S")
        assert result <= 1.0

        # Create new field with all rejections
        for _ in range(10):
            log_suggestion_feedback("A", "G", "S", 0.05, "rejected")
        result = adjust_confidence_from_feedback(0.05, "G", "S")
        assert result >= 0.0

    def test_field_source_combo_takes_priority(self, _isolated_feedback_store):
        """Field+source combo stats override field-only or source-only."""
        # Field "Beskrivelse" overall: 90% acceptance
        for _ in range(9):
            log_suggestion_feedback("A", "Beskrivelse", "PDF", 0.8, "approved")
        for _ in range(1):
            log_suggestion_feedback("A", "Beskrivelse", "PDF", 0.8, "rejected")
        # But "Beskrivelse" + "Produsent" specifically: 70% rejection
        for _ in range(7):
            log_suggestion_feedback("A", "Beskrivelse", "Produsent", 0.8, "rejected")
        for _ in range(3):
            log_suggestion_feedback("A", "Beskrivelse", "Produsent", 0.8, "approved")

        # For PDF source: should boost
        result_pdf = adjust_confidence_from_feedback(0.80, "Beskrivelse", "PDF")
        assert result_pdf >= 0.80

        # For Produsent source: should penalize
        result_mfr = adjust_confidence_from_feedback(0.80, "Beskrivelse", "Produsent")
        assert result_mfr < 0.80


class TestLowQualityPatterns:
    def test_detects_high_rejection_pattern(self, _isolated_feedback_store):
        for _ in range(8):
            log_suggestion_feedback("A", "Spesifikasjon", "AI", 0.6, "rejected")
        for _ in range(2):
            log_suggestion_feedback("A", "Spesifikasjon", "AI", 0.6, "approved")

        patterns = identify_low_quality_patterns()
        assert len(patterns) > 0
        assert any(p["field"] == "Spesifikasjon" for p in patterns)
        assert any(p["rejection_rate"] > 0.5 for p in patterns)

    def test_no_patterns_when_high_acceptance(self, _isolated_feedback_store):
        for _ in range(10):
            log_suggestion_feedback("A", "Produktnavn", "PDF", 0.9, "approved")
        patterns = identify_low_quality_patterns()
        spec_patterns = [p for p in patterns if p["field"] == "Produktnavn"]
        assert len(spec_patterns) == 0

    def test_no_patterns_below_threshold(self, _isolated_feedback_store):
        """Not enough data → no patterns reported."""
        for _ in range(3):
            log_suggestion_feedback("A", "F", "S", 0.5, "rejected")
        patterns = identify_low_quality_patterns()
        field_patterns = [p for p in patterns if p["field"] == "F"]
        assert len(field_patterns) == 0

    def test_pattern_has_recommendation(self, _isolated_feedback_store):
        for _ in range(10):
            log_suggestion_feedback("A", "Beskrivelse", "BadSource", 0.5, "rejected")
        patterns = identify_low_quality_patterns()
        assert any("Deaktiver" in p.get("recommendation", "") for p in patterns)


class TestAutoApprovalThreshold:
    def test_default_threshold(self, _isolated_feedback_store):
        """No data → default 0.75."""
        assert get_auto_approval_threshold("Produktnavn") == 0.75

    def test_high_rejection_raises_threshold(self, _isolated_feedback_store):
        for _ in range(4):
            log_suggestion_feedback("A", "Beskrivelse", "S", 0.8, "rejected")
        for _ in range(6):
            log_suggestion_feedback("A", "Beskrivelse", "S", 0.8, "approved")
        # 40% rejection → should tighten
        threshold = get_auto_approval_threshold("Beskrivelse")
        assert threshold > 0.75

    def test_low_rejection_lowers_threshold(self, _isolated_feedback_store):
        for _ in range(10):
            log_suggestion_feedback("A", "Produktnavn", "S", 0.8, "approved")
        # 0% rejection with 10 observations → can loosen
        threshold = get_auto_approval_threshold("Produktnavn")
        assert threshold < 0.75


class TestFeedbackSummary:
    def test_summary_structure(self, _isolated_feedback_store):
        log_suggestion_feedback("A", "Produktnavn", "PDF", 0.8, "approved")
        log_suggestion_feedback("A", "Beskrivelse", "Produsent", 0.7, "rejected")

        summary = get_feedback_summary()
        assert summary["total_feedback_entries"] == 2
        assert "Produktnavn" in summary["by_field"]
        assert "PDF" in summary["by_source"]
        assert "auto_approval_accuracy" in summary
        assert "low_quality_patterns" in summary
        assert "confidence_adjustment_active" in summary

    def test_empty_summary(self, _isolated_feedback_store):
        summary = get_feedback_summary()
        assert summary["total_feedback_entries"] == 0
        assert summary["confidence_adjustment_active"] is False


class TestFieldSourceStats:
    def test_acceptance_rate(self):
        s = FieldSourceStats(approved=8, rejected=2)
        assert abs(s.acceptance_rate - 0.8) < 0.01

    def test_rejection_rate(self):
        s = FieldSourceStats(approved=3, rejected=7)
        assert abs(s.rejection_rate - 0.7) < 0.01

    def test_zero_total(self):
        s = FieldSourceStats()
        assert s.acceptance_rate == 0.0
        assert s.rejection_rate == 0.0
        assert s.avg_confidence == 0.0
