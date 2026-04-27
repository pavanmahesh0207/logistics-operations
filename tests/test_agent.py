"""
Tests for the freight-bill processing agent's core decision logic.
Run with:  venv/bin/python -m pytest tests/ -v
"""
import pytest
import asyncio
from unittest.mock import patch, AsyncMock, MagicMock

from app.agent.nodes import (
    _get_lane_rate, _expected_base_charge, _effective_fuel_pct,
    compute_confidence_node,
)
from app.config import settings


# ── Rate calculation helpers ──────────────────────────────────────────────────

class TestRateHelpers:
    def test_lane_rate_found(self):
        contract = {"rate_card": [{"lane": "DEL-BLR", "rate_per_kg": 15.00, "min_charge": 6000}]}
        assert _get_lane_rate(contract, "DEL-BLR")["rate_per_kg"] == 15.00

    def test_lane_rate_not_found(self):
        contract = {"rate_card": [{"lane": "DEL-BLR", "rate_per_kg": 15.00}]}
        assert _get_lane_rate(contract, "BOM-AHM") is None

    def test_per_kg_base_charge(self):
        rate = {"rate_per_kg": 15.00, "min_charge": 6000.00}
        assert _expected_base_charge(rate, 850.0, "kg", "2025-02-15") == 12750.0

    def test_per_kg_minimum_charge_applied(self):
        rate = {"rate_per_kg": 5.00, "min_charge": 6000.00}
        # 100kg * 5 = 500, but min_charge is 6000
        assert _expected_base_charge(rate, 100.0, "kg", "2025-02-15") == 6000.0

    def test_ftl_flat_billing(self):
        rate = {"unit": "FTL", "rate_per_unit": 48000.0, "min_charge": 48000.0}
        assert _expected_base_charge(rate, 7800.0, "FTL", "2025-03-01") == 48000.0

    def test_ftl_alternate_per_kg_billing(self):
        rate = {"unit": "FTL", "rate_per_unit": 48000.0, "alternate_rate_per_kg": 6.50, "min_charge": 48000.0}
        assert _expected_base_charge(rate, 7800.0, "kg", "2025-03-01") == 50700.0

    def test_fuel_surcharge_no_revision(self):
        rate = {"fuel_surcharge_percent": 8}
        assert _effective_fuel_pct(rate, "2025-01-01") == 8.0

    def test_fuel_surcharge_after_revision(self):
        rate = {"fuel_surcharge_percent": 12, "revised_on": "2024-10-01",
                "revised_fuel_surcharge_percent": 18}
        assert _effective_fuel_pct(rate, "2024-11-20") == 18.0

    def test_fuel_surcharge_before_revision(self):
        rate = {"fuel_surcharge_percent": 12, "revised_on": "2024-10-01",
                "revised_fuel_surcharge_percent": 18}
        assert _effective_fuel_pct(rate, "2024-09-30") == 12.0


# ── Confidence scoring ────────────────────────────────────────────────────────

def _make_state(overrides: dict) -> dict:
    """Build a minimal agent state for confidence testing."""
    base = {
        "freight_bill_id": "FB-TEST-001",
        "freight_bill_data": {"id": "FB-TEST-001", "shipment_reference": None},
        "matched_carrier": {"id": "CAR001"},
        "candidate_contracts": [{"id": "CC-001"}],
        "selected_contract": {"id": "CC-001"},
        "matched_shipment": None,
        "matched_bols": [],
        "duplicate_check": {"is_duplicate": False},
        "weight_check": {"passed": True, "deviation_pct": 0.0},
        "charge_check": {"passed": True, "deviation_pct": 0.0},
        "date_check": {"passed": True, "expired_contract": False},
        "partial_delivery_check": {},
        "unit_mismatch_check": {"has_mismatch": False},
        "over_billing_check": {"is_over_billed": False},
        "confidence_score": 0.0,
        "confidence_breakdown": {},
        "decision": "",
        "evidence_chain": [],
        "carrier_name_normalized": None,
        "decision_explanation": "",
        "awaiting_review": False,
        "reviewer_decision": None,
        "error": None,
    }
    base.update(overrides)
    return base


class TestConfidenceScore:
    def _run(self, state):
        return asyncio.run(compute_confidence_node(state))

    def test_clean_match_scores_above_threshold(self):
        """FB-2025-101 scenario: all checks pass."""
        state = _make_state({
            "matched_carrier": {"id": "CAR001"},
            "candidate_contracts": [{"id": "CC-001"}],
            "matched_shipment": {"id": "SHP-2025-002"},
            "matched_bols": [{"id": "BOL-2025-002", "actual_weight_kg": 850}],
            "weight_check": {"passed": True, "deviation_pct": 0.5},
            "charge_check": {"passed": True, "deviation_pct": 0.0},
        })
        result = self._run(state)
        assert result["confidence_score"] >= settings.auto_approve_threshold

    def test_duplicate_scores_near_zero(self):
        """FB-2025-109: duplicate bill should score very low."""
        state = _make_state({
            "duplicate_check": {"is_duplicate": True, "duplicate_of": "FB-2025-101"},
        })
        result = self._run(state)
        assert result["confidence_score"] < settings.auto_dispute_threshold

    def test_unknown_carrier_scores_low(self):
        """FB-2025-110: unknown carrier reduces score significantly."""
        state = _make_state({
            "matched_carrier": None,
            "candidate_contracts": [],
            "selected_contract": None,
        })
        result = self._run(state)
        assert result["confidence_score"] < 50

    def test_expired_contract_penalised(self):
        """FB-2025-106 scenario: billing against expired contract."""
        state = _make_state({
            "candidate_contracts": [{"id": "CC-OLD", "_expired": True}],
            "date_check": {"passed": False, "expired_contract": True},
        })
        result = self._run(state)
        assert result["confidence_score"] < settings.auto_approve_threshold

    def test_over_billing_penalised(self):
        """FB-2025-104: over-billing detected against shipment total."""
        state = _make_state({
            "matched_shipment": {"id": "SHP-2025-001"},
            "over_billing_check": {
                "is_over_billed": True,
                "total_billed_kg": 2300,
                "shipment_kg": 2000,
                "message": "Over-billing detected",
            },
        })
        result = self._run(state)
        assert result["confidence_score"] < settings.auto_approve_threshold

    def test_weight_mismatch_severe_penalised(self):
        """Weight deviation >10% should severely penalise the score."""
        state = _make_state({
            "matched_bols": [{"id": "BOL-001", "actual_weight_kg": 1200}],
            "weight_check": {"passed": False, "deviation_pct": 25.0, "bol_weight": 1200, "billed_weight": 1500},
        })
        result = self._run(state)
        assert result["confidence_breakdown"].get("weight_deviation_severe") == -30

    def test_ambiguous_contracts_penalised(self):
        """Multiple overlapping contracts reduce confidence."""
        state = _make_state({
            "candidate_contracts": [
                {"id": "CC-001"}, {"id": "CC-002"}, {"id": "CC-003"}
            ],
        })
        result = self._run(state)
        assert result["confidence_breakdown"].get("contract_ambiguity") == -10
