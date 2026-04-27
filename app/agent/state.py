"""Agent state schema for the freight-bill processing pipeline."""
from typing import Annotated, Any, Optional
from typing_extensions import TypedDict
from langgraph.graph.message import add_messages


class FreightBillState(TypedDict):
    # ── Input ────────────────────────────────────────────────────────────────
    freight_bill_id: str
    freight_bill_data: dict  # raw payload

    # ── Graph traversal results ──────────────────────────────────────────────
    matched_carrier: Optional[dict]
    candidate_contracts: list[dict]   # may be >1 when lanes overlap
    selected_contract: Optional[dict]
    matched_shipment: Optional[dict]
    matched_bols: list[dict]

    # ── Validation results (set by validate_charges_node) ────────────────────
    duplicate_check: dict     # {is_duplicate, duplicate_of}
    weight_check: dict        # {passed, bol_weight, billed_weight, deviation_pct, message}
    charge_check: dict        # {passed, expected_base, billed_base, deviation_pct, message}
    date_check: dict          # {passed, contract_active, expired_contract, message}
    partial_delivery_check: dict   # {is_partial, previously_billed_kg, remaining_kg}
    unit_mismatch_check: dict      # {has_mismatch, message}
    over_billing_check: dict       # {is_over_billed, total_billed_kg, shipment_kg, message}

    # ── Scoring & decision ────────────────────────────────────────────────────
    confidence_score: float        # 0–100
    confidence_breakdown: dict     # component → delta
    decision: str                  # auto_approved | flag_review | disputed | duplicate

    # ── Evidence chain ────────────────────────────────────────────────────────
    evidence_chain: list[dict]     # ordered log of what the agent found / decided

    # ── LLM enrichment ────────────────────────────────────────────────────────
    carrier_name_normalized: Optional[str]
    decision_explanation: str      # human-readable summary

    # ── Human-in-the-loop ────────────────────────────────────────────────────
    awaiting_review: bool
    reviewer_decision: Optional[dict]  # populated after interrupt resume

    # ── Error capture ────────────────────────────────────────────────────────
    error: Optional[str]
