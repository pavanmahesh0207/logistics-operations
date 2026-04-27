"""
LangGraph node functions for the freight-bill processing agent.

Node execution order:
  normalize_carrier → match_entities → check_duplicate
    → validate_charges → compute_confidence
    → generate_explanation → decide_action
    → [human_review? interrupt] → finalize
"""
import asyncio
import logging
from datetime import date, datetime, timezone as _tz
from typing import Any

from langchain_groq import ChatGroq
from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.types import interrupt

from app.config import settings
from app.db import neo4j_db as neo4j
from app.db.postgres import AsyncSessionLocal
from app.models.postgres_models import FreightBill, FreightBillDecision
from app.agent.state import FreightBillState
from sqlalchemy import select, func

log = logging.getLogger(__name__)

# ── LLM helper ──────────────────────────────────────────────────────────────

def _get_llm() -> ChatGroq:
    return ChatGroq(
        api_key=settings.groq_api_key,
        model=settings.groq_model,
        temperature=0,
    )


def _today() -> date:
    return date.today()


def _evidence(step: str, detail: Any) -> dict:
    return {"step": step, "detail": detail, "timestamp": datetime.now(_tz.utc).isoformat()}


# ── Node 1: Normalize carrier name with LLM ──────────────────────────────────

async def normalize_carrier_node(state: FreightBillState) -> dict:
    """
    Use the LLM to fuzzy-match the incoming carrier_name against known carriers.
    Avoids failing hard on abbreviations, spacing, or capitalisation differences.
    """
    fb = state["freight_bill_data"]
    carrier_name_raw = fb.get("carrier_name", "")
    carrier_id_raw = fb.get("carrier_id")
    fb_id = state.get("freight_bill_id", fb.get("id", "?"))

    log.info("[1/9] normalize_carrier | %s | carrier_name=%r | has_carrier_id=%s",
             fb_id, carrier_name_raw, bool(carrier_id_raw))

    evidence = list(state.get("evidence_chain", []))

    # If carrier_id is given, we don't need LLM normalisation
    if carrier_id_raw:
        log.info("  ↳ carrier_id=%s provided directly — skipping LLM", carrier_id_raw)
        evidence.append(_evidence("normalize_carrier", {
            "method": "carrier_id_direct",
            "carrier_id": carrier_id_raw,
        }))
        return {
            "carrier_name_normalized": carrier_name_raw,
            "evidence_chain": evidence,
        }

    # Ask the LLM to map the name to one of the known carriers
    known_carriers = await neo4j.run_query(
        "MATCH (c:Carrier) RETURN c.id AS id, c.name AS name, c.carrier_code AS code"
    )
    known_list = "\n".join(f"- {c['id']}: {c['name']} ({c['code']})" for c in known_carriers)

    log.info("  ↳ no carrier_id — querying LLM to fuzzy-match %r against %d known carriers",
             carrier_name_raw, len(known_carriers))
    try:
        llm = _get_llm()
        response = await llm.ainvoke([
            SystemMessage(content=(
                "You are a logistics data-normalisation assistant. "
                "Given a carrier name from an invoice and a list of registered carriers, "
                "return only the carrier_id that best matches, or 'UNKNOWN' if no good match exists. "
                "Reply with ONLY the carrier_id string or UNKNOWN — no explanation."
            )),
            HumanMessage(content=(
                f"Invoice carrier name: {carrier_name_raw}\n\n"
                f"Registered carriers:\n{known_list}"
            )),
        ])
        normalized_id = response.content.strip()
        log.info("  ↳ LLM matched: %r → %s", carrier_name_raw, normalized_id)
        evidence.append(_evidence("normalize_carrier", {
            "raw_name": carrier_name_raw,
            "llm_matched_id": normalized_id,
        }))
        return {
            "carrier_name_normalized": normalized_id if normalized_id != "UNKNOWN" else None,
            "evidence_chain": evidence,
        }
    except Exception as exc:
        log.warning("LLM carrier normalisation failed: %s", exc)
        evidence.append(_evidence("normalize_carrier", {"error": str(exc)}))
        return {"carrier_name_normalized": None, "evidence_chain": evidence}


# ── Node 2: Match entities via Neo4j graph traversal ─────────────────────────

async def match_entities_node(state: FreightBillState) -> dict:
    """
    Traverse the Neo4j graph to find:
      - The carrier node
      - All active contracts covering the lane on the bill date
      - Shipment and BOLs (if shipment_reference is present)
    """
    fb = state["freight_bill_data"]
    evidence = list(state.get("evidence_chain", []))

    # Resolve carrier_id: from payload or from LLM normalisation
    carrier_id = fb.get("carrier_id") or state.get("carrier_name_normalized")
    lane = fb.get("lane", "")
    bill_date_str = fb.get("bill_date", str(_today()))
    shipment_ref = fb.get("shipment_reference")
    fb_id_log = state.get("freight_bill_id", fb.get("id", "?"))

    log.info("[2/9] match_entities | %s | carrier_id=%s | lane=%s | bill_date=%s | shipment_ref=%s",
             fb_id_log, carrier_id, lane, bill_date_str, shipment_ref)

    # ── Carrier lookup ────────────────────────────────────────────────────────
    matched_carrier = None
    if carrier_id:
        rows = await neo4j.run_query(
            "MATCH (c:Carrier {id: $id}) RETURN c",
            {"id": carrier_id},
        )
        if rows:
            matched_carrier = dict(rows[0]["c"])

    log.info("  ↳ carrier lookup: found=%s (id=%s)", matched_carrier is not None, carrier_id)
    evidence.append(_evidence("match_carrier", {
        "carrier_id_used": carrier_id,
        "found": matched_carrier is not None,
        "carrier": matched_carrier,
    }))

    # ── Contract lookup (active on bill_date, covering the lane) ─────────────
    candidate_contracts = []
    if carrier_id:
        contract_rows = await neo4j.run_query(
            """
            MATCH (c:Carrier {id: $carrier_id})-[:HAS_CONTRACT]->(cc:Contract)
                  -[:COVERS_LANE]->(l:Lane {code: $lane})
            WHERE cc.effective_date <= $bill_date <= cc.expiry_date
            RETURN cc
            """,
            {
                "carrier_id": carrier_id,
                "lane": lane,
                "bill_date": bill_date_str,
            },
        )
        candidate_contracts = [dict(r["cc"]) for r in contract_rows]

        # Also surface expired contracts so we can produce evidence
        if not candidate_contracts:
            expired_rows = await neo4j.run_query(
                """
                MATCH (c:Carrier {id: $carrier_id})-[:HAS_CONTRACT]->(cc:Contract)
                      -[:COVERS_LANE]->(l:Lane {code: $lane})
                WHERE cc.expiry_date < $bill_date
                RETURN cc ORDER BY cc.expiry_date DESC LIMIT 1
                """,
                {"carrier_id": carrier_id, "lane": lane, "bill_date": bill_date_str},
            )
            if expired_rows:
                candidate_contracts = [dict(expired_rows[0]["cc"]) | {"_expired": True}]

    log.info("  ↳ contracts found: %d %s",
             len(candidate_contracts), [c.get("id") for c in candidate_contracts])
    evidence.append(_evidence("match_contracts", {
        "lane": lane,
        "bill_date": bill_date_str,
        "count": len(candidate_contracts),
        "contracts": [c.get("id") for c in candidate_contracts],
    }))

    # ── Shipment & BOL lookup ─────────────────────────────────────────────────
    matched_shipment = None
    matched_bols: list[dict] = []

    if shipment_ref:
        shp_rows = await neo4j.run_query(
            "MATCH (s:Shipment {id: $id}) RETURN s",
            {"id": shipment_ref},
        )
        if shp_rows:
            matched_shipment = dict(shp_rows[0]["s"])

            bol_rows = await neo4j.run_query(
                "MATCH (b:BOL)-[:DOCUMENTS]->(s:Shipment {id: $id}) RETURN b",
                {"id": shipment_ref},
            )
            matched_bols = [dict(r["b"]) for r in bol_rows]

    log.info("  ↳ shipment: found=%s | BOLs: %d", matched_shipment is not None, len(matched_bols))
    evidence.append(_evidence("match_shipment_bol", {
        "shipment_ref": shipment_ref,
        "shipment_found": matched_shipment is not None,
        "bol_count": len(matched_bols),
    }))

    # ── Select best contract ──────────────────────────────────────────────────
    # Prefer contract explicitly linked to the matched shipment;
    # otherwise prefer the most-recently effective active contract.
    selected_contract = None
    if candidate_contracts:
        if matched_shipment:
            contract_on_shp = matched_shipment.get("contract_id")
            for cc in candidate_contracts:
                if cc.get("id") == contract_on_shp:
                    selected_contract = cc
                    break
        if not selected_contract:
            valid = [c for c in candidate_contracts if not c.get("_expired")]
            selected_contract = max(valid, key=lambda c: c.get("effective_date", ""), default=None)
            if not selected_contract:
                selected_contract = candidate_contracts[0]

    log.info("  ↳ selected contract: %s",
             selected_contract.get("id") if selected_contract else "none")
    return {
        "matched_carrier": matched_carrier,
        "candidate_contracts": candidate_contracts,
        "selected_contract": selected_contract,
        "matched_shipment": matched_shipment,
        "matched_bols": matched_bols,
        "evidence_chain": evidence,
    }


# ── Node 3: Duplicate detection ───────────────────────────────────────────────

async def check_duplicate_node(state: FreightBillState) -> dict:
    """
    Check whether this bill_number + carrier_id combination already exists
    in PostgreSQL (i.e., has been processed before).
    """
    fb = state["freight_bill_data"]
    evidence = list(state.get("evidence_chain", []))

    bill_number = fb.get("bill_number")
    carrier_id = fb.get("carrier_id") or state.get("carrier_name_normalized")
    current_id = fb.get("id")
    fb_id_log = state.get("freight_bill_id", current_id or "?")

    log.info("[3/9] check_duplicate | %s | bill_number=%s | carrier_id=%s",
             fb_id_log, bill_number, carrier_id)

    async with AsyncSessionLocal() as session:
        stmt = select(FreightBill).where(
            FreightBill.bill_number == bill_number,
            FreightBill.carrier_id == carrier_id,
            FreightBill.id != current_id,
        )
        result = await session.execute(stmt)
        existing = result.scalars().first()

    is_duplicate = existing is not None
    duplicate_of = existing.id if existing else None

    if is_duplicate:
        log.warning("  ↳ DUPLICATE detected — matches existing bill %s", duplicate_of)
    else:
        log.info("  ↳ no duplicate found")

    evidence.append(_evidence("check_duplicate", {
        "bill_number": bill_number,
        "is_duplicate": is_duplicate,
        "duplicate_of": duplicate_of,
    }))

    return {
        "duplicate_check": {"is_duplicate": is_duplicate, "duplicate_of": duplicate_of},
        "evidence_chain": evidence,
    }


# ── Node 4: Validate charges (deterministic rules — no LLM) ──────────────────

def _get_lane_rate(contract: dict, lane: str) -> dict | None:
    """Find the rate entry for a lane in a contract's rate_card.
    Handles Neo4j returning rate_card as a JSON string."""
    import json as _json
    rate_card = contract.get("rate_card", [])
    if isinstance(rate_card, str):
        try:
            rate_card = _json.loads(rate_card)
        except (ValueError, TypeError):
            return None
    for entry in rate_card:
        if entry.get("lane") == lane:
            return entry
    return None


def _expected_base_charge(rate_entry: dict, billed_weight: float,
                           billing_unit: str, bill_date_str: str) -> float | None:
    """
    Compute the expected base charge from first principles.
    Handles per-kg, FTL flat, and FTL alternate-per-kg billing.
    """
    if rate_entry is None:
        return None

    unit = rate_entry.get("unit", "kg")

    if unit == "FTL":
        if billing_unit == "FTL":
            return float(rate_entry["rate_per_unit"])
        # alternate per-kg billing (FTL contract but carrier billed by kg)
        alt = rate_entry.get("alternate_rate_per_kg")
        if alt:
            return billed_weight * float(alt)
        return None

    # Standard per-kg
    rate = float(rate_entry.get("rate_per_kg", 0))
    min_charge = float(rate_entry.get("min_charge", 0))
    raw = billed_weight * rate
    return max(raw, min_charge)


def _effective_fuel_pct(rate_entry: dict, bill_date_str: str) -> float:
    """
    Return the fuel surcharge percentage that was in effect on the bill date.
    Accounts for mid-term revisions.
    """
    revised_on = rate_entry.get("revised_on")
    if revised_on and bill_date_str >= revised_on:
        return float(rate_entry.get("revised_fuel_surcharge_percent",
                                    rate_entry["fuel_surcharge_percent"]))
    return float(rate_entry.get("fuel_surcharge_percent", 0))


async def validate_charges_node(state: FreightBillState) -> dict:
    """Deterministic validation of weight, charges, dates, and partial delivery."""
    fb = state["freight_bill_data"]
    evidence = list(state.get("evidence_chain", []))

    bill_date_str: str = str(fb.get("bill_date", ""))
    billed_weight: float = float(fb.get("billed_weight_kg", 0))
    billed_base: float = float(fb.get("base_charge", 0))
    billed_fuel: float = float(fb.get("fuel_surcharge", 0))
    billing_unit: str = fb.get("billing_unit", "kg")
    lane: str = fb.get("lane", "")
    shipment_ref = fb.get("shipment_reference")
    fb_id_log = state.get("freight_bill_id", fb.get("id", "?"))

    log.info("[4/9] validate_charges | %s | weight=%.0fkg | base=%.2f | lane=%s | date=%s",
             fb_id_log, billed_weight, billed_base, lane, bill_date_str)

    contract = state.get("selected_contract")
    matched_bols = state.get("matched_bols", [])
    matched_shipment = state.get("matched_shipment")

    # ── Date / contract validity check ───────────────────────────────────────
    date_check: dict = {"passed": False, "contract_active": False,
                        "expired_contract": False, "message": "No contract found"}
    if contract:
        expired = contract.get("_expired", False)
        effective = contract.get("effective_date", "")
        expiry = contract.get("expiry_date", "")
        active = (not expired) and (effective <= bill_date_str <= expiry)
        date_check = {
            "passed": active,
            "contract_active": active,
            "expired_contract": expired,
            "effective_date": effective,
            "expiry_date": expiry,
            "message": "Contract active" if active else (
                "Contract expired" if expired else "Bill date outside contract window"
            ),
        }
    log.info("  ↳ date check: %s", date_check["message"])
    evidence.append(_evidence("date_check", date_check))

    # ── Weight check against BOL ──────────────────────────────────────────────
    weight_check: dict = {"passed": True, "message": "No BOL to compare", "deviation_pct": 0.0}
    if matched_bols:
        # Use the BOL with the closest actual_weight to the billed_weight
        bol = matched_bols[0]
        bol_weight = float(bol.get("actual_weight_kg", 0))
        if bol_weight > 0:
            deviation_pct = abs(billed_weight - bol_weight) / bol_weight * 100
            passed = deviation_pct <= settings.weight_tolerance_pct
            weight_check = {
                "passed": passed,
                "bol_id": bol.get("id"),
                "bol_weight": bol_weight,
                "billed_weight": billed_weight,
                "deviation_pct": round(deviation_pct, 2),
                "message": f"Weight OK (Δ{deviation_pct:.1f}%)" if passed
                           else f"Weight mismatch: billed {billed_weight}kg vs BOL {bol_weight}kg (Δ{deviation_pct:.1f}%)",
            }
    log.info("  ↳ weight check: %s", weight_check["message"])
    evidence.append(_evidence("weight_check", weight_check))

    # ── Charge validation ─────────────────────────────────────────────────────
    charge_check: dict = {"passed": True, "message": "No contract to compare", "deviation_pct": 0.0}
    unit_mismatch_check: dict = {"has_mismatch": False, "message": ""}

    if contract:
        rate_entry = _get_lane_rate(contract, lane)
        if rate_entry:
            expected_base = _expected_base_charge(rate_entry, billed_weight,
                                                  billing_unit, bill_date_str)
            fuel_pct = _effective_fuel_pct(rate_entry, bill_date_str)
            expected_fuel = (expected_base * fuel_pct / 100) if expected_base else None

            # Detect FTL contract billed higher as per-kg
            if rate_entry.get("unit") == "FTL" and billing_unit == "kg":
                ftl_base = float(rate_entry["rate_per_unit"])
                if expected_base and expected_base > ftl_base:
                    unit_mismatch_check = {
                        "has_mismatch": True,
                        "ftl_rate": ftl_base,
                        "per_kg_total": expected_base,
                        "message": (
                            f"Carrier billed per-kg (₹{expected_base:,.0f}) "
                            f"which exceeds FTL flat rate (₹{ftl_base:,.0f}). "
                            "Per-kg billing is contractually allowed but results in higher charge."
                        ),
                    }

            if expected_base is not None:
                deviation_pct = abs(billed_base - expected_base) / expected_base * 100 if expected_base else 0
                passed = deviation_pct <= settings.charge_tolerance_pct
                charge_check = {
                    "passed": passed,
                    "expected_base": round(expected_base, 2),
                    "billed_base": billed_base,
                    "deviation_pct": round(deviation_pct, 2),
                    "fuel_pct_used": fuel_pct,
                    "billed_fuel": billed_fuel,
                    "expected_fuel": round(expected_fuel, 2) if expected_fuel else None,
                    "message": (
                        f"Charges OK (Δ{deviation_pct:.1f}%)" if passed
                        else f"Rate mismatch: billed ₹{billed_base:,.2f} vs expected ₹{expected_base:,.2f} (Δ{deviation_pct:.1f}%)"
                    ),
                }

    log.info("  ↳ charge check: %s", charge_check["message"])
    if unit_mismatch_check.get("has_mismatch"):
        log.warning("  ↳ unit mismatch: %s", unit_mismatch_check["message"])
    evidence.append(_evidence("charge_check", charge_check))
    evidence.append(_evidence("unit_mismatch_check", unit_mismatch_check))

    # ── Partial delivery / over-billing check ─────────────────────────────────
    partial_delivery_check: dict = {"is_partial": False, "previously_billed_kg": 0.0}
    over_billing_check: dict = {"is_over_billed": False, "message": ""}

    if matched_shipment:
        shipment_total_kg = float(matched_shipment.get("total_weight_kg", 0))
        current_fb_id = fb.get("id")

        # Sum billed weights for this shipment from other freight bills
        async with AsyncSessionLocal() as session:
            stmt = select(func.sum(FreightBill.billed_weight_kg)).where(
                FreightBill.shipment_reference == matched_shipment["id"],
                FreightBill.id != current_fb_id,
                FreightBill.status.notin_(["disputed", "rejected", "duplicate"]),
            )
            result = await session.execute(stmt)
            previously_billed_kg = float(result.scalar() or 0)

        total_billed = previously_billed_kg + billed_weight
        remaining_kg = shipment_total_kg - previously_billed_kg

        partial_delivery_check = {
            "is_partial": previously_billed_kg > 0,
            "shipment_total_kg": shipment_total_kg,
            "previously_billed_kg": previously_billed_kg,
            "remaining_kg": round(remaining_kg, 2),
        }

        if total_billed > shipment_total_kg * 1.02:  # >2% over
            over_billing_check = {
                "is_over_billed": True,
                "total_billed_kg": round(total_billed, 2),
                "shipment_kg": shipment_total_kg,
                "excess_kg": round(total_billed - shipment_total_kg, 2),
                "message": (
                    f"Over-billing detected: total billed {total_billed:,.0f}kg "
                    f"exceeds shipment weight {shipment_total_kg:,.0f}kg"
                ),
            }

    if over_billing_check.get("is_over_billed"):
        log.warning("  ↳ OVER-BILLING: %s", over_billing_check["message"])
    elif matched_shipment:
        log.info("  ↳ over-billing check: OK (prev_billed=%.0fkg + current=%.0fkg of %.0fkg total)",
                 partial_delivery_check.get("previously_billed_kg", 0), billed_weight,
                 partial_delivery_check.get("shipment_total_kg", 0))
    evidence.append(_evidence("partial_delivery_check", partial_delivery_check))
    evidence.append(_evidence("over_billing_check", over_billing_check))

    return {
        "date_check": date_check,
        "weight_check": weight_check,
        "charge_check": charge_check,
        "unit_mismatch_check": unit_mismatch_check,
        "partial_delivery_check": partial_delivery_check,
        "over_billing_check": over_billing_check,
        "evidence_chain": evidence,
    }


# ── Node 5: Compute confidence score ──────────────────────────────────────────

async def compute_confidence_node(state: FreightBillState) -> dict:
    """
    Score the freight bill 0–100 using deterministic signals.

    Component weights:
      Carrier identified          +20
      Active contract found       +20  (−10 if multiple contracts → ambiguity)
      Shipment reference valid    +15
      BOL found                   +10
      Weight within tolerance     +15  (−15 to −30 if not)
      Charges within tolerance    +15  (−15 to −25 if not)
      No duplicate                 +5

    Penalty overrides:
      Expired contract            −40
      Unknown carrier             −30
      Duplicate detected          −80
      Over-billing detected       −25
      Unit mismatch               −10
    """
    fb = state["freight_bill_data"]
    evidence = list(state.get("evidence_chain", []))
    breakdown: dict[str, int] = {}
    score = 0
    fb_id_log = state.get("freight_bill_id", fb.get("id", "?"))

    log.info("[5/9] compute_confidence | %s", fb_id_log)

    # Carrier
    if state.get("matched_carrier"):
        score += 20; breakdown["carrier_match"] = 20
    else:
        score -= 30; breakdown["unknown_carrier"] = -30

    # Contract
    contracts = state.get("candidate_contracts", [])
    if contracts:
        active = [c for c in contracts if not c.get("_expired")]
        if active:
            score += 20; breakdown["contract_found"] = 20
            if len(active) > 1:
                score -= 10; breakdown["contract_ambiguity"] = -10
        else:
            # Only expired contract found
            score += 5; breakdown["contract_expired_only"] = 5
            score -= 40; breakdown["expired_contract"] = -40
    else:
        breakdown["no_contract"] = 0

    # Date check
    date_check = state.get("date_check", {})
    if date_check.get("expired_contract") and not any(
        k == "expired_contract" for k in breakdown
    ):
        score -= 40; breakdown["expired_contract"] = -40

    # Shipment
    if state.get("matched_shipment"):
        score += 15; breakdown["shipment_match"] = 15
    elif fb.get("shipment_reference"):
        # Reference given but not found
        score -= 10; breakdown["shipment_ref_not_found"] = -10
    else:
        score -= 5; breakdown["no_shipment_ref"] = -5

    # BOL
    if state.get("matched_bols"):
        score += 10; breakdown["bol_found"] = 10

    # Weight check
    wc = state.get("weight_check", {})
    dev = wc.get("deviation_pct", 0.0)
    if wc.get("passed", True) and dev == 0.0 and not state.get("matched_bols"):
        pass  # no BOL to compare — neutral
    elif wc.get("passed"):
        score += 15; breakdown["weight_match"] = 15
    elif dev <= 10:
        score -= 15; breakdown["weight_deviation_mild"] = -15
    else:
        score -= 30; breakdown["weight_deviation_severe"] = -30

    # Charge check
    cc = state.get("charge_check", {})
    cdev = cc.get("deviation_pct", 0.0)
    if cc.get("passed", True) and cdev == 0.0 and not state.get("selected_contract"):
        pass  # no contract to compare — neutral
    elif cc.get("passed"):
        score += 15; breakdown["charge_match"] = 15
    elif cdev <= 10:
        score -= 15; breakdown["charge_deviation_mild"] = -15
    else:
        score -= 25; breakdown["charge_deviation_severe"] = -25

    # Unit mismatch
    if state.get("unit_mismatch_check", {}).get("has_mismatch"):
        score -= 10; breakdown["unit_mismatch"] = -10

    # Over-billing
    if state.get("over_billing_check", {}).get("is_over_billed"):
        score -= 25; breakdown["over_billing"] = -25

    # Duplicate
    dup = state.get("duplicate_check", {})
    if dup.get("is_duplicate"):
        score -= 80; breakdown["duplicate"] = -80
    else:
        score += 5; breakdown["no_duplicate"] = 5

    final_score = max(0, min(100, score))
    log.info("  ↳ score: %d/100 | breakdown: %s", final_score, breakdown)
    evidence.append(_evidence("confidence_score", {
        "score": final_score,
        "breakdown": breakdown,
    }))

    return {
        "confidence_score": final_score,
        "confidence_breakdown": breakdown,
        "evidence_chain": evidence,
    }


# ── Node 6: LLM explanation ────────────────────────────────────────────────────

async def generate_explanation_node(state: FreightBillState) -> dict:
    """Use the LLM to produce a concise, plain-language decision rationale."""
    fb = state["freight_bill_data"]
    score = state.get("confidence_score", 0)
    breakdown = state.get("confidence_breakdown", {})
    fb_id_log = state.get("freight_bill_id", fb.get("id", "?"))

    log.info("[6/9] generate_explanation | %s | score=%d", fb_id_log, score)

    # Build a compact summary for the LLM to narrate
    summary = {
        "freight_bill_id": fb.get("id"),
        "carrier": fb.get("carrier_name"),
        "lane": fb.get("lane"),
        "billed_amount": fb.get("total_amount"),
        "confidence_score": score,
        "score_components": breakdown,
        "duplicate": state.get("duplicate_check", {}).get("is_duplicate"),
        "weight_check": state.get("weight_check", {}).get("message"),
        "charge_check": state.get("charge_check", {}).get("message"),
        "date_check": state.get("date_check", {}).get("message"),
        "over_billing": state.get("over_billing_check", {}).get("message"),
        "unit_mismatch": state.get("unit_mismatch_check", {}).get("message"),
    }

    try:
        log.info("  ↳ calling LLM for plain-language explanation …")
        llm = _get_llm()
        response = await llm.ainvoke([
            SystemMessage(content=(
                "You are an expert logistics accounts-payable reviewer. "
                "Given a structured analysis of a freight bill, write a 2–4 sentence "
                "plain-language explanation of the decision. Be specific about what "
                "matched, what didn't, and why the confidence score is what it is."
            )),
            HumanMessage(content=str(summary)),
        ])
        explanation = response.content.strip()
        log.info("  ↳ explanation: %.120s", explanation)
    except Exception as exc:
        log.warning("  ↳ LLM explanation failed: %s — using fallback", exc)
        explanation = f"Automated analysis complete. Confidence score: {score:.0f}/100."

    return {"decision_explanation": explanation}


# ── Node 7: Routing decision ──────────────────────────────────────────────────

async def decide_action_node(state: FreightBillState) -> dict:
    """
    Map confidence score and special signals to a decision label.

    auto_approved  → confidence ≥ 80, no duplicate, no over-billing
    disputed       → duplicate detected  OR  over-billing detected  OR score < 40
    flag_review    → anything in between (score 40–79) → will trigger interrupt
    """
    score = state.get("confidence_score", 0)
    dup = state.get("duplicate_check", {}).get("is_duplicate", False)
    over_billed = state.get("over_billing_check", {}).get("is_over_billed", False)
    fb_id_log = state.get("freight_bill_id", "?")

    log.info("[7/9] decide_action | %s | score=%.0f | duplicate=%s | over_billed=%s",
             fb_id_log, score, dup, over_billed)

    if dup:
        decision = "duplicate"
    elif over_billed or score < settings.auto_dispute_threshold:
        decision = "disputed"
    elif score >= settings.auto_approve_threshold:
        decision = "auto_approved"
    else:
        decision = "flag_review"

    log.info("  ↳ DECISION → %s", decision.upper())
    evidence = list(state.get("evidence_chain", []))
    evidence.append(_evidence("decide_action", {
        "confidence_score": score,
        "decision": decision,
        "auto_approve_threshold": settings.auto_approve_threshold,
        "auto_dispute_threshold": settings.auto_dispute_threshold,
    }))

    return {"decision": decision, "awaiting_review": False, "evidence_chain": evidence}


# ── Node 8: Human review interrupt ────────────────────────────────────────────

async def human_review_node(state: FreightBillState) -> dict:
    """
    Pause the agent and hand control back to a human reviewer.
    The interrupt() call serialises the current state into the checkpointer;
    execution resumes when POST /review/{id} provides a Command(resume=…).
    """
    evidence = list(state.get("evidence_chain", []))

    # Update the DB record to reflect it's awaiting review
    fb_id = state["freight_bill_id"]
    log.info("[8/9] human_review | %s | score=%.0f — INTERRUPTING, waiting for human reviewer",
             fb_id, state.get("confidence_score", 0))

    async with AsyncSessionLocal() as session:
        fb = await session.get(FreightBill, fb_id)
        if fb:
            fb.status = "pending_review"
            await session.commit()

    evidence.append(_evidence("human_review", {
        "status": "awaiting_reviewer_input",
        "confidence_score": state.get("confidence_score"),
        "agent_decision": state.get("decision"),
    }))

    # ── interrupt() pauses here until reviewer resumes via Command(resume=…) ──
    reviewer_input = interrupt({
        "freight_bill_id": fb_id,
        "confidence_score": state.get("confidence_score"),
        "agent_recommendation": state.get("decision"),
        "explanation": state.get("decision_explanation"),
        "evidence_summary": evidence[-6:],  # last 6 evidence steps
    })

    log.info("  ↳ reviewer response received | %s | decision=%s | reviewer=%s",
             fb_id,
             reviewer_input.get("decision") if isinstance(reviewer_input, dict) else reviewer_input,
             reviewer_input.get("reviewer_id") if isinstance(reviewer_input, dict) else "?")
    evidence.append(_evidence("reviewer_input_received", reviewer_input))

    return {
        "awaiting_review": False,
        "reviewer_decision": reviewer_input,
        "evidence_chain": evidence,
    }


# ── Node 9: Finalize — write decision to PostgreSQL + Neo4j ──────────────────

async def finalize_node(state: FreightBillState) -> dict:
    """
    Persist the final decision (agent or human-reviewed) to:
      - PostgreSQL: update freight_bill status, write audit record
      - Neo4j: link FreightBill node to matched Contract / Shipment / BOL nodes
    """
    fb_id = state["freight_bill_id"]
    fb = state["freight_bill_data"]
    evidence = list(state.get("evidence_chain", []))
    reviewer_decision = state.get("reviewer_decision") or {}

    # Final decision: reviewer overrides agent decision
    final_decision = reviewer_decision.get("decision") or state.get("decision", "flag_review")
    decided_by = reviewer_decision.get("reviewer_id", "agent")
    notes = reviewer_decision.get("notes", state.get("decision_explanation", ""))

    log.info("[9/9] finalize | %s | final_decision=%s | decided_by=%s",
             fb_id, final_decision, decided_by)

    # Map decision label to freight_bill status
    status_map = {
        "auto_approved": "auto_approved",
        "flag_review":   "pending_review",
        "disputed":      "disputed",
        "duplicate":     "duplicate",
        "human_approved": "approved",
        "human_disputed": "disputed",
        "human_modified": "approved",
    }
    fb_status = status_map.get(final_decision, "pending_review")

    # ── PostgreSQL writes ─────────────────────────────────────────────────────
    async with AsyncSessionLocal() as session:
        record = await session.get(FreightBill, fb_id)
        if record:
            record.status = fb_status
            session.add(record)

        decision_rec = FreightBillDecision(
            freight_bill_id=fb_id,
            decision=final_decision,
            confidence_score=state.get("confidence_score"),
            evidence=evidence,
            decided_by=decided_by,
            notes=notes,
        )
        session.add(decision_rec)
        await session.commit()

    # ── Neo4j relationship writes ─────────────────────────────────────────────
    selected_contract = state.get("selected_contract")
    matched_shipment = state.get("matched_shipment")
    matched_bols = state.get("matched_bols", [])

    if selected_contract:
        await neo4j.run_write(
            """
            MATCH (f:FreightBill {id: $fb_id})
            MATCH (cc:Contract {id: $cc_id})
            MERGE (f)-[:MATCHED_TO {decision: $decision, confidence: $score}]->(cc)
            """,
            {
                "fb_id": fb_id,
                "cc_id": selected_contract["id"],
                "decision": final_decision,
                "score": state.get("confidence_score"),
            },
        )

    if matched_shipment:
        await neo4j.run_write(
            """
            MATCH (f:FreightBill {id: $fb_id})
            MATCH (s:Shipment {id: $shp_id})
            MERGE (f)-[:REFERENCES_SHIPMENT]->(s)
            """,
            {"fb_id": fb_id, "shp_id": matched_shipment["id"]},
        )

    for bol in matched_bols:
        await neo4j.run_write(
            """
            MATCH (f:FreightBill {id: $fb_id})
            MATCH (b:BOL {id: $bol_id})
            MERGE (f)-[:LINKED_TO_BOL]->(b)
            """,
            {"fb_id": fb_id, "bol_id": bol["id"]},
        )

    evidence.append(_evidence("finalize", {
        "final_decision": final_decision,
        "decided_by": decided_by,
        "fb_status": fb_status,
    }))

    log.info("  ✓ %s COMPLETE | decision=%s | status=%s | decided_by=%s",
             fb_id, final_decision.upper(), fb_status, decided_by)

    return {"evidence_chain": evidence}
