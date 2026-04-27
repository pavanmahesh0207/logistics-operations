"""
FastAPI routes for the freight-bill processing system.

Endpoints:
  POST  /freight-bills         — ingest a freight bill, trigger the agent
  GET   /freight-bills/{id}    — get current state, decision, and evidence
  GET   /review-queue          — list bills awaiting human review
  POST  /review/{id}           — submit reviewer decision, resume the agent
"""
import asyncio
import logging
from datetime import date, datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from langgraph.types import Command

from app.db.postgres import get_db, AsyncSessionLocal
from app.models.postgres_models import FreightBill, FreightBillDecision
from app.agent.graph import freight_bill_graph
from app.db import neo4j_db as neo4j

log = logging.getLogger(__name__)
router = APIRouter()


# ── Pydantic request / response models ────────────────────────────────────────

class FreightBillIngest(BaseModel):
    id: str
    carrier_id: Optional[str] = None
    carrier_name: str
    bill_number: str
    bill_date: str
    shipment_reference: Optional[str] = None
    lane: str
    billed_weight_kg: float
    rate_per_kg: Optional[float] = None
    billing_unit: str = "kg"
    base_charge: float
    fuel_surcharge: float
    gst_amount: float
    total_amount: float


class ReviewDecision(BaseModel):
    decision: str = Field(
        ...,
        description="One of: human_approved, human_disputed, human_modified",
    )
    notes: Optional[str] = None
    reviewer_id: str = Field(default="ops-reviewer")
    modified_amount: Optional[float] = None   # for human_modified


# ── Background task: run the agent ────────────────────────────────────────────

async def _run_agent(fb_id: str, fb_data: dict):
    """Invoke the LangGraph agent for a freight bill (runs in background)."""
    thread_config = {"configurable": {"thread_id": fb_id}}
    log.info("━━━ AGENT START ━━━ %s | lane=%s | carrier=%s | amount=%.2f",
             fb_id, fb_data.get("lane"), fb_data.get("carrier_name"), fb_data.get("total_amount", 0))

    initial_state = {
        "freight_bill_id": fb_id,
        "freight_bill_data": fb_data,
        "matched_carrier": None,
        "candidate_contracts": [],
        "selected_contract": None,
        "matched_shipment": None,
        "matched_bols": [],
        "duplicate_check": {},
        "weight_check": {},
        "charge_check": {},
        "date_check": {},
        "partial_delivery_check": {},
        "unit_mismatch_check": {},
        "over_billing_check": {},
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

    try:
        # ainvoke returns when the graph completes OR when interrupt() is called
        await freight_bill_graph.ainvoke(initial_state, config=thread_config)

        # Check post-run: if graph is interrupted, status was already set in human_review_node
        snapshot = freight_bill_graph.get_state(thread_config)
        if snapshot.next:
            # Graph still has pending nodes — it's interrupted
            log.info("━━━ AGENT PAUSED ━━━ %s | awaiting human review (next nodes: %s)",
                     fb_id, list(snapshot.next))
        else:
            log.info("━━━ AGENT DONE ━━━ %s | processing complete", fb_id)

    except Exception as exc:
        log.exception("Agent error for freight bill %s: %s", fb_id, exc)
        async with AsyncSessionLocal() as session:
            fb = await session.get(FreightBill, fb_id)
            if fb:
                fb.status = "error"
                await session.commit()


# ── POST /freight-bills ────────────────────────────────────────────────────────

@router.post("/freight-bills", status_code=202)
async def ingest_freight_bill(
    payload: FreightBillIngest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """
    Ingest a freight bill and asynchronously trigger the agent pipeline.
    Returns immediately with the created record; processing happens in background.
    """
    fb_id = payload.id

    # Idempotency: reject if already processed
    existing = await db.get(FreightBill, fb_id)
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"Freight bill {fb_id} already exists (status: {existing.status})",
        )

    log.info("POST /freight-bills | ingesting %s | carrier=%s | lane=%s | amount=%.2f",
             fb_id, payload.carrier_name, payload.lane, payload.total_amount)
    fb_data = payload.model_dump()
    fb = FreightBill(
        id=fb_id,
        carrier_id=payload.carrier_id,
        carrier_name=payload.carrier_name,
        bill_number=payload.bill_number,
        bill_date=date.fromisoformat(payload.bill_date),
        shipment_reference=payload.shipment_reference,
        lane=payload.lane,
        billed_weight_kg=payload.billed_weight_kg,
        rate_per_kg=payload.rate_per_kg,
        billing_unit=payload.billing_unit,
        base_charge=payload.base_charge,
        fuel_surcharge=payload.fuel_surcharge,
        gst_amount=payload.gst_amount,
        total_amount=payload.total_amount,
        status="processing",
        langgraph_thread_id=fb_id,
        raw_payload=fb_data,
    )
    db.add(fb)
    await db.commit()
    await db.refresh(fb)

    # Upsert a FreightBill node in Neo4j for graph traversal
    await neo4j.run_write(
        """
        MERGE (f:FreightBill {id: $id})
        SET f.carrier_name = $carrier_name,
            f.bill_number  = $bill_number,
            f.bill_date    = $bill_date,
            f.lane         = $lane,
            f.total_amount = $total_amount
        """,
        {
            "id": fb_id,
            "carrier_name": payload.carrier_name,
            "bill_number": payload.bill_number,
            "bill_date": payload.bill_date,
            "lane": payload.lane,
            "total_amount": payload.total_amount,
        },
    )

    # Trigger agent in background
    background_tasks.add_task(_run_agent, fb_id, fb_data)
    log.info("POST /freight-bills | %s saved — agent queued in background", fb_id)

    return {
        "id": fb_id,
        "status": "processing",
        "message": "Freight bill accepted. Agent processing started.",
    }


# ── GET /freight-bills/{id} ────────────────────────────────────────────────────

@router.get("/freight-bills/{fb_id}")
async def get_freight_bill(fb_id: str, db: AsyncSession = Depends(get_db)):
    """Return the current state, decision, confidence score, and evidence chain."""
    fb = await db.get(FreightBill, fb_id)
    if not fb:
        raise HTTPException(status_code=404, detail=f"Freight bill {fb_id} not found")

    # Fetch all decisions (audit trail)
    stmt = select(FreightBillDecision).where(
        FreightBillDecision.freight_bill_id == fb_id
    ).order_by(FreightBillDecision.created_at)
    result = await db.execute(stmt)
    decisions = result.scalars().all()

    latest = decisions[-1] if decisions else None

    # Pull current LangGraph state so callers can see the full evidence chain
    # even before finalize runs
    thread_config = {"configurable": {"thread_id": fb_id}}
    try:
        snapshot = freight_bill_graph.get_state(thread_config)
        agent_state = dict(snapshot.values) if snapshot else {}
    except Exception:
        agent_state = {}

    return {
        "id": fb.id,
        "carrier_name": fb.carrier_name,
        "bill_number": fb.bill_number,
        "bill_date": str(fb.bill_date),
        "lane": fb.lane,
        "total_amount": fb.total_amount,
        "status": fb.status,
        "confidence_score": latest.confidence_score if latest else agent_state.get("confidence_score"),
        "decision": latest.decision if latest else agent_state.get("decision"),
        "decision_explanation": (
            latest.notes if latest else agent_state.get("decision_explanation")
        ),
        "evidence_chain": latest.evidence if latest else agent_state.get("evidence_chain", []),
        "decisions_history": [
            {
                "id": d.id,
                "decision": d.decision,
                "confidence_score": d.confidence_score,
                "decided_by": d.decided_by,
                "notes": d.notes,
                "created_at": d.created_at.isoformat() if d.created_at else None,
            }
            for d in decisions
        ],
        "created_at": fb.created_at.isoformat() if fb.created_at else None,
        "updated_at": fb.updated_at.isoformat() if fb.updated_at else None,
    }


# ── GET /review-queue ──────────────────────────────────────────────────────────

@router.get("/review-queue")
async def get_review_queue(db: AsyncSession = Depends(get_db)):
    """List all freight bills currently waiting for human review."""
    stmt = select(FreightBill).where(FreightBill.status == "pending_review")
    result = await db.execute(stmt)
    bills = result.scalars().all()

    queue = []
    for fb in bills:
        thread_config = {"configurable": {"thread_id": fb.id}}
        try:
            snapshot = freight_bill_graph.get_state(thread_config)
            state = dict(snapshot.values) if snapshot else {}
        except Exception:
            state = {}

        queue.append({
            "id": fb.id,
            "carrier_name": fb.carrier_name,
            "bill_number": fb.bill_number,
            "bill_date": str(fb.bill_date),
            "lane": fb.lane,
            "total_amount": fb.total_amount,
            "status": fb.status,
            "confidence_score": state.get("confidence_score"),
            "agent_recommendation": state.get("decision"),
            "decision_explanation": state.get("decision_explanation"),
            "confidence_breakdown": state.get("confidence_breakdown", {}),
            "created_at": fb.created_at.isoformat() if fb.created_at else None,
        })

    return {"count": len(queue), "items": queue}


# ── POST /review/{id} ──────────────────────────────────────────────────────────

@router.post("/review/{fb_id}")
async def submit_review(
    fb_id: str,
    body: ReviewDecision,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """
    Submit a human reviewer decision and resume the paused agent.

    The agent's interrupt() call returns the reviewer payload, then the
    finalize node writes the final decision to PostgreSQL and Neo4j.
    """
    fb = await db.get(FreightBill, fb_id)
    if not fb:
        raise HTTPException(status_code=404, detail=f"Freight bill {fb_id} not found")

    if fb.status != "pending_review":
        raise HTTPException(
            status_code=409,
            detail=f"Freight bill {fb_id} is not pending review (current status: {fb.status})",
        )

    allowed = {"human_approved", "human_disputed", "human_modified"}
    if body.decision not in allowed:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid decision '{body.decision}'. Must be one of: {', '.join(allowed)}",
        )

    reviewer_payload = {
        "decision": body.decision,
        "notes": body.notes,
        "reviewer_id": body.reviewer_id,
        "modified_amount": body.modified_amount,
    }

    async def _resume():
        thread_config = {"configurable": {"thread_id": fb_id}}
        log.info("POST /review/%s | resuming agent | decision=%s | reviewer=%s",
                 fb_id, body.decision, body.reviewer_id)
        try:
            await freight_bill_graph.ainvoke(
                Command(resume=reviewer_payload),
                config=thread_config,
            )
            log.info("━━━ AGENT RESUMED+DONE ━━━ %s | reviewer=%s | decision=%s",
                     fb_id, body.reviewer_id, body.decision)
        except Exception as exc:
            log.exception("Agent resume error for %s: %s", fb_id, exc)

    background_tasks.add_task(_resume)

    return {
        "id": fb_id,
        "status": "resuming",
        "message": f"Review decision '{body.decision}' accepted. Agent resuming.",
        "reviewer_id": body.reviewer_id,
    }


# ── GET /metrics (bonus: observability) ───────────────────────────────────────

@router.get("/metrics")
async def get_metrics(db: AsyncSession = Depends(get_db)):
    """Simple dashboard endpoint showing agent performance metrics."""
    from sqlalchemy import func, case

    # Decision distribution
    stmt = select(
        FreightBillDecision.decision,
        func.count().label("count"),
        func.avg(FreightBillDecision.confidence_score).label("avg_confidence"),
    ).group_by(FreightBillDecision.decision)
    result = await db.execute(stmt)
    decision_stats = [
        {
            "decision": row.decision,
            "count": row.count,
            "avg_confidence": round(row.avg_confidence or 0, 1),
        }
        for row in result
    ]

    # Status distribution
    stmt2 = select(
        FreightBill.status,
        func.count().label("count"),
    ).group_by(FreightBill.status)
    result2 = await db.execute(stmt2)
    status_stats = [{"status": row.status, "count": row.count} for row in result2]

    # Total billed amount by decision
    stmt3 = select(
        FreightBillDecision.decision,
        func.sum(FreightBill.total_amount).label("total_amount"),
    ).join(FreightBill, FreightBill.id == FreightBillDecision.freight_bill_id).group_by(
        FreightBillDecision.decision
    )
    result3 = await db.execute(stmt3)
    amount_stats = [
        {"decision": row.decision, "total_amount": round(row.total_amount or 0, 2)}
        for row in result3
    ]

    return {
        "decision_distribution": decision_stats,
        "status_distribution": status_stats,
        "amount_by_decision": amount_stats,
    }
