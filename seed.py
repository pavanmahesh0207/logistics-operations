"""
Seed the database from seed_data_logistics.json.

Run once (or idempotently) on startup. Populates:
  - PostgreSQL: carriers, carrier_contracts, shipments, bills_of_lading
  - Neo4j:      Carrier, Contract, Lane, Shipment, BOL nodes + relationships

FreightBill nodes are added to Neo4j when a freight bill is ingested via the API.
"""
import json
import logging
from datetime import date
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.db.postgres import AsyncSessionLocal
from app.db import neo4j_db as neo4j
from app.models.postgres_models import (
    Carrier, CarrierContract, Shipment, BillOfLading,
)

log = logging.getLogger(__name__)

SEED_FILE = Path(__file__).parent / "seed_data_logistics.json"


def _parse_date(s: str | None) -> date | None:
    return date.fromisoformat(s) if s else None


# ── PostgreSQL seeders ─────────────────────────────────────────────────────────

async def _seed_carriers(session: AsyncSession, data: list[dict]):
    added = 0
    for c in data:
        existing = await session.get(Carrier, c["id"])
        if existing:
            log.debug("  carrier %s already exists — skipping", c["id"])
            continue
        log.info("  seeding carrier: %s (%s)", c["id"], c["name"])
        session.add(Carrier(
            id=c["id"],
            name=c["name"],
            carrier_code=c["carrier_code"],
            gstin=c.get("gstin"),
            bank_account=c.get("bank_account"),
            status=c.get("status", "active"),
            onboarded_on=_parse_date(c.get("onboarded_on")),
        ))
        added += 1
    await session.flush()
    log.info("  carriers: %d inserted, %d already existed", added, len(data) - added)


async def _seed_contracts(session: AsyncSession, data: list[dict]):
    added = 0
    for cc in data:
        existing = await session.get(CarrierContract, cc["id"])
        if existing:
            log.debug("  contract %s already exists — skipping", cc["id"])
            continue
        log.info("  seeding contract: %s (%s, %s→%s)",
                 cc["id"], cc["carrier_id"], cc["effective_date"], cc["expiry_date"])
        session.add(CarrierContract(
            id=cc["id"],
            carrier_id=cc["carrier_id"],
            effective_date=_parse_date(cc["effective_date"]),
            expiry_date=_parse_date(cc["expiry_date"]),
            status=cc.get("status", "active"),
            notes=cc.get("notes"),
            rate_card=cc["rate_card"],
        ))
        added += 1
    await session.flush()
    log.info("  contracts: %d inserted, %d already existed", added, len(data) - added)


async def _seed_shipments(session: AsyncSession, data: list[dict]):
    added = 0
    for s in data:
        existing = await session.get(Shipment, s["id"])
        if existing:
            log.debug("  shipment %s already exists — skipping", s["id"])
            continue
        log.info("  seeding shipment: %s (carrier=%s, lane=%s, %.0fkg)",
                 s["id"], s["carrier_id"], s["lane"], s.get("total_weight_kg", 0))
        session.add(Shipment(
            id=s["id"],
            carrier_id=s["carrier_id"],
            contract_id=s["contract_id"],
            lane=s["lane"],
            shipment_date=_parse_date(s["shipment_date"]),
            status=s.get("status", "in_transit"),
            total_weight_kg=s.get("total_weight_kg"),
            notes=s.get("notes"),
        ))
        added += 1
    await session.flush()
    log.info("  shipments: %d inserted, %d already existed", added, len(data) - added)


async def _seed_bols(session: AsyncSession, data: list[dict]):
    added = 0
    for b in data:
        existing = await session.get(BillOfLading, b["id"])
        if existing:
            log.debug("  BOL %s already exists — skipping", b["id"])
            continue
        log.info("  seeding BOL: %s (shipment=%s, %.0fkg)",
                 b["id"], b["shipment_id"], b.get("actual_weight_kg", 0))
        session.add(BillOfLading(
            id=b["id"],
            shipment_id=b["shipment_id"],
            delivery_date=_parse_date(b.get("delivery_date")),
            actual_weight_kg=b.get("actual_weight_kg"),
            notes=b.get("notes") or b.get("_note"),
        ))
        added += 1
    await session.flush()
    log.info("  BOLs: %d inserted, %d already existed", added, len(data) - added)


# ── Neo4j seeders ──────────────────────────────────────────────────────────────

async def _neo4j_seed_carrier(c: dict):
    log.info("  Neo4j carrier: %s (%s)", c["id"], c["name"])
    await neo4j.run_write(
        """
        MERGE (car:Carrier {id: $id})
        SET car.name = $name, car.carrier_code = $code, car.status = $status
        """,
        {"id": c["id"], "name": c["name"],
         "code": c["carrier_code"], "status": c.get("status", "active")},
    )


async def _neo4j_seed_contract(cc: dict):
    log.info("  Neo4j contract: %s → %d lane(s)", cc["id"], len(cc.get("rate_card", [])))
    # Contract node
    await neo4j.run_write(
        """
        MERGE (c:Contract {id: $id})
        SET c.carrier_id = $carrier_id,
            c.effective_date = $eff,
            c.expiry_date = $exp,
            c.status = $status,
            c.notes = $notes,
            c.rate_card = $rate_card
        """,
        {
            "id": cc["id"],
            "carrier_id": cc["carrier_id"],
            "eff": cc["effective_date"],
            "exp": cc["expiry_date"],
            "status": cc.get("status", "active"),
            "notes": cc.get("notes", ""),
            "rate_card": json.dumps(cc["rate_card"]),
        },
    )
    # HAS_CONTRACT edge
    await neo4j.run_write(
        """
        MATCH (car:Carrier {id: $carrier_id})
        MATCH (c:Contract {id: $cc_id})
        MERGE (car)-[:HAS_CONTRACT]->(c)
        """,
        {"carrier_id": cc["carrier_id"], "cc_id": cc["id"]},
    )
    # Lane nodes + COVERS_LANE edges
    for rate in cc.get("rate_card", []):
        lane_code = rate["lane"]
        lane_desc = rate.get("description", lane_code)
        await neo4j.run_write(
            """
            MERGE (l:Lane {code: $code})
            SET l.description = $desc
            """,
            {"code": lane_code, "desc": lane_desc},
        )
        await neo4j.run_write(
            """
            MATCH (c:Contract {id: $cc_id})
            MATCH (l:Lane {code: $lane})
            MERGE (c)-[:COVERS_LANE]->(l)
            """,
            {"cc_id": cc["id"], "lane": lane_code},
        )


async def _neo4j_seed_shipment(s: dict):
    log.info("  Neo4j shipment: %s (lane=%s)", s["id"], s["lane"])
    await neo4j.run_write(
        """
        MERGE (shp:Shipment {id: $id})
        SET shp.lane = $lane,
            shp.shipment_date = $date,
            shp.status = $status,
            shp.total_weight_kg = $weight,
            shp.carrier_id = $carrier_id,
            shp.contract_id = $contract_id
        """,
        {
            "id": s["id"], "lane": s["lane"],
            "date": s["shipment_date"], "status": s.get("status", "in_transit"),
            "weight": s.get("total_weight_kg", 0),
            "carrier_id": s["carrier_id"], "contract_id": s["contract_id"],
        },
    )
    await neo4j.run_write(
        """
        MATCH (car:Carrier {id: $carrier_id})
        MATCH (shp:Shipment {id: $id})
        MERGE (shp)-[:ASSIGNED_TO]->(car)
        """,
        {"carrier_id": s["carrier_id"], "id": s["id"]},
    )
    await neo4j.run_write(
        """
        MATCH (c:Contract {id: $contract_id})
        MATCH (shp:Shipment {id: $id})
        MERGE (shp)-[:COVERED_BY]->(c)
        """,
        {"contract_id": s["contract_id"], "id": s["id"]},
    )
    await neo4j.run_write(
        """
        MATCH (l:Lane {code: $lane})
        MATCH (shp:Shipment {id: $id})
        MERGE (shp)-[:ROUTES_THROUGH]->(l)
        """,
        {"lane": s["lane"], "id": s["id"]},
    )


async def _neo4j_seed_bol(b: dict):
    log.info("  Neo4j BOL: %s (shipment=%s)", b["id"], b["shipment_id"])
    await neo4j.run_write(
        """
        MERGE (bol:BOL {id: $id})
        SET bol.actual_weight_kg = $weight,
            bol.delivery_date = $date,
            bol.shipment_id = $shp_id
        """,
        {
            "id": b["id"],
            "weight": b.get("actual_weight_kg", 0),
            "date": b.get("delivery_date", ""),
            "shp_id": b["shipment_id"],
        },
    )
    await neo4j.run_write(
        """
        MATCH (bol:BOL {id: $bol_id})
        MATCH (shp:Shipment {id: $shp_id})
        MERGE (bol)-[:DOCUMENTS]->(shp)
        """,
        {"bol_id": b["id"], "shp_id": b["shipment_id"]},
    )


# ── Entry point ────────────────────────────────────────────────────────────────

async def run_seed():
    seed_path = SEED_FILE
    if not seed_path.exists():
        log.error("Seed file not found at %s", seed_path)
        return

    with open(seed_path) as f:
        seed = json.load(f)

    carriers = seed.get("carriers", [])
    contracts = seed.get("carrier_contracts", [])
    shipments = seed.get("shipments", [])
    bols = seed.get("bills_of_lading", [])

    log.info("Seeding PostgreSQL — %d carriers, %d contracts, %d shipments, %d BOLs",
             len(carriers), len(contracts), len(shipments), len(bols))
    async with AsyncSessionLocal() as session:
        await _seed_carriers(session, carriers)
        await _seed_contracts(session, contracts)
        await _seed_shipments(session, shipments)
        await _seed_bols(session, bols)
        await session.commit()
    log.info("PostgreSQL seed complete.")

    log.info("Seeding Neo4j — %d carriers, %d contracts, %d shipments, %d BOLs",
             len(carriers), len(contracts), len(shipments), len(bols))
    for c in carriers:
        await _neo4j_seed_carrier(c)
    for cc in contracts:
        await _neo4j_seed_contract(cc)
    for s in shipments:
        await _neo4j_seed_shipment(s)
    for b in bols:
        await _neo4j_seed_bol(b)
    log.info("Neo4j seed complete.")
