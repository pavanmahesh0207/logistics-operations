"""
Full demo — clean slate + interactive HITL + scenario validation.

Steps:
  1. Reset DB (freight bills + decisions only, reference data kept)
  2. Ingest all 10 seed freight bills via the API
  3. Wait for the agent to finish processing
  4. Show raw agent decisions
  5. Human-in-the-loop: ask YOU to decide on pending_review bills
  6. Final validation: compare every result against the expected outcome
     from the _scenario field in seed_data_logistics.json
  7. Metrics summary
"""
import asyncio, json, time, urllib.request, urllib.error, sys, os, pathlib

sys.path.insert(0, str(pathlib.Path(__file__).parent))
os.chdir(pathlib.Path(__file__).parent)

from dotenv import load_dotenv
load_dotenv()

import asyncpg
from neo4j import AsyncGraphDatabase

BASE = "http://localhost:8000"

# ── Seed bills (exact copy from seed_data_logistics.json, with _scenario) ─────
BILLS = [
    {
        "_scenario": "Clean match — freight bill references shipment, weight and charges match exactly. Should auto-approve.",
        "id": "FB-2025-101", "carrier_id": "CAR001", "carrier_name": "Safexpress Logistics",
        "bill_number": "SFX/2025/00234", "bill_date": "2025-02-15",
        "shipment_reference": "SHP-2025-002", "lane": "DEL-BLR",
        "billed_weight_kg": 850, "rate_per_kg": 15.00,
        "base_charge": 12750.00, "fuel_surcharge": 1020.00,
        "gst_amount": 2479.00, "total_amount": 16249.00,
    },
    {
        "_scenario": "Ambiguous — carrier has 3 overlapping contracts covering DEL-BOM lane at different rates. No shipment reference. Agent must choose which contract applies.",
        "id": "FB-2025-102", "carrier_id": "CAR001", "carrier_name": "Safexpress Logistics",
        "bill_number": "SFX/2025/00251", "bill_date": "2025-04-10",
        "shipment_reference": None, "lane": "DEL-BOM",
        "billed_weight_kg": 600, "rate_per_kg": 13.20,
        "base_charge": 7920.00, "fuel_surcharge": 712.80,
        "gst_amount": 1553.90, "total_amount": 10186.70,
    },
    {
        "_scenario": "Partial delivery — shipment was for 2000 kg, first BOL delivered 1200 kg, this bill covers remaining 800 kg on a second truck. Agent must trace prior freight bills.",
        "id": "FB-2025-103", "carrier_id": "CAR001", "carrier_name": "Safexpress Logistics",
        "bill_number": "SFX/2025/00245", "bill_date": "2025-02-25",
        "shipment_reference": "SHP-2025-001", "lane": "DEL-BOM",
        "billed_weight_kg": 800, "rate_per_kg": 12.50,
        "base_charge": 10000.00, "fuel_surcharge": 800.00,
        "gst_amount": 1944.00, "total_amount": 12744.00,
    },
    {
        "_scenario": "OVER-BILLING — BOL confirms 1200 kg delivered on first truck, but this bill claims 1500 kg. Previous bill (FB-2025-103) already billed 800 kg for remaining load. Net over-billing vs actual delivery. Classic case the system must catch.",
        "id": "FB-2025-104", "carrier_id": "CAR001", "carrier_name": "Safexpress Logistics",
        "bill_number": "SFX/2025/00267", "bill_date": "2025-03-15",
        "shipment_reference": "SHP-2025-001", "lane": "DEL-BOM",
        "billed_weight_kg": 1500, "rate_per_kg": 12.50,
        "base_charge": 18750.00, "fuel_surcharge": 1500.00,
        "gst_amount": 3645.00, "total_amount": 23895.00,
    },
    {
        "_scenario": "Rate drift — billed rate (Rs 8.70/kg) is ~8.75% above the contracted rate (Rs 8.00/kg). Small enough to miss, large enough to matter. Should flag.",
        "id": "FB-2025-105", "carrier_id": "CAR002", "carrier_name": "Delhivery Freight",
        "bill_number": "DEL/25-26/1089", "bill_date": "2025-01-25",
        "shipment_reference": "SHP-2025-004", "lane": "BLR-CHN",
        "billed_weight_kg": 1200, "rate_per_kg": 8.70,
        "base_charge": 10440.00, "fuel_surcharge": 730.80,
        "gst_amount": 2010.74, "total_amount": 13181.54,
    },
    {
        "_scenario": "Expired contract — bill date (2025-03-20) falls after the only contract (CC-2023-TCI-001) expired (2024-06-30). A newer contract exists with different pricing/units.",
        "id": "FB-2025-106", "carrier_id": "CAR003", "carrier_name": "TCI Express",
        "bill_number": "TCI/2025/00047", "bill_date": "2025-03-20",
        "shipment_reference": None, "lane": "BOM-AHM",
        "billed_weight_kg": 4500, "rate_per_kg": 7.50,
        "base_charge": 33750.00, "fuel_surcharge": 2025.00,
        "gst_amount": 6439.50, "total_amount": 42214.50,
    },
    {
        "_scenario": "Unit of measure mismatch — contract is per FTL (Rs 48,000/FTL). Bill is per-kg (7800 kg x Rs 6.50/kg). Semantically valid alternate billing but requires unit reconciliation.",
        "id": "FB-2025-107", "carrier_id": "CAR003", "carrier_name": "TCI Express",
        "bill_number": "TCI/2025/00052", "bill_date": "2025-03-01",
        "shipment_reference": "SHP-2025-005", "lane": "BOM-AHM",
        "billed_weight_kg": 7800, "rate_per_kg": 6.50, "billing_unit": "kg",
        "base_charge": 50700.00, "fuel_surcharge": 3042.00,
        "gst_amount": 9673.56, "total_amount": 63415.56,
    },
    {
        "_scenario": "Contract revision — bill date (2024-11-20) is after fuel surcharge revision (2024-10-01). Must use revised 18%, not original 12%. Bill correctly uses revised rate — should pass.",
        "id": "FB-2025-108", "carrier_id": "CAR004", "carrier_name": "Blue Dart Aviation",
        "bill_number": "BDA/24-25/4567", "bill_date": "2024-11-20",
        "shipment_reference": "SHP-2025-006", "lane": "DEL-BOM-AIR",
        "billed_weight_kg": 250, "rate_per_kg": 85.00,
        "base_charge": 21250.00, "fuel_surcharge": 3825.00,
        "gst_amount": 4513.50, "total_amount": 29588.50,
    },
    {
        "_scenario": "Duplicate bill — same bill number and carrier as FB-2025-101, re-submitted. Must detect and reject/flag.",
        "id": "FB-2025-109", "carrier_id": "CAR001", "carrier_name": "Safexpress Logistics",
        "bill_number": "SFX/2025/00234", "bill_date": "2025-02-15",
        "shipment_reference": "SHP-2025-002", "lane": "DEL-BLR",
        "billed_weight_kg": 850, "rate_per_kg": 15.00,
        "base_charge": 12750.00, "fuel_surcharge": 1020.00,
        "gst_amount": 2479.00, "total_amount": 16249.00,
    },
    {
        "_scenario": "Unknown carrier — 'Gati KWE' has no carrier record, no contract. Spot carrier for emergency shipment. Must escalate to human review.",
        "id": "FB-2025-110", "carrier_id": None, "carrier_name": "Gati KWE Logistics",
        "bill_number": "GAT/2025/00089", "bill_date": "2025-03-25",
        "shipment_reference": None, "lane": "CHN-DEL",
        "billed_weight_kg": 350, "rate_per_kg": 22.00,
        "base_charge": 7700.00, "fuel_surcharge": 924.00,
        "gst_amount": 1552.32, "total_amount": 10176.32,
    },
]

# ───────────────────────────────────────────────────────────────────────────────
# EXPECTED OUTCOMES per _scenario
#   statuses : set of final statuses that count as PASS
#   verdict_label : what the system is supposed to do and why
# ───────────────────────────────────────────────────────────────────────────────
EXPECTED = {
    "FB-2025-101": {
        "statuses": {"auto_approved"},
        "verdict_label": "MUST auto_approved — clean match, all checks pass, score=100",
    },
    "FB-2025-102": {
        # Ambiguous → flagged; human may approve or dispute — both valid
        "statuses": {"pending_review", "approved", "disputed"},
        "verdict_label": "MUST flag for human review — 3 overlapping contracts, no shipment ref",
    },
    "FB-2025-103": {
        # 800 kg billed vs 1200 kg BOL → 33% weight deviation → disputed or flagged
        "statuses": {"disputed", "pending_review", "approved"},
        "verdict_label": "EXPECT dispute/flag — BOL shows 1200 kg, bill is 800 kg (no 2nd BOL exists yet)",
    },
    "FB-2025-104": {
        # 800+1500 = 2300 kg > 2000 kg shipment → over-billing → disputed
        "statuses": {"disputed"},
        "verdict_label": "MUST dispute — over-billing: 800+1500=2300 kg exceeds 2000 kg shipment",
    },
    "FB-2025-105": {
        # Rate drift 8.75% → flagged for review; human may approve or dispute
        "statuses": {"pending_review", "approved", "disputed"},
        "verdict_label": "MUST flag for human review — rate drift 8.75% above contracted Rs 8.00/kg",
    },
    "FB-2025-106": {
        # Billing at expired CC-2023-TCI-001 rates → disputed
        "statuses": {"disputed"},
        "verdict_label": "MUST dispute — expired contract CC-2023-TCI-001 (expired 2024-06-30)",
    },
    "FB-2025-107": {
        # Per-kg alternate billing is contractually allowed on FTL contract
        "statuses": {"auto_approved", "approved", "pending_review"},
        "verdict_label": "EXPECT approve — per-kg alternate billing is valid under CC-2024-TCI-002",
    },
    "FB-2025-108": {
        # Revised fuel surcharge (18%) used correctly → full match
        "statuses": {"auto_approved"},
        "verdict_label": "MUST auto_approve — revised fuel surcharge (18% post 2024-10-01) correct",
    },
    "FB-2025-109": {
        # Exact same bill_number + carrier_id as FB-2025-101
        "statuses": {"duplicate"},
        "verdict_label": "MUST detect duplicate — bill SFX/2025/00234 already processed as FB-2025-101",
    },
    "FB-2025-110": {
        # Score=0 (unknown carrier -30, no contract, no shipment) → auto-disputed
        "statuses": {"disputed", "pending_review"},
        "verdict_label": "MUST dispute or flag — unknown carrier 'Gati KWE', no contract, score=0",
    },
}

# ── HTTP helpers ───────────────────────────────────────────────────────────────

def http_get(path):
    with urllib.request.urlopen(f"{BASE}{path}", timeout=10) as r:
        return json.loads(r.read())

def http_post(path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{BASE}{path}", data=data, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read()), None
    except urllib.error.HTTPError as e:
        return None, (e.code, json.loads(e.read()))

# ── Formatting ─────────────────────────────────────────────────────────────────

def sep(title=""):
    if title:
        print(f"\n{'='*70}")
        print(f"  {title}")
        print(f"{'='*70}")

STATUS_ICON = {
    "auto_approved":  "✅ AUTO-APPROVED",
    "approved":       "✅ APPROVED (human)",
    "disputed":       "❌ DISPUTED",
    "duplicate":      "🔁 DUPLICATE",
    "pending_review": "⏸  PENDING REVIEW",
    "processing":     "⚙️  PROCESSING",
    "error":          "💥 ERROR",
}

# ── Step 1: Reset ──────────────────────────────────────────────────────────────

async def reset_databases():
    sep("STEP 1 — RESETTING DATABASES")
    pg_url = os.getenv("POSTGRES_SYNC_URL",
                       "postgresql://logistics:logistics@localhost:5432/logistics")
    print("  Connecting to PostgreSQL …")
    conn = await asyncpg.connect(pg_url)
    d = await conn.execute("DELETE FROM freight_bill_decisions")
    f = await conn.execute("DELETE FROM freight_bills")
    await conn.close()
    print(f"  PostgreSQL — {d} decisions + {f} freight_bills deleted")

    print("  Connecting to Neo4j …")
    driver = AsyncGraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "logistics123")),
    )
    async with driver.session() as session:
        r = await session.run("MATCH (f:FreightBill) DETACH DELETE f RETURN count(f) AS n")
        rec = await r.single()
        print(f"  Neo4j — {rec['n'] if rec else 0} FreightBill node(s) deleted")
    await driver.close()
    print("  Reference data (Carriers / Contracts / Lanes / Shipments / BOLs) kept intact")

# ── Step 2: Ingest ─────────────────────────────────────────────────────────────

def ingest_all():
    sep("STEP 2 — INGESTING ALL 10 FREIGHT BILLS")
    ids = []
    for b in BILLS:
        payload = {k: v for k, v in b.items() if not k.startswith("_")}
        resp, err = http_post("/freight-bills", payload)
        if err:
            code, body = err
            print(f"  {b['id']}  ERROR {code}: {body.get('detail')}")
        else:
            scenario_short = b["_scenario"][:58]
            print(f"  {b['id']}  queued  [{scenario_short}…]")
            ids.append(b["id"])
    return ids

# ── Step 3: Wait ───────────────────────────────────────────────────────────────

def wait_for_agent(seconds=15):
    sep(f"STEP 3 — WAITING {seconds}s FOR AGENT TO PROCESS ALL BILLS …")
    for i in range(seconds, 0, -3):
        print(f"  {i}s remaining …", end="\r", flush=True)
        time.sleep(3)
    print(" " * 40)

# ── Step 4: Show agent raw decisions ──────────────────────────────────────────

def collect_results(ids):
    return {fb_id: http_get(f"/freight-bills/{fb_id}") for fb_id in ids}

def show_agent_decisions(results):
    sep("STEP 4 — AGENT DECISIONS (before human review)")
    print(f"  {'Bill':<14} {'Score':>5}  {'Agent Status':<26}  Scenario")
    print("  " + "─"*80)
    for b in BILLS:
        fb_id = b["id"]
        if fb_id not in results:
            continue
        d = results[fb_id]
        status  = d.get("status") or "-"
        score   = d.get("confidence_score")
        score_s = f"{score:.0f}" if score is not None else "  -"
        icon    = STATUS_ICON.get(status, f"? {status}")
        label   = b["_scenario"][:42] + "…"
        print(f"  {fb_id:<14} {score_s:>5}  {icon:<26}  {label}")

# ── Step 5: Human-in-the-loop ──────────────────────────────────────────────────

def handle_human_review(results):
    pending = [(fb_id, d) for fb_id, d in results.items()
               if d.get("status") == "pending_review"]
    if not pending:
        print("\n  No bills require human review — all decided automatically.")
        return

    sep(f"STEP 5 — HUMAN-IN-THE-LOOP ({len(pending)} bill(s) awaiting your decision)")

    for fb_id, d in pending:
        scenario = next((b["_scenario"] for b in BILLS if b["id"] == fb_id), "")
        print(f"\n  ┌─── REVIEW REQUEST {'─'*48}")
        print(f"  │  Bill ID    : {fb_id}")
        print(f"  │  Carrier    : {d.get('carrier_name')}")
        print(f"  │  Lane       : {d.get('lane')}")
        print(f"  │  Amount     : Rs {d.get('total_amount'):,.2f}")
        print(f"  │  Score      : {d.get('confidence_score')}/100")
        print(f"  │  Agent rec  : {d.get('decision')}")
        print(f"  │  Scenario   : {scenario[:68]}")
        expl = (d.get("decision_explanation") or "").strip()
        if expl:
            words = expl.split()
            line, lines = [], []
            for w in words:
                if len(" ".join(line + [w])) > 64:
                    lines.append(" ".join(line)); line = [w]
                else:
                    line.append(w)
            if line: lines.append(" ".join(line))
            print(f"  │  Explanation:")
            for ln in lines:
                print(f"  │    {ln}")
        print(f"  └{'─'*68}")

        print(f"\n  Options:  [1] human_approved   [2] human_disputed   [3] human_modified")
        while True:
            choice = input(f"  Your decision for {fb_id} (1/2/3): ").strip()
            if choice in ("1", "2", "3"):
                break
            print("  Please enter 1, 2, or 3.")

        decision = {"1": "human_approved", "2": "human_disputed", "3": "human_modified"}[choice]
        notes    = input(f"  Notes (optional): ").strip() or "Reviewed via run_demo.py"
        reviewer = input(f"  Reviewer ID (default: ops-reviewer): ").strip() or "ops-reviewer"

        resp, err = http_post(f"/review/{fb_id}", {
            "decision": decision, "notes": notes, "reviewer_id": reviewer,
        })
        if err:
            print(f"  ERROR: submission failed: {err}")
        else:
            print(f"  Submitted '{decision}' — agent resuming …")

    print("\n  Waiting 6s for agent to finalize reviewed bills …")
    time.sleep(6)

# ── Step 6: Scenario validation ────────────────────────────────────────────────

PASS    = "PASS   "
FAIL    = "FAIL   "

def show_validation(ids):
    sep("STEP 6 — SCENARIO VALIDATION (system vs seed_data expected outcomes)")
    print(f"  {'Bill':<14} {'Score':>5}  {'Final Status':<22}  Result")
    print(f"  {'':14} {'':5}  {'':22}  Expected & Scenario")
    print("  " + "═"*80)

    pass_count = fail_count = 0

    for b in BILLS:
        fb_id = b["id"]
        if fb_id not in ids:
            continue

        d = http_get(f"/freight-bills/{fb_id}")
        final_status = d.get("status") or "-"
        score        = d.get("confidence_score")
        score_s      = f"{score:.0f}" if score is not None else "  -"
        icon         = STATUS_ICON.get(final_status, f"? {final_status}")

        exp              = EXPECTED.get(fb_id, {})
        expected_statuses = exp.get("statuses", set())
        verdict_label    = exp.get("verdict_label", "?")

        if final_status in expected_statuses:
            verdict = f"✅ {PASS}"
            pass_count += 1
        else:
            verdict = f"❌ {FAIL}"
            fail_count += 1

        print(f"  {fb_id:<14} {score_s:>5}  {icon:<26}  {verdict}")
        print(f"  {'':14} {'':5}  {'':26}  Expected : {verdict_label}")
        print(f"  {'':14} {'':5}  {'':26}  Scenario : {b['_scenario'][:65]}")
        print()

    total = pass_count + fail_count
    print("  " + "═"*80)
    print(f"\n  RESULT:  {pass_count}/{total} PASS   {fail_count}/{total} FAIL")
    if fail_count == 0:
        print("  All 10 scenarios match expected outcomes.")
    else:
        print(f"  {fail_count} scenario(s) did not match — see FAIL rows above.")

# ── Step 7: Metrics ────────────────────────────────────────────────────────────

def show_metrics():
    sep("STEP 7 — METRICS SUMMARY")
    m = http_get("/metrics")
    print("  Decision distribution:")
    for row in sorted(m.get("decision_distribution", []), key=lambda r: -r["count"]):
        bar = "█" * row["count"]
        print(f"    {row['decision']:<22} {bar:<12} count={row['count']}  avg_score={row['avg_confidence']:.0f}")
    print("\n  Status distribution:")
    for row in sorted(m.get("status_distribution", []), key=lambda r: -r["count"]):
        print(f"    {row['status']:<22} count={row['count']}")
    print("\n  Amount by decision (Rs):")
    for row in sorted(m.get("amount_by_decision", []), key=lambda r: -r["total_amount"]):
        print(f"    {row['decision']:<22}  {row['total_amount']:>12,.2f}")

# ── Main ───────────────────────────────────────────────────────────────────────

async def main():
    print("\n" + "█"*70)
    print("  FREIGHT BILL PROCESSING — FULL DEMO WITH SCENARIO VALIDATION")
    print("█"*70)

    try:
        h = http_get("/health")
        print(f"\n  Server OK  |  model: {h.get('groq_model')}")
    except Exception:
        print("  Server is not running. Start uvicorn first.")
        sys.exit(1)

    await reset_databases()
    ids = ingest_all()
    if not ids:
        return

    wait_for_agent(15)

    results = collect_results(ids)
    show_agent_decisions(results)
    handle_human_review(results)
    show_validation(ids)
    show_metrics()

    print("\n" + "█"*70)
    print("  DEMO COMPLETE")
    print("█"*70 + "\n")


asyncio.run(main())

