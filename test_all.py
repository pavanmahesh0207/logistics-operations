"""
End-to-end test script — ingests all 10 seed freight bills,
waits for agent processing, prints a decision summary, then polls metrics.
"""
import urllib.request
import urllib.error
import json
import time

BASE = "http://localhost:8000"

BILLS = [
    {"id":"FB-2025-101","carrier_id":"CAR001","carrier_name":"Safexpress Logistics","bill_number":"SFX/2025/00234","bill_date":"2025-02-15","shipment_reference":"SHP-2025-002","lane":"DEL-BLR","billed_weight_kg":850,"rate_per_kg":15.00,"base_charge":12750.00,"fuel_surcharge":1020.00,"gst_amount":2479.00,"total_amount":16249.00},
    {"id":"FB-2025-102","carrier_id":"CAR001","carrier_name":"Safexpress Logistics","bill_number":"SFX/2025/00251","bill_date":"2025-04-10","shipment_reference":None,"lane":"DEL-BOM","billed_weight_kg":600,"rate_per_kg":13.20,"base_charge":7920.00,"fuel_surcharge":712.80,"gst_amount":1553.90,"total_amount":10186.70},
    {"id":"FB-2025-103","carrier_id":"CAR001","carrier_name":"Safexpress Logistics","bill_number":"SFX/2025/00245","bill_date":"2025-02-25","shipment_reference":"SHP-2025-001","lane":"DEL-BOM","billed_weight_kg":800,"rate_per_kg":12.50,"base_charge":10000.00,"fuel_surcharge":800.00,"gst_amount":1944.00,"total_amount":12744.00},
    {"id":"FB-2025-104","carrier_id":"CAR001","carrier_name":"Safexpress Logistics","bill_number":"SFX/2025/00267","bill_date":"2025-03-15","shipment_reference":"SHP-2025-001","lane":"DEL-BOM","billed_weight_kg":1500,"rate_per_kg":12.50,"base_charge":18750.00,"fuel_surcharge":1500.00,"gst_amount":3645.00,"total_amount":23895.00},
    {"id":"FB-2025-105","carrier_id":"CAR002","carrier_name":"Delhivery Freight","bill_number":"DEL/25-26/1089","bill_date":"2025-01-25","shipment_reference":"SHP-2025-004","lane":"BLR-CHN","billed_weight_kg":1200,"rate_per_kg":8.70,"base_charge":10440.00,"fuel_surcharge":730.80,"gst_amount":2010.74,"total_amount":13181.54},
    {"id":"FB-2025-106","carrier_id":"CAR003","carrier_name":"TCI Express","bill_number":"TCI/2025/00047","bill_date":"2025-03-20","shipment_reference":None,"lane":"BOM-AHM","billed_weight_kg":4500,"rate_per_kg":7.50,"base_charge":33750.00,"fuel_surcharge":2025.00,"gst_amount":6439.50,"total_amount":42214.50},
    {"id":"FB-2025-107","carrier_id":"CAR003","carrier_name":"TCI Express","bill_number":"TCI/2025/00052","bill_date":"2025-03-01","shipment_reference":"SHP-2025-005","lane":"BOM-AHM","billed_weight_kg":7800,"rate_per_kg":6.50,"billing_unit":"kg","base_charge":50700.00,"fuel_surcharge":3042.00,"gst_amount":9673.56,"total_amount":63415.56},
    {"id":"FB-2025-108","carrier_id":"CAR004","carrier_name":"Blue Dart Aviation","bill_number":"BDA/24-25/4567","bill_date":"2024-11-20","shipment_reference":"SHP-2025-006","lane":"DEL-BOM-AIR","billed_weight_kg":250,"rate_per_kg":85.00,"base_charge":21250.00,"fuel_surcharge":3825.00,"gst_amount":4513.50,"total_amount":29588.50},
    {"id":"FB-2025-109","carrier_id":"CAR001","carrier_name":"Safexpress Logistics","bill_number":"SFX/2025/00234","bill_date":"2025-02-15","shipment_reference":"SHP-2025-002","lane":"DEL-BLR","billed_weight_kg":850,"rate_per_kg":15.00,"base_charge":12750.00,"fuel_surcharge":1020.00,"gst_amount":2479.00,"total_amount":16249.00},
    {"id":"FB-2025-110","carrier_id":None,"carrier_name":"Gati KWE Logistics","bill_number":"GAT/2025/00089","bill_date":"2025-03-25","shipment_reference":None,"lane":"CHN-DEL","billed_weight_kg":350,"rate_per_kg":22.00,"base_charge":7700.00,"fuel_surcharge":924.00,"gst_amount":1552.32,"total_amount":10176.32},
]


def get(path):
    with urllib.request.urlopen(f"{BASE}{path}", timeout=10) as r:
        return json.loads(r.read())


def post(path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{BASE}{path}", data=data, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read()), None
    except urllib.error.HTTPError as e:
        return None, (e.code, json.loads(e.read()))


# ── Step 1: Ingest ────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("STEP 1 — INGEST ALL 10 FREIGHT BILLS")
print("="*60)
ingested = []
for b in BILLS:
    resp, err = post("/freight-bills", b)
    if err:
        code, body = err
        if code == 409:
            print(f"  {b['id']}  SKIP (already exists)")
            ingested.append(b["id"])
        else:
            print(f"  {b['id']}  ERROR {code}: {body.get('detail')}")
    else:
        print(f"  {b['id']}  → accepted (status: {resp.get('status')})")
        ingested.append(b["id"])

# ── Step 2: Wait for agent to process ─────────────────────────────────────────
print("\n" + "="*60)
print("STEP 2 — WAITING 12s FOR AGENT TO FINISH PROCESSING …")
print("="*60)
time.sleep(12)

# ── Step 3: Poll results ──────────────────────────────────────────────────────
print("\n" + "="*60)
print("STEP 3 — DECISION RESULTS")
print("="*60)
print(f"  {'Bill ID':<15} {'Status':<18} {'Decision':<20} {'Score':>6}  Explanation (truncated)")
print("  " + "-"*95)

pending_review = []
for fb_id in ingested:
    d = get(f"/freight-bills/{fb_id}")
    status   = d.get("status") or "-"
    decision = d.get("decision") or "-"
    score    = d.get("confidence_score")
    score_s  = f"{score:.0f}" if score is not None else "  -"
    expl     = (d.get("decision_explanation") or "")[:70]
    print(f"  {fb_id:<15} {status:<18} {decision:<20} {score_s:>6}  {expl}")
    if status == "pending_review":
        pending_review.append(fb_id)

# ── Step 4: Review queue ──────────────────────────────────────────────────────
print("\n" + "="*60)
print("STEP 4 — REVIEW QUEUE")
print("="*60)
queue = get("/review-queue")
print(f"  Bills awaiting human review: {queue['count']}")
for item in queue.get("items", []):
    print(f"    {item['id']} | score={item.get('confidence_score')} | recommendation={item.get('agent_recommendation')}")

# ── Step 5: Submit review decisions ───────────────────────────────────────────
if pending_review:
    print("\n" + "="*60)
    print("STEP 5 — SUBMITTING HUMAN REVIEW DECISIONS")
    print("="*60)
    for fb_id in pending_review:
        decision = "human_approved" if fb_id in ("FB-2025-102", "FB-2025-108") else "human_disputed"
        resp, err = post(f"/review/{fb_id}", {
            "decision": decision,
            "notes": f"Reviewed by test script",
            "reviewer_id": "test-ops-001",
        })
        if err:
            print(f"  {fb_id} review → ERROR {err}")
        else:
            print(f"  {fb_id} review → {resp.get('message')}")

    print("  Waiting 5s for agent to finalize …")
    time.sleep(5)

# ── Step 6: Metrics ───────────────────────────────────────────────────────────
print("\n" + "="*60)
print("STEP 6 — METRICS SUMMARY")
print("="*60)
m = get("/metrics")
print("  Decision distribution:")
for row in m.get("decision_distribution", []):
    print(f"    {row['decision']:<20} count={row['count']}  avg_confidence={row['avg_confidence']}")
print("  Status distribution:")
for row in m.get("status_distribution", []):
    print(f"    {row['status']:<20} count={row['count']}")
print("  Amount by decision:")
for row in m.get("amount_by_decision", []):
    print(f"    {row['decision']:<20} total=₹{row['total_amount']:,.2f}")

print("\n" + "="*60)
print("ALL DONE")
print("="*60 + "\n")
