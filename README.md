# Freight Bill Processing System

A stateful agentic backend that ingests carrier freight bills, matches them to contracts and shipments, validates charges, and makes approve/dispute/review decisions — with human-in-the-loop support for low-confidence cases.

---

## How to Run Locally

### Option A — Docker Compose (recommended)

```bash
# Clone the repository
git clone https://github.com/pavanmahesh0207/logistics-operations.git
cd logistics-operations

# Add your Groq API key
echo 'GROQ_API_KEY="your_key_here"' > .env

# Start everything (PostgreSQL + Neo4j + app)
docker-compose up --build
```

The API is available at `http://localhost:8000`.
Neo4j browser: `http://localhost:7474` (user: `neo4j`, password: `logistics123`).

### Option B — Local Python (requires running Postgres + Neo4j separately)

```bash
# Start Postgres and Neo4j via Docker
docker run -d --name pg -e POSTGRES_USER=logistics -e POSTGRES_PASSWORD=logistics \
  -e POSTGRES_DB=logistics -p 5432:5432 postgres:16-alpine

docker run -d --name neo4j -e NEO4J_AUTH=neo4j/logistics123 \
  -p 7474:7474 -p 7687:7687 neo4j:5-community

# Create venv and install deps
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Set env vars
cp .env.example .env   # then edit with your Groq key

# Run the app (seed data loads automatically on startup)
uvicorn app.main:app --reload
```

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/freight-bills` | Ingest a freight bill; triggers the agent |
| `GET`  | `/freight-bills/{id}` | Current state, decision, confidence, evidence chain |
| `GET`  | `/review-queue` | Bills awaiting human review |
| `POST` | `/review/{id}` | Submit reviewer decision; resumes agent |
| `GET`  | `/metrics` | Decision distribution, status stats |
| `GET`  | `/health` | Liveness check |

### Example: ingest a freight bill

```bash
curl -X POST http://localhost:8000/freight-bills \
  -H "Content-Type: application/json" \
  -d '{
    "id": "FB-2025-101",
    "carrier_id": "CAR001",
    "carrier_name": "Safexpress Logistics",
    "bill_number": "SFX/2025/00234",
    "bill_date": "2025-02-15",
    "shipment_reference": "SHP-2025-002",
    "lane": "DEL-BLR",
    "billed_weight_kg": 850,
    "rate_per_kg": 15.00,
    "base_charge": 12750.00,
    "fuel_surcharge": 1020.00,
    "gst_amount": 2479.00,
    "total_amount": 16249.00
  }'
```

### Example: submit a reviewer decision

```bash
curl -X POST http://localhost:8000/review/FB-2025-102 \
  -H "Content-Type: application/json" \
  -d '{
    "decision": "human_approved",
    "notes": "Confirmed CC-2025-SFX-003 applies — correct rate",
    "reviewer_id": "ops-001"
  }'
```

---

## Schema Design

### PostgreSQL (relational)

Used for structured transactional data, the audit trail, and agent state that must survive queries.

| Table | Purpose |
|-------|---------|
| `carriers` | Master carrier registry |
| `carrier_contracts` | Contracts with JSONB rate cards (supports complex/variable structures) |
| `shipments` | Delivery records tied to a carrier + contract + lane |
| `bills_of_lading` | Physical delivery receipts with actual weights |
| `freight_bills` | Incoming invoices with their processing status |
| `freight_bill_decisions` | Append-only audit log — every decision written here |

**Why JSONB for rate cards?** Rate card structures vary across carriers (per-kg, FTL, alternate billing, mid-term revisions). JSONB avoids schema churn and lets the validation logic handle heterogeneous structures deterministically at runtime.

**Why `status` as a column?** The agent writes status transitions (`processing → pending_review → auto_approved`). A simple column is easier to query for the review queue than reconstructing state from events.

### Neo4j (graph)

Used for relationship traversal: _"which contracts cover this lane for this carrier on this date?"_

**Node types:**
```
(Carrier)  → [:HAS_CONTRACT]    → (Contract)
(Contract) → [:COVERS_LANE]     → (Lane)
(Shipment) → [:ASSIGNED_TO]     → (Carrier)
(Shipment) → [:COVERED_BY]      → (Contract)
(Shipment) → [:ROUTES_THROUGH]  → (Lane)
(BOL)      → [:DOCUMENTS]       → (Shipment)
(FreightBill) → [:MATCHED_TO]   → (Contract)
(FreightBill) → [:REFERENCES_SHIPMENT] → (Shipment)
(FreightBill) → [:LINKED_TO_BOL] → (BOL)
```

**Why Neo4j?** The core matching problem is a multi-hop graph query: "start at a carrier, walk to active contracts for a lane, walk to linked shipments, walk to BOLs." In a relational schema this needs 3–4 joins; in a graph it's a single `MATCH` pattern, is naturally index-backed, and extends easily (e.g., add a `Consignee` node later). Neo4j also lets you visualise the entire freight network in its browser UI.

---

## Confidence Score

The agent scores each freight bill 0–100. Components:

| Signal | Points |
|--------|--------|
| Carrier identified (id or LLM normalised) | +20 |
| Active contract found for lane + date | +20 |
| Multiple contracts overlap (ambiguity) | −10 |
| Shipment reference matches DB | +15 |
| BOL found for shipment | +10 |
| Weight within ±3% of BOL | +15 |
| Base charge within ±2% of contracted rate | +15 |
| No duplicate bill detected | +5 |
| **Penalties** | |
| Expired contract (no active one found) | −40 |
| Unknown carrier | −30 |
| Weight deviation 3–10% | −15 |
| Weight deviation >10% | −30 |
| Charge deviation 3–10% | −15 |
| Charge deviation >10% | −25 |
| Unit-of-measure mismatch (higher billing) | −10 |
| Over-billing vs shipment total | −25 |
| Duplicate bill | −80 |

**Decision thresholds:**
- ≥ 80 → `auto_approved` (no human needed)
- 40–79 → `flag_review` (interrupt → human review)
- < 40 or over-billing → `disputed` (auto-disputed)
- Duplicate → `duplicate` (auto-rejected)

The score is intentionally conservative — it's cheaper to flag a borderline bill than to auto-approve an incorrect one.

---

## Human-in-the-Loop (Interrupt / Resume Pattern)

When the agent reaches the `decide_action` node and the decision is `flag_review`, the graph routes to `human_review_node`. Inside that node:

```python
reviewer_input = interrupt({
    "freight_bill_id": fb_id,
    "confidence_score": ...,
    "agent_recommendation": ...,
    "evidence_summary": ...,
})
# Execution pauses HERE — state serialised to MemorySaver (checkpointer)
# When POST /review/{id} is called, Command(resume=reviewer_payload) restarts here
return {"reviewer_decision": reviewer_input, ...}
```

The `interrupt()` call (LangGraph 1.x) serialises the entire graph state to the checkpointer and raises an internal exception that `ainvoke` catches — returning control to the API handler. The freight bill's Postgres status is set to `pending_review`.

When `POST /review/{id}` is called:
```python
await freight_bill_graph.ainvoke(
    Command(resume=reviewer_payload),
    config={"configurable": {"thread_id": fb_id}},
)
```
LangGraph restores the saved state, the `interrupt()` call returns `reviewer_payload`, and execution continues into `finalize_node` which writes the human decision to the audit trail.

**Checkpointer trade-off:** The current implementation uses `MemorySaver` (in-process memory). State is lost on server restart. For production, swap in `PostgresSaver` from `langgraph-checkpoint-postgres` — it persists checkpoints to the same Postgres instance.

---

## What I'd Do Differently With More Time

1. **Persistent checkpointer** — Replace `MemorySaver` with `PostgresSaver` so interrupt state survives restarts.
2. **Background worker** — Move agent invocation from FastAPI `BackgroundTasks` to Celery/ARQ/Redis queue for reliable delivery and retries.
3. **Webhook / SSE** — Push decision results to callers instead of polling `GET /freight-bills/{id}`.
4. **Rate card versioning** — Track changes to contract rate cards over time (currently updated in-place).
5. **GCP deployment** — Cloud Run for the API, Cloud SQL for Postgres, Neo4j AuraDB (or self-hosted on GCE), Secret Manager for the Groq key.
6. **More test coverage** — Parametrised tests for each of the 10 seed scenarios.

---

## Deliberate Trade-offs

| Decision | Rationale |
|----------|-----------|
| MemorySaver over PostgresSaver | Zero extra setup; sufficient for a local demo. Documented upgrade path. |
| JSONB rate cards in Postgres | Avoids creating a rigid `rate_tiers` table for a format that changes per carrier. |
| GST hardcoded at 18% | Freight GST in India is uniformly 18%; parameterised if needed. |
| Background agent execution | `ainvoke` is synchronous w.r.t. graph state but non-blocking from the API's perspective. Simpler than a task queue for this scope. |
| `interrupt_before` skipped in favour of `interrupt()` inside the node | Lets the node receive the reviewer's decision as a return value — cleaner than out-of-band state mutation. |
