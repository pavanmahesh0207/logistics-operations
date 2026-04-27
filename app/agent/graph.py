"""
Build and compile the freight-bill processing LangGraph.

Graph flow:
  START
    ↓
  normalize_carrier          (LLM: fuzzy-match carrier name)
    ↓
  match_entities             (Neo4j: find carrier / contracts / shipment / BOLs)
    ↓
  check_duplicate            (Postgres: detect re-submissions)
    ↓
  validate_charges           (deterministic: weight, rate, date, over-billing)
    ↓
  compute_confidence         (score 0–100)
    ↓
  generate_explanation       (LLM: plain-language rationale)
    ↓
  decide_action              (route based on score + signals)
    ↓
  ┌──────────────────────────────────────────────────┐
  │  auto_approved / disputed / duplicate            │  → finalize
  │  flag_review                                     │  → human_review → finalize
  └──────────────────────────────────────────────────┘
    ↓
  END
"""
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver

from app.agent.state import FreightBillState
from app.agent.nodes import (
    normalize_carrier_node,
    match_entities_node,
    check_duplicate_node,
    validate_charges_node,
    compute_confidence_node,
    generate_explanation_node,
    decide_action_node,
    human_review_node,
    finalize_node,
)

# ── In-memory checkpointer ────────────────────────────────────────────────────
# Persists interrupt state for the duration of the server process.
# Trade-off: state is lost on server restart.
# Production upgrade: swap MemorySaver for PostgresSaver (langgraph-checkpoint-postgres).
checkpointer = MemorySaver()


def _route_after_decide(state: FreightBillState) -> str:
    decision = state.get("decision", "flag_review")
    if decision in ("auto_approved", "disputed", "duplicate"):
        return "finalize"
    return "human_review"


def build_graph():
    builder = StateGraph(FreightBillState)

    builder.add_node("normalize_carrier",    normalize_carrier_node)
    builder.add_node("match_entities",       match_entities_node)
    builder.add_node("check_duplicate",      check_duplicate_node)
    builder.add_node("validate_charges",     validate_charges_node)
    builder.add_node("compute_confidence",   compute_confidence_node)
    builder.add_node("generate_explanation", generate_explanation_node)
    builder.add_node("decide_action",        decide_action_node)
    builder.add_node("human_review",         human_review_node)
    builder.add_node("finalize",             finalize_node)

    builder.add_edge(START,                  "normalize_carrier")
    builder.add_edge("normalize_carrier",    "match_entities")
    builder.add_edge("match_entities",       "check_duplicate")
    builder.add_edge("check_duplicate",      "validate_charges")
    builder.add_edge("validate_charges",     "compute_confidence")
    builder.add_edge("compute_confidence",   "generate_explanation")
    builder.add_edge("generate_explanation", "decide_action")

    builder.add_conditional_edges(
        "decide_action",
        _route_after_decide,
        {"finalize": "finalize", "human_review": "human_review"},
    )

    builder.add_edge("human_review", "finalize")
    builder.add_edge("finalize",     END)

    # interrupt() is called explicitly inside human_review_node — no interrupt_before needed
    return builder.compile(checkpointer=checkpointer)


# Module-level singleton — created once on import
freight_bill_graph = build_graph()
