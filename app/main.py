"""
FastAPI application entry point.

Startup sequence:
1. Create PostgreSQL tables (if not exist)
2. Init Neo4j schema constraints
3. Seed reference data from seed_data_logistics.json
4. Start accepting requests
"""
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.db.postgres import init_db
from app.db.neo4j_db import init_graph_schema, close_driver
from app.api.routes import router
from seed import run_seed

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
# Quieten noisy third-party loggers so agent steps stand out
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("groq").setLevel(logging.WARNING)
logging.getLogger("neo4j").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Initialising PostgreSQL schema …")
    await init_db()

    log.info("Initialising Neo4j constraints …")
    await init_graph_schema()

    log.info("Running seed …")
    await run_seed()

    log.info("Application ready.")
    yield

    log.info("Closing Neo4j driver …")
    await close_driver()


app = FastAPI(
    title="Freight Bill Processing API",
    description=(
        "Stateful LangGraph agent that matches, validates, and decides on freight bills "
        "using PostgreSQL (relational) + Neo4j (graph) data layers."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.get("/health")
async def health():
    return {"status": "ok", "groq_model": settings.groq_model}


@app.get("/", include_in_schema=False)
async def serve_frontend():
    return FileResponse(FRONTEND_DIR / "index.html")
