"""Neo4j driver singleton with helper query methods."""
import logging
from neo4j import GraphDatabase, AsyncGraphDatabase, AsyncDriver
from app.config import settings

log = logging.getLogger(__name__)

_driver: AsyncDriver | None = None


def get_driver() -> AsyncDriver:
    global _driver
    if _driver is None:
        log.info("Neo4j driver — connecting to %s", settings.neo4j_uri)
        _driver = AsyncGraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )
    return _driver


async def close_driver():
    global _driver
    if _driver:
        log.info("Neo4j driver — closing connection")
        await _driver.close()
        _driver = None


async def init_graph_schema():
    """Create constraints and indexes for all node types."""
    driver = get_driver()
    log.info("Neo4j — creating schema constraints (idempotent)")
    async with driver.session() as session:
        constraints = [
            "CREATE CONSTRAINT carrier_id IF NOT EXISTS FOR (c:Carrier) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT contract_id IF NOT EXISTS FOR (c:Contract) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT lane_code IF NOT EXISTS FOR (l:Lane) REQUIRE l.code IS UNIQUE",
            "CREATE CONSTRAINT shipment_id IF NOT EXISTS FOR (s:Shipment) REQUIRE s.id IS UNIQUE",
            "CREATE CONSTRAINT bol_id IF NOT EXISTS FOR (b:BOL) REQUIRE b.id IS UNIQUE",
            "CREATE CONSTRAINT fb_id IF NOT EXISTS FOR (f:FreightBill) REQUIRE f.id IS UNIQUE",
        ]
        for stmt in constraints:
            label = stmt.split("CONSTRAINT ")[1].split(" ")[0]
            log.info("  Neo4j constraint: %s", label)
            await session.run(stmt)
    log.info("Neo4j — schema constraints ready")


async def run_query(query: str, params: dict | None = None) -> list[dict]:
    """Run a read query and return all records as dicts."""
    driver = get_driver()
    async with driver.session() as session:
        result = await session.run(query, params or {})
        return [dict(r) async for r in result]


async def run_write(query: str, params: dict | None = None):
    """Run a write query."""
    driver = get_driver()
    async with driver.session() as session:
        await session.run(query, params or {})
