"""Async PostgreSQL engine and session factory."""
import logging
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from app.config import settings

log = logging.getLogger(__name__)

engine = create_async_engine(settings.postgres_url, echo=False, pool_pre_ping=True)

AsyncSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


async def init_db():
    """Create all tables on startup."""
    from app.models.postgres_models import Base
    log.info("PostgreSQL — running CREATE TABLE IF NOT EXISTS for all models")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    log.info("PostgreSQL — schema ready")
