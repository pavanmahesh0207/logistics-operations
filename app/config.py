from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # PostgreSQL
    postgres_url: str = "postgresql+asyncpg://logistics:logistics@localhost:5432/logistics"
    postgres_sync_url: str = "postgresql://logistics:logistics@localhost:5432/logistics"

    # Neo4j
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "logistics123"

    # Groq LLM
    groq_api_key: str = ""
    groq_model: str = "llama-3.3-70b-versatile"

    # Agent thresholds
    auto_approve_threshold: float = 80.0
    auto_dispute_threshold: float = 40.0

    # GST rate
    gst_rate: float = 0.18

    # Tolerance bands
    weight_tolerance_pct: float = 3.0   # ±3%
    charge_tolerance_pct: float = 2.0   # ±2%


settings = Settings()
