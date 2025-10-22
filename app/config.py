from functools import lru_cache
from typing import List

from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    app_name: str = Field("Conversion Central API", env="APP_NAME")
    database_url: str = Field(
        "postgresql+psycopg2://postgres:postgres@localhost:5432/conversion_central",
        env="DATABASE_URL",
    )
    ingestion_database_url: str | None = Field(
        default=None,
        env="INGESTION_DATABASE_URL",
        description=(
            "SQL Server connection string used for storing ingested data."
            " Example: mssql+pyodbc://sa:YourStrong!Passw0rd@localhost:1433/conversion_ingestion"
            "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        ),
    )
    enable_sql_server_sync: bool = Field(True, env="ENABLE_SQL_SERVER_SYNC")
    frontend_origins: List[str] = Field(
        default_factory=lambda: [
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "https://wescollins.duckdns.org",
            "http://wescollins.duckdns.org",
        ],
        env="FRONTEND_ORIGINS",
        description="Comma-separated list of allowed CORS origins for the frontend UI.",
    )

    class Config:
        env_file = ".env"
        case_sensitive = False

        @classmethod
        def parse_env_var(cls, field_name, raw_value):
            if field_name == "frontend_origins" and isinstance(raw_value, str):
                return [origin.strip() for origin in raw_value.split(",") if origin.strip()]
            return raw_value


@lru_cache()
def get_settings() -> Settings:
    return Settings()
