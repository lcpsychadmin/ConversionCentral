from functools import lru_cache
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

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
