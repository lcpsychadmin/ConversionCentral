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
            "Optional SQLAlchemy connection string override for the Databricks SQL warehouse."
            " When unset, the application composes a connection string from the Databricks"
            " workspace settings stored in the database."
        ),
    )
    databricks_host: str | None = Field(
        default=None,
        env="DATABRICKS_HOST",
        description=(
            "Default Databricks workspace host (e.g. adb-123456789012345.7.azuredatabricks.net)."
        ),
    )
    databricks_http_path: str | None = Field(
        default=None,
        env="DATABRICKS_HTTP_PATH",
        description="Default SQL warehouse HTTP path (e.g. /sql/1.0/warehouses/<warehouse-id>).",
    )
    databricks_catalog: str | None = Field(
        default=None,
        env="DATABRICKS_CATALOG",
        description="Fallback catalog to use when no catalog is stored in the database settings.",
    )
    databricks_schema: str | None = Field(
        default=None,
        env="DATABRICKS_SCHEMA",
        description="Fallback schema to use when no schema is stored in the database settings.",
    )
    databricks_constructed_schema: str | None = Field(
        default=None,
        env="DATABRICKS_CONSTRUCTED_SCHEMA",
        description="Fallback schema dedicated to constructed data tables when not stored in the database settings.",
    )
    databricks_ingestion_method: str | None = Field(
        default=None,
        env="DATABRICKS_INGESTION_METHOD",
        description="Preferred Databricks ingestion method ('sql' or 'spark') when no database setting overrides it.",
    )
    databricks_spark_compute: str | None = Field(
        default=None,
        env="DATABRICKS_SPARK_COMPUTE",
        description=(
            "Preferred compute mode for Spark ingestion ('classic' for all-purpose clusters or 'serverless' for SQL warehouses)."
        ),
    )
    databricks_ingestion_batch_rows: int | None = Field(
        default=None,
        env="DATABRICKS_INGESTION_BATCH_ROWS",
        description="Default number of rows to include in each Databricks insert batch when settings do not specify a value.",
        ge=1,
        le=100000,
    )
    databricks_token: str | None = Field(
        default=None,
        env="DATABRICKS_TOKEN",
        description="Optional personal access token used when the database has no stored token.",
    )
    enable_constructed_table_sync: bool = Field(
        False,
        env="ENABLE_CONSTRUCTED_TABLE_SYNC",
        description=(
            "When true the application will attempt to mirror constructed tables into the"
            " configured Databricks warehouse."
        ),
    )
    ingestion_run_timeout_minutes: int = Field(
        120,
        env="INGESTION_RUN_TIMEOUT_MINUTES",
        description=(
            "Maximum number of minutes an ingestion run is allowed to remain in the RUNNING"
            " state before it is marked as failed by the watchdog."
        ),
        ge=5,
        le=1440,
    )
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
            if field_name == "databricks_spark_compute" and isinstance(raw_value, str):
                lowered = raw_value.strip().lower()
                return lowered or None
            return raw_value


@lru_cache()
def get_settings() -> Settings:
    return Settings()
