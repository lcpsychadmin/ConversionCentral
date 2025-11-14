from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Optional

try:
    import databricks.sqlalchemy  # noqa: F401 ensures SQLAlchemy dialect registration
except ModuleNotFoundError:  # pragma: no cover - optional dependency for dialect registration
    _HAS_DATABRICKS_DIALECT = False
else:
    _HAS_DATABRICKS_DIALECT = True
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError


@dataclass(frozen=True)
class DatabricksConnectionParams:
    workspace_host: str
    http_path: str
    access_token: str
    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    constructed_schema: Optional[str] = None
    ingestion_batch_rows: Optional[int] = None
    ingestion_method: str = "sql"
    spark_compute: Optional[str] = None


def build_sqlalchemy_url(params: DatabricksConnectionParams) -> URL:
    """Compose a SQLAlchemy URL for a Databricks SQL warehouse."""
    query: dict[str, str] = {"http_path": params.http_path}
    if params.catalog:
        query["catalog"] = params.catalog
    if params.schema_name:
        query["schema"] = params.schema_name

    return URL.create(
        drivername="databricks",
        username="token",
        password=params.access_token,
        host=params.workspace_host,
        port=443,
        database="default",
        query=query,
    )


def sanitize_url(url: URL) -> str:
    """Render the URL without exposing secrets for logging or responses."""
    return url.render_as_string(hide_password=True)


class DatabricksConnectionError(Exception):
    """Raised when testing a Databricks connection fails."""


def test_databricks_connection(
    params: DatabricksConnectionParams,
    *,
    timeout_seconds: int = 10,
) -> tuple[float, str]:
    """Attempt to open a Databricks connection and execute a lightweight probe."""

    if not _HAS_DATABRICKS_DIALECT:
        raise DatabricksConnectionError(
            "Databricks SQL dependencies are not installed. Install 'databricks-sql-connector[sqlalchemy]' to enable connection testing."
        )

    url = build_sqlalchemy_url(params)
    connect_args = {"connect_timeout": timeout_seconds}
    engine = create_engine(url, pool_pre_ping=True, connect_args=connect_args)

    start = perf_counter()
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        sanitized = sanitize_url(url)
        message = str(getattr(exc, "orig", exc)) or "Unknown error"
        raise DatabricksConnectionError(
            f"Unable to connect to Databricks warehouse for {sanitized}: {message}"
        ) from exc
    finally:
        engine.dispose()

    elapsed_ms = (perf_counter() - start) * 1000.0
    return elapsed_ms, sanitize_url(url)
