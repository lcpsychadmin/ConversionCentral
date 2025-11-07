from __future__ import annotations

from urllib.parse import parse_qsl, urlparse

from sqlalchemy.engine import URL

from app.schemas import SystemConnectionType
from app.ingestion.engine import get_ingestion_connection_params
from app.services.databricks_sql import DatabricksConnectionParams, build_sqlalchemy_url


class UnsupportedConnectionError(ValueError):
    """Raised when a system connection cannot be converted into an SQLAlchemy URL."""


def resolve_sqlalchemy_url(connection_type: SystemConnectionType, connection_string: str) -> URL:
    if connection_type == SystemConnectionType.JDBC:
        return _convert_jdbc_to_sqlalchemy_url(connection_string)
    raise UnsupportedConnectionError(f"Unsupported connection type: {connection_type.value}")


_SUPPORTED_JDBC_DIALECTS: dict[str, str] = {
    "databricks": "databricks",
    "postgresql": "postgresql+psycopg",
    "sqlserver": "mssql+pyodbc",
    "mssql": "mssql+pyodbc",
}


def _convert_jdbc_to_sqlalchemy_url(connection_string: str) -> URL:
    if not connection_string.startswith("jdbc:"):
        raise UnsupportedConnectionError("Only JDBC connection strings are supported at this time.")

    raw_url = connection_string[len("jdbc:") :]
    parsed = urlparse(raw_url)

    if not parsed.scheme:
        raise UnsupportedConnectionError("JDBC connection string is missing a database dialect.")

    dialect = parsed.scheme.lower()
    if dialect not in _SUPPORTED_JDBC_DIALECTS:
        raise UnsupportedConnectionError(
            f"Unsupported JDBC dialect '{parsed.scheme}'. Supported dialects: {', '.join(sorted(_SUPPORTED_JDBC_DIALECTS))}."
        )

    if dialect == "databricks":
        return _convert_databricks_connection(parsed)

    drivername = _SUPPORTED_JDBC_DIALECTS[dialect]

    if not parsed.hostname:
        raise UnsupportedConnectionError("Connection string must include a hostname.")

    database = parsed.path.lstrip("/") if parsed.path else None
    if not database:
        raise UnsupportedConnectionError("Connection string must include a database name.")

    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query: dict[str, str] | None = dict(query_pairs) if query_pairs else None

    if drivername.startswith("mssql+pyodbc"):
        query = query or {}
        query.setdefault("TrustServerCertificate", "yes")
        query.setdefault("driver", "ODBC Driver 18 for SQL Server")

    return URL.create(
        drivername=drivername,
        username=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port,
        database=database,
        query=query,
    )


def _convert_databricks_connection(parsed) -> URL:
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query: dict[str, str] = dict(query_pairs) if query_pairs else {}

    try:
        params = get_ingestion_connection_params()
    except RuntimeError as exc:
        raise UnsupportedConnectionError(
            "Databricks SQL warehouse configuration is missing. Configure the warehouse via settings before browsing the catalog."
        ) from exc

    host = parsed.hostname or params.workspace_host
    http_path = query.get("http_path") or params.http_path
    catalog = query.get("catalog") or params.catalog
    schema_name = query.get("schema") or params.schema_name

    access_token = parsed.password or params.access_token
    if not access_token:
        raise UnsupportedConnectionError(
            "Databricks connection requires an access token configured via Databricks settings or environment variables."
        )

    if not host or not http_path:
        raise UnsupportedConnectionError(
            "Databricks connection requires both workspace host and http_path values."
        )

    effective = DatabricksConnectionParams(
        workspace_host=host,
        http_path=http_path,
        access_token=access_token,
        catalog=catalog,
        schema_name=schema_name,
        constructed_schema=params.constructed_schema,
        ingestion_batch_rows=params.ingestion_batch_rows,
        ingestion_method=params.ingestion_method,
    )
    return build_sqlalchemy_url(effective)
