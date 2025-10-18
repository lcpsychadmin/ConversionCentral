from __future__ import annotations

from urllib.parse import parse_qsl, urlparse

from sqlalchemy.engine import URL

from app.schemas import SystemConnectionType


class UnsupportedConnectionError(ValueError):
    """Raised when a system connection cannot be converted into an SQLAlchemy URL."""


def resolve_sqlalchemy_url(connection_type: SystemConnectionType, connection_string: str) -> URL:
    if connection_type == SystemConnectionType.JDBC:
        return _convert_jdbc_to_sqlalchemy_url(connection_string)
    raise UnsupportedConnectionError(f"Unsupported connection type: {connection_type.value}")


_SUPPORTED_JDBC_DIALECTS: dict[str, str] = {
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
