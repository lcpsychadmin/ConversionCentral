from __future__ import annotations

from time import perf_counter
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from app.schemas.entities import SystemConnectionType
from app.services.connection_resolver import (
    UnsupportedConnectionError,
    resolve_sqlalchemy_url,
)


class ConnectionTestError(Exception):
    """Raised when validating or testing a system connection fails."""


def test_connection(
    connection_type: SystemConnectionType,
    connection_string: str,
    *,
    timeout_seconds: int = 5,
) -> tuple[float, str]:
    """Attempt to establish a connection and run a simple validation query.

    Returns the elapsed time in milliseconds and the sanitized URL (without password)
    when successful. Raises :class:`ConnectionTestError` on failure.
    """

    try:
        url = resolve_sqlalchemy_url(connection_type, connection_string)
    except UnsupportedConnectionError as exc:
        raise ConnectionTestError(str(exc)) from exc

    connect_args: dict[str, Any] = {}
    if url.drivername.startswith("postgresql"):
        connect_args["connect_timeout"] = timeout_seconds
    elif url.drivername.startswith("mssql+pyodbc"):
        connect_args["timeout"] = timeout_seconds

    engine = create_engine(url, pool_pre_ping=True, connect_args=connect_args)

    start = perf_counter()
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        sanitized = url.render_as_string(hide_password=True)
        message = str(getattr(exc, "orig", exc))
        raise ConnectionTestError(
            f"Unable to reach database for {sanitized}: {message}"
        ) from exc
    finally:
        engine.dispose()

    elapsed_ms = (perf_counter() - start) * 1000.0
    return elapsed_ms, url.render_as_string(hide_password=True)