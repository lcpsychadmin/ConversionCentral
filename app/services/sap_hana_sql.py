from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Optional

try:  # pragma: no cover - optional dependency for dialect registration
    import sqlalchemy_hana.dialect  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover - dialect optional in some environments
    _HAS_HANA_DIALECT = False
else:  # pragma: no cover - import has side effects only
    _HAS_HANA_DIALECT = True

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError


@dataclass(frozen=True)
class SapHanaConnectionParams:
    host: str
    port: int
    username: str
    password: str
    database_name: str
    schema_name: Optional[str] = None
    tenant: Optional[str] = None
    use_ssl: bool = True
    ingestion_batch_rows: Optional[int] = None


def build_sqlalchemy_url(params: SapHanaConnectionParams) -> URL:
    query: dict[str, str] = {"databaseName": params.database_name}
    if params.tenant:
        query["tenant"] = params.tenant
    if params.schema_name:
        query["currentSchema"] = params.schema_name
    if params.use_ssl:
        query["encrypt"] = "true"
    return URL.create(
        drivername="hana",
        username=params.username,
        password=params.password,
        host=params.host,
        port=params.port,
        database=params.database_name,
        query=query,
    )


def sanitize_url(url: URL) -> str:
    return url.render_as_string(hide_password=True)


class SapHanaConnectionError(Exception):
    """Raised when SAP HANA connectivity checks fail."""


def test_sap_hana_connection(
    params: SapHanaConnectionParams,
    *,
    timeout_seconds: int = 10,
) -> tuple[float, str]:
    if not _HAS_HANA_DIALECT:
        raise SapHanaConnectionError(
            "SAP HANA SQLAlchemy dialect is not installed. Install 'sqlalchemy-hana' to enable connection testing."
        )

    url = build_sqlalchemy_url(params)
    connect_args = {"timeout": timeout_seconds}
    engine = create_engine(url, pool_pre_ping=True, connect_args=connect_args)

    start = perf_counter()
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1 FROM DUMMY"))
    except SQLAlchemyError as exc:  # pragma: no cover - requires real HANA connection
        sanitized = sanitize_url(url)
        message = str(getattr(exc, "orig", exc)) or "Unknown error"
        raise SapHanaConnectionError(
            f"Unable to connect to SAP HANA warehouse for {sanitized}: {message}"
        ) from exc
    finally:
        engine.dispose()

    elapsed_ms = (perf_counter() - start) * 1000.0
    return elapsed_ms, sanitize_url(url)
