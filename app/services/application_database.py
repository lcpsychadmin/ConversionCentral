from __future__ import annotations

from pathlib import Path
from time import perf_counter
from typing import Optional

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select, text, update
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.models import ApplicationDatabaseSetting
from app.schemas import (
    ApplicationDatabaseApplyRequest,
    ApplicationDatabaseConnectionInput,
    ApplicationDatabaseEngine,
    ApplicationDatabaseTestRequest,
    ApplicationDatabaseTestResult,
)


class ApplicationDatabaseError(Exception):
    """Base exception for application database operations."""


class ApplicationDatabaseConnectionError(ApplicationDatabaseError):
    """Raised when validating connectivity to the selected database fails."""


class ApplicationDatabaseMigrationError(ApplicationDatabaseError):
    """Raised when Alembic migrations cannot be applied to the selected database."""


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ALEMBIC_INI = _PROJECT_ROOT / "alembic.ini"


def get_active_setting(db: Session) -> ApplicationDatabaseSetting | None:
    stmt = (
        select(ApplicationDatabaseSetting)
        .order_by(ApplicationDatabaseSetting.is_active.desc(), ApplicationDatabaseSetting.applied_at.desc())
        .limit(1)
    )
    return db.execute(stmt).scalars().first()


def test_application_database(
    payload: ApplicationDatabaseTestRequest,
    *,
    settings: Optional[Settings] = None,
) -> ApplicationDatabaseTestResult:
    resolved_settings = settings or get_settings()

    try:
        target_url, _, display_name = _resolve_connection_details(
            payload.engine, payload.connection, resolved_settings
        )
        latency_ms = _probe_database(target_url, payload.engine)
    except ApplicationDatabaseConnectionError as exc:
        return ApplicationDatabaseTestResult(success=False, message=str(exc))

    return ApplicationDatabaseTestResult(
        success=True,
        message=f"Connection successful for {display_name}.",
        latency_ms=latency_ms,
    )


def apply_application_database_setting(
    db: Session,
    payload: ApplicationDatabaseApplyRequest,
    *,
    settings: Optional[Settings] = None,
) -> ApplicationDatabaseSetting:
    resolved_settings = settings or get_settings()

    target_url, stored_url, connection_display = _resolve_connection_details(
        payload.engine, payload.connection, resolved_settings
    )
    _probe_database(target_url, payload.engine)
    _run_migrations(target_url)

    display_name = payload.display_name.strip() if payload.display_name else None

    try:
        db.execute(update(ApplicationDatabaseSetting).values(is_active=False))
        setting = ApplicationDatabaseSetting(
            engine=payload.engine.value,
            connection_url=stored_url,
            connection_display=connection_display,
            is_active=True,
            display_name=display_name,
        )
        db.add(setting)
        db.commit()
    except Exception:
        db.rollback()
        raise

    db.refresh(setting)

    from app.database import refresh_runtime_engine

    refresh_runtime_engine(target_url)
    return setting


def _resolve_connection_details(
    engine: ApplicationDatabaseEngine,
    connection: Optional[ApplicationDatabaseConnectionInput],
    settings: Settings,
) -> tuple[str, str | None, str]:
    if engine is ApplicationDatabaseEngine.DEFAULT_POSTGRES:
        target_url = settings.database_url
        display = _sanitize_url(target_url)
        return target_url, None, display

    if not connection:
        raise ApplicationDatabaseConnectionError("Connection details are required for the selected engine.")

    if engine is ApplicationDatabaseEngine.CUSTOM_POSTGRES:
        target_url = _build_postgres_url(connection)
    elif engine is ApplicationDatabaseEngine.SQLSERVER:
        target_url = _build_sqlserver_url(connection)
    else:
        raise ApplicationDatabaseConnectionError(f"Unsupported application database engine: {engine}.")

    display = _sanitize_url(target_url)
    return target_url, target_url, display


def _build_postgres_url(connection: ApplicationDatabaseConnectionInput) -> str:
    options = _normalize_options(connection.options)
    if connection.use_ssl:
        options.setdefault("sslmode", "require")

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=(connection.username or "").strip() or None,
        password=(connection.password or "").strip() or None,
        host=(connection.host or "").strip() or None,
        port=connection.port or 5432,
        database=(connection.database or "").strip() or None,
        query=options or None,
    )
    return str(url)


def _build_sqlserver_url(connection: ApplicationDatabaseConnectionInput) -> str:
    options = _normalize_options(connection.options)
    if connection.use_ssl:
        options.setdefault("Encrypt", "yes")
        options.setdefault("TrustServerCertificate", "no")
    options.setdefault("driver", "ODBC Driver 18 for SQL Server")

    url = URL.create(
        drivername="mssql+pyodbc",
        username=(connection.username or "").strip() or None,
        password=(connection.password or "").strip() or None,
        host=(connection.host or "").strip() or None,
        port=connection.port or 1433,
        database=(connection.database or "").strip() or None,
        query=options,
    )
    return str(url)


def _normalize_options(options: Optional[dict[str, str]]) -> dict[str, str]:
    if not options:
        return {}
    normalized: dict[str, str] = {}
    for key, value in options.items():
        if value is None:
            continue
        stripped = value.strip()
        if stripped:
            normalized[key] = stripped
    return normalized


def _probe_database(target_url: str, _engine_choice: ApplicationDatabaseEngine, timeout_seconds: int = 5) -> float:
    url = make_url(target_url)
    connect_args: dict[str, object] = {}

    if url.drivername.startswith("postgresql"):
        connect_args["connect_timeout"] = timeout_seconds
    elif url.drivername.startswith("mssql"):
        connect_args["timeout"] = timeout_seconds

    sqlalchemy_engine = create_engine(
        target_url,
        pool_pre_ping=True,
        future=True,
        connect_args=connect_args,
    )
    start_time = perf_counter()

    try:
        with sqlalchemy_engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        sanitized = _sanitize_url(target_url)
        message = str(getattr(exc, "orig", exc))
        raise ApplicationDatabaseConnectionError(f"Unable to connect to {sanitized}: {message}") from exc
    finally:
        sqlalchemy_engine.dispose()

    elapsed_ms = (perf_counter() - start_time) * 1000.0
    return elapsed_ms


def _run_migrations(database_url: str) -> None:
    url = make_url(database_url)

    if url.drivername.startswith("sqlite"):
        # Tests rely on SQLite; Alembic scripts target Postgres features.
        return

    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", database_url)
    config.set_main_option("script_location", str(_PROJECT_ROOT / "migrations"))
    config.attributes["configure_logger"] = False

    try:
        command.upgrade(config, "head")
    except Exception as exc:  # Alembic does not expose a common base error
        raise ApplicationDatabaseMigrationError(f"Failed to apply migrations: {exc}") from exc


def _sanitize_url(url: str) -> str:
    return make_url(url).render_as_string(hide_password=True)
