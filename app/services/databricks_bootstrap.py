"""Helpers for auto-creating managed Databricks resources."""

from __future__ import annotations

import logging
from typing import Optional, Sequence
from urllib.parse import urlencode

from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import NoSuchModuleError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.ingestion.engine import get_ingestion_connection_params
from app.models import System, SystemConnection
from app.services.data_quality_metadata import ensure_data_quality_metadata
from app.services.databricks_sql import DatabricksConnectionParams, build_sqlalchemy_url

logger = logging.getLogger(__name__)

_MANAGED_SYSTEM_NAME = "Databricks Warehouse"
_MANAGED_SYSTEM_PHYSICAL_NAME = "databricks_warehouse"
_MANAGED_SYSTEM_DESCRIPTION = "Managed Databricks SQL warehouse used for constructed data."  # noqa: E501
_MANAGED_CONNECTION_NOTES = (
    "Managed Databricks SQL warehouse connection. Credentials are resolved from Databricks settings."
)


def build_databricks_connection_string(
    *,
    host: str,
    http_path: str,
    catalog: Optional[str],
    schema_name: Optional[str],
) -> str:
    query: dict[str, str] = {"http_path": http_path}
    if catalog:
        query["catalog"] = catalog
    if schema_name:
        query["schema"] = schema_name

    parameters = urlencode(query)
    return f"jdbc:databricks://token:@{host}:443/default?{parameters}"


def get_managed_databricks_connection_string(
    *, catalog_override: Optional[str] = None, schema_override: Optional[str] = None
) -> str:
    """Return the JDBC connection string for the managed Databricks warehouse."""

    params = get_ingestion_connection_params()
    catalog = (
        catalog_override.strip()
        if isinstance(catalog_override, str) and catalog_override.strip()
        else params.catalog
    )
    explicit_schema = (
        schema_override.strip() if isinstance(schema_override, str) and schema_override.strip() else None
    )
    schema_name = explicit_schema if explicit_schema else params.schema_name

    if catalog and not explicit_schema:
        schema_name = None
    return build_databricks_connection_string(
        host=params.workspace_host,
        http_path=params.http_path,
        catalog=catalog,
        schema_name=schema_name,
    )


def _ensure_system(session: Session) -> System:
    system = (
        session.execute(
            select(System).where(System.name == _MANAGED_SYSTEM_NAME)
        )
        .scalars()
        .first()
    )

    if system is None:
        system = System(
            name=_MANAGED_SYSTEM_NAME,
            physical_name=_MANAGED_SYSTEM_PHYSICAL_NAME,
            description=_MANAGED_SYSTEM_DESCRIPTION,
            system_type="warehouse",
            status="active",
        )
        session.add(system)
        session.flush()
        return system

    changed = False

    if system.physical_name != _MANAGED_SYSTEM_PHYSICAL_NAME:
        system.physical_name = _MANAGED_SYSTEM_PHYSICAL_NAME
        changed = True
    if system.description != _MANAGED_SYSTEM_DESCRIPTION:
        system.description = _MANAGED_SYSTEM_DESCRIPTION
        changed = True
    if system.system_type != "warehouse":
        system.system_type = "warehouse"
        changed = True
    if system.status != "active":
        system.status = "active"
        changed = True

    if changed:
        session.add(system)

    return system


def _ensure_connection(session: Session, system: System, connection_string: str) -> None:
    connection = (
        session.execute(
            select(SystemConnection).where(SystemConnection.system_id == system.id)
        )
        .scalars()
        .first()
    )

    if connection is None:
        session.add(
            SystemConnection(
                system_id=system.id,
                connection_type="jdbc",
                connection_string=connection_string,
                auth_method="username_password",
                active=True,
                ingestion_enabled=False,
                notes=_MANAGED_CONNECTION_NOTES,
            )
        )
        return

    changed = False

    if connection.connection_type != "jdbc":
        connection.connection_type = "jdbc"
        changed = True
    if connection.connection_string != connection_string:
        connection.connection_string = connection_string
        changed = True
    if connection.auth_method != "username_password":
        connection.auth_method = "username_password"
        changed = True
    if connection.active is not True:
        connection.active = True
        changed = True
    if connection.ingestion_enabled is not False:
        connection.ingestion_enabled = False
        changed = True
    if connection.notes != _MANAGED_CONNECTION_NOTES:
        connection.notes = _MANAGED_CONNECTION_NOTES
        changed = True

    if changed:
        session.add(connection)


def ensure_databricks_connection() -> None:
    """Ensure a managed Databricks system and connection record exist."""

    try:
        params = get_ingestion_connection_params()
    except RuntimeError as exc:
        logger.info("Skipping Databricks connection bootstrap: %s", exc)
        return

    host = params.workspace_host.strip()
    http_path = params.http_path.strip()

    if not host or not http_path:
        logger.info("Databricks configuration incomplete; skipping managed connection bootstrap.")
        return

    ensure_databricks_schemas(params)
    ensure_data_quality_metadata(params)

    connection_string = build_databricks_connection_string(
        host=host,
        http_path=http_path,
        catalog=params.catalog,
        schema_name=params.schema_name,
    )

    with SessionLocal() as session:
        system = _ensure_system(session)
        _ensure_connection(session, system, connection_string)
        session.commit()


def ensure_databricks_schemas(params: DatabricksConnectionParams) -> None:
    """Create the configured Databricks schemas if they do not already exist."""

    schemas = _collect_schema_targets(params)
    if not schemas:
        return

    try:
        engine = create_engine(build_sqlalchemy_url(params), pool_pre_ping=True, future=True)
    except NoSuchModuleError as exc:  # pragma: no cover - depends on optional dialect
        logger.warning("Databricks dialect unavailable; skipping schema bootstrap: %s", exc)
        return

    try:
        with engine.begin() as connection:
            for schema in schemas:
                identifier = _format_schema_identifier(params.catalog, schema)
                statement = text(f"CREATE SCHEMA IF NOT EXISTS {identifier}")
                try:
                    connection.execute(statement)
                except SQLAlchemyError as exc:
                    logger.warning("Unable to create Databricks schema %s: %s", identifier, exc)
    finally:
        engine.dispose()


def drop_databricks_schemas(
    params: DatabricksConnectionParams,
    schemas: Sequence[str] | None = None,
) -> None:
    """Drop Databricks schemas that were provisioned by the application."""

    if schemas is None:
        targets = _collect_schema_targets(params)
    else:
        targets = []
        for candidate in schemas:
            if not candidate:
                continue
            normalized = candidate.strip()
            if not normalized:
                continue
            if normalized not in targets:
                targets.append(normalized)

    if not targets:
        return

    try:
        engine = create_engine(build_sqlalchemy_url(params), pool_pre_ping=True, future=True)
    except NoSuchModuleError as exc:  # pragma: no cover - depends on optional dialect
        logger.warning("Databricks dialect unavailable; skipping schema teardown: %s", exc)
        return

    try:
        with engine.begin() as connection:
            for schema in targets:
                identifier = _format_schema_identifier(params.catalog, schema)
                statement = text(f"DROP SCHEMA IF EXISTS {identifier} CASCADE")
                try:
                    connection.execute(statement)
                except SQLAlchemyError as exc:
                    logger.warning("Unable to drop Databricks schema %s: %s", identifier, exc)
    finally:
        engine.dispose()


def delete_managed_databricks_system() -> None:
    """Remove the managed system + connection records created for Databricks."""

    with SessionLocal() as session:
        system = (
            session.execute(select(System).where(System.name == _MANAGED_SYSTEM_NAME))
            .scalars()
            .first()
        )
        if not system:
            return
        session.delete(system)
        session.commit()


def teardown_databricks_environment(params: DatabricksConnectionParams | None) -> None:
    """Drop managed schemas and metadata when a connection is deleted."""

    if params:
        try:
            drop_databricks_schemas(params)
        except Exception as exc:  # pragma: no cover - defensive logging only
            logger.warning("Unable to drop Databricks schemas during teardown: %s", exc)

    delete_managed_databricks_system()


def _collect_schema_targets(params: DatabricksConnectionParams) -> list[str]:
    seen: list[str] = []
    for candidate in (params.schema_name, params.constructed_schema, params.data_quality_schema):
        if not candidate:
            continue
        normalized = candidate.strip()
        if not normalized:
            continue
        if normalized not in seen:
            seen.append(normalized)
    return seen


def _format_schema_identifier(catalog: str | None, schema: str) -> str:
    if catalog:
        return f"{_escape_identifier(catalog)}.{_escape_identifier(schema)}"
    return _escape_identifier(schema)


def _escape_identifier(identifier: str) -> str:
    cleaned = identifier.strip().replace("`", "")
    return f"`{cleaned}`"
