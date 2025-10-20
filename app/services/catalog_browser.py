from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Iterable

from sqlalchemy import MetaData, Table as SqlTable, create_engine, inspect, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.schemas import SystemConnectionType
from app.services.connection_resolver import (
    UnsupportedConnectionError,
    resolve_sqlalchemy_url,
)


class ConnectionCatalogError(Exception):
    """Raised when browsing a source catalog fails."""


@dataclass(frozen=True)
class CatalogTable:
    schema_name: str
    table_name: str
    table_type: str | None
    column_count: int | None
    estimated_rows: int | None


class TablePreviewError(Exception):
    """Raised when retrieving sample data from a source table fails."""


@dataclass(frozen=True)
class TablePreview:
    columns: list[str]
    rows: list[dict[str, object]]


def fetch_connection_catalog(
    connection_type: SystemConnectionType,
    connection_string: str,
) -> list[CatalogTable]:
    """Return metadata about tables and views discoverable for a system connection."""

    try:
        url = resolve_sqlalchemy_url(connection_type, connection_string)
    except UnsupportedConnectionError as exc:
        raise ConnectionCatalogError(str(exc)) from exc

    engine = create_engine(url, pool_pre_ping=True)

    try:
        inspector = inspect(engine)
        schema_names = _safe_get_schema_names(inspector)
        row_estimates = _fetch_row_estimates(engine, url.drivername)

        catalog: list[CatalogTable] = []
        excluded = {"pg_catalog", "information_schema"}

        for schema in sorted(schema_names):
            if schema in excluded or schema.startswith("pg_toast"):
                continue

            for table_name in _iter_table_like_names(inspector, schema):
                column_count = _safe_column_count(inspector, table_name.name, schema)
                estimated_rows = row_estimates.get((schema, table_name.name))
                catalog.append(
                    CatalogTable(
                        schema_name=schema,
                        table_name=table_name.name,
                        table_type=table_name.kind,
                        column_count=column_count,
                        estimated_rows=estimated_rows,
                    )
                )

        return sorted(catalog, key=lambda item: (item.schema_name, item.table_name))
    except SQLAlchemyError as exc:
        raise ConnectionCatalogError(str(exc)) from exc
    finally:
        engine.dispose()


def fetch_table_preview(
    connection_type: SystemConnectionType,
    connection_string: str,
    schema_name: str | None,
    table_name: str,
    limit: int = 100,
) -> TablePreview:
    if not table_name:
        raise TablePreviewError("Table name is required for preview.")
    if limit < 1:
        raise TablePreviewError("Preview limit must be positive.")

    try:
        url = resolve_sqlalchemy_url(connection_type, connection_string)
    except UnsupportedConnectionError as exc:
        raise TablePreviewError(str(exc)) from exc

    engine = create_engine(url, pool_pre_ping=True, future=True)

    try:
        metadata = MetaData()
        table = SqlTable(
            table_name,
            metadata,
            schema=schema_name or None,
            autoload_with=engine,
        )
        stmt = select(table).limit(limit)

        rows: list[dict[str, object]] = []
        columns: list[str] = []

        with engine.connect() as connection:
            result = connection.execute(stmt)
            columns = list(result.keys())
            for mapped in result.mappings():
                rows.append({key: _serialize_preview_value(value) for key, value in mapped.items()})

        return TablePreview(columns=columns, rows=rows)
    except SQLAlchemyError as exc:
        raise TablePreviewError(str(exc)) from exc
    finally:
        engine.dispose()


@dataclass(frozen=True)
class _TableLike:
    name: str
    kind: str


def _iter_table_like_names(inspector, schema: str) -> Iterable[_TableLike]:  # pragma: no cover - thin wrappers
    try:
        for table_name in inspector.get_table_names(schema=schema):
            yield _TableLike(table_name, "table")
    except SQLAlchemyError:
        pass

    try:
        for view_name in inspector.get_view_names(schema=schema):
            yield _TableLike(view_name, "view")
    except SQLAlchemyError:
        pass

    get_materialized = getattr(inspector, "get_materialized_view_names", None)
    if callable(get_materialized):
        try:
            for mat_view in get_materialized(schema=schema):
                yield _TableLike(mat_view, "materialized_view")
        except SQLAlchemyError:
            pass


def _safe_get_schema_names(inspector) -> list[str]:
    try:
        return list(inspector.get_schema_names())
    except SQLAlchemyError as exc:  # pragma: no cover - inspector behaviour
        raise ConnectionCatalogError(str(exc)) from exc


def _safe_column_count(inspector, table_name: str, schema: str) -> int | None:
    try:
        columns = inspector.get_columns(table_name, schema=schema)
    except SQLAlchemyError:
        return None
    return len(columns)


def _fetch_row_estimates(engine: Engine, drivername: str) -> dict[tuple[str, str], int]:
    if not drivername.startswith("postgresql"):
        return {}

    query = text(
        """
        SELECT ns.nspname AS schema_name,
               cls.relname AS table_name,
               COALESCE(cls.reltuples, 0)::bigint AS row_estimate
        FROM pg_class AS cls
        JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace
        WHERE cls.relkind IN ('r', 'p', 'm', 'f', 'v')
        """
    )

    estimates: dict[tuple[str, str], int] = {}
    try:
        with engine.connect() as connection:
            result = connection.execute(query)
            for row in result:
                schema = getattr(row, "schema_name", None)
                table = getattr(row, "table_name", None)
                value = getattr(row, "row_estimate", None)
                if schema and table and value is not None:
                    estimates[(schema, table)] = int(value)
    except SQLAlchemyError:
        return {}

    return estimates


def _serialize_preview_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    return value
