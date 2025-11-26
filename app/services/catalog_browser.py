from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Iterable, Optional

from sqlalchemy import (
    MetaData,
    Table as SqlTable,
    bindparam,
    create_engine,
    inspect,
    select,
    text,
)
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


@dataclass(frozen=True)
class SourceTableColumn:
    name: str
    type_name: str
    length: int | None
    numeric_precision: int | None
    numeric_scale: int | None
    nullable: bool


def fetch_source_table_columns(
    connection_type: SystemConnectionType,
    connection_string: str,
    schema_name: str | None,
    table_name: str,
) -> list[SourceTableColumn]:
    """Return metadata about columns for a specific source table."""
    if not table_name:
        raise ConnectionCatalogError("Table name is required.")

    try:
        url = resolve_sqlalchemy_url(connection_type, connection_string)
    except UnsupportedConnectionError as exc:
        raise ConnectionCatalogError(str(exc)) from exc

    engine = create_engine(url, pool_pre_ping=True)

    try:
        inspector = inspect(engine)
        columns_info = inspector.get_columns(table_name, schema=schema_name or None)

        result: list[SourceTableColumn] = []
        for col in columns_info:
            col_type = col.get("type")
            type_name = str(col_type) if col_type else "UNKNOWN"
            
            # Extract length from string type representation if available
            length: int | None = None
            if col_type and hasattr(col_type, "length"):
                length = col_type.length  # type: ignore

            # Extract numeric precision and scale
            numeric_precision: int | None = None
            numeric_scale: int | None = None
            if col_type:
                if hasattr(col_type, "precision"):
                    numeric_precision = col_type.precision  # type: ignore
                if hasattr(col_type, "scale"):
                    numeric_scale = col_type.scale  # type: ignore

            result.append(
                SourceTableColumn(
                    name=col.get("name", ""),
                    type_name=type_name,
                    length=length,
                    numeric_precision=numeric_precision,
                    numeric_scale=numeric_scale,
                    nullable=col.get("nullable", True),
                )
            )

        return result
    except SQLAlchemyError as exc:
        raise ConnectionCatalogError(str(exc)) from exc
    finally:
        engine.dispose()


def fetch_connection_catalog(
    connection_type: SystemConnectionType,
    connection_string: str,
) -> list[CatalogTable]:
    """Return metadata about tables and views discoverable for a system connection."""

    try:
        url = resolve_sqlalchemy_url(connection_type, connection_string)
    except UnsupportedConnectionError as exc:
        raise ConnectionCatalogError(str(exc)) from exc

    drivername = (url.drivername or "").lower()
    query_params = dict(url.query) if url.query else {}

    engine_kwargs = {"pool_pre_ping": True}
    if drivername.startswith("databricks"):
        engine_kwargs["connect_args"] = {"timeout": 20}

    engine = create_engine(url, **engine_kwargs)

    try:
        if drivername.startswith("databricks"):
            return _fetch_databricks_catalog(
                engine,
                catalog=_normalize_string(query_params.get("catalog")),
                schema=_normalize_string(query_params.get("schema")),
            )

        inspector = inspect(engine)
        schema_names = _safe_get_schema_names(inspector)
        row_estimates = _fetch_row_estimates(engine, url.drivername)
        column_counts = _fetch_column_counts(engine, url.drivername, schema_names)
        is_postgres = url.drivername.startswith("postgresql")

        catalog: list[CatalogTable] = []
        excluded = {"pg_catalog", "information_schema"}

        for schema in sorted(schema_names):
            if schema in excluded or schema.startswith("pg_toast"):
                continue

            for table_name in _iter_table_like_names(inspector, schema):
                if is_postgres:
                    column_count = column_counts.get((schema, table_name.name))
                else:
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


def _fetch_column_counts(
    engine: Engine, drivername: str, schema_names: Iterable[str]
) -> dict[tuple[str, str], int]:
    if not drivername.startswith("postgresql"):
        return {}

    filtered = [schema for schema in schema_names if schema and not schema.startswith("pg_toast")]
    if not filtered:
        return {}

    # Use a single metadata query instead of per-table inspection to avoid timeouts on large catalogs.
    query = (
        text(
            """
            SELECT table_schema,
                   table_name,
                   COUNT(*) AS column_count
            FROM information_schema.columns
            WHERE table_schema IN :schemas
            GROUP BY table_schema, table_name
            """
        )
        .bindparams(bindparam("schemas", expanding=True))
    )

    counts: dict[tuple[str, str], int] = {}
    try:
        with engine.connect() as connection:
            result = connection.execute(query, {"schemas": tuple(filtered)})
            for row in result:
                schema = getattr(row, "table_schema", None)
                table = getattr(row, "table_name", None)
                value = getattr(row, "column_count", None)
                if schema and table and value is not None:
                    counts[(schema, table)] = int(value)
    except SQLAlchemyError:
        return {}

    return counts


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


def _fetch_databricks_catalog(
    engine: Engine,
    *,
    catalog: Optional[str],
    schema: Optional[str],
) -> list[CatalogTable]:
    normalized_catalog = _normalize_string(catalog)
    normalized_schema = _normalize_string(schema)

    tables_relation = _databricks_information_schema_relation("tables", normalized_catalog)
    columns_relation = _databricks_information_schema_relation("columns", normalized_catalog)

    columns = _fetch_databricks_column_counts(engine, columns_relation, normalized_schema)
    tables = _fetch_databricks_table_rows(engine, tables_relation, normalized_schema)

    catalog_entries: list[CatalogTable] = []
    for row in tables:
        schema_name = _row_value(row, "table_schema", "schema_name", "database_name")
        table_name = _row_value(row, "table_name")
        table_type = _row_value(row, "table_type")
        if not schema_name or not table_name:
            continue
        column_count = columns.get((schema_name, table_name))
        catalog_entries.append(
            CatalogTable(
                schema_name=str(schema_name),
                table_name=str(table_name),
                table_type=str(table_type) if table_type is not None else None,
                column_count=column_count,
                estimated_rows=None,
            )
        )

    return sorted(catalog_entries, key=lambda item: (item.schema_name, item.table_name))


def _fetch_databricks_table_rows(engine: Engine, relation: str, schema: Optional[str]):
    conditions: list[str] = []
    params: dict[str, str] = {}
    if schema:
        conditions.append("table_schema = :schema")
        params["schema"] = schema

    statement = f"SELECT table_schema, table_name, table_type FROM {relation}"
    if conditions:
        statement += " WHERE " + " AND ".join(conditions)
    statement += " ORDER BY table_schema, table_name"

    with engine.connect() as connection:
        result = connection.execute(text(statement), params)
        return list(result)


def _fetch_databricks_column_counts(
    engine: Engine,
    relation: str,
    schema: Optional[str],
) -> dict[tuple[str, str], int]:
    conditions: list[str] = []
    params: dict[str, str] = {}
    if schema:
        conditions.append("table_schema = :schema")
        params["schema"] = schema

    statement = f"""
        SELECT table_schema,
               table_name,
               COUNT(*) AS column_count
        FROM {relation}
    """
    if conditions:
        statement += " WHERE " + " AND ".join(conditions)
    statement += " GROUP BY table_schema, table_name"

    counts: dict[tuple[str, str], int] = {}
    with engine.connect() as connection:
        result = connection.execute(text(statement), params)
        for row in result:
            schema_name = _row_value(row, "table_schema", "schema_name", "database_name")
            table_name = _row_value(row, "table_name")
            value = _row_value(row, "column_count")
            if schema_name and table_name and value is not None:
                counts[(str(schema_name), str(table_name))] = int(value)
    return counts


def _databricks_information_schema_relation(object_name: str, catalog: Optional[str]) -> str:
    safe_object = _quote_identifier(object_name)
    info_schema = "`information_schema`"
    if catalog:
        return f"{_quote_identifier(catalog)}.{info_schema}.{safe_object}"
    return f"{info_schema}.{safe_object}"


def _quote_identifier(identifier: str) -> str:
    cleaned = (identifier or "").strip().replace("`", "")
    return f"`{cleaned}`"


def _row_value(row, *candidates: str):
    mapping = getattr(row, "_mapping", None)
    if mapping:
        normalized = {str(key).lower(): value for key, value in mapping.items()}
        for candidate in candidates:
            lowered = candidate.lower()
            if lowered in normalized and normalized[lowered] is not None:
                return normalized[lowered]
    for candidate in candidates:
        direct = getattr(row, candidate, None)
        if direct is not None:
            return direct
        upper = getattr(row, candidate.upper(), None)
        if upper is not None:
            return upper
    return None


def _normalize_string(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    trimmed = value.strip()
    return trimmed or None
