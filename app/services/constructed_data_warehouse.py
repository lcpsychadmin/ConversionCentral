from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import re
from typing import Any, Iterable
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.config import get_settings
from app.ingestion.engine import (
    get_ingestion_connection_params,
    get_ingestion_engine,
)
from app.models import ConstructedTable
from app.services.constructed_data_store import ConstructedDataRecord
from app.services.constructed_schema_resolver import resolve_constructed_schema

logger = logging.getLogger(__name__)


class ConstructedDataWarehouseError(RuntimeError):
    """Raised when syncing constructed data to the warehouse fails."""


@dataclass(frozen=True)
class _ColumnSpec:
    field_name: str
    column_name: str
    logical_type: str
    sql_type: str
    is_nullable: bool
    parameter_name: str

    @property
    def quoted_name(self) -> str:
        return _quote_identifier(self.column_name)


@dataclass(frozen=True)
class _TableSchema:
    constructed_table_id: UUID
    schema_name: str
    table_name: str
    columns: tuple[_ColumnSpec, ...]

    @property
    def qualified_name(self) -> str:
        schema_sql = _format_schema_path(self.schema_name)
        if schema_sql:
            return f"{schema_sql}.{_quote_identifier(self.table_name)}"
        return _quote_identifier(self.table_name)

    @property
    def signature(self) -> tuple[tuple[str, str, bool], ...]:
        return tuple((column.column_name, column.sql_type, column.is_nullable) for column in self.columns)

    @property
    def schema_show_path(self) -> str:
        return _format_schema_path(self.schema_name)


_RESERVED_COLUMN_NAMES = {"row_id", "row_identifier", "created_at", "updated_at"}


class ConstructedDataWarehouse:
    """Manage constructed data tables inside the Databricks warehouse."""

    DEFAULT_SCHEMA = "constructed_data"

    def __init__(self, *, engine: Engine | None = None, schema: str | None = None) -> None:
        settings = get_settings()
        self.engine = engine or get_ingestion_engine()
        dialect_name = (self.engine.dialect.name or "").lower()
        self._warehouse_enabled = dialect_name.startswith("databricks")
        override_url = settings.ingestion_database_url

        if not self._warehouse_enabled:
            # Non-Databricks overrides (e.g. local Postgres/sqlite) cannot execute warehouse DDL.
            self.schema = ""
        elif dialect_name == "sqlite":  # Used during tests; no warehouse sync required
            self.schema = ""
            self._warehouse_enabled = False
        elif schema:
            self.schema = schema
        elif override_url:
            self.schema = self.DEFAULT_SCHEMA
        else:
            params = get_ingestion_connection_params()
            self.schema = params.constructed_schema or params.schema_name or self.DEFAULT_SCHEMA

        if not self.schema and self._warehouse_enabled:
            raise ConstructedDataWarehouseError("Constructed data schema is not configured for Databricks storage.")

        self._ensured_signatures: dict[UUID, tuple[tuple[str, str, bool], ...]] = {}

    # Public API -----------------------------------------------------------------

    def upsert_records(self, table: ConstructedTable, records: Iterable[ConstructedDataRecord]) -> None:
        if not self._warehouse_enabled:
            return

        record_list = list(records)
        if not record_list:
            return

        schema = self._build_schema(table)
        self._ensure_table(schema)

        logger.info(
            "Upserting %d constructed rows into %s",
            len(record_list),
            schema.qualified_name,
        )

        insert_columns = self._build_insert_columns(schema)
        insert_statement = self._build_insert_statement(schema, insert_columns)
        delete_statement = text(f"DELETE FROM {schema.qualified_name} WHERE row_id = :row_id")

        try:
            with self.engine.begin() as connection:
                for record in record_list:
                    params = self._build_parameters(schema, record)
                    connection.execute(delete_statement, {"row_id": params["row_id"]})
                    connection.execute(insert_statement, params)
        except SQLAlchemyError as exc:  # pragma: no cover - difficult to reproduce without warehouse
            logger.error("Failed to upsert constructed data rows into warehouse table %s: %s", schema.table_name, exc)
            raise ConstructedDataWarehouseError("Failed to persist constructed data to warehouse") from exc

    def delete_rows(self, table: ConstructedTable, row_ids: Iterable[UUID]) -> None:
        if not self._warehouse_enabled:
            return

        row_id_list = [str(row_id) for row_id in row_ids]
        if not row_id_list:
            return

        schema = self._build_schema(table)
        if not self._table_exists(schema):
            return

        delete_statement = text(f"DELETE FROM {schema.qualified_name} WHERE row_id = :row_id")

        try:
            with self.engine.begin() as connection:
                for row_id in row_id_list:
                    connection.execute(delete_statement, {"row_id": row_id})
        except SQLAlchemyError as exc:  # pragma: no cover - difficult to reproduce without warehouse
            logger.error("Failed to delete constructed data rows from warehouse table %s: %s", schema.table_name, exc)
            raise ConstructedDataWarehouseError("Failed to delete constructed data from warehouse") from exc

    def ensure_table(self, table: ConstructedTable) -> None:
        """Ensure the physical warehouse table exists, creating or altering it when needed."""

        if not self._warehouse_enabled:
            return

        schema = self._build_schema(table)
        self._ensure_table(schema)

    def drop_table(self, table: ConstructedTable) -> None:
        """Drop the physical warehouse table for the provided constructed table."""

        if not self._warehouse_enabled:
            return

        schema = self._build_schema(table)
        if not self._table_exists(schema):
            self._ensured_signatures.pop(schema.constructed_table_id, None)
            return

        drop_statement = text(f"DROP TABLE IF EXISTS {schema.qualified_name}")

        try:
            with self.engine.begin() as connection:
                connection.execute(drop_statement)
        except SQLAlchemyError as exc:  # pragma: no cover - difficult to reproduce without warehouse
            logger.error("Failed to drop constructed data warehouse table %s: %s", schema.table_name, exc)
            raise ConstructedDataWarehouseError("Failed to drop constructed data warehouse table") from exc

        self._ensured_signatures.pop(schema.constructed_table_id, None)

    # Internal helpers -----------------------------------------------------------

    def _build_schema(self, table: ConstructedTable) -> _TableSchema:
        if not table.name:
            table_name = f"constructed_{table.id.hex[:8]}"
        else:
            table_name = table.name

        schema_name = resolve_constructed_schema(
            table=table,
            system=None,
            fallback_schema=self.schema,
        )

        columns: list[_ColumnSpec] = []
        seen_names: set[str] = set()
        sorted_fields = sorted(table.fields, key=lambda field: (field.display_order or 0, field.name))

        for index, field in enumerate(sorted_fields):
            logical_type = (field.data_type or "string").lower()
            sql_type = _map_field_type_to_databricks(logical_type)
            parameter_name = f"col_{index}"

            column_name = _generate_column_name(field.name, seen_names)
            seen_names.add(column_name.lower())

            if column_name in _RESERVED_COLUMN_NAMES:
                raise ConstructedDataWarehouseError(
                    f"Field name '{field.name}' conflicts with a reserved column name in constructed table '{table_name}'"
                )

            columns.append(
                _ColumnSpec(
                    field_name=field.name,
                    column_name=column_name,
                    logical_type=logical_type,
                    sql_type=sql_type,
                    is_nullable=field.is_nullable,
                    parameter_name=parameter_name,
                )
            )

        return _TableSchema(
            constructed_table_id=table.id,
            schema_name=schema_name,
            table_name=table_name,
            columns=tuple(columns),
        )

    def _ensure_table(self, schema: _TableSchema) -> None:
        signature = schema.signature
        if self._ensured_signatures.get(schema.constructed_table_id) == signature:
            return

        schema_sql = schema.schema_show_path
        if not schema_sql:
            raise ConstructedDataWarehouseError("Constructed data schema is not configured for Databricks storage.")

        create_schema_stmt = text(f"CREATE SCHEMA IF NOT EXISTS {schema_sql}")
        create_table_stmt = self._build_create_table_statement(schema)

        table_exists = self._table_exists(schema)
        needs_recreation = False
        if table_exists:
            needs_recreation = self._table_needs_recreation(schema)

        try:
            with self.engine.begin() as connection:
                logger.debug(
                    "Ensuring Databricks table %s with columns %s",
                    schema.table_name,
                    [column.column_name for column in schema.columns],
                )
                connection.execute(create_schema_stmt)

                if table_exists and needs_recreation:
                    logger.info(
                        "Recreating Databricks constructed table '%s' to align with metadata",
                        schema.table_name,
                    )
                    connection.execute(text(f"DROP TABLE IF EXISTS {schema.qualified_name}"))
                    table_exists = False

                if not table_exists:
                    connection.execute(create_table_stmt)
                else:
                    connection.execute(create_table_stmt)
                    self._add_missing_columns(connection, schema)

                self._ensure_change_feed_enabled(connection, schema)
        except SQLAlchemyError as exc:  # pragma: no cover - difficult to reproduce without warehouse
            logger.error("Failed to ensure constructed table %s exists in warehouse: %s", schema.table_name, exc)
            raise ConstructedDataWarehouseError("Unable to prepare constructed data warehouse table") from exc

        self._ensured_signatures[schema.constructed_table_id] = signature

    def _build_create_table_statement(self, schema: _TableSchema) -> Any:
        columns_sql = [
            f"{_quote_identifier('row_id')} STRING NOT NULL",
            f"{_quote_identifier('row_identifier')} STRING",
        ]
        for column in schema.columns:
            nullability = "" if column.is_nullable else " NOT NULL"
            columns_sql.append(f"{column.quoted_name} {column.sql_type}{nullability}")
        columns_sql.extend(
            [
                f"{_quote_identifier('created_at')} TIMESTAMP NOT NULL",
                f"{_quote_identifier('updated_at')} TIMESTAMP NOT NULL",
            ]
        )
        columns_clause = ",\n                ".join(columns_sql)
        statement = (
            f"CREATE TABLE IF NOT EXISTS {schema.qualified_name} (\n                {columns_clause}\n            ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
        return text(statement)

    def _add_missing_columns(self, connection, schema: _TableSchema) -> None:
        existing_columns = self._fetch_existing_columns(connection, schema)
        missing_dynamic = [
            column for column in schema.columns if column.column_name.lower() not in existing_columns
        ]
        if not missing_dynamic:
            return

        additions = ", ".join(f"{column.quoted_name} {column.sql_type}" for column in missing_dynamic)
        alter_stmt = text(f"ALTER TABLE {schema.qualified_name} ADD COLUMNS ({additions})")
        connection.execute(alter_stmt)

    def _fetch_existing_columns(self, connection, schema: _TableSchema) -> set[str]:
        stmt = text(f"SHOW COLUMNS IN {schema.qualified_name}")
        result = connection.execute(stmt)
        return {row[0].lower() for row in result.fetchall()}

    def _table_needs_recreation(self, schema: _TableSchema) -> bool:
        try:
            with self.engine.connect() as connection:
                existing_columns = self._fetch_existing_columns(connection, schema)
        except SQLAlchemyError:  # pragma: no cover - best effort detection when table missing
            return False

        expected_columns = {name.lower() for name in _RESERVED_COLUMN_NAMES}
        expected_columns.update(column.column_name.lower() for column in schema.columns)

        extra_columns = existing_columns - expected_columns
        if extra_columns:
            logger.info(
                "Found deprecated columns on constructed table '%s': %s",
                schema.table_name,
                ", ".join(sorted(extra_columns)),
            )
            return True

        return False

    def _table_exists(self, schema: _TableSchema) -> bool:
        if not self._warehouse_enabled:
            return False
        schema_sql = schema.schema_show_path
        if not schema_sql:
            return False
        table_literal = _quote_string_literal(schema.table_name)
        stmt = text(f"SHOW TABLES IN {schema_sql} LIKE {table_literal}")
        try:
            with self.engine.connect() as connection:
                rows = connection.execute(stmt).fetchall()
        except SQLAlchemyError as exc:  # pragma: no cover - dependent on warehouse state
            message = str(exc).upper()
            if "SCHEMA_NOT_FOUND" in message:
                return False
            raise
        return len(rows) > 0

    def _ensure_change_feed_enabled(self, connection, schema: _TableSchema) -> None:
        """Ensure change data feed stays enabled even if the table already existed."""

        alter_stmt = text(
            f"ALTER TABLE {schema.qualified_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
        connection.execute(alter_stmt)

    def _build_insert_columns(self, schema: _TableSchema) -> list[_ColumnSpec]:
        columns: list[_ColumnSpec] = [
            _ColumnSpec("row_id", "row_id", "string", "STRING", False, "row_id"),
            _ColumnSpec("row_identifier", "row_identifier", "string", "STRING", True, "row_identifier"),
        ]
        columns.extend(schema.columns)
        columns.extend(
            [
                _ColumnSpec("created_at", "created_at", "timestamp", "TIMESTAMP", False, "created_at"),
                _ColumnSpec("updated_at", "updated_at", "timestamp", "TIMESTAMP", False, "updated_at"),
            ]
        )
        return columns

    def _build_insert_statement(self, schema: _TableSchema, columns: list[_ColumnSpec]):
        column_names = ", ".join(_quote_identifier(column.column_name) for column in columns)
        parameter_names = ", ".join(f":{column.parameter_name}" for column in columns)
        statement = f"INSERT INTO {schema.qualified_name} ({column_names}) VALUES ({parameter_names})"
        return text(statement)

    def _build_parameters(self, schema: _TableSchema, record: ConstructedDataRecord) -> dict[str, Any]:
        params: dict[str, Any] = {
            "row_id": str(record.id),
            "row_identifier": record.row_identifier,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
        }

        for column in schema.columns:
            value = record.payload.get(column.field_name)
            params[column.parameter_name] = _coerce_value(value, column.logical_type)

        return params


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def _format_schema_path(path: str) -> str:
    parts = [part.strip() for part in path.split(".") if part.strip()]
    if not parts:
        return ""
    return ".".join(_quote_identifier(part) for part in parts)


def _map_field_type_to_databricks(data_type: str) -> str:
    normalized = (data_type or "string").lower()
    mapping = {
        "string": "STRING",
        "text": "STRING",
        "uuid": "STRING",
        "integer": "INT",
        "int": "INT",
        "bigint": "BIGINT",
        "decimal": "DECIMAL(38, 12)",
        "numeric": "DECIMAL(38, 12)",
        "float": "FLOAT",
        "double": "DOUBLE",
        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",
        "date": "DATE",
        "datetime": "TIMESTAMP",
        "timestamp": "TIMESTAMP",
        "time": "STRING",
    }
    return mapping.get(normalized, "STRING")


def _coerce_value(value: Any, logical_type: str) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        if value.strip() == "":
            if logical_type in {"string", "text", "uuid", "time"}:
                return ""
            return None

    try:
        if logical_type in {"string", "text", "uuid", "time"}:
            return str(value)
        if logical_type in {"integer", "int"}:
            if isinstance(value, int):
                return value
            return int(value)
        if logical_type == "bigint":
            if isinstance(value, int):
                return value
            return int(value)
        if logical_type in {"float", "double"}:
            if isinstance(value, (float, int)):
                return float(value)
            return float(str(value))
        if logical_type in {"decimal", "numeric"}:
            if isinstance(value, Decimal):
                return value
            return Decimal(str(value))
        if logical_type in {"boolean", "bool"}:
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return bool(value)
            lowered = str(value).strip().lower()
            if lowered in {"true", "1", "yes", "y"}:
                return True
            if lowered in {"false", "0", "no", "n"}:
                return False
            return None
        if logical_type == "date":
            if isinstance(value, date) and not isinstance(value, datetime):
                return value
            if isinstance(value, datetime):
                return value.date()
            return date.fromisoformat(str(value))
        if logical_type in {"datetime", "timestamp"}:
            if isinstance(value, datetime):
                return value
            return datetime.fromisoformat(str(value))
    except (ValueError, TypeError):
        logger.debug("Could not coerce value '%s' to logical type '%s'; keeping original", value, logical_type)
        return value

    return value


def _generate_column_name(raw_name: str | None, existing_names: set[str]) -> str:
    """Generate a warehouse-safe column name while keeping collisions predictable."""

    base = (raw_name or "").strip().lower()
    if not base:
        base = "column"

    sanitized = re.sub(r"[^0-9a-z]+", "_", base)
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")

    if not sanitized:
        sanitized = "column"

    if sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"

    candidate = sanitized
    suffix = 1
    while candidate.lower() in existing_names:
        candidate = f"{sanitized}_{suffix}"
        suffix += 1

    return candidate


def _quote_string_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
