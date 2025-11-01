from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Mapping, MutableMapping, Sequence

from sqlalchemy import Column, MetaData, Table, insert, text
from sqlalchemy import types as satypes
from sqlalchemy.engine import Engine

from app.ingestion import get_ingestion_engine
from app.models import Field, Table as TableModel

_IDENTIFIER_RE = re.compile(r"[^A-Za-z0-9_]")


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type_: object
    nullable: bool


class DatabricksIngestionStorage:
    """Create and load staging tables inside the configured Databricks SQL warehouse."""

    def __init__(self, *, engine: Engine | None = None, default_schema: str = "default"):
        self.engine = engine or get_ingestion_engine()
        self.default_schema = default_schema

    def ensure_table(self, table_model: TableModel) -> Table:
        schema = table_model.schema_name or self.default_schema
        if self.engine.dialect.name == "sqlite":
            schema = None
        table_name = self._normalize_identifier(table_model.physical_name or table_model.name)

        specs = [self._build_column_spec(field) for field in table_model.fields]
        metadata = MetaData(schema=schema or None)
        if not specs:
            raise ValueError("Cannot create ingestion table without defined fields.")

        columns = [Column(spec.name, spec.type_, nullable=spec.nullable) for spec in specs]

        ingestion_table = Table(table_name, metadata, *columns, extend_existing=True)
        self._ensure_schema_exists(schema)
        metadata.create_all(self.engine, tables=[ingestion_table])
        return ingestion_table

    def load_rows(
        self,
        table_model: TableModel,
        rows: Sequence[Mapping[str, object]],
        *,
        replace: bool = False,
    ) -> int:
        if not rows:
            return 0

        ingestion_table = self.ensure_table(table_model)
        normalized_rows = [self._normalize_row(row, ingestion_table) for row in rows]

        with self.engine.begin() as connection:
            if replace:
                qualified = self._qualify_table(ingestion_table)
                connection.execute(text(f"DELETE FROM {qualified}"))
            result = connection.execute(insert(ingestion_table), normalized_rows)
        return result.rowcount or 0

    def _normalize_row(
        self, row: Mapping[str, object], ingestion_table: Table
    ) -> MutableMapping[str, object]:
        normalized: MutableMapping[str, object] = {}
        normalized_keys = {
            self._normalize_identifier(key): key
            for key in row.keys()
        }
        for column in ingestion_table.columns:
            key = column.name
            if key in row:
                normalized[key] = row[key]
            elif key in normalized_keys:
                source_key = normalized_keys[key]
                normalized[key] = row[source_key]
            else:
                # Attempt case-insensitive lookup for convenience.
                matches = [
                    candidate
                    for candidate in row.keys()
                    if candidate.lower() == key.lower()
                ]
                if matches:
                    normalized[key] = row[matches[0]]
                else:
                    normalized[key] = None
        return normalized

    def _build_column_spec(self, field: Field) -> ColumnSpec:
        name = self._normalize_identifier(field.name)
        type_ = self._map_field_type(field)
        nullable = not field.system_required
        return ColumnSpec(name=name, type_=type_, nullable=nullable)

    def _map_field_type(self, field: Field):
        normalized = (field.field_type or "").lower()
        length = field.field_length or 0
        scale = field.decimal_places or 0

        if normalized in {"string", "str", "nvarchar", "nchar", "text"}:
            size = length or 255
            return satypes.String(length=size)
        if normalized in {"varchar", "char"}:
            size = length or 255
            return satypes.String(length=size)
        if normalized in {"int", "integer"}:
            return satypes.Integer()
        if normalized in {"smallint"}:
            return satypes.SmallInteger()
        if normalized in {"bigint"}:
            return satypes.BigInteger()
        if normalized in {"float", "double", "real"}:
            return satypes.Float()
        if normalized in {"decimal", "numeric"}:
            precision = length or 18
            return satypes.Numeric(precision=precision, scale=scale)
        if normalized in {"datetime", "datetime2", "timestamp"}:
            return satypes.DateTime(timezone=True)
        if normalized in {"date"}:
            return satypes.Date()
        if normalized in {"time"}:
            return satypes.Time()
        if normalized in {"bool", "boolean", "bit"}:
            return satypes.Boolean()
        # Fallback to NVARCHAR if unknown.
        size = length or 255
        return satypes.String(length=size)

    def _ensure_schema_exists(self, schema: str) -> None:
        if not schema:
            return
        if self.engine.dialect.name == "sqlite":
            return
        stmt = text(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
        with self.engine.begin() as connection:
            connection.execute(stmt)

    def _qualify_table(self, table: Table) -> str:
        schema = table.schema
        if not schema and self.engine.dialect.name != "sqlite":
            schema = self.default_schema
        if schema:
            return f"`{schema}`.`{table.name}`"
        return f"`{table.name}`"

    def _normalize_identifier(self, value: str) -> str:
        sanitized = _IDENTIFIER_RE.sub("_", value.strip())
        if sanitized and sanitized[0].isdigit():
            sanitized = f"_{sanitized}"
        return sanitized or "column"
