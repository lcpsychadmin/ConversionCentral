from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha1
from typing import Iterator, Mapping, MutableMapping, Sequence

from sqlalchemy import Column, MetaData, Table, insert, text
from sqlalchemy.engine import Engine
from sqlalchemy.sql import sqltypes

from app.ingestion import get_ingestion_engine
from app.ingestion.engine import get_ingestion_connection_params


@dataclass(frozen=True)
class LoadPlan:
    schema: str
    table_name: str
    replace: bool
    deduplicate: bool


class DatabricksTableLoader:
    """Lightweight loader that mirrors source rows into Databricks staging tables."""

    def __init__(
        self,
        *,
        engine: Engine | None = None,
        default_schema: str | None = None,
        batch_rows: int | None = None,
    ) -> None:
        self.engine = engine or get_ingestion_engine()
        self._default_schema_override = (
            default_schema.strip() if isinstance(default_schema, str) and default_schema.strip() else None
        )
        self._batch_rows_override = batch_rows if isinstance(batch_rows, int) and batch_rows > 0 else None
        self._default_schema = "default"
        self._insert_batch_rows = 1000
        self._apply_connection_defaults()

    def load_rows(
        self,
        plan: LoadPlan,
        rows: Sequence[Mapping[str, object]],
        columns: Sequence[Column],
    ) -> int:
        if not rows:
            return 0

        self._apply_connection_defaults()
        schema = plan.schema or self._default_schema
        table_name = self._normalize_identifier(plan.table_name)
        column_specs = self._infer_column_specs(columns, rows[0])
        target_table = self._ensure_table(schema, table_name, column_specs, plan.deduplicate)

        prepared_rows = [self._prepare_row(row, plan.deduplicate) for row in rows]

        with self.engine.begin() as connection:
            if plan.replace:
                qualified = self._qualify(schema, table_name)
                connection.execute(text(f"DELETE FROM {qualified}"))
            total = 0
            for chunk in self._iter_chunks(prepared_rows, self._insert_batch_rows):
                result = connection.execute(insert(target_table), chunk)
                total += result.rowcount or len(chunk)
        return total

    @property
    def default_schema(self) -> str:
        self._apply_connection_defaults()
        return self._default_schema

    def _apply_connection_defaults(self) -> None:
        schema = self._default_schema_override
        batch_rows = self._batch_rows_override
        params = None
        try:
            params = get_ingestion_connection_params()
        except RuntimeError:
            params = None

        if schema is None and params:
            candidate = params.schema_name or params.constructed_schema
            if candidate:
                schema = candidate.strip() or None
        if schema is None:
            schema = "default"
        if isinstance(schema, str):
            schema = schema.strip() or "default"

        if batch_rows is None and params and isinstance(params.ingestion_batch_rows, int):
            if params.ingestion_batch_rows > 0:
                batch_rows = params.ingestion_batch_rows
        if batch_rows is None:
            batch_rows = 1000

        self._default_schema = schema
        self._insert_batch_rows = batch_rows

    def _ensure_table(
        self,
        schema: str,
        table_name: str,
        column_specs: Mapping[str, sqltypes.TypeEngine],
        deduplicate: bool,
    ) -> Table:
        metadata = MetaData(schema=schema or None)
        columns = [Column(name, type_, nullable=True) for name, type_ in column_specs.items()]
        if deduplicate:
            columns.append(Column("__cc_row_hash", sqltypes.String(length=40), nullable=False))

        table = Table(table_name, metadata, *columns, extend_existing=True)
        self._ensure_schema_exists(schema)
        metadata.create_all(self.engine, tables=[table])
        return table

    @staticmethod
    def _iter_chunks(
        rows: Sequence[MutableMapping[str, object]],
        chunk_size: int,
    ) -> Iterator[Sequence[MutableMapping[str, object]]]:
        if chunk_size <= 0:
            chunk_size = len(rows)
        chunk_size = max(1, chunk_size)
        for index in range(0, len(rows), chunk_size):
            yield rows[index : index + chunk_size]

    def _infer_column_specs(
        self,
        columns: Sequence[Column],
        sample_row: Mapping[str, object],
    ) -> dict[str, sqltypes.TypeEngine]:
        specs: dict[str, sqltypes.TypeEngine] = {}
        for column in columns:
            normalized = self._normalize_identifier(column.name)
            specs[normalized] = self._map_column_type(column.type)
        for key, value in sample_row.items():
            normalized = self._normalize_identifier(key)
            specs.setdefault(normalized, self._map_python_type(value))
        if not specs:
            specs["__cc_placeholder"] = sqltypes.String()
        return specs

    def _map_column_type(self, type_: sqltypes.TypeEngine) -> sqltypes.TypeEngine:
        if isinstance(type_, sqltypes.Boolean):
            return sqltypes.Boolean()
        if isinstance(type_, (sqltypes.Integer, sqltypes.BigInteger, sqltypes.SmallInteger)):
            return sqltypes.BigInteger()
        if isinstance(type_, sqltypes.Numeric):
            precision = getattr(type_, "precision", None) or 38
            scale = getattr(type_, "scale", None) or 10
            return sqltypes.Numeric(precision=precision, scale=scale)
        if isinstance(type_, sqltypes.Float):
            return sqltypes.Float()
        if isinstance(type_, sqltypes.DateTime):
            return sqltypes.DateTime(timezone=True)
        if isinstance(type_, sqltypes.Date):
            return sqltypes.Date()
        if isinstance(type_, sqltypes.Time):
            return sqltypes.Time()
        if isinstance(type_, sqltypes.LargeBinary):
            return sqltypes.LargeBinary()
        length = getattr(type_, "length", None)
        if length and length <= 4000:
            return sqltypes.String(length=length)
        return sqltypes.String()

    def _map_python_type(self, value: object) -> sqltypes.TypeEngine:
        if isinstance(value, bool):
            return sqltypes.Boolean()
        if isinstance(value, int):
            return sqltypes.BigInteger()
        if isinstance(value, float):
            return sqltypes.Float()
        if hasattr(value, "tzinfo") or hasattr(value, "isoformat"):
            return sqltypes.DateTime(timezone=True)
        if value is None:
            return sqltypes.String()
        return sqltypes.String()

    def _prepare_row(self, row: Mapping[str, object], deduplicate: bool) -> MutableMapping[str, object]:
        normalized: MutableMapping[str, object] = {}
        for key, value in row.items():
            normalized[self._normalize_identifier(key)] = value
        if deduplicate:
            digest = sha1()
            for key in sorted(normalized.keys()):
                if key == "__cc_row_hash":
                    continue
                digest.update(key.encode("utf-8"))
                digest.update(str(normalized[key]).encode("utf-8", errors="ignore"))
            normalized["__cc_row_hash"] = digest.hexdigest()
        return normalized

    def _qualify(self, schema: str, table: str) -> str:
        if schema:
            return f"`{schema}`.`{table}`"
        return f"`{table}`"

    def _ensure_schema_exists(self, schema: str | None) -> None:
        if not schema:
            return
        if self.engine.dialect.name == "sqlite":  # sqlite does not support CREATE SCHEMA
            return
        stmt = text(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
        with self.engine.begin() as connection:
            connection.execute(stmt)

    def _normalize_identifier(self, value: str) -> str:
        sanitized = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in value.strip())
        if sanitized and sanitized[0].isdigit():
            sanitized = f"_{sanitized}"
        return sanitized or "column"


def build_loader_plan(
    *,
    schema: str | None,
    table_name: str,
    replace: bool,
    deduplicate: bool,
) -> LoadPlan:
    return LoadPlan(
        schema or "",
        table_name,
        replace,
        deduplicate,
    )
