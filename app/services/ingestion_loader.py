from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha1
from typing import Mapping, MutableMapping, Sequence

from sqlalchemy import Column, MetaData, Table, insert, text
from sqlalchemy.engine import Engine
from sqlalchemy.sql import sqltypes

from app.ingestion.engine import get_ingestion_engine


@dataclass(frozen=True)
class LoadPlan:
    schema: str
    table_name: str
    replace: bool
    deduplicate: bool


class SqlServerTableLoader:
    """Lightweight loader that mirrors source rows into SQL Server staging tables."""

    def __init__(self, *, engine: Engine | None = None):
        self.engine = engine or get_ingestion_engine()
        self._dialect = self.engine.dialect.name.lower()

    def load_rows(
        self,
        plan: LoadPlan,
        rows: Sequence[Mapping[str, object]],
        columns: Sequence[Column],
    ) -> int:
        if not rows:
            return 0

        schema = plan.schema or "dbo"
        table_name = self._normalize_identifier(plan.table_name)
        column_specs = self._infer_column_specs(columns, rows[0])
        self._ensure_table(schema, table_name, column_specs, plan.deduplicate)

        metadata = MetaData()
        target_table = Table(
            table_name,
            metadata,
            schema=schema,
            autoload_with=self.engine,
        )

        prepared_rows = [self._prepare_row(row, plan.deduplicate) for row in rows]

        with self.engine.begin() as connection:
            if plan.replace:
                qualified = self._qualify(schema, table_name)
                connection.execute(text(f"TRUNCATE TABLE {qualified}"))
            result = connection.execute(insert(target_table), prepared_rows)
        return result.rowcount or 0

    def _ensure_table(
        self,
        schema: str,
        table_name: str,
        column_specs: Mapping[str, str],
        deduplicate: bool,
    ) -> None:
        column_lines = [f"[{name}] {definition} NULL" for name, definition in column_specs.items()]
        if deduplicate:
            column_lines.append("[__cc_row_hash] CHAR(40) NOT NULL")

        columns_sql = ",\n    ".join(column_lines)
        create_sql = (
            "IF OBJECT_ID('[{schema}].[{table}]', 'U') IS NULL BEGIN\n"
            "    CREATE TABLE [{schema}].[{table}] (\n    {columns}\n    );\n"
            "END"
        ).format(schema=schema, table=table_name, columns=columns_sql)

        with self.engine.begin() as connection:
            connection.execute(text(create_sql))
            if deduplicate:
                index_name = f"UX_{table_name}_hash"
                qualified = f"[{schema}].[{table_name}]"
                index_sql = (
                    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = :index_name AND object_id = OBJECT_ID(:qualified)) "
                    "BEGIN "
                    f"CREATE UNIQUE NONCLUSTERED INDEX [{index_name}] ON {qualified} ([__cc_row_hash]) WITH (IGNORE_DUP_KEY = ON); "
                    "END"
                )
                connection.execute(
                    text(index_sql),
                    {
                        "index_name": index_name,
                        "qualified": qualified,
                    },
                )

    def _infer_column_specs(
        self,
        columns: Sequence[Column],
        sample_row: Mapping[str, object],
    ) -> dict[str, str]:
        specs: dict[str, str] = {}
        for column in columns:
            normalized = self._normalize_identifier(column.name)
            specs[normalized] = self._map_column_type(column.type)
        for key, value in sample_row.items():
            normalized = self._normalize_identifier(key)
            specs.setdefault(normalized, self._map_python_type(value))
        if not specs:
            specs["__cc_placeholder"] = "NVARCHAR(MAX)"
        return specs

    def _map_column_type(self, type_: sqltypes.TypeEngine) -> str:
        if isinstance(type_, sqltypes.Boolean):
            return "BIT"
        if isinstance(type_, (sqltypes.Integer, sqltypes.BigInteger, sqltypes.SmallInteger)):
            return "BIGINT"
        if isinstance(type_, sqltypes.Numeric):
            precision = getattr(type_, "precision", None) or 38
            scale = getattr(type_, "scale", None) or 10
            return f"DECIMAL({precision},{scale})"
        if isinstance(type_, sqltypes.Float):
            return "FLOAT"
        if isinstance(type_, sqltypes.DateTime):
            return "DATETIME2"
        if isinstance(type_, sqltypes.Date):
            return "DATE"
        if isinstance(type_, sqltypes.Time):
            return "TIME"
        if isinstance(type_, sqltypes.LargeBinary):
            return "VARBINARY(MAX)"
        length = getattr(type_, "length", None)
        if length and length <= 4000:
            return f"NVARCHAR({length})"
        return "NVARCHAR(MAX)"

    def _map_python_type(self, value: object) -> str:
        if isinstance(value, bool):
            return "BIT"
        if isinstance(value, int):
            return "BIGINT"
        if isinstance(value, float):
            return "FLOAT"
        if hasattr(value, "tzinfo") or hasattr(value, "isoformat"):
            # Covers datetime/date/time like objects.
            return "DATETIME2"
        if value is None:
            return "NVARCHAR(MAX)"
        return "NVARCHAR(MAX)"

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
            return f"[{schema}].[{table}]"
        return f"[{table}]"

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
        schema or "dbo",
        table_name,
        replace,
        deduplicate,
    )
