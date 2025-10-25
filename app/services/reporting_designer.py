from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set
from uuid import UUID

from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.models.entities import Field, SystemConnection, Table
from app.schemas import SystemConnectionType
from app.schemas.reporting import (
    ReportAggregateFn,
    ReportDesignerColumn,
    ReportDesignerDefinition,
    ReportDesignerJoin,
    ReportJoinType,
    ReportPreviewResponse,
    ReportSortDirection,
)
from app.services.connection_resolver import UnsupportedConnectionError, resolve_sqlalchemy_url


class ReportPreviewError(Exception):
    """Domain error raised when a preview cannot be generated."""


@dataclass
class _JoinEdge:
    join: ReportDesignerJoin
    source_table_id: UUID
    target_table_id: UUID


def _resolve_connection_identity(connection: SystemConnection) -> tuple[SystemConnectionType, str] | None:
    if not connection.active:
        return None

    raw_type = connection.connection_type
    if isinstance(raw_type, SystemConnectionType):
        connection_type = raw_type
    elif raw_type:
        try:
            connection_type = SystemConnectionType(str(raw_type).lower())
        except ValueError:
            return None
    else:
        return None

    connection_string = (connection.connection_string or "").strip()
    if not connection_string:
        return None

    return connection_type, connection_string


class ReportSqlBuilder:
    """Compose a SQL statement for a reporting designer definition."""

    def __init__(self, definition: ReportDesignerDefinition, tables: Dict[UUID, Table], fields: Dict[UUID, Field], limit: int) -> None:
        self.definition = definition
        self.tables = tables
        self.fields = fields
        self.limit = limit
        self.alias_map: Dict[UUID, str] = {}
        self.selected_columns: List[str] = []
        self.column_aliases: List[str] = []
        self.group_by_refs: List[str] = []
        self.order_by_clauses: List[str] = []
        self.where_clauses: List[str] = []
        self.bind_params = {"preview_limit": limit}

    def build(self) -> str:
        if not self.definition.tables:
            raise ReportPreviewError("Add at least one table before running a preview.")

        from_clause = self._build_from_clause()
        self._build_select_and_ordering()
        self._build_criteria()

        select_sql = ",\n       ".join(self.selected_columns) if self.selected_columns else "1"
        sql_parts = [f"SELECT\n       {select_sql}\n  FROM {from_clause}"]

        if self.where_clauses:
            sql_parts.append(f" WHERE {' OR '.join(self.where_clauses)}")

        if self.group_by_refs:
            sql_parts.append(f" GROUP BY {', '.join(self.group_by_refs)}")

        if self.order_by_clauses:
            sql_parts.append(f" ORDER BY {', '.join(self.order_by_clauses)}")

        sql_parts.append(" LIMIT :preview_limit")

        return "".join(sql_parts)

    def _build_from_clause(self) -> str:
        joins = list(self.definition.joins)
        table_ids = {placement.table_id for placement in self.definition.tables}

        base_table_id = self._determine_base_table(joins, table_ids)
        base_table = self._require_table(base_table_id)
        base_alias = self._register_alias(base_table_id)

        clause = f"{self._qualify_table(base_table)} AS {base_alias}"
        joined_tables: Set[UUID] = {base_table_id}
        remaining: List[ReportDesignerJoin] = joins.copy()

        while remaining:
            progress = False
            for join in remaining[:]:
                if join.source_table_id in joined_tables and join.target_table_id not in joined_tables:
                    clause += self._compose_join(join, reverse=False)
                    joined_tables.add(join.target_table_id)
                    remaining.remove(join)
                    progress = True
                elif join.target_table_id in joined_tables and join.source_table_id not in joined_tables:
                    clause += self._compose_join(join, reverse=True)
                    joined_tables.add(join.source_table_id)
                    remaining.remove(join)
                    progress = True

            if not progress:
                unresolved = {j.source_table_id for j in remaining} | {j.target_table_id for j in remaining}
                missing = unresolved - joined_tables
                raise ReportPreviewError(
                    "Unable to assemble joins for the selected tables. Ensure all tables are connected."
                    f" Unjoined tables: {', '.join(str(value) for value in missing)}"
                )

        return clause

    def _determine_base_table(self, joins: List[ReportDesignerJoin], table_ids: Set[UUID]) -> UUID:
        if joins:
            return joins[0].source_table_id
        if len(table_ids) == 1:
            return next(iter(table_ids))
        raise ReportPreviewError("Add joins between tables to preview multi-table reports.")

    def _compose_join(self, join: ReportDesignerJoin, *, reverse: bool) -> str:
        left_table_id = join.target_table_id if reverse else join.source_table_id
        right_table_id = join.source_table_id if reverse else join.target_table_id

        left_alias = self._alias_for(left_table_id)
        right_alias = self._register_alias(right_table_id)

        left_field_id = join.target_field_id if reverse else join.source_field_id
        right_field_id = join.source_field_id if reverse else join.target_field_id

        if left_field_id is None or right_field_id is None:
            raise ReportPreviewError("Joins in the preview must specify both source and target fields.")

        left_field = self._require_field(left_field_id)
        right_field = self._require_field(right_field_id)

        sql_join = self._resolve_join_keyword(join.join_type, reverse)

        right_table = self._require_table(right_table_id)

        left_column = f"{left_alias}.{self._quote_identifier(left_field.name)}"
        right_column = f"{right_alias}.{self._quote_identifier(right_field.name)}"

        return (
            f"\n       {sql_join} {self._qualify_table(right_table)} AS {right_alias}"
            f" ON {left_column} = {right_column}"
        )

    def _resolve_join_keyword(self, join_type: ReportJoinType, reverse: bool) -> str:
        if join_type == ReportJoinType.INNER:
            return "INNER JOIN"
        if join_type == ReportJoinType.LEFT:
            return "RIGHT JOIN" if reverse else "LEFT JOIN"
        if join_type == ReportJoinType.RIGHT:
            return "LEFT JOIN" if reverse else "RIGHT JOIN"
        raise ReportPreviewError(f"Unsupported join type: {join_type}.")

    def _build_select_and_ordering(self) -> None:
        alias_usage: Dict[str, int] = {}
        select_fragments: List[str] = []
        group_by_refs: Set[str] = set()
        order_by_fragments: List[str] = []

        for column in sorted(self.definition.columns, key=lambda col: col.order):
            field = self.fields.get(column.field_id)
            if not field:
                raise ReportPreviewError(f"Unable to locate metadata for field {column.field_id}.")

            table_alias = self._alias_for(field.table_id)
            if not table_alias:
                raise ReportPreviewError(
                    "Select at least one field from a table that participates in the current joins."
                )

            base_expression = f"{table_alias}.{self._quote_identifier(field.name)}"
            aggregate = column.aggregate
            display_label = column.field_name or field.name

            expression, contributes_to_group = self._wrap_expression(base_expression, aggregate)

            alias = self._dedupe_alias(display_label, alias_usage)
            select_fragments.append(f"{expression} AS {self._quote_identifier(alias)}")
            self.column_aliases.append(alias)

            if contributes_to_group:
                group_by_refs.add(expression)

            if column.sort != ReportSortDirection.NONE:
                direction = "ASC" if column.sort == ReportSortDirection.ASC else "DESC"
                order_by_fragments.append(f"{self._quote_identifier(alias)} {direction}")

        if not select_fragments:
            raise ReportPreviewError("Choose at least one output field to preview results.")

        self.selected_columns = select_fragments
        if self.definition.grouping_enabled:
            self.group_by_refs = sorted(group_by_refs)
        else:
            self.group_by_refs = []
        self.order_by_clauses = order_by_fragments

    def _wrap_expression(self, expression: str, aggregate: Optional[ReportAggregateFn]) -> tuple[str, bool]:
        if aggregate is None or aggregate == ReportAggregateFn.GROUP_BY:
            return expression, True

        if aggregate == ReportAggregateFn.SUM:
            return f"SUM({expression})", False
        if aggregate == ReportAggregateFn.AVG:
            return f"AVG({expression})", False
        if aggregate == ReportAggregateFn.MIN:
            return f"MIN({expression})", False
        if aggregate == ReportAggregateFn.MAX:
            return f"MAX({expression})", False
        if aggregate == ReportAggregateFn.COUNT:
            return f"COUNT({expression})", False

        raise ReportPreviewError(
            f"The {aggregate.value} aggregate is not supported in the preview yet."
        )

    def _build_criteria(self) -> None:
        row_groups: List[str] = []
        rows = max([self.definition.criteria_row_count] + [len(col.criteria) for col in self.definition.columns])

        for row_index in range(rows):
            clauses: List[str] = []
            for column in self.definition.columns:
                if row_index >= len(column.criteria):
                    continue
                raw = column.criteria[row_index].strip()
                if not raw:
                    continue
                self._validate_criteria(raw)
                field = self.fields.get(column.field_id)
                if not field:
                    raise ReportPreviewError(f"Unable to locate metadata for field {column.field_id}.")
                alias = self._alias_for(field.table_id)
                if not alias:
                    raise ReportPreviewError(
                        "Filter criteria reference tables that are not part of the preview joins."
                    )
                column_ref = f"{alias}.{self._quote_identifier(field.name)}"
                clauses.append(f"({column_ref} {raw})")
            if clauses:
                row_groups.append(f"({' AND '.join(clauses)})")

        self.where_clauses = row_groups

    def _validate_criteria(self, expression: str) -> None:
        lowered = expression.lower()
        if ";" in expression or "--" in expression or "/*" in lowered or "*/" in lowered:
            raise ReportPreviewError("Preview filters cannot contain SQL comments or statement delimiters.")

    def _alias_for(self, table_id: UUID) -> str:
        alias = self.alias_map.get(table_id)
        if alias:
            return alias
        table = self.tables.get(table_id)
        if not table:
            raise ReportPreviewError(f"Table {table_id} is not part of the current layout.")
        return self._register_alias(table_id)

    def _register_alias(self, table_id: UUID) -> str:
        if table_id in self.alias_map:
            return self.alias_map[table_id]
        alias = f"t{len(self.alias_map) + 1}"
        self.alias_map[table_id] = alias
        return alias

    def _require_table(self, table_id: UUID) -> Table:
        table = self.tables.get(table_id)
        if not table:
            raise ReportPreviewError(f"Unable to locate metadata for table {table_id}.")
        return table

    def _require_field(self, field_id: UUID) -> Field:
        field = self.fields.get(field_id)
        if not field:
            raise ReportPreviewError(f"Unable to locate metadata for field {field_id}.")
        return field

    def _qualify_table(self, table: Table) -> str:
        name = self._quote_identifier(table.physical_name)
        if table.schema_name:
            return f"{self._quote_identifier(table.schema_name)}.{name}"
        return name

    def _quote_identifier(self, value: str) -> str:
        escaped = value.replace('"', '""')
        return f'"{escaped}"'

    def _dedupe_alias(self, base: str, usage: Dict[str, int]) -> str:
        candidate = base or "column"
        count = usage.get(candidate, 0)
        usage[candidate] = count + 1
        if count == 0:
            return candidate
        return f"{candidate}_{count}"


def generate_report_preview(db: Session, definition: ReportDesignerDefinition, *, limit: int) -> ReportPreviewResponse:
    table_ids = {placement.table_id for placement in definition.tables}
    if not table_ids:
        raise ReportPreviewError("Add at least one table before running a preview.")

    field_ids: Set[UUID] = {
        column.field_id for column in definition.columns
    }
    field_ids.update(join.source_field_id for join in definition.joins if join.source_field_id)
    field_ids.update(join.target_field_id for join in definition.joins if join.target_field_id)

    tables = {
        table.id: table
        for table in db.execute(
            select(Table).where(Table.id.in_(table_ids))
        ).scalars()
    }

    missing_tables = table_ids - set(tables)
    if missing_tables:
        raise ReportPreviewError(
            "Some tables referenced by the designer are no longer available: "
            + ", ".join(str(item) for item in missing_tables)
        )

    if field_ids:
        fields = {
            field.id: field
            for field in db.execute(
                select(Field).where(Field.id.in_(field_ids))
            ).scalars()
        }
    else:
        fields = {}

    missing_fields = field_ids - set(fields)
    if missing_fields:
        raise ReportPreviewError(
            "Some fields referenced by the designer are no longer available: "
            + ", ".join(str(item) for item in missing_fields)
        )

    table_sources: List[tuple[Table, str | None, tuple[SystemConnectionType, str] | None]] = []
    for placement in definition.tables:
        table = tables.get(placement.table_id)
        if not table:
            continue
        system = getattr(table, "system", None)
        connection_identity: tuple[SystemConnectionType, str] | None = None
        if system is not None:
            for connection in getattr(system, "connections", []) or []:
                connection_identity = _resolve_connection_identity(connection)
                if connection_identity:
                    break
        table_sources.append((table, getattr(system, "name", None), connection_identity))

    unique_external_connections = {
        identity for _, _, identity in table_sources if identity is not None
    }
    has_local_tables = any(identity is None for _, _, identity in table_sources)

    if unique_external_connections and has_local_tables:
        missing_tables = sorted({table.name for table, _, identity in table_sources if identity is None})
        detail = (
            " Tables without a configured connection: " + ", ".join(missing_tables)
            if missing_tables
            else ""
        )
        raise ReportPreviewError(
            "Preview requires all selected tables to share the same active connection." + detail
        )

    if len(unique_external_connections) > 1:
        system_names = sorted(
            {
                system_name or table.name
                for table, system_name, identity in table_sources
                if identity is not None
            }
        )
        raise ReportPreviewError(
            "Preview currently supports a single source connection. Selected tables span multiple systems: "
            + ", ".join(system_names)
        )

    external_connection = next(iter(unique_external_connections)) if unique_external_connections else None

    builder = ReportSqlBuilder(definition, tables, fields, limit)
    sql = builder.build()

    records_raw = []
    started = time.perf_counter()
    try:
        if external_connection is None:
            result = db.execute(text(sql), builder.bind_params)
            records_raw = result.mappings().all()
        else:
            connection_type, connection_string = external_connection
            try:
                url = resolve_sqlalchemy_url(connection_type, connection_string)
            except UnsupportedConnectionError as exc:
                raise ReportPreviewError(str(exc)) from exc

            engine = create_engine(url, future=True, pool_pre_ping=True)
            try:
                with engine.connect() as connection:
                    result = connection.execute(text(sql), builder.bind_params)
                    records_raw = result.mappings().all()
            finally:
                engine.dispose()
    except SQLAlchemyError as exc:  # pragma: no cover - defensive guard
        raise ReportPreviewError(str(exc)) from exc
    finally:
        duration_ms = (time.perf_counter() - started) * 1000.0

    rows = [dict(record) for record in records_raw]

    return ReportPreviewResponse(
        sql=sql,
        limit=limit,
        rowCount=len(rows),
        durationMs=duration_ms,
        columns=builder.column_aliases,
        rows=rows,
    )


__all__ = ["generate_report_preview", "ReportPreviewError"]
