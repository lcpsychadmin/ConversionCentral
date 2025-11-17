from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set
from uuid import UUID

from sqlalchemy import create_engine, select, text, func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.ingestion.engine import get_ingestion_connection_params, get_ingestion_engine
from app.models.entities import (
    ConnectionTableSelection,
    ConstructedTable,
    Field,
    SystemConnection,
    Table,
)
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
from app.services.constructed_data_warehouse import (
    ConstructedDataWarehouse,
    ConstructedDataWarehouseError,
)
from app.services.scheduled_ingestion import (
    build_ingestion_schema_name,
    build_ingestion_table_name,
)
from app.services.table_filters import table_is_databricks_eligible


_MISSING_TABLE_PATTERN = re.compile(r'relation "(?P<name>[^"]+)" does not exist', re.IGNORECASE)


def _extract_missing_table_name(message: str) -> str | None:
    match = _MISSING_TABLE_PATTERN.search(message)
    if match:
        return match.group("name")
    return None


class ReportPreviewError(Exception):
    """Domain error raised when a preview cannot be generated."""


@dataclass
class _JoinEdge:
    join: ReportDesignerJoin
    source_table_id: UUID
    target_table_id: UUID


@dataclass(frozen=True)
class _ConnectionHandle:
    kind: str
    connection_type: SystemConnectionType | None = None
    connection_string: str | None = None
    dialect: str | None = None


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

    def __init__(
        self,
        definition: ReportDesignerDefinition,
        tables: Dict[UUID, Table],
        fields: Dict[UUID, Field],
        limit: int,
        *,
        quote_char: str = '"',
        schema_overrides: Dict[UUID, str] | None = None,
        name_overrides: Dict[UUID, str] | None = None,
        column_overrides: Dict[UUID, str] | None = None,
    ) -> None:
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
        self.quote_char = quote_char
        self.schema_overrides = schema_overrides or {}
        self.name_overrides = name_overrides or {}
        self.column_overrides = column_overrides or {}

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

        left_column = self._column_ref(left_alias, left_field)
        right_column = self._column_ref(right_alias, right_field)

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

            base_expression = self._column_ref(table_alias, field)
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
                column_ref = self._column_ref(alias, field)
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

    def _column_ref(self, table_alias: str, field: Field) -> str:
        column_name = self.column_overrides.get(field.id, field.name)
        return f"{table_alias}.{self._quote_identifier(column_name)}"

    def _qualify_table(self, table: Table) -> str:
        physical_name = self.name_overrides.get(table.id, table.physical_name)
        if not physical_name:
            raise ReportPreviewError(f"Table {table.id} is missing a physical name for preview.")

        name = self._quote_identifier(physical_name)
        schema_name = self.schema_overrides.get(table.id, table.schema_name)
        schema_sql = self._format_schema_path(schema_name)
        if schema_sql:
            return f"{schema_sql}.{name}"
        return name

    def _quote_identifier(self, value: str) -> str:
        quote = self.quote_char
        if quote == '"':
            escaped = value.replace('"', '""')
        elif quote == "`":
            escaped = value.replace("`", "``")
        else:
            escaped = value.replace(quote, quote * 2)
        return f"{quote}{escaped}{quote}"

    def _format_schema_path(self, schema_name: str | None) -> str:
        if not schema_name:
            return ""
        parts = [part.strip() for part in schema_name.split(".") if part and part.strip()]
        if not parts:
            return ""
        return ".".join(self._quote_identifier(part) for part in parts)

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

    table_sources: List[tuple[Table, str | None, _ConnectionHandle | None]] = []
    schema_overrides: Dict[UUID, str] = {}
    name_overrides: Dict[UUID, str] = {}
    field_column_overrides: Dict[UUID, str] = {}
    constructed_tables_to_ensure: Dict[UUID, ConstructedTable] = {}
    managed_handle: _ConnectionHandle | None = None
    managed_handle_error: ReportPreviewError | None = None
    managed_schema_path: str | None = None
    managed_engine_dialect: str | None = None
    constructed_column_maps: Dict[UUID, Dict[str, str]] = {}

    def _infer_connection_dialect(
        connection_type: SystemConnectionType | None, connection_string: str | None
    ) -> str | None:
        if connection_type is None or not connection_string:
            return None
        if connection_type == SystemConnectionType.JDBC:
            lowered = connection_string.lower()
            if lowered.startswith("jdbc:databricks:"):
                return "databricks"
            if lowered.startswith("jdbc:postgresql:"):
                return "postgresql"
            if lowered.startswith("jdbc:mysql:"):
                return "mysql"
            if lowered.startswith("jdbc:sqlserver:") or lowered.startswith("jdbc:mssql:"):
                return "mssql"
        return None

    def _normalize_constructed_column_name(raw_name: str | None, seen_lower: set[str]) -> str:
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
        while candidate.lower() in seen_lower:
            candidate = f"{sanitized}_{suffix}"
            suffix += 1

        return candidate

    def _resolve_constructed_column_map(constructed_table: ConstructedTable) -> Dict[str, str]:
        cached = constructed_column_maps.get(constructed_table.id)
        if cached is not None:
            return cached

        seen_lower: set[str] = set()
        mapping: Dict[str, str] = {}
        sorted_fields = sorted(
            constructed_table.fields,
            key=lambda item: ((getattr(item, "display_order", 0) or 0), item.name or ""),
        )

        for constructed_field in sorted_fields:
            column_name = _normalize_constructed_column_name(constructed_field.name, seen_lower)
            seen_lower.add(column_name.lower())
            mapping[constructed_field.name] = column_name

        constructed_column_maps[constructed_table.id] = mapping
        return mapping

    def _collapse_external_connections(connections: Set[_ConnectionHandle]) -> _ConnectionHandle | None:
        if not connections:
            return None
        if len(connections) == 1:
            return next(iter(connections))

        databricks_connections = [
            handle for handle in connections if "databricks" in (handle.dialect or "").lower()
        ]
        if len(databricks_connections) != len(connections):
            return None

        try:
            managed_candidate = _ensure_managed_handle()
        except ReportPreviewError:
            return None

        if "databricks" not in (managed_engine_dialect or "").lower():
            return None

        return managed_candidate

    def _ensure_managed_handle() -> _ConnectionHandle:
        nonlocal managed_handle, managed_handle_error, managed_engine_dialect
        if managed_handle is not None:
            return managed_handle
        if managed_handle_error is not None:
            raise managed_handle_error
        try:
            engine = get_ingestion_engine()
            managed_engine_dialect = engine.dialect.name
        except RuntimeError as exc:
            managed_handle_error = ReportPreviewError(
                "Constructed tables require the Databricks SQL warehouse to be configured."
            )
            raise managed_handle_error from exc
        except SQLAlchemyError as exc:  # pragma: no cover - defensive guard
            managed_handle_error = ReportPreviewError(str(exc))
            raise managed_handle_error

        managed_handle = _ConnectionHandle(kind="managed", dialect=managed_engine_dialect)
        return managed_handle

    def _resolve_managed_schema_path() -> str:
        nonlocal managed_schema_path, managed_handle_error, managed_engine_dialect
        if managed_schema_path is not None:
            return managed_schema_path

        if managed_engine_dialect == "sqlite":
            managed_schema_path = ""
            return managed_schema_path

        try:
            params = get_ingestion_connection_params()
        except RuntimeError as exc:
            managed_handle_error = ReportPreviewError(
                "Constructed tables require schema details in the Databricks SQL settings."
            )
            raise managed_handle_error from exc

        schema_part = (
            params.constructed_schema
            or params.schema_name
            or ConstructedDataWarehouse.DEFAULT_SCHEMA
        )
        schema_part = schema_part.strip() if isinstance(schema_part, str) else schema_part
        catalog_part = params.catalog.strip() if isinstance(params.catalog, str) else params.catalog

        if not schema_part:
            managed_handle_error = ReportPreviewError(
                "Constructed tables require a target schema in the Databricks SQL settings."
            )
            raise managed_handle_error

        parts: list[str] = []
        if catalog_part:
            parts.append(catalog_part)
        parts.append(schema_part)

        managed_schema_path = ".".join(parts)
        return managed_schema_path

    selection_cache: Dict[tuple[str | None, str], ConnectionTableSelection | None] = {}

    def _find_catalog_selection(schema_candidate: str | None, table_candidate: str) -> ConnectionTableSelection | None:
        key = (schema_candidate, table_candidate)
        if key in selection_cache:
            return selection_cache[key]

        query = (
            db.query(ConnectionTableSelection)
            .join(ConnectionTableSelection.system_connection)
            .filter(func.lower(ConnectionTableSelection.table_name) == table_candidate)
        )
        if schema_candidate:
            query = query.filter(func.lower(ConnectionTableSelection.schema_name) == schema_candidate)

        selections = query.all()
        chosen: ConnectionTableSelection | None = None

        for candidate in selections:
            connection = getattr(candidate, "system_connection", None)
            if connection is None:
                continue
            resolved = _resolve_connection_identity(connection)
            if not resolved:
                continue
            connection_type, connection_string = resolved
            connection_dialect = _infer_connection_dialect(connection_type, connection_string)
            if connection_dialect == "databricks":
                chosen = candidate
                break
            if chosen is None:
                chosen = candidate

        selection_cache[key] = chosen
        return chosen

    def _sanitize_system_prefix(value: str | None) -> str | None:
        if not value:
            return None
        sanitized = re.sub(r"[^0-9a-z]+", "_", value.lower())
        sanitized = re.sub(r"_+", "_", sanitized).strip("_")
        return sanitized or None

    def _locate_catalog_selection(table: Table) -> ConnectionTableSelection | None:
        physical = (table.physical_name or "").strip()
        display = (table.name or "").strip()
        name_variants: list[str] = []

        for candidate in [physical, display]:
            lowered = candidate.lower()
            if lowered and lowered not in name_variants:
                name_variants.append(lowered)

        schema_raw = (table.schema_name or "").strip()
        schema_variants: list[str | None] = [None]
        if schema_raw:
            lowered_schema = schema_raw.lower()
            schema_variants.append(lowered_schema)
            if "." in lowered_schema:
                schema_variants.append(lowered_schema.split(".")[-1])

        system = getattr(table, "system", None)
        system_prefixes: list[str] = []
        if system is not None:
            for label in (getattr(system, "physical_name", None), getattr(system, "name", None)):
                sanitized = _sanitize_system_prefix(label)
                if sanitized and sanitized not in system_prefixes:
                    system_prefixes.append(sanitized)

        physical_lower = physical.lower()
        for prefix in system_prefixes:
            if prefix and physical_lower.startswith(f"{prefix}_"):
                stripped = physical_lower[len(prefix) + 1 :]
                if stripped and stripped not in name_variants:
                    name_variants.append(stripped)

        if schema_raw:
            schema_prefix = schema_raw.lower().replace(".", "_")
            if physical_lower.startswith(f"{schema_prefix}_"):
                stripped = physical_lower[len(schema_prefix) + 1 :]
                if stripped and stripped not in name_variants:
                    name_variants.append(stripped)

        if not name_variants:
            return None

        for schema_candidate in schema_variants:
            for table_candidate in name_variants:
                selection = _find_catalog_selection(schema_candidate, table_candidate)
                if selection:
                    return selection

        return None

    def _table_requires_databricks(table: Table) -> bool:
        if table_is_databricks_eligible(table):
            return True

        schema_name = (table.schema_name or "").strip()
        physical_name = (table.physical_name or "").strip()

        def _has_databricks_delimiter(value: str) -> bool:
            return "-" in value or "." in value

        if schema_name and _has_databricks_delimiter(schema_name):
            return True

        if physical_name and _has_databricks_delimiter(physical_name):
            return True

        return False

    for placement in definition.tables:
        table = tables.get(placement.table_id)
        if not table:
            continue
        system = getattr(table, "system", None)
        connection_identity: _ConnectionHandle | None = None
        definition_tables = getattr(table, "definition_tables", []) or []
        is_constructed = any(getattr(item, "is_construction", False) for item in definition_tables)
        requires_databricks = _table_requires_databricks(table)
        selection_match: ConnectionTableSelection | None = None
        ingestion_managed = False
        ingestion_source_connection: SystemConnection | None = None

        if is_constructed:
            try:
                connection_identity = _ensure_managed_handle()
            except ReportPreviewError as exc:
                raise ReportPreviewError(
                    f"Constructed table '{table.name}' cannot be previewed until the Databricks warehouse is configured."
                ) from exc
            try:
                schema_path = _resolve_managed_schema_path()
            except ReportPreviewError as exc:
                raise ReportPreviewError(
                    f"Constructed table '{table.name}' cannot be previewed until the Databricks schema is configured."
                ) from exc
            if schema_path:
                schema_overrides[table.id] = schema_path

            constructed_binding = next(
                (item for item in definition_tables if getattr(item, "is_construction", False)),
                None,
            )
            constructed_model = getattr(constructed_binding, "constructed_table", None)
            if constructed_model is None:
                raise ReportPreviewError(
                    f"Constructed table '{table.name}' has not been generated. Run the data construction sync before previewing."
                )
            constructed_name = getattr(constructed_model, "name", None)
            if constructed_name:
                name_overrides[table.id] = constructed_name
            if constructed_model and constructed_model.id not in constructed_tables_to_ensure:
                constructed_tables_to_ensure[constructed_model.id] = constructed_model
                column_map = _resolve_constructed_column_map(constructed_model)
                if column_map:
                    for table_field in getattr(table, "fields", []) or []:
                        override = column_map.get(table_field.name)
                        if override:
                            field_column_overrides[table_field.id] = override
        elif system is not None:
            fallback_handle: _ConnectionHandle | None = None
            for connection in getattr(system, "connections", []) or []:
                resolved = _resolve_connection_identity(connection)
                if not resolved:
                    continue
                connection_type, connection_string = resolved
                connection_dialect = _infer_connection_dialect(connection_type, connection_string)
                handle = _ConnectionHandle(
                    kind="system",
                    connection_type=connection_type,
                    connection_string=connection_string,
                    dialect=connection_dialect,
                )
                if (
                    getattr(connection, "ingestion_enabled", False)
                    and ingestion_source_connection is None
                    and connection_dialect != "databricks"
                ):
                    ingestion_source_connection = connection
                if connection_dialect == "databricks":
                    connection_identity = handle
                    break
                if fallback_handle is None:
                    fallback_handle = handle
            if connection_identity is None and fallback_handle is not None:
                connection_identity = fallback_handle
        if connection_identity is None:
            selection_match = _locate_catalog_selection(table)
            if selection_match is not None:
                resolved = _resolve_connection_identity(selection_match.system_connection)
                if resolved:
                    connection_type, connection_string = resolved
                    connection_dialect = _infer_connection_dialect(connection_type, connection_string)
                    connection_identity = _ConnectionHandle(
                        kind="system",
                        connection_type=connection_type,
                        connection_string=connection_string,
                        dialect=connection_dialect,
                    )
        if selection_match is not None and getattr(selection_match.system_connection, "ingestion_enabled", False):
            resolved = _resolve_connection_identity(selection_match.system_connection)
            if resolved:
                sel_type, sel_string = resolved
                selection_dialect = _infer_connection_dialect(sel_type, sel_string)
                if selection_dialect != "databricks" and ingestion_source_connection is None:
                    ingestion_source_connection = selection_match.system_connection

        if (
            connection_identity is not None
            and (connection_identity.dialect or "").lower() == "databricks"
        ):
            ingestion_source_connection = None

        ingestion_connection = ingestion_source_connection

        if ingestion_connection is not None:
            ingestion_schema = build_ingestion_schema_name(ingestion_connection)
            try:
                connection_identity = _ensure_managed_handle()
            except ReportPreviewError as exc:
                raise ReportPreviewError(
                    f"Table '{table.name}' cannot be previewed until the ingestion warehouse connection is configured."
                ) from exc

            ingestion_managed = True
            if ingestion_schema and (selection_match is not None or not getattr(table, "schema_name", None)):
                schema_overrides[table.id] = ingestion_schema
            if selection_match is not None:
                ingestion_table_name: str | None = None
                try:
                    ingestion_table_name = build_ingestion_table_name(
                        ingestion_connection,
                        selection_match,
                    )
                except Exception:  # pragma: no cover - defensive guard
                    ingestion_table_name = None
                if ingestion_table_name:
                    name_overrides[table.id] = ingestion_table_name

        if (
            selection_match is not None
            and selection_match.schema_name
            and not getattr(table, "schema_name", None)
            and not ingestion_managed
        ):
            schema_overrides[table.id] = selection_match.schema_name.strip()

        if requires_databricks or ingestion_managed:
            is_databricks_identity = (
                connection_identity is not None
                and (connection_identity.dialect or "").lower() == "databricks"
            )
            if not is_databricks_identity:
                try:
                    connection_identity = _ensure_managed_handle()
                except ReportPreviewError as exc:
                    warehouse_label = "ingestion warehouse" if ingestion_managed else "Databricks warehouse"
                    raise ReportPreviewError(
                        f"Table '{table.name}' cannot be previewed until the {warehouse_label} connection is configured."
                    ) from exc

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
        # Try to collapse multiple external connections into a single handle. First attempt
        # databricks-specific collapse (existing behavior). If that fails, allow collapsing
        # when all external connections share the exact same connection string (same remote
        # source configured multiple times), which is a reasonable convenience for setups
        # where the same system was registered more than once.
        collapsed = _collapse_external_connections(unique_external_connections)
        if collapsed is None:
            # Check if all external handles have identical connection_string and connection_type
            connection_strings = { (handle.connection_type, handle.connection_string) for handle in unique_external_connections }
            if len(connection_strings) == 1:
                # Safe to treat them as a single external connection
                collapsed = next(iter(unique_external_connections))

        if collapsed is None:
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

        unique_external_connections = {collapsed}

    external_connection = next(iter(unique_external_connections)) if unique_external_connections else None

    is_managed_connection = (
        external_connection is not None and external_connection.kind == "managed"
    )
    managed_dialect = (external_connection.dialect or "").lower() if is_managed_connection else ""
    is_databricks_managed = is_managed_connection and managed_dialect.startswith("databricks")
    external_dialect = (external_connection.dialect or "").lower() if external_connection else ""
    is_databricks_external = (
        external_connection is not None
        and external_connection.kind != "managed"
        and external_dialect.startswith("databricks")
    )

    if (
        constructed_tables_to_ensure
        and is_databricks_managed
    ):
        try:
            warehouse = ConstructedDataWarehouse()
        except ConstructedDataWarehouseError as exc:  # pragma: no cover - defensive guard
            raise ReportPreviewError(str(exc)) from exc

        for constructed_table in constructed_tables_to_ensure.values():
            try:
                warehouse.ensure_table(constructed_table)
            except ConstructedDataWarehouseError as exc:  # pragma: no cover - defensive guard
                raise ReportPreviewError(str(exc)) from exc

    quote_char = "`" if (is_databricks_managed or is_databricks_external) else '"'

    builder = ReportSqlBuilder(
        definition,
        tables,
        fields,
        limit,
        quote_char=quote_char,
        schema_overrides=schema_overrides,
        name_overrides=name_overrides,
        column_overrides=field_column_overrides,
    )
    sql = builder.build()

    records_raw = []
    started = time.perf_counter()
    try:
        if external_connection is None:
            result = db.execute(text(sql), builder.bind_params)
            records_raw = result.mappings().all()
        elif is_managed_connection:
            engine = get_ingestion_engine()
            with engine.connect() as connection:
                result = connection.execute(text(sql), builder.bind_params)
                records_raw = result.mappings().all()
        else:
            connection_type = external_connection.connection_type
            connection_string = external_connection.connection_string
            if not connection_type or not connection_string:
                raise ReportPreviewError(
                    "Active system connections require both a connection type and connection string."
                )
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
        original = getattr(exc, "orig", None)
        if original is not None:
            sqlstate = getattr(original, "pgcode", None) or getattr(original, "sqlstate", None)
            message = str(original)
            if (sqlstate == "42P01") or ("UndefinedTable" in type(original).__name__):
                missing_table = _extract_missing_table_name(message)
                if missing_table:
                    raise ReportPreviewError(
                        f"Preview failed because warehouse table '{missing_table}' no longer exists. Refresh the catalog or run a new ingestion."
                    ) from exc
                raise ReportPreviewError(
                    "Preview failed because a required warehouse table no longer exists. Refresh the catalog or rerun ingestion."
                ) from exc
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
