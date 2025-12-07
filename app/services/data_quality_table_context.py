from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Tuple
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import func, or_
from sqlalchemy.orm import Session, joinedload, object_session

from app.models import ConnectionTableSelection, DataDefinition, DataDefinitionTable, System, SystemConnection
from app.services.data_quality_keys import build_table_group_id, build_table_id
from app.services.table_filters import connection_is_databricks


def _safe_lower(value: str | None) -> str:
    return (value or "").strip().lower()


def _matches_selection(
    selection_schema: str | None,
    selection_table: str | None,
    table_schema: str | None,
    logical_name: str | None,
    physical_name: str | None,
) -> bool:
    sel_schema = _safe_lower(selection_schema)
    sel_table = _safe_lower(selection_table)
    table_schema_norm = _safe_lower(table_schema)
    logical_norm = _safe_lower(logical_name)
    physical_norm = _safe_lower(physical_name)

    if sel_table and sel_table not in {logical_norm, physical_norm}:
        return False

    if not table_schema_norm:
        return True

    return sel_schema == table_schema_norm


def _find_selection_for_table(
    table_link: DataDefinitionTable,
    table_schema: str | None,
    logical_name: str | None,
    physical_name: str | None,
) -> ConnectionTableSelection | None:
    table = table_link.table
    definition: DataDefinition | None = table_link.data_definition
    system: System | None = None

    if table and table.system:
        system = table.system
    elif definition and definition.system:
        system = definition.system

    if system is not None:
        for connection in getattr(system, "connections", []) or []:
            if not getattr(connection, "active", False):
                continue
            for selection in getattr(connection, "catalog_selections", []) or []:
                if _matches_selection(
                    selection.schema_name,
                    selection.table_name,
                    table_schema,
                    logical_name,
                    physical_name,
                ):
                    return selection

    session = object_session(table_link)
    if session is None:
        return None

    normalized_schema = _safe_lower(table_schema)
    normalized_names = {
        value for value in [_safe_lower(logical_name), _safe_lower(physical_name)] if value
    }

    query = (
        session.query(ConnectionTableSelection)
        .join(SystemConnection)
        .filter(SystemConnection.active.is_(True))
    )

    if normalized_schema:
        query = query.filter(func.lower(ConnectionTableSelection.schema_name) == normalized_schema)
    if normalized_names:
        query = query.filter(func.lower(ConnectionTableSelection.table_name).in_(tuple(normalized_names)))

    return query.first()


def _find_global_databricks_connections(table_link: DataDefinitionTable) -> list[SystemConnection]:
    session = object_session(table_link)
    if session is None:
        return []

    return (
        session.query(SystemConnection)
        .filter(
            SystemConnection.active.is_(True),
            or_(
                SystemConnection.use_databricks_managed_connection.is_(True),
                func.lower(SystemConnection.connection_string).like("jdbc:databricks:%"),
            ),
        )
        .all()
    )


@dataclass(frozen=True)
class TableContext:
    data_definition_table_id: UUID
    data_definition_id: UUID
    data_object_id: UUID
    application_id: UUID
    product_team_id: UUID | None
    table_group_id: str
    table_id: str | None
    schema_name: str | None
    table_name: str | None
    physical_name: str | None


def _build_table_context(table_link: DataDefinitionTable) -> TableContext:
    definition: DataDefinition | None = table_link.data_definition
    if (
        definition is None
        or definition.id is None
        or definition.data_object_id is None
        or definition.system is None
        or definition.system.id is None
    ):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Data definition table is not linked to a valid system and data object",
        )

    system: System = definition.system
    data_object = definition.data_object
    if data_object is None or data_object.id is None:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Data definition table is not linked to a valid data object",
        )

    data_object_id = definition.data_object_id

    linked_table = table_link.table
    constructed_table = table_link.constructed_table

    matching_logical_name = linked_table.name if linked_table is not None else table_link.alias
    matching_physical_name = linked_table.physical_name if linked_table is not None else None
    matching_schema = linked_table.schema_name if linked_table is not None else None

    logical_name = table_link.alias or matching_logical_name
    physical_name = matching_physical_name
    table_schema = matching_schema

    if constructed_table is not None:
        logical_name = constructed_table.name or logical_name
        physical_name = constructed_table.name or physical_name
        table_schema = constructed_table.schema_name or table_schema

    is_constructed = bool(constructed_table is not None or table_link.is_construction)

    linked_selection: ConnectionTableSelection | None = table_link.connection_table_selection
    if linked_selection is None:
        linked_selection = _find_selection_for_table(
            table_link,
            matching_schema,
            matching_logical_name,
            matching_physical_name,
        )

    table_group_id: str | None = None
    table_id: str | None = None
    deduped_connections: list[SystemConnection] = []
    seen_ids: set[UUID] = set()

    def _add_connection(connection: SystemConnection | None) -> None:
        if connection is None or connection.id is None:
            return
        if connection.id in seen_ids:
            return
        seen_ids.add(connection.id)
        deduped_connections.append(connection)

    _add_connection(table_link.system_connection)
    if linked_selection and linked_selection.system_connection:
        _add_connection(linked_selection.system_connection)

    explicit_connections_present = bool(deduped_connections)

    if not explicit_connections_present:
        for connection in getattr(system, "connections", []) or []:
            _add_connection(connection)

    if not deduped_connections and is_constructed:
        for connection in _find_global_databricks_connections(table_link):
            _add_connection(connection)

    active_connections = [connection for connection in deduped_connections if connection.active]
    databricks_connections = [
        connection for connection in active_connections if connection_is_databricks(connection)
    ]

    if linked_selection and linked_selection.system_connection and linked_selection.system_connection.active:
        chosen_connection = linked_selection.system_connection
        table_group_id = build_table_group_id(chosen_connection.id, data_object_id)
        table_id = build_table_id(linked_selection.id, data_object_id)
    else:
        candidate_connections = (
            databricks_connections if (is_constructed and databricks_connections) else active_connections
        )

        if not candidate_connections:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                detail=(
                    "No active connection is assigned to this data definition table. "
                    "Assign a catalog selection or connection to continue."
                ),
            )

        chosen_connection = candidate_connections[0]

        if len(candidate_connections) > 1 and not is_constructed:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                detail=(
                    "Multiple active connections are assigned to this data definition table; "
                    "add a catalog selection so profiling can determine the correct connection."
                ),
            )

        if len(candidate_connections) > 1 and is_constructed:
            normalized_schema = _safe_lower(table_schema)
            matched_connection = None
            if normalized_schema:
                for connection in candidate_connections:
                    connection_schema = getattr(connection, "schema_name", None)
                    if _safe_lower(connection_schema) == normalized_schema:
                        matched_connection = connection
                        break
            if matched_connection is None and databricks_connections:
                matched_connection = databricks_connections[0]
            chosen_connection = matched_connection or candidate_connections[0]

        table_group_id = build_table_group_id(chosen_connection.id, data_object_id)
        table_id = None

    return TableContext(
        data_definition_table_id=table_link.id,
        data_definition_id=definition.id,
        data_object_id=data_object_id,
        application_id=system.id,
        product_team_id=data_object.process_area_id if data_object else None,
        table_group_id=table_group_id,
        table_id=table_id,
        schema_name=table_schema,
        table_name=logical_name,
        physical_name=physical_name,
    )


def resolve_table_context(db: Session, data_definition_table_id: UUID) -> TableContext:
    table_link = (
        db.query(DataDefinitionTable)
        .options(
            joinedload(DataDefinitionTable.data_definition).joinedload(DataDefinition.system),
            joinedload(DataDefinitionTable.data_definition).joinedload(DataDefinition.data_object),
            joinedload(DataDefinitionTable.table),
            joinedload(DataDefinitionTable.constructed_table),
            joinedload(DataDefinitionTable.connection_table_selection).joinedload(
                ConnectionTableSelection.system_connection
            ),
        )
        .filter(DataDefinitionTable.id == data_definition_table_id)
        .one_or_none()
    )

    if table_link is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Data definition table not found")

    return _build_table_context(table_link)


def resolve_table_contexts_for_data_object(
    db: Session, data_object_id: UUID
) -> Tuple[List[TableContext], List[UUID]]:
    table_links: Iterable[DataDefinitionTable] = (
        db.query(DataDefinitionTable)
        .join(DataDefinition)
        .options(
            joinedload(DataDefinitionTable.data_definition).joinedload(DataDefinition.system),
            joinedload(DataDefinitionTable.data_definition).joinedload(DataDefinition.data_object),
            joinedload(DataDefinitionTable.table),
            joinedload(DataDefinitionTable.constructed_table),
            joinedload(DataDefinitionTable.connection_table_selection).joinedload(
                ConnectionTableSelection.system_connection
            ),
        )
        .filter(DataDefinition.data_object_id == data_object_id)
        .all()
    )

    contexts: List[TableContext] = []
    skipped: List[UUID] = []
    for table_link in table_links:
        try:
            contexts.append(_build_table_context(table_link))
        except HTTPException:
            skipped.append(table_link.id)
            continue
    return contexts, skipped


def resolve_all_table_contexts(db: Session) -> List[TableContext]:
    table_links: Iterable[DataDefinitionTable] = (
        db.query(DataDefinitionTable)
        .join(DataDefinition)
        .options(
            joinedload(DataDefinitionTable.data_definition).joinedload(DataDefinition.system),
            joinedload(DataDefinitionTable.data_definition).joinedload(DataDefinition.data_object),
            joinedload(DataDefinitionTable.table),
            joinedload(DataDefinitionTable.constructed_table),
        )
        .all()
    )

    contexts: List[TableContext] = []
    for table_link in table_links:
        try:
            contexts.append(_build_table_context(table_link))
        except HTTPException:
            continue
    return contexts
