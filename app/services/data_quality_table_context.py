from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Tuple
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy.orm import Session, joinedload, selectinload

from app.models import DataDefinition, DataDefinitionTable, System, SystemConnection
from app.services.data_quality_keys import build_table_group_id, build_table_id


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

    logical_name = table_link.table.name if table_link.table is not None else None
    physical_name = table_link.table.physical_name if table_link.table is not None else None
    table_schema = table_link.table.schema_name if table_link.table is not None else None

    table_group_id: str | None = None
    table_id: str | None = None

    for connection in system.connections or []:
        if not connection.active:
            continue
        for selection in connection.catalog_selections or []:
            if _matches_selection(
                selection.schema_name,
                selection.table_name,
                table_schema,
                logical_name,
                physical_name,
            ):
                table_group_id = build_table_group_id(connection.id, data_object_id)
                table_id = build_table_id(selection.id, data_object_id)
                break
        if table_group_id:
            break

    if table_group_id is None:
        first_connection = next((conn for conn in system.connections or [] if conn.active), None)
        if first_connection is None:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                detail="No active system connection is available for this data definition table",
            )
        table_group_id = build_table_group_id(first_connection.id, data_object_id)

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
            joinedload(DataDefinitionTable.data_definition)
            .joinedload(DataDefinition.system)
            .joinedload(System.connections)
            .selectinload(SystemConnection.catalog_selections),
            joinedload(DataDefinitionTable.data_definition).joinedload(DataDefinition.data_object),
            joinedload(DataDefinitionTable.table),
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
            joinedload(DataDefinitionTable.data_definition)
            .joinedload(DataDefinition.system)
            .joinedload(System.connections)
            .selectinload(SystemConnection.catalog_selections),
            joinedload(DataDefinitionTable.data_definition).joinedload(DataDefinition.data_object),
            joinedload(DataDefinitionTable.table),
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
            joinedload(DataDefinitionTable.data_definition)
            .joinedload(DataDefinition.system)
            .joinedload(System.connections)
            .selectinload(SystemConnection.catalog_selections),
            joinedload(DataDefinitionTable.data_definition).joinedload(DataDefinition.data_object),
            joinedload(DataDefinitionTable.table),
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
