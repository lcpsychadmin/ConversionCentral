from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Set, Tuple
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.models import (
    ConnectionTableSelection,
    DataDefinition,
    DataDefinitionTable,
    DataObject,
    DataObjectSystem,
    Table,
)

_PROJECT_PREFIX = "workspace:"
_CONNECTION_PREFIX = "conn:"
_TABLE_GROUP_PREFIX = "group:"
_TABLE_PREFIX = "selection:"


def _split_identifier(value: str | None) -> list[str]:
    text = (value or "").strip().lower()
    return text.split(":") if text else []


def _normalize_identifier(value: UUID | str) -> str:
    text = str(value)
    return text.strip().lower()


def build_project_key(workspace_id: UUID | str) -> str:
    return f"{_PROJECT_PREFIX}{_normalize_identifier(workspace_id)}"


def build_connection_id(connection_id: UUID | str, data_object_id: UUID | str) -> str:
    return f"{_CONNECTION_PREFIX}{_normalize_identifier(connection_id)}:{_normalize_identifier(data_object_id)}"


def build_table_group_id(connection_id: UUID | str, data_object_id: UUID | str) -> str:
    return f"{_TABLE_GROUP_PREFIX}{_normalize_identifier(connection_id)}:{_normalize_identifier(data_object_id)}"


def build_table_id(selection_id: UUID | str, data_object_id: UUID | str) -> str:
    return f"{_TABLE_PREFIX}{_normalize_identifier(selection_id)}:{_normalize_identifier(data_object_id)}"


def parse_project_key(project_key: str) -> Tuple[Optional[str], Optional[str]]:
    if not project_key:
        return None, None
    parts = project_key.split(":")
    if len(parts) >= 2 and parts[0] == "workspace":
        return parts[1], None
    return None, None


def parse_connection_id(value: str | None) -> Tuple[Optional[str], Optional[str]]:
    parts = _split_identifier(value)
    if len(parts) >= 3 and parts[0] == _CONNECTION_PREFIX.rstrip(":"):
        return parts[1], parts[2]
    return None, None


def parse_table_group_id(value: str | None) -> Tuple[Optional[str], Optional[str]]:
    parts = _split_identifier(value)
    if len(parts) >= 3 and parts[0] == _TABLE_GROUP_PREFIX.rstrip(":"):
        return parts[1], parts[2]
    return None, None


def project_keys_for_data_object(db: Session, data_object_id: Optional[UUID | str]) -> Set[str]:
    if not data_object_id:
        return set()
    stmt = select(DataObject.workspace_id).where(DataObject.id == data_object_id)
    workspace_ids = db.execute(stmt).scalars().all()
    return {build_project_key(workspace_id) for workspace_id in workspace_ids if workspace_id}


def project_keys_for_system(db: Session, system_id: Optional[UUID | str]) -> Set[str]:
    if not system_id:
        return set()
    stmt = (
        select(DataObject.workspace_id)
        .join(DataObjectSystem, DataObjectSystem.data_object_id == DataObject.id)
        .where(DataObjectSystem.system_id == system_id)
        .distinct()
    )
    workspace_ids = db.execute(stmt).scalars().all()
    return {build_project_key(workspace_id) for workspace_id in workspace_ids if workspace_id}


def project_keys_for_table(db: Session, table_id: Optional[UUID | str]) -> Set[str]:
    if not table_id:
        return set()
    stmt = (
        select(DataObject.workspace_id)
        .join(DataDefinition, DataDefinition.data_object_id == DataObject.id)
        .join(DataDefinitionTable, DataDefinitionTable.data_definition_id == DataDefinition.id)
        .where(DataDefinitionTable.table_id == table_id)
        .distinct()
    )
    keys: Set[str] = set()
    for workspace_id in db.execute(stmt).scalars():
        if workspace_id:
            keys.add(build_project_key(workspace_id))
    return keys


def project_keys_for_table_selection(
    db: Session,
    selection: Optional[ConnectionTableSelection],
) -> Set[str]:
    if selection is None:
        return set()

    stmt = (
        select(DataObject.workspace_id)
        .join(DataDefinition, DataDefinition.data_object_id == DataObject.id)
        .join(DataDefinitionTable, DataDefinitionTable.data_definition_id == DataDefinition.id)
        .where(DataDefinitionTable.connection_table_selection_id == selection.id)
        .distinct()
    )

    keys: Set[str] = set()
    for workspace_id in db.execute(stmt).scalars():
        if workspace_id:
            keys.add(build_project_key(workspace_id))

    if keys:
        return keys

    # Fallback for legacy definitions that have not yet been linked to selections.
    normalized_table = (selection.table_name or "").strip().lower()
    normalized_schema = (selection.schema_name or "").strip().lower()

    if not normalized_table:
        return set()

    stmt = (
        select(DataObject.workspace_id)
        .join(DataDefinition, DataDefinition.data_object_id == DataObject.id)
        .join(DataDefinitionTable, DataDefinitionTable.data_definition_id == DataDefinition.id)
        .join(Table, Table.id == DataDefinitionTable.table_id)
        .where(
            or_(
                func.lower(Table.physical_name) == normalized_table,
                func.lower(Table.name) == normalized_table,
            )
        )
        .distinct()
    )

    if normalized_schema:
        stmt = stmt.where(func.lower(func.coalesce(Table.schema_name, "")) == normalized_schema)

    for workspace_id in db.execute(stmt).scalars():
        if workspace_id:
            keys.add(build_project_key(workspace_id))

    if keys:
        return keys

    return keys


@dataclass(frozen=True)
class DefinitionTableKey:
    schema_name: str
    table_name: str

    @classmethod
    def from_table(cls, schema_name: Optional[str], *names: Optional[str]) -> Set["DefinitionTableKey"]:
        normalized_schema = (schema_name or "").strip().lower()
        keys: Set[DefinitionTableKey] = set()
        for candidate in names:
            if not candidate:
                continue
            normalized_name = candidate.strip().lower()
            if not normalized_name:
                continue
            keys.add(cls(schema_name=normalized_schema, table_name=normalized_name))
        return keys


def create_definition_table_key_set(definitions: Iterable[DataDefinition]) -> Set[DefinitionTableKey]:
    keys: Set[DefinitionTableKey] = set()
    for definition in definitions:
        for table_link in definition.tables:
            table = table_link.table
            if table is None:
                continue
            keys.update(
                DefinitionTableKey.from_table(
                    table.schema_name,
                    table.physical_name,
                    table.name,
                )
            )
    return keys


def selection_matches_keys(
    schema_name: Optional[str],
    table_name: Optional[str],
    keys: Set[DefinitionTableKey],
) -> bool:
    if not keys:
        return True
    if not table_name:
        return False
    candidate = DefinitionTableKey(
        schema_name=(schema_name or "").strip().lower(),
        table_name=table_name.strip().lower(),
    )
    return candidate in keys
