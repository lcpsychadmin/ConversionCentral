"""Helpers for locating catalog selections for data definition tables."""

from __future__ import annotations

from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import object_session

from app.models import ConnectionTableSelection, DataDefinitionTable, System, SystemConnection


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


def _system_for_table(table_link: DataDefinitionTable) -> Optional[System]:
    table = getattr(table_link, "table", None)
    if table and table.system:
        return table.system
    definition = getattr(table_link, "data_definition", None)
    if definition and definition.system:
        return definition.system
    return None


def resolve_connection_table_selection(table_link: DataDefinitionTable) -> ConnectionTableSelection | None:
    """Locate the catalog selection that corresponds to the provided definition table."""

    selection = getattr(table_link, "connection_table_selection", None)
    if selection is not None:
        return selection

    table = getattr(table_link, "table", None)
    definition = getattr(table_link, "data_definition", None)

    if table is None or definition is None:
        return None

    matching_schema = getattr(table, "schema_name", None)
    matching_logical_name = getattr(table, "name", None)
    matching_physical_name = getattr(table, "physical_name", None)

    system = _system_for_table(table_link)
    if system is not None:
        for connection in getattr(system, "connections", []) or []:
            if not getattr(connection, "active", False):
                continue
            for candidate in getattr(connection, "catalog_selections", []) or []:
                if _matches_selection(
                    candidate.schema_name,
                    candidate.table_name,
                    matching_schema,
                    matching_logical_name,
                    matching_physical_name,
                ):
                    return candidate

    session = object_session(table_link)
    if session is None:
        return None

    normalized_schema = _safe_lower(matching_schema)
    normalized_names = {
        value for value in [_safe_lower(matching_logical_name), _safe_lower(matching_physical_name)] if value
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


__all__ = ["resolve_connection_table_selection"]
