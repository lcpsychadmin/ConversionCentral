"""Helpers for deriving ingestion schema and table names."""

from __future__ import annotations

import re

from app.models import ConnectionTableSelection, SystemConnection


def _sanitize_part(value: str | None) -> str:
    if not value:
        return "segment"
    sanitized = re.sub(r"[^A-Za-z0-9]+", "_", value.strip())
    sanitized = sanitized.strip("_")
    return sanitized.lower() or "segment"


def _should_prefix_schema(selection: ConnectionTableSelection) -> bool:
    table_type = (getattr(selection, "table_type", "") or "").strip().lower()
    return table_type != "constructed"


def build_ingestion_schema_name(connection: SystemConnection) -> str | None:
    system = getattr(connection, "system", None)
    base_name: str | None = None
    if system and system.name:
        base_name = system.name
    elif system and getattr(system, "physical_name", None):
        base_name = system.physical_name
    elif getattr(connection, "id", None):
        base_name = f"connection_{str(connection.id)[:8]}"

    if not base_name:
        return None
    return _sanitize_part(base_name)


def build_ingestion_table_name(
    _connection: SystemConnection,
    selection: ConnectionTableSelection,
) -> str:
    table_part = _sanitize_part(selection.table_name)
    if not _should_prefix_schema(selection):
        return table_part

    schema_part = _sanitize_part(selection.schema_name or "default")
    return "_".join(part for part in (schema_part, table_part) if part)
