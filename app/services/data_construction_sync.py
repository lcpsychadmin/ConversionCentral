from __future__ import annotations

import re
from logging import getLogger
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy.orm import Session, selectinload

from app.config import get_settings
from app.models import (
    ConstructedField,
    ConstructedTable,
    DataDefinition,
    DataDefinitionField,
    DataDefinitionTable,
    System,
)
from app.schemas import ConstructedTableStatus, SystemConnectionType
from app.services.constructed_table_manager import (
    ConstructedTableManagerError,
    create_or_update_constructed_table,
    drop_constructed_table,
)

if TYPE_CHECKING:  # pragma: no cover - import guarded for type checking only
    from app.models import SystemConnection


logger = getLogger(__name__)

_FIELD_TYPE_MAP: dict[str, str] = {
    "varchar": "string",
    "nvarchar": "string",
    "char": "string",
    "nchar": "string",
    "text": "text",
    "string": "string",
    "uuid": "uuid",
    "uniqueidentifier": "uuid",
    "int": "integer",
    "integer": "integer",
    "smallint": "integer",
    "bigint": "bigint",
    "numeric": "decimal",
    "decimal": "decimal",
    "number": "decimal",
    "float": "float",
    "double": "double",
    "real": "float",
    "bit": "boolean",
    "boolean": "boolean",
    "bool": "boolean",
    "date": "date",
    "datetime": "datetime",
    "datetime2": "datetime",
    "timestamp": "timestamp",
    "time": "time",
}


def sync_construction_tables_for_definition(definition_id: UUID, db: Session) -> None:
    """Ensure constructed tables mirror the current data definition configuration."""
    definition = (
        db.query(DataDefinition)
        .options(
            selectinload(DataDefinition.system).selectinload(System.connections),
            selectinload(DataDefinition.tables)
            .selectinload(DataDefinitionTable.table),
            selectinload(DataDefinition.tables)
            .selectinload(DataDefinitionTable.fields)
            .selectinload(DataDefinitionField.field),
            selectinload(DataDefinition.tables)
            .selectinload(DataDefinitionTable.constructed_table)
            .selectinload(ConstructedTable.fields),
        )
        .filter(DataDefinition.id == definition_id)
        .one()
    )

    settings = get_settings()
    system = definition.system
    active_constructed_table_ids: set[UUID] = set()

    for table in definition.tables:
        if table.is_construction:
            constructed_table = _ensure_constructed_table(definition, table, db)
            _sync_constructed_fields(table, constructed_table, db)
            constructed_table.status = ConstructedTableStatus.APPROVED.value
            active_constructed_table_ids.add(constructed_table.id)

            if settings.enable_sql_server_sync:
                _sync_sql_server_table(system, table, constructed_table)
        else:
            if table.constructed_table:
                _drop_sql_table(system, table.constructed_table, settings.enable_sql_server_sync)
                db.delete(table.constructed_table)

    if not active_constructed_table_ids:
        stale_tables = (
            db.query(ConstructedTable)
            .filter(ConstructedTable.data_definition_id == definition.id)
            .all()
        )
    else:
        stale_tables = (
            db.query(ConstructedTable)
            .filter(
                ConstructedTable.data_definition_id == definition.id,
                ConstructedTable.id.notin_(active_constructed_table_ids),
            )
            .all()
        )

    for constructed_table in stale_tables:
        _drop_sql_table(system, constructed_table, settings.enable_sql_server_sync)
        db.delete(constructed_table)


def _ensure_constructed_table(
    definition: DataDefinition,
    definition_table: DataDefinitionTable,
    db: Session,
) -> ConstructedTable:
    constructed_table = definition_table.constructed_table

    if not constructed_table:
        constructed_table = ConstructedTable(
            data_definition_id=definition.id,
            data_definition_table_id=definition_table.id,
            name=_build_constructed_table_name(definition_table),
            description=definition_table.description or definition_table.table.description,
            purpose=_build_constructed_table_purpose(definition_table),
        )
        db.add(constructed_table)
        db.flush()
        definition_table.constructed_table = constructed_table
    else:
        constructed_table.data_definition_id = definition.id
        constructed_table.data_definition_table_id = definition_table.id
        if definition_table.description:
            constructed_table.description = definition_table.description
        constructed_table.purpose = _build_constructed_table_purpose(definition_table)

    return constructed_table


def _sync_constructed_fields(
    definition_table: DataDefinitionTable,
    constructed_table: ConstructedTable,
    db: Session,
) -> None:
    existing_fields_by_name: dict[str, ConstructedField] = {
        field.name: field for field in constructed_table.fields
    }
    desired_field_names: set[str] = set()

    for definition_field in definition_table.fields:
        source_field = definition_field.field
        field_name = source_field.name
        desired_field_names.add(field_name)

        data_type = _map_field_type(source_field.field_type)
        is_nullable = not source_field.system_required
        description = definition_field.notes or source_field.description

        constructed_field = existing_fields_by_name.get(field_name)
        if constructed_field:
            constructed_field.data_type = data_type
            constructed_field.is_nullable = is_nullable
            constructed_field.description = description
        else:
            constructed_field = ConstructedField(
                constructed_table_id=constructed_table.id,
                name=field_name,
                data_type=data_type,
                is_nullable=is_nullable,
                description=description,
            )
            db.add(constructed_field)

    for field_name, constructed_field in list(existing_fields_by_name.items()):
        if field_name not in desired_field_names:
            db.delete(constructed_field)


def _sync_sql_server_table(
    system: System,
    definition_table: DataDefinitionTable,
    constructed_table: ConstructedTable,
) -> None:
    connection = _find_sql_server_connection(system)
    if not connection:
        logger.info(
            "Skipping SQL Server sync for constructed table %s because no active connection was found",
            constructed_table.name,
        )
        return

    try:
        connection_type = SystemConnectionType(connection.connection_type)
    except ValueError:
        logger.warning(
            "Unsupported connection type '%s' for system '%s' when syncing constructed table %s",
            connection.connection_type,
            system.name,
            constructed_table.name,
        )
        return

    schema_name = (definition_table.table.schema_name or "dbo") if definition_table.table else "dbo"

    try:
        create_or_update_constructed_table(
            connection_type=connection_type,
            connection_string=connection.connection_string,
            schema_name=schema_name,
            table_name=constructed_table.name,
            fields=constructed_table.fields,
        )
    except ConstructedTableManagerError as exc:
        logger.warning(
            "Failed to sync constructed table '%s' to SQL Server: %s",
            constructed_table.name,
            exc,
        )


def _drop_sql_table(
    system: System,
    constructed_table: ConstructedTable,
    sync_enabled: bool,
) -> None:
    if not sync_enabled:
        return
    connection = _find_sql_server_connection(system)
    if not connection:
        return

    try:
        connection_type = SystemConnectionType(connection.connection_type)
    except ValueError:
        logger.warning(
            "Unsupported connection type '%s' for system '%s' when dropping constructed table %s",
            connection.connection_type,
            system.name,
            constructed_table.name,
        )
        return

    schema_name = "dbo"
    if constructed_table.data_definition_table and constructed_table.data_definition_table.table:
        schema_name = constructed_table.data_definition_table.table.schema_name or "dbo"

    try:
        drop_constructed_table(
            connection_type=connection_type,
            connection_string=connection.connection_string,
            schema_name=schema_name,
            table_name=constructed_table.name,
        )
    except ConstructedTableManagerError as exc:
        logger.warning(
            "Failed to drop constructed table '%s' from SQL Server: %s",
            constructed_table.name,
            exc,
        )


def _find_sql_server_connection(system: System):
    for connection in getattr(system, "connections", []) or []:
        if (
            connection.active
            and connection.connection_type
            and connection.connection_type.lower() == "jdbc"
            and "mssql" in (connection.connection_string or "").lower()
        ):
            return connection
    return None


def _build_constructed_table_name(definition_table: DataDefinitionTable) -> str:
    base_name = definition_table.alias or (
        definition_table.table.name if definition_table.table else "constructed"
    )
    slug = _slugify(base_name)
    return f"constructed_{slug}_{str(definition_table.id)[:8]}"


def _build_constructed_table_purpose(definition_table: DataDefinitionTable) -> str:
    label = definition_table.alias or (
        definition_table.table.name if definition_table.table else "table"
    )
    return f"Data construction table for {label}"


def _map_field_type(field_type: str | None) -> str:
    if not field_type:
        return "string"
    return _FIELD_TYPE_MAP.get(field_type.lower(), "string")


def _slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", value.lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "table"
