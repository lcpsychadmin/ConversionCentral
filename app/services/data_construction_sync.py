from __future__ import annotations

import re
from logging import getLogger
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy.orm import Session, selectinload

from app.config import get_settings
from app.constants.audit_fields import AUDIT_FIELD_NAME_SET
from app.models import (
    ConstructedDataValidationRule,
    ConstructedField,
    ConstructedTable,
    DataDefinition,
    DataDefinitionField,
    DataDefinitionTable,
    System,
)
from app.schemas import ConstructedTableStatus, SystemConnectionType
from app.services.constructed_data_warehouse import (
    ConstructedDataWarehouse,
    ConstructedDataWarehouseError,
)
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

_TYPE_VALIDATION_PATTERNS: dict[str, tuple[str, str]] = {
    "integer": (r"^-?\d+$", "{field} must be a whole number."),
    "bigint": (r"^-?\d+$", "{field} must be a whole number."),
    "decimal": (r"^-?\d+(\.\d+)?$", "{field} must be a numeric value."),
    "float": (r"^-?\d+(\.\d+)?$", "{field} must be a numeric value."),
    "double": (r"^-?\d+(\.\d+)?$", "{field} must be a numeric value."),
    "uuid": (
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
        "{field} must be a valid UUID.",
    ),
    "date": (r"^\d{4}-\d{2}-\d{2}$", "{field} must be a date in YYYY-MM-DD format."),
    "datetime": (
        r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$",
        "{field} must be a datetime in YYYY-MM-DD HH:MM:SS format.",
    ),
    "timestamp": (
        r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$",
        "{field} must be a datetime in YYYY-MM-DD HH:MM:SS format.",
    ),
    "time": (r"^\d{2}:\d{2}(:\d{2})?$", "{field} must be a time in HH:MM or HH:MM:SS format."),
    "boolean": (r"^(?i:true|false|0|1)$", "{field} must be true or false."),
}

CONSTRUCTED_TABLE_SCHEMA = "construction_data"


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
            selectinload(DataDefinition.tables)
            .selectinload(DataDefinitionTable.constructed_table)
            .selectinload(ConstructedTable.validation_rules),
        )
        .filter(DataDefinition.id == definition_id)
        .one()
    )

    settings = get_settings()
    system = definition.system
    try:
        warehouse = ConstructedDataWarehouse()
    except Exception as exc:  # pragma: no cover - defensive fallback
        logger.warning(
            "Skipping Databricks warehouse sync for definition %s due to configuration error: %s",
            definition.id,
            exc,
        )
        warehouse = None
    active_constructed_table_ids: set[UUID] = set()

    for table in definition.tables:
        if table.is_construction:
            constructed_table = _ensure_constructed_table(definition, table, db)
            _sync_constructed_fields(table, constructed_table, db)
            db.flush()
            _sync_validation_rules(table, constructed_table, db)
            constructed_table.status = ConstructedTableStatus.APPROVED.value
            active_constructed_table_ids.add(constructed_table.id)

            if settings.enable_constructed_table_sync:
                _sync_sql_server_table(system, table, constructed_table)
            if warehouse:
                try:
                    warehouse.ensure_table(constructed_table)
                except ConstructedDataWarehouseError as exc:
                    logger.warning(
                        "Failed to ensure Databricks constructed table '%s': %s",
                        constructed_table.name,
                        exc,
                    )
        else:
            if table.constructed_table:
                _drop_sql_table(system, table.constructed_table, settings.enable_constructed_table_sync)
                if warehouse:
                    try:
                        warehouse.drop_table(table.constructed_table)
                    except ConstructedDataWarehouseError as exc:
                        logger.warning(
                            "Failed to drop Databricks constructed table '%s': %s",
                            table.constructed_table.name,
                            exc,
                        )
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
        _drop_sql_table(system, constructed_table, settings.enable_constructed_table_sync)
        if warehouse:
            try:
                warehouse.drop_table(constructed_table)
            except ConstructedDataWarehouseError as exc:
                logger.warning(
                    "Failed to drop Databricks constructed table '%s': %s",
                    constructed_table.name,
                    exc,
                )
        db.delete(constructed_table)


def delete_constructed_tables_for_definition(definition_id: UUID, db: Session) -> None:
    """Clean up constructed tables and external warehouse artifacts before definition removal."""
    definition = (
        db.query(DataDefinition)
        .options(
            selectinload(DataDefinition.system).selectinload(System.connections),
            selectinload(DataDefinition.constructed_tables),
        )
        .filter(DataDefinition.id == definition_id)
        .one_or_none()
    )

    if not definition:
        return

    settings = get_settings()

    try:
        warehouse = ConstructedDataWarehouse()
    except Exception as exc:  # pragma: no cover - defensive fallback
        logger.warning(
            "Skipping Databricks warehouse cleanup for definition %s due to configuration error: %s",
            definition_id,
            exc,
        )
        warehouse = None

    system = definition.system

    for constructed_table in list(definition.constructed_tables):
        if system:
            _drop_sql_table(system, constructed_table, settings.enable_constructed_table_sync)
        if warehouse:
            try:
                warehouse.drop_table(constructed_table)
            except ConstructedDataWarehouseError as exc:
                logger.warning(
                    "Failed to drop Databricks constructed table '%s' during definition delete: %s",
                    constructed_table.name,
                    exc,
                )
        if constructed_table in definition.constructed_tables:
            definition.constructed_tables.remove(constructed_table)
        db.delete(constructed_table)

    db.flush()


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
        display_order = definition_field.display_order

        constructed_field = existing_fields_by_name.get(field_name)
        if constructed_field:
            constructed_field.data_type = data_type
            constructed_field.is_nullable = is_nullable
            constructed_field.description = description
            constructed_field.display_order = display_order
        else:
            constructed_field = ConstructedField(
                constructed_table_id=constructed_table.id,
                name=field_name,
                data_type=data_type,
                is_nullable=is_nullable,
                description=description,
                display_order=display_order,
            )
            db.add(constructed_field)
            # Keep relationship collection in sync so downstream consumers see new fields immediately
            if constructed_field not in constructed_table.fields:
                constructed_table.fields.append(constructed_field)
            existing_fields_by_name[field_name] = constructed_field

    for field_name, constructed_field in list(existing_fields_by_name.items()):
        if field_name not in desired_field_names:
            db.delete(constructed_field)


def _serialize_config_value(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple(sorted((k, _serialize_config_value(v)) for k, v in value.items()))
    if isinstance(value, list):
        return tuple(_serialize_config_value(item) for item in value)
    return value


def _make_rule_key(
    rule_type: str, field_id: UUID | None, configuration: dict[str, Any]
) -> tuple[str, UUID | None, Any]:
    return (
        rule_type,
        field_id,
        _serialize_config_value(configuration),
    )


def _generate_auto_rule_specs(
    definition_field: DataDefinitionField,
    source_field,
    constructed_field: ConstructedField,
) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []

    if not constructed_field.id or not source_field:
        return specs

    field_name = constructed_field.name
    if field_name.lower() in AUDIT_FIELD_NAME_SET:
        return specs

    field_id = constructed_field.id

    if getattr(source_field, "system_required", False):
        specs.append(
            {
                "name": f"{field_name} is required",
                "description": "Auto-generated required constraint based on data definition metadata.",
                "rule_type": "required",
                "field_id": field_id,
                "configuration": {"fieldName": field_name},
                "error_message": f"{field_name} is required.",
                "is_active": True,
                "applies_to_new_only": False,
            }
        )

    if getattr(definition_field, "is_unique", False):
        specs.append(
            {
                "name": f"{field_name} must be unique",
                "description": "Auto-generated uniqueness constraint based on data definition metadata.",
                "rule_type": "unique",
                "field_id": field_id,
                "configuration": {"fieldName": field_name},
                "error_message": f"{field_name} must be unique.",
                "is_active": True,
                "applies_to_new_only": False,
            }
        )

    max_length = getattr(source_field, "field_length", None)
    if max_length and constructed_field.data_type in {"string", "text"}:
        pattern = rf"^.{{0,{max_length}}}$"
        specs.append(
            {
                "name": f"{field_name} length",
                "description": "Auto-generated length constraint based on data definition metadata.",
                "rule_type": "pattern",
                "field_id": field_id,
                "configuration": {"fieldName": field_name, "pattern": pattern},
                "error_message": f"{field_name} must be at most {max_length} characters.",
                "is_active": True,
                "applies_to_new_only": False,
            }
        )

    data_type = constructed_field.data_type.lower() if constructed_field.data_type else ""
    pattern_entry = _TYPE_VALIDATION_PATTERNS.get(data_type)
    if pattern_entry:
        pattern, message_template = pattern_entry
        decimal_places = getattr(source_field, "decimal_places", None)
        if data_type in {"decimal", "float", "double"} and decimal_places is not None:
            if decimal_places <= 0:
                pattern = r"^-?\d+$"
                message_template = "{field} must be a whole number."
            else:
                pattern = rf"^-?\d+(\.\d{{1,{decimal_places}}})?$"
                plural = "s" if decimal_places != 1 else ""
                message_template = f"{{field}} may include up to {decimal_places} decimal place{plural}."

        specs.append(
            {
                "name": f"{field_name} type",
                "description": "Auto-generated type constraint based on data definition metadata.",
                "rule_type": "pattern",
                "field_id": field_id,
                "configuration": {"fieldName": field_name, "pattern": pattern},
                "error_message": message_template.format(field=field_name),
                "is_active": True,
                "applies_to_new_only": False,
            }
        )

    return specs


def _sync_validation_rules(
    definition_table: DataDefinitionTable,
    constructed_table: ConstructedTable,
    db: Session,
) -> None:
    constructed_fields_by_name: dict[str, ConstructedField] = {
        field.name: field for field in constructed_table.fields
    }

    existing_auto_rules: dict[tuple[str, UUID | None, Any], ConstructedDataValidationRule] = {
        _make_rule_key(rule.rule_type, rule.field_id, rule.configuration): rule
        for rule in constructed_table.validation_rules
        if rule.is_system_generated
    }

    desired_specs: list[dict[str, Any]] = []
    for definition_field in definition_table.fields:
        source_field = getattr(definition_field, "field", None)
        if not source_field:
            continue
        constructed_field = constructed_fields_by_name.get(source_field.name)
        if not constructed_field:
            continue
        desired_specs.extend(
            _generate_auto_rule_specs(definition_field, source_field, constructed_field)
        )

    seen_keys: set[tuple[str, UUID | None, Any]] = set()

    for spec in desired_specs:
        key = _make_rule_key(spec["rule_type"], spec["field_id"], spec["configuration"])
        if key in seen_keys:
            continue
        existing_rule = existing_auto_rules.pop(key, None)
        if existing_rule:
            existing_rule.name = spec["name"]
            existing_rule.description = spec["description"]
            existing_rule.configuration = spec["configuration"]
            existing_rule.error_message = spec["error_message"]
            existing_rule.is_active = spec["is_active"]
            existing_rule.applies_to_new_only = spec["applies_to_new_only"]
            seen_keys.add(key)
            continue

        new_rule = ConstructedDataValidationRule(
            constructed_table_id=constructed_table.id,
            field_id=spec["field_id"],
            name=spec["name"],
            description=spec["description"],
            rule_type=spec["rule_type"],
            configuration=spec["configuration"],
            error_message=spec["error_message"],
            is_active=spec["is_active"],
            applies_to_new_only=spec["applies_to_new_only"],
            is_system_generated=True,
        )
        db.add(new_rule)
        constructed_table.validation_rules.append(new_rule)
        seen_keys.add(key)

    for obsolete_rule in existing_auto_rules.values():
        if obsolete_rule in constructed_table.validation_rules:
            constructed_table.validation_rules.remove(obsolete_rule)
        db.delete(obsolete_rule)


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

    schema_name = CONSTRUCTED_TABLE_SCHEMA

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

    schema_name = CONSTRUCTED_TABLE_SCHEMA

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
