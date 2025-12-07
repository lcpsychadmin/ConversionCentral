from uuid import UUID
from typing import List, Sequence
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session, selectinload

from app.config import get_settings
from app.constants.audit_fields import AUDIT_FIELD_DEFINITIONS, AUDIT_FIELD_NAME_SET
from app.database import get_db
from app.models import (
    ConnectionTableSelection,
    DataDefinition,
    DataDefinitionField,
    DataDefinitionRelationship,
    DataDefinitionTable,
    DataObject,
    DataObjectSystem,
    Field,
    System,
    SystemConnection,
    Table,
)
from app.schemas import (
    DataDefinitionCreate,
    DataDefinitionRead,
    DataDefinitionRelationshipCreate,
    DataDefinitionRelationshipRead,
    DataDefinitionRelationshipUpdate,
    DataDefinitionUpdate,
    TableRead,
)
from app.services.catalog_browser import ConnectionCatalogError, fetch_source_table_columns
from app.services.data_construction_sync import (
    delete_constructed_tables_for_definition,
    sync_construction_tables_for_definition,
)
from app.services.data_quality_provisioning import trigger_data_quality_provisioning
from app.services.workspace_scope import resolve_workspace_id
from app.services.table_filters import (
    connection_is_databricks,
    table_is_databricks_eligible,
)


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data-definitions", tags=["Data Definitions"])


@router.get("/databricks-tables", response_model=List[TableRead])
def list_databricks_tables(db: Session = Depends(get_db)) -> List[TableRead]:
    tables = (
        db.query(Table)
        .options(
            selectinload(Table.system),
            selectinload(Table.definition_tables),
        )
        .order_by(Table.name.asc())
        .all()
    )

    return [table for table in tables if table_is_databricks_eligible(table)]


def _definition_query(db: Session):
    return (
        db.query(DataDefinition)
        .options(
            selectinload(DataDefinition.system),
            selectinload(DataDefinition.data_object).selectinload(DataObject.process_area),
            selectinload(DataDefinition.tables).selectinload(DataDefinitionTable.table),
            selectinload(DataDefinition.tables)
            .selectinload(DataDefinitionTable.fields)
            .selectinload(DataDefinitionField.field),
            selectinload(DataDefinition.tables)
            .selectinload(DataDefinitionTable.system_connection),
            selectinload(DataDefinition.tables)
            .selectinload(DataDefinitionTable.connection_table_selection),
            selectinload(DataDefinition.relationships)
            .selectinload(DataDefinitionRelationship.primary_field)
            .selectinload(DataDefinitionField.field),
            selectinload(DataDefinition.relationships)
            .selectinload(DataDefinitionRelationship.foreign_field)
            .selectinload(DataDefinitionField.field),
            selectinload(DataDefinition.tables).selectinload(DataDefinitionTable.constructed_table),
        )
        .order_by(DataDefinition.created_at.desc())
    )


def _relationship_query(definition_id: UUID, db: Session):
    return (
        db.query(DataDefinitionRelationship)
        .options(
            selectinload(DataDefinitionRelationship.primary_field).selectinload(
                DataDefinitionField.field
            ),
            selectinload(DataDefinitionRelationship.foreign_field).selectinload(
                DataDefinitionField.field
            ),
        )
        .filter(DataDefinitionRelationship.data_definition_id == definition_id)
        .order_by(DataDefinitionRelationship.created_at.asc())
    )


def _get_definition_or_404(definition_id: UUID, db: Session) -> DataDefinition:
    definition = _definition_query(db).filter(DataDefinition.id == definition_id).one_or_none()
    if not definition:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data definition not found")
    return definition


def _refresh_data_quality_metadata(reason: str) -> None:
    """Trigger metadata reprovisioning when definitions change."""

    settings = get_settings()
    if not getattr(settings, "databricks_data_quality_auto_manage_tables", True):
        return
    trigger_data_quality_provisioning(reason=reason, wait=True)


def _ensure_link_exists(data_object_id: UUID, system_id: UUID, db: Session) -> None:
    link = (
        db.query(DataObjectSystem)
        .filter(
            DataObjectSystem.data_object_id == data_object_id,
            DataObjectSystem.system_id == system_id,
        )
        .first()
    )
    if not link:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Data object is not associated with the specified system.",
        )


def _ensure_audit_fields_for_definition_table(
    definition_table: DataDefinitionTable, db: Session
) -> None:
    """Guarantee the audit fields exist on the constructed table definition."""
    table = definition_table.table or db.get(Table, definition_table.table_id)
    if not table:
        return

    existing_table_fields_by_name = {
        field.name.lower(): field for field in table.fields
    }
    existing_table_fields_by_id = {field.id: field for field in table.fields}
    existing_definition_fields_by_id = {
        definition_field.field_id: definition_field for definition_field in definition_table.fields
    }

    non_audit_display_orders = [
        definition_field.display_order
        for definition_field in definition_table.fields
        if existing_table_fields_by_id.get(definition_field.field_id)
        and existing_table_fields_by_id[definition_field.field_id].name.lower() not in AUDIT_FIELD_NAME_SET
    ]
    max_non_audit_display_order = max(non_audit_display_orders, default=-1)
    next_display_order = max_non_audit_display_order + 1

    for spec in AUDIT_FIELD_DEFINITIONS:
        lookup_name = spec["name"].lower()
        field = existing_table_fields_by_name.get(lookup_name)
        if not field:
            field = Field(
                table_id=table.id,
                name=spec["name"],
                description=spec.get("description"),
                field_type=spec["field_type"],
                field_length=spec.get("field_length"),
                decimal_places=spec.get("decimal_places"),
                system_required=spec.get("system_required", False),
                business_process_required=spec.get("business_process_required", False),
                suppressed_field=False,
                active=True,
                reference_table=spec.get("reference_table"),
            )
            db.add(field)
            db.flush()
            table.fields.append(field)
            existing_table_fields_by_name[lookup_name] = field
            existing_table_fields_by_id[field.id] = field
        else:
            # Align core metadata for pre-existing audit fields
            if spec.get("description") and field.description != spec["description"]:
                field.description = spec["description"]
            if field.field_type != spec["field_type"]:
                field.field_type = spec["field_type"]
            if spec.get("field_length") is not None and field.field_length != spec.get("field_length"):
                field.field_length = spec.get("field_length")
            if spec.get("decimal_places") is not None and field.decimal_places != spec.get("decimal_places"):
                field.decimal_places = spec.get("decimal_places")
            desired_system_required = spec.get("system_required")
            if desired_system_required is not None and field.system_required != desired_system_required:
                field.system_required = desired_system_required
            desired_bpr = spec.get("business_process_required")
            if desired_bpr is not None and field.business_process_required != desired_bpr:
                field.business_process_required = desired_bpr
            if field.reference_table != spec.get("reference_table"):
                field.reference_table = spec.get("reference_table")

        if field.id not in existing_definition_fields_by_id:
            definition_field = DataDefinitionField(
                definition_table_id=definition_table.id,
                field_id=field.id,
                notes=spec.get("notes"),
                display_order=next_display_order,
                is_unique=False,
            )
            db.add(definition_field)
            db.flush()
            definition_table.fields.append(definition_field)
            existing_definition_fields_by_id[field.id] = definition_field
        else:
            notes = spec.get("notes")
            if notes is not None:
                existing_definition_fields_by_id[field.id].notes = notes
            existing_definition_fields_by_id[field.id].display_order = next_display_order
            existing_definition_fields_by_id[field.id].is_unique = False

        next_display_order += 1


def _remove_audit_fields_for_definition_table(
    definition_table: DataDefinitionTable, db: Session
) -> None:
    """Remove audit definition fields when construction mode is disabled."""
    for definition_field in list(definition_table.fields):
        field = definition_field.field
        if field and field.name.lower() in AUDIT_FIELD_NAME_SET:
            if definition_field not in db.deleted:
                db.delete(definition_field)
            definition_table.fields.remove(definition_field)


def _to_payload_dict(payload):
    if hasattr(payload, "dict"):
        return payload.dict()
    return payload


def _extract_payload_value(payload: dict, snake_key: str, camel_key: str):
    if snake_key in payload:
        return payload.get(snake_key), True
    if camel_key in payload:
        return payload.get(camel_key), True
    return None, False


def _validate_selection_matches_table(selection: ConnectionTableSelection, table: Table) -> None:
    selection_table = (selection.table_name or "").strip().lower()
    table_name = ((table.physical_name or table.name) or "").strip().lower()
    if selection_table and table_name and selection_table != table_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Selected catalog entry does not match the requested table.",
        )

    selection_schema = (selection.schema_name or "").strip().lower()
    table_schema = (table.schema_name or "").strip().lower()
    if selection_schema and table_schema and selection_schema != table_schema:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Selected catalog entry does not match the requested schema.",
        )


def _resolve_connection_assignment(
    table_payload: dict,
    table: Table,
    db: Session,
) -> tuple[UUID | None, UUID | None, bool, bool]:
    selection_id, selection_present = _extract_payload_value(
        table_payload,
        "connection_table_selection_id",
        "connectionTableSelectionId",
    )
    connection_id, connection_present = _extract_payload_value(
        table_payload,
        "system_connection_id",
        "systemConnectionId",
    )

    if selection_id:
        selection = db.get(ConnectionTableSelection, selection_id)
        if not selection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Catalog selection not found.",
            )
        _validate_selection_matches_table(selection, table)
        if not selection.system_connection_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Catalog selection is not linked to an active connection.",
            )
        connection_id = selection.system_connection_id
        connection_present = True
    elif selection_present and selection_id is None:
        selection_id = None

    if connection_id:
        connection = db.get(SystemConnection, connection_id)
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="System connection not found.",
            )
    elif connection_present and connection_id is None:
        connection_id = None

    return connection_id, selection_id, (connection_present or selection_present), selection_present


def _build_tables(definition: DataDefinition, tables_payload, db: Session) -> None:
    seen_tables: set[UUID] = set()
    seen_load_orders: set[int] = set()
    for table_payload in tables_payload:
        table_data = _to_payload_dict(table_payload)
        table_id = table_data["table_id"]
        load_order = table_data.get("load_order")

        if load_order is not None:
            if not isinstance(load_order, int) or load_order <= 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Table load order must be a positive integer when provided.",
                )
            if load_order in seen_load_orders:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Duplicate table load order detected in payload.",
                )
            seen_load_orders.add(load_order)

        if table_id in seen_tables:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Duplicate table detected in payload.",
            )
        seen_tables.add(table_id)

        table = db.get(Table, table_id)
        if not table:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
        if table.system_id != definition.system_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Table does not belong to the selected system.",
            )

        (
            connection_id,
            selection_id,
            _connection_specified,
            _selection_specified,
        ) = _resolve_connection_assignment(table_data, table, db)

        definition_table = DataDefinitionTable(
            data_definition_id=definition.id,
            table_id=table_id,
            alias=table_data.get("alias"),
            description=table_data.get("description"),
            load_order=load_order,
            is_construction=bool(table_data.get("is_construction", False)),
            system_connection_id=connection_id,
            connection_table_selection_id=selection_id,
        )
        db.add(definition_table)
        db.flush()
        definition_table.table = table

        seen_fields: set[UUID] = set()
        for index, field_payload in enumerate(table_data.get("fields", [])):
            field_data = _to_payload_dict(field_payload)
            field_id = field_data["field_id"]

            if field_id in seen_fields:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Duplicate field detected in payload for table.",
                )
            seen_fields.add(field_id)

            field = db.get(Field, field_id)
            if not field:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")
            if field.table_id != table.id:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Field does not belong to the selected table.",
                )

            display_order = field_data.get("display_order")
            if display_order is None or not isinstance(display_order, int):
                display_order = index

            definition_field = DataDefinitionField(
                definition_table_id=definition_table.id,
                field_id=field_id,
                notes=field_data.get("notes"),
                display_order=display_order,
                is_unique=bool(field_data.get("is_unique", False)),
            )
            db.add(definition_field)
            db.flush()
            definition_table.fields.append(definition_field)

        if definition_table.is_construction:
            _ensure_audit_fields_for_definition_table(definition_table, db)


def _update_tables_preserve_relationships(
    definition: DataDefinition, tables_payload, db: Session
) -> list[DataDefinitionTable]:
    seen_tables: set[UUID] = set()
    seen_load_orders: set[int] = set()

    existing_tables_by_table_id: dict[UUID, DataDefinitionTable] = {
        table.table_id: table for table in definition.tables
    }

    payload_table_ids: set[UUID] = set()
    tables_to_remove: list[DataDefinitionTable] = []

    for table_payload in tables_payload:
        table_data = _to_payload_dict(table_payload)
        table_id = table_data["table_id"]
        payload_table_ids.add(table_id)

        load_order = table_data.get("load_order")

        if load_order is not None:
            if not isinstance(load_order, int) or load_order <= 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Table load order must be a positive integer when provided.",
                )
            if load_order in seen_load_orders:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Duplicate table load order detected in payload.",
                )
            seen_load_orders.add(load_order)

        if table_id in seen_tables:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Duplicate table detected in payload.",
            )
        seen_tables.add(table_id)

        table = db.get(Table, table_id)
        if not table:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
        if table.system_id != definition.system_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Table does not belong to the selected system.",
            )

        definition_table = existing_tables_by_table_id.get(table_id)
        if not definition_table:
            definition_table = DataDefinitionTable(
                data_definition_id=definition.id,
                table_id=table_id,
                alias=table_data.get("alias"),
                description=table_data.get("description"),
                load_order=load_order,
                is_construction=bool(table_data.get("is_construction", False)),
            )
            db.add(definition_table)
            db.flush()
            existing_tables_by_table_id[table_id] = definition_table
            definition_table.table = table
        else:
            definition_table.alias = table_data.get("alias")
            definition_table.description = table_data.get("description")
            definition_table.load_order = load_order
            definition_table.is_construction = bool(table_data.get("is_construction", False))
            if definition_table.table is None:
                definition_table.table = table

        (
            connection_id,
            selection_id,
            connection_specified,
            selection_specified,
        ) = _resolve_connection_assignment(table_data, table, db)

        if connection_specified:
            definition_table.system_connection_id = connection_id
        if selection_specified:
            definition_table.connection_table_selection_id = selection_id

        existing_fields_by_field_id: dict[UUID, DataDefinitionField] = {
            field.field_id: field for field in definition_table.fields
        }
        table_fields_by_id: dict[UUID, Field] = {field.id: field for field in table.fields}
        desired_field_ids: set[UUID] = set()
        seen_fields: set[UUID] = set()

        for index, field_payload in enumerate(table_data.get("fields", [])):
            field_data = _to_payload_dict(field_payload)
            field_id = field_data["field_id"]

            if field_id in seen_fields:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Duplicate field detected in payload for table.",
                )
            seen_fields.add(field_id)
            desired_field_ids.add(field_id)

            field = db.get(Field, field_id)
            if not field:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")
            if field.table_id != table.id:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Field does not belong to the selected table.",
                )

            display_order = field_data.get("display_order")
            if display_order is None or not isinstance(display_order, int):
                display_order = index

            definition_field = existing_fields_by_field_id.get(field_id)
            if definition_field:
                definition_field.notes = field_data.get("notes")
                definition_field.display_order = display_order
                definition_field.is_unique = bool(field_data.get("is_unique", False))
            else:
                definition_field = DataDefinitionField(
                    definition_table_id=definition_table.id,
                    field_id=field_id,
                    notes=field_data.get("notes"),
                    display_order=display_order,
                    is_unique=bool(field_data.get("is_unique", False)),
                )
                db.add(definition_field)
                db.flush()
                existing_fields_by_field_id[field_id] = definition_field
                definition_table.fields.append(definition_field)

        for field_id, definition_field in list(existing_fields_by_field_id.items()):
            table_field = table_fields_by_id.get(field_id)
            if table_field and table_field.name.lower() in AUDIT_FIELD_NAME_SET:
                continue
            if field_id not in desired_field_ids:
                db.delete(definition_field)
                if definition_field in definition_table.fields:
                    definition_table.fields.remove(definition_field)

        if definition_table.is_construction:
            _ensure_audit_fields_for_definition_table(definition_table, db)
        else:
            _remove_audit_fields_for_definition_table(definition_table, db)

    for table in list(definition.tables):
        if table.table_id not in payload_table_ids:
            table.is_construction = False
            tables_to_remove.append(table)

    return tables_to_remove


def _delete_definition_tables(
    tables_to_remove: Sequence[DataDefinitionTable], db: Session
) -> None:
    for table in tables_to_remove:
        if table in db.deleted:
            continue

        if table.constructed_table and table.constructed_table not in db.deleted:
            db.delete(table.constructed_table)

        parent = table.data_definition
        if parent and table in parent.tables:
            parent.tables.remove(table)

        db.delete(table)


def _get_relationship_or_404(
    definition_id: UUID, relationship_id: UUID, db: Session
) -> DataDefinitionRelationship:
    relationship = (
        _relationship_query(definition_id, db)
        .filter(DataDefinitionRelationship.id == relationship_id)
        .one_or_none()
    )
    if not relationship:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Relationship not found")
    return relationship


def _ensure_definition_field(
    definition: DataDefinition, field_id: UUID, role: str, db: Session
) -> DataDefinitionField:
    field = (
        db.query(DataDefinitionField)
        .options(selectinload(DataDefinitionField.definition_table))
        .filter(DataDefinitionField.id == field_id)
        .one_or_none()
    )
    if not field:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"{role} field not found")
    if field.definition_table.data_definition_id != definition.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{role} field does not belong to this data definition.",
        )
    return field


@router.post("", response_model=DataDefinitionRead, status_code=status.HTTP_201_CREATED)
def create_data_definition(
    payload: DataDefinitionCreate, db: Session = Depends(get_db)
) -> DataDefinitionRead:
    data_object = db.get(DataObject, payload.data_object_id)
    if not data_object:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data object not found")
    if not db.get(System, payload.system_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="System not found")

    _ensure_link_exists(payload.data_object_id, payload.system_id, db)

    existing = (
        db.query(DataDefinition)
        .filter(
            DataDefinition.data_object_id == payload.data_object_id,
            DataDefinition.system_id == payload.system_id,
        )
        .first()
    )
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A data definition already exists for this data object and system.",
        )
    workspace_id = data_object.workspace_id or resolve_workspace_id(db, None)

    definition = DataDefinition(
        data_object_id=payload.data_object_id,
        system_id=payload.system_id,
        description=payload.description,
        workspace_id=workspace_id,
    )
    db.add(definition)
    db.flush()

    _build_tables(definition, payload.tables, db)
    db.flush()
    sync_construction_tables_for_definition(definition.id, db)
    db.commit()
    _refresh_data_quality_metadata("data-definition-created")
    return _get_definition_or_404(definition.id, db)


@router.get(
    "/{definition_id}/relationships",
    response_model=list[DataDefinitionRelationshipRead],
)
def list_relationships(
    definition_id: UUID, db: Session = Depends(get_db)
) -> list[DataDefinitionRelationshipRead]:
    _get_definition_or_404(definition_id, db)
    return _relationship_query(definition_id, db).all()


@router.post(
    "/{definition_id}/relationships",
    response_model=DataDefinitionRelationshipRead,
    status_code=status.HTTP_201_CREATED,
)
def create_relationship(
    definition_id: UUID,
    payload: DataDefinitionRelationshipCreate,
    db: Session = Depends(get_db),
) -> DataDefinitionRelationshipRead:
    definition = _get_definition_or_404(definition_id, db)

    primary_field = _ensure_definition_field(definition, payload.primary_field_id, "Primary", db)
    foreign_field = _ensure_definition_field(definition, payload.foreign_field_id, "Foreign", db)

    if primary_field.id == foreign_field.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Primary and foreign fields must be different.",
        )
    if primary_field.definition_table_id == foreign_field.definition_table_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Relationships must connect fields from different tables.",
        )

    existing_relationship = (
        db.query(DataDefinitionRelationship)
        .filter(
            DataDefinitionRelationship.data_definition_id == definition.id,
            DataDefinitionRelationship.primary_field_id == payload.primary_field_id,
            DataDefinitionRelationship.foreign_field_id == payload.foreign_field_id,
        )
        .one_or_none()
    )
    if existing_relationship:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A relationship between the selected fields already exists.",
        )

    relationship = DataDefinitionRelationship(
        data_definition_id=definition.id,
        primary_table_id=primary_field.definition_table_id,
        primary_field_id=primary_field.id,
        foreign_table_id=foreign_field.definition_table_id,
        foreign_field_id=foreign_field.id,
        join_type=payload.join_type,
        notes=payload.notes,
    )
    db.add(relationship)
    db.commit()
    return _get_relationship_or_404(definition.id, relationship.id, db)


@router.get(
    "/{definition_id}/relationships/{relationship_id}",
    response_model=DataDefinitionRelationshipRead,
)
def get_relationship(
    definition_id: UUID,
    relationship_id: UUID,
    db: Session = Depends(get_db),
) -> DataDefinitionRelationshipRead:
    _get_definition_or_404(definition_id, db)
    return _get_relationship_or_404(definition_id, relationship_id, db)


@router.put(
    "/{definition_id}/relationships/{relationship_id}",
    response_model=DataDefinitionRelationshipRead,
)
def update_relationship(
    definition_id: UUID,
    relationship_id: UUID,
    payload: DataDefinitionRelationshipUpdate,
    db: Session = Depends(get_db),
) -> DataDefinitionRelationshipRead:
    definition = _get_definition_or_404(definition_id, db)
    relationship = _get_relationship_or_404(definition_id, relationship_id, db)

    update_data = payload.dict(exclude_unset=True)

    if "primary_field_id" in update_data:
        primary_field = _ensure_definition_field(
            definition, update_data["primary_field_id"], "Primary", db
        )
        relationship.primary_field_id = primary_field.id
        relationship.primary_table_id = primary_field.definition_table_id

    if "foreign_field_id" in update_data:
        foreign_field = _ensure_definition_field(
            definition, update_data["foreign_field_id"], "Foreign", db
        )
        relationship.foreign_field_id = foreign_field.id
        relationship.foreign_table_id = foreign_field.definition_table_id

    if relationship.primary_field_id == relationship.foreign_field_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Primary and foreign fields must be different.",
        )
    if relationship.primary_table_id == relationship.foreign_table_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Relationships must connect different tables.",
        )

    if "join_type" in update_data:
        relationship.join_type = update_data["join_type"]

    if "notes" in update_data:
        relationship.notes = update_data["notes"]

    db.commit()
    return _get_relationship_or_404(definition.id, relationship.id, db)


@router.delete(
    "/{definition_id}/relationships/{relationship_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_relationship(
    definition_id: UUID,
    relationship_id: UUID,
    db: Session = Depends(get_db),
) -> None:
    _get_definition_or_404(definition_id, db)
    relationship = _get_relationship_or_404(definition_id, relationship_id, db)
    db.delete(relationship)
    db.commit()


@router.get("", response_model=list[DataDefinitionRead])
def list_data_definitions(
    data_object_id: UUID | None = Query(default=None),
    system_id: UUID | None = Query(default=None),
    workspace_id: UUID | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[DataDefinitionRead]:
    query = _definition_query(db)
    if data_object_id:
        query = query.filter(DataDefinition.data_object_id == data_object_id)
    if system_id:
        query = query.filter(DataDefinition.system_id == system_id)

    if workspace_id is not None:
        resolved_workspace_id = resolve_workspace_id(db, workspace_id)
        query = query.filter(DataDefinition.workspace_id == resolved_workspace_id)
    elif data_object_id is None:
        resolved_workspace_id = resolve_workspace_id(db, None)
        query = query.filter(DataDefinition.workspace_id == resolved_workspace_id)

    return query.all()


@router.get("/data-objects/{data_object_id}/available-source-tables")
def get_available_source_tables(
    data_object_id: UUID,
    db: Session = Depends(get_db),
) -> list[dict]:
    """
    Get all selected tables from system connections of a data object's systems.
    Returns tables grouped by system and connection.
    """
    # Get the data object
    data_object = db.query(DataObject).filter(DataObject.id == data_object_id).one_or_none()
    if not data_object:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data object not found")
    
    # Get the systems associated with this data object
    system_links = (
        db.query(DataObjectSystem)
        .filter(DataObjectSystem.data_object_id == data_object_id)
        .all()
    )
    system_ids = [link.system_id for link in system_links]
    
    if not system_ids:
        return []
    
    # Get all system connections (some seed data omits explicit system links)
    connections: list[SystemConnection] = db.query(SystemConnection).all()
    scoped_connections = [
        connection for connection in connections if connection.system_id in system_ids
    ]

    allowed_tables: list[dict] = []
    seen_keys: set[tuple[str | None, str]] = set()

    def append_tables_from_connections(target_connections: list[SystemConnection]) -> None:
        if not target_connections:
            return

        connection_ids = [connection.id for connection in target_connections]
        selections = (
            db.query(ConnectionTableSelection)
            .filter(ConnectionTableSelection.system_connection_id.in_(connection_ids))
            .all()
        )

        for selection in selections:
            key = ((selection.schema_name or "").lower(), selection.table_name.lower())
            if key in seen_keys:
                continue
            seen_keys.add(key)
            allowed_tables.append(
                {
                    "catalogName": getattr(selection, "catalog_name", None),
                    "schemaName": selection.schema_name,
                    "tableName": selection.table_name,
                    "tableType": selection.table_type,
                    "columnCount": selection.column_count,
                    "estimatedRows": selection.estimated_rows,
                    "selectionId": selection.id,
                    "systemConnectionId": selection.system_connection_id,
                }
            )

    # Always prefer system-scoped connections so tables stay prioritized to the selected system.
    append_tables_from_connections(scoped_connections)

    if not allowed_tables:
        # Fallback to every connection so locally profiled sources (that may not be linked to a
        # system) still remain available.
        fallback_connections = [
            connection for connection in connections if connection not in scoped_connections
        ]
        append_tables_from_connections(fallback_connections if fallback_connections else connections)

    return allowed_tables


@router.get("/source-table-columns/{data_object_id}")
def get_source_table_columns(
    data_object_id: UUID,
    schema_name: str = Query(...),
    table_name: str = Query(...),
    db: Session = Depends(get_db),
) -> list[dict]:
    """
    Get column metadata for a specific source table from a data object's system connections.
    Finds the appropriate connection that has this table selected and retrieves column details.
    """
    # Get the data object
    data_object = db.query(DataObject).filter(DataObject.id == data_object_id).one_or_none()
    if not data_object:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data object not found")
    
    # Get the systems associated with this data object
    system_links = (
        db.query(DataObjectSystem)
        .filter(DataObjectSystem.data_object_id == data_object_id)
        .all()
    )
    system_ids = [link.system_id for link in system_links]
    
    if not system_ids:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No systems associated with this data object"
        )
    
    # Get all system connections for these systems
    connections = db.query(SystemConnection).all()

    if not connections:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No connections found for this data object's systems",
        )

    connection_with_table: SystemConnection | None = None
    databricks_candidates: list[SystemConnection] = []

    for conn in connections:
        if connection_is_databricks(conn):
            databricks_candidates.append(conn)

        selection = (
            db.query(ConnectionTableSelection)
            .filter(
                ConnectionTableSelection.system_connection_id == conn.id,
                ConnectionTableSelection.schema_name == schema_name,
                ConnectionTableSelection.table_name == table_name,
            )
            .one_or_none()
        )
        if selection:
            connection_with_table = conn
            break

    candidate_connections: list[SystemConnection]
    if connection_with_table:
        candidate_connections = [connection_with_table]
    else:
        if not databricks_candidates:
            logger.info(
                "No Databricks connection with selection for %s.%s; attempting global Databricks connections.",
                schema_name,
                table_name,
            )
            global_connections = db.query(SystemConnection).all()
            databricks_candidates = [
                connection
                for connection in global_connections
                if connection_is_databricks(connection)
            ]

        candidate_connections = databricks_candidates

    if not candidate_connections:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table {schema_name}.{table_name} not found in any connection",
        )

    last_error: ConnectionCatalogError | None = None
    schema_param = schema_name or None
    table_param = table_name or None

    for candidate in candidate_connections:
        try:
            columns = fetch_source_table_columns(
                connection_type=candidate.connection_type,
                connection_string=candidate.connection_string,
                schema_name=schema_param,
                table_name=table_param,
            )
        except ConnectionCatalogError as exc:
            logger.warning(
                "Failed to fetch column metadata from connection %s: %s",
                candidate.id,
                exc,
            )
            last_error = exc
            continue

        result = [
            {
                "name": col.name,
                "typeName": col.type_name,
                "length": col.length,
                "numericPrecision": col.numeric_precision,
                "numericScale": col.numeric_scale,
                "nullable": col.nullable,
            }
            for col in columns
        ]
        return result

    if last_error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch column metadata: {last_error}",
        )

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Table {schema_name}.{table_name} not found in any connection",
    )


@router.get("/{definition_id}", response_model=DataDefinitionRead)
def get_data_definition(definition_id: UUID, db: Session = Depends(get_db)) -> DataDefinitionRead:
    return _get_definition_or_404(definition_id, db)


@router.put("/{definition_id}", response_model=DataDefinitionRead)
def update_data_definition(
    definition_id: UUID,
    payload: DataDefinitionUpdate,
    db: Session = Depends(get_db),
) -> DataDefinitionRead:
    definition = _get_definition_or_404(definition_id, db)

    update_data = payload.dict(exclude_unset=True)
    if "description" in update_data:
        definition.description = update_data["description"]

    tables_to_remove: list[DataDefinitionTable] = []
    if "tables" in update_data:
        tables_to_remove = _update_tables_preserve_relationships(
            definition, update_data["tables"], db
        )

    db.flush()
    sync_construction_tables_for_definition(definition.id, db)
    _delete_definition_tables(tables_to_remove, db)
    db.commit()
    _refresh_data_quality_metadata("data-definition-updated")
    return _get_definition_or_404(definition.id, db)


@router.delete("/{definition_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_data_definition(definition_id: UUID, db: Session = Depends(get_db)) -> None:
    definition = _get_definition_or_404(definition_id, db)
    delete_constructed_tables_for_definition(definition.id, db)
    db.delete(definition)
    db.commit()
    _refresh_data_quality_metadata("data-definition-deleted")