from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session, selectinload

from app.database import get_db
from app.models import (
    DataDefinition,
    DataDefinitionField,
    DataDefinitionRelationship,
    DataDefinitionTable,
    DataObject,
    DataObjectSystem,
    Field,
    System,
    Table,
    SystemConnection,
    ConnectionTableSelection,
)
from app.schemas import (
    DataDefinitionCreate,
    DataDefinitionRead,
    DataDefinitionRelationshipCreate,
    DataDefinitionRelationshipRead,
    DataDefinitionRelationshipUpdate,
    DataDefinitionUpdate,
)

router = APIRouter(prefix="/data-definitions", tags=["Data Definitions"])


def _definition_query(db: Session):
    return (
        db.query(DataDefinition)
        .options(
            selectinload(DataDefinition.system),
            selectinload(DataDefinition.tables).selectinload(DataDefinitionTable.table),
            selectinload(DataDefinition.tables)
            .selectinload(DataDefinitionTable.fields)
            .selectinload(DataDefinitionField.field),
            selectinload(DataDefinition.relationships)
            .selectinload(DataDefinitionRelationship.primary_field)
            .selectinload(DataDefinitionField.field),
            selectinload(DataDefinition.relationships)
            .selectinload(DataDefinitionRelationship.foreign_field)
            .selectinload(DataDefinitionField.field),
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


def _to_payload_dict(payload):
    if hasattr(payload, "dict"):
        return payload.dict()
    return payload


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

        definition_table = DataDefinitionTable(
            data_definition_id=definition.id,
            table_id=table_id,
            alias=table_data.get("alias"),
            description=table_data.get("description"),
            load_order=load_order,
        )
        db.add(definition_table)
        db.flush()

        seen_fields: set[UUID] = set()
        for field_payload in table_data.get("fields", []):
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

            definition_field = DataDefinitionField(
                definition_table_id=definition_table.id,
                field_id=field_id,
                notes=field_data.get("notes"),
            )
            db.add(definition_field)
            db.flush()


def _update_tables_preserve_relationships(
    definition: DataDefinition, tables_payload, db: Session
) -> None:
    seen_tables: set[UUID] = set()
    seen_load_orders: set[int] = set()

    existing_tables_by_table_id: dict[UUID, DataDefinitionTable] = {
        table.table_id: table for table in definition.tables
    }

    payload_table_ids: set[UUID] = set()

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
            )
            db.add(definition_table)
            db.flush()
            existing_tables_by_table_id[table_id] = definition_table
        else:
            definition_table.alias = table_data.get("alias")
            definition_table.description = table_data.get("description")
            definition_table.load_order = load_order

        existing_fields_by_field_id: dict[UUID, DataDefinitionField] = {
            field.field_id: field for field in definition_table.fields
        }
        desired_field_ids: set[UUID] = set()
        seen_fields: set[UUID] = set()

        for field_payload in table_data.get("fields", []):
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

            definition_field = existing_fields_by_field_id.get(field_id)
            if definition_field:
                definition_field.notes = field_data.get("notes")
            else:
                definition_field = DataDefinitionField(
                    definition_table_id=definition_table.id,
                    field_id=field_id,
                    notes=field_data.get("notes"),
                )
                db.add(definition_field)
                db.flush()

        for field_id, definition_field in list(existing_fields_by_field_id.items()):
            if field_id not in desired_field_ids:
                db.delete(definition_field)

    for table in list(definition.tables):
        if table.table_id not in payload_table_ids:
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
    if not db.get(DataObject, payload.data_object_id):
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

    definition = DataDefinition(
        data_object_id=payload.data_object_id,
        system_id=payload.system_id,
        description=payload.description,
    )
    db.add(definition)
    db.flush()

    _build_tables(definition, payload.tables, db)

    db.commit()
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

    relationship = DataDefinitionRelationship(
        data_definition_id=definition.id,
        primary_table_id=primary_field.definition_table_id,
        primary_field_id=primary_field.id,
        foreign_table_id=foreign_field.definition_table_id,
        foreign_field_id=foreign_field.id,
        relationship_type=payload.relationship_type,
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

    if "relationship_type" in update_data:
        relationship.relationship_type = update_data["relationship_type"]

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
    db: Session = Depends(get_db),
) -> list[DataDefinitionRead]:
    query = _definition_query(db)
    if data_object_id:
        query = query.filter(DataDefinition.data_object_id == data_object_id)
    if system_id:
        query = query.filter(DataDefinition.system_id == system_id)
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
    
    # Get all system connections for these systems
    connections = (
        db.query(SystemConnection)
        .filter(SystemConnection.system_id.in_(system_ids))
        .all()
    )
    connection_ids = [conn.id for conn in connections]
    
    if not connection_ids:
        return []
    
    # Get all selected tables from these connections
    selections = (
        db.query(ConnectionTableSelection)
        .filter(ConnectionTableSelection.system_connection_id.in_(connection_ids))
        .all()
    )
    
    # Format the results
    result = []
    for selection in selections:
        result.append({
            "schemaName": selection.schema_name,
            "tableName": selection.table_name,
            "tableType": selection.table_type,
            "columnCount": selection.column_count,
            "estimatedRows": selection.estimated_rows
        })
    
    return result



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

    if "tables" in update_data:
        _update_tables_preserve_relationships(definition, update_data["tables"], db)

    db.commit()
    return _get_definition_or_404(definition.id, db)


@router.delete("/{definition_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_data_definition(definition_id: UUID, db: Session = Depends(get_db)) -> None:
    definition = _get_definition_or_404(definition_id, db)
    db.delete(definition)
    db.commit()