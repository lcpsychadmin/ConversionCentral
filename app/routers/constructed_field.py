from logging import getLogger
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session, selectinload

from app.database import get_db
from app.models import ConstructedField, ConstructedTable, DataDefinition, ExecutionContext, MockCycle, System
from app.schemas import (
    ConstructedFieldCreate,
    ConstructedFieldRead,
    ConstructedFieldUpdate,
    SystemConnectionType,
)
from app.services.constructed_table_manager import (
    ConstructedTableManagerError,
    create_or_update_constructed_table,
)

logger = getLogger(__name__)

router = APIRouter(prefix="/constructed-fields", tags=["Constructed Fields"])


def _get_constructed_field_or_404(constructed_field_id: UUID, db: Session) -> ConstructedField:
    constructed_field = db.get(ConstructedField, constructed_field_id)
    if not constructed_field:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed field not found",
        )
    return constructed_field


def _ensure_constructed_table_exists(constructed_table_id: UUID, db: Session) -> ConstructedTable:
    constructed_table = db.get(ConstructedTable, constructed_table_id)
    if not constructed_table:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed table not found",
        )
    return constructed_table


def _sync_constructed_table_to_sql_server(
    constructed_table: ConstructedTable, db: Session
) -> None:
    """
    Sync a constructed table to SQL Server by creating/updating it with current fields.
    
    Raises HTTPException on failure.
    """
    # Load the full tree: ExecutionContext -> MockCycle -> System with connections
    constructed_table = db.query(ConstructedTable).options(
        selectinload(ConstructedTable.execution_context).selectinload(
            ExecutionContext.mock_cycle
        ),
        selectinload(ConstructedTable.fields),
        selectinload(ConstructedTable.data_definition).selectinload(DataDefinition.system),
    ).filter(ConstructedTable.id == constructed_table.id).one()

    system = None
    if constructed_table.data_definition and constructed_table.data_definition.system:
        system = constructed_table.data_definition.system
    elif constructed_table.execution_context and constructed_table.execution_context.mock_cycle:
        system = getattr(constructed_table.execution_context.mock_cycle, "system", None)

    if not system:
        logger.info(
            "Skipping SQL Server sync for constructed table %s due to missing system context",
            constructed_table.id,
        )
        return

    # Get the active SQL Server connection for this system
    sql_server_connection = (
        db.query(System)
        .options(selectinload(System.connections))
        .filter(System.id == system.id)
        .one()
        .connections
    )
    
    # Find active JDBC connection to SQL Server (mssql dialect)
    sql_conn = None
    for conn in sql_server_connection:
        if (
            conn.active
            and conn.connection_type.lower() == "jdbc"
            and "mssql" in conn.connection_string.lower()
        ):
            sql_conn = conn
            break

    if not sql_conn:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No active SQL Server connection found for this system",
        )

    try:
        create_or_update_constructed_table(
            connection_type=SystemConnectionType.JDBC,
            connection_string=sql_conn.connection_string,
            schema_name="dbo",  # Default schema
            table_name=constructed_table.name,
            fields=constructed_table.fields,
        )
    except ConstructedTableManagerError as exc:
        logger.error(f"Failed to sync constructed table {constructed_table.id}: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create/update table in SQL Server: {str(exc)}",
        )


@router.post("", response_model=ConstructedFieldRead, status_code=status.HTTP_201_CREATED)
def create_constructed_field(
    payload: ConstructedFieldCreate, db: Session = Depends(get_db)
) -> ConstructedFieldRead:
    constructed_table = _ensure_constructed_table_exists(payload.constructed_table_id, db)

    constructed_field = ConstructedField(**payload.dict())
    db.add(constructed_field)
    db.flush()

    # Sync the entire table to SQL Server with all fields
    _sync_constructed_table_to_sql_server(constructed_table, db)

    db.commit()
    db.refresh(constructed_field)
    return constructed_field


@router.get("", response_model=list[ConstructedFieldRead])
def list_constructed_fields(db: Session = Depends(get_db)) -> list[ConstructedFieldRead]:
    return db.query(ConstructedField).all()


@router.get("/{constructed_field_id}", response_model=ConstructedFieldRead)
def get_constructed_field(
    constructed_field_id: UUID, db: Session = Depends(get_db)
) -> ConstructedFieldRead:
    return _get_constructed_field_or_404(constructed_field_id, db)


@router.put("/{constructed_field_id}", response_model=ConstructedFieldRead)
def update_constructed_field(
    constructed_field_id: UUID,
    payload: ConstructedFieldUpdate,
    db: Session = Depends(get_db),
) -> ConstructedFieldRead:
    constructed_field = _get_constructed_field_or_404(constructed_field_id, db)
    update_data = payload.dict(exclude_unset=True)

    constructed_table_id = update_data.get("constructed_table_id")
    if constructed_table_id:
        constructed_table = _ensure_constructed_table_exists(constructed_table_id, db)
    else:
        constructed_table = db.get(ConstructedTable, constructed_field.constructed_table_id)

    for field, value in update_data.items():
        setattr(constructed_field, field, value)

    db.flush()

    # Sync the entire table to SQL Server with all fields
    _sync_constructed_table_to_sql_server(constructed_table, db)

    db.commit()
    db.refresh(constructed_field)
    return constructed_field


@router.delete("/{constructed_field_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_constructed_field(
    constructed_field_id: UUID, db: Session = Depends(get_db)
) -> None:
    constructed_field = _get_constructed_field_or_404(constructed_field_id, db)
    constructed_table = db.get(ConstructedTable, constructed_field.constructed_table_id)
    
    db.delete(constructed_field)
    db.flush()

    # Sync the entire table to SQL Server with remaining fields
    _sync_constructed_table_to_sql_server(constructed_table, db)

    db.commit()
