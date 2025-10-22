from logging import getLogger
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session, selectinload

from app.database import get_db
from app.models import (
    ConstructedTable,
    ConstructedTableApproval,
    DataDefinition,
    ExecutionContext,
    MockCycle,
    System,
)
from app.schemas import (
    ConstructedTableApprovalDecision,
    ConstructedTableCreate,
    ConstructedTableRead,
    ConstructedTableStatus,
    ConstructedTableUpdate,
    SystemConnectionType,
)
from app.services.constructed_table_manager import (
    ConstructedTableManagerError,
    create_or_update_constructed_table,
    drop_constructed_table,
)

logger = getLogger(__name__)

router = APIRouter(prefix="/constructed-tables", tags=["Constructed Tables"])


def _get_constructed_table_or_404(constructed_table_id: UUID, db: Session) -> ConstructedTable:
    constructed_table = db.get(ConstructedTable, constructed_table_id)
    if not constructed_table:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed table not found",
        )
    return constructed_table


def _ensure_execution_context_exists(execution_context_id: UUID, db: Session) -> None:
    if not db.get(ExecutionContext, execution_context_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution context not found",
        )


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
        mock_cycle = constructed_table.execution_context.mock_cycle
        system = getattr(mock_cycle, "system", None)

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
        if constructed_table.fields:
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


def _has_approved_decisions(constructed_table_id: UUID, db: Session) -> bool:
    return (
        db.query(ConstructedTableApproval)
        .filter(
            ConstructedTableApproval.constructed_table_id == constructed_table_id,
            ConstructedTableApproval.decision
            == ConstructedTableApprovalDecision.APPROVED.value,
        )
        .count()
        > 0
    )


@router.post("", response_model=ConstructedTableRead, status_code=status.HTTP_201_CREATED)
def create_constructed_table(
    payload: ConstructedTableCreate, db: Session = Depends(get_db)
) -> ConstructedTableRead:
    _ensure_execution_context_exists(payload.execution_context_id, db)

    constructed_table = ConstructedTable(**payload.dict())
    db.add(constructed_table)
    db.flush()
    
    # Note: Table creation in SQL Server is deferred until fields are added
    # This is because a table needs at least one field to be created
    
    db.commit()
    db.refresh(constructed_table)
    return constructed_table


@router.get("", response_model=list[ConstructedTableRead])
def list_constructed_tables(db: Session = Depends(get_db)) -> list[ConstructedTableRead]:
    return db.query(ConstructedTable).all()


@router.get("/{constructed_table_id}", response_model=ConstructedTableRead)
def get_constructed_table(
    constructed_table_id: UUID, db: Session = Depends(get_db)
) -> ConstructedTableRead:
    return _get_constructed_table_or_404(constructed_table_id, db)


@router.put("/{constructed_table_id}", response_model=ConstructedTableRead)
def update_constructed_table(
    constructed_table_id: UUID,
    payload: ConstructedTableUpdate,
    db: Session = Depends(get_db),
) -> ConstructedTableRead:
    constructed_table = _get_constructed_table_or_404(constructed_table_id, db)

    update_data = payload.dict(exclude_unset=True)

    execution_context_id = update_data.get("execution_context_id")
    if execution_context_id:
        _ensure_execution_context_exists(execution_context_id, db)

    status_value = update_data.get("status")
    if status_value and isinstance(status_value, ConstructedTableStatus):
        status_value = status_value.value
    if status_value == ConstructedTableStatus.APPROVED.value and not _has_approved_decisions(
        constructed_table_id, db
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Constructed table must have at least one approved decision before activation",
        )

    for field, value in update_data.items():
        if isinstance(value, ConstructedTableStatus):
            value = value.value
        setattr(constructed_table, field, value)

    db.commit()
    db.refresh(constructed_table)
    return constructed_table


@router.delete("/{constructed_table_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_constructed_table(
    constructed_table_id: UUID, db: Session = Depends(get_db)
) -> None:
    constructed_table = _get_constructed_table_or_404(constructed_table_id, db)
    
    # Try to drop the table from SQL Server if it exists
    # Load necessary relationships
    constructed_table = db.query(ConstructedTable).options(
        selectinload(ConstructedTable.execution_context).selectinload(
            ExecutionContext.mock_cycle
        ),
        selectinload(ConstructedTable.data_definition).selectinload(DataDefinition.system),
    ).filter(ConstructedTable.id == constructed_table_id).one()

    system = None
    if constructed_table.data_definition and constructed_table.data_definition.system:
        system = constructed_table.data_definition.system
    elif constructed_table.execution_context and constructed_table.execution_context.mock_cycle:
        system = getattr(constructed_table.execution_context.mock_cycle, "system", None)

    if not system:
        db.delete(constructed_table)
        db.commit()
        return
    
    # Get the active SQL Server connection
    sql_server_connection = (
        db.query(System)
        .options(selectinload(System.connections))
        .filter(System.id == system.id)
        .one()
        .connections
    )
    
    sql_conn = None
    for conn in sql_server_connection:
        if (
            conn.active
            and conn.connection_type.lower() == "jdbc"
            and "mssql" in conn.connection_string.lower()
        ):
            sql_conn = conn
            break
    
    if sql_conn:
        try:
            drop_constructed_table(
                connection_type=SystemConnectionType.JDBC,
                connection_string=sql_conn.connection_string,
                schema_name="dbo",
                table_name=constructed_table.name,
            )
        except ConstructedTableManagerError as exc:
            logger.warning(f"Could not drop table from SQL Server: {exc}")
            # Don't fail the delete if table cleanup fails
    
    db.delete(constructed_table)
    db.commit()
