from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import System, Table, SystemConnection
from app.schemas import TableCreate, TableRead, TableUpdate, ConnectionTablePreview
from app.services.catalog_browser import fetch_table_preview, TablePreviewError
from app.schemas import SystemConnectionType

router = APIRouter(prefix="/tables", tags=["Tables"])


def _get_table_or_404(table_id: UUID, db: Session) -> Table:
    table = db.get(Table, table_id)
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    return table


@router.post("", response_model=TableRead, status_code=status.HTTP_201_CREATED)
def create_table(payload: TableCreate, db: Session = Depends(get_db)) -> TableRead:
    if not db.get(System, payload.system_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="System not found")

    table = Table(**payload.dict())
    db.add(table)
    db.commit()
    db.refresh(table)
    return table


@router.get("", response_model=list[TableRead])
def list_tables(db: Session = Depends(get_db)) -> list[TableRead]:
    return db.query(Table).all()


@router.get("/{table_id}", response_model=TableRead)
def get_table(table_id: UUID, db: Session = Depends(get_db)) -> TableRead:
    return _get_table_or_404(table_id, db)


@router.put("/{table_id}", response_model=TableRead)
def update_table(
    table_id: UUID, payload: TableUpdate, db: Session = Depends(get_db)
) -> TableRead:
    table = _get_table_or_404(table_id, db)

    update_data = payload.dict(exclude_unset=True)
    system_id = update_data.get("system_id")
    if system_id and not db.get(System, system_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="System not found")

    for field, value in update_data.items():
        setattr(table, field, value)

    db.commit()
    db.refresh(table)
    return table


@router.delete("/{table_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_table(table_id: UUID, db: Session = Depends(get_db)) -> None:
    table = _get_table_or_404(table_id, db)
    db.delete(table)
    db.commit()


@router.get("/{table_id}/preview", response_model=ConnectionTablePreview)
def preview_table(
    table_id: UUID,
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
) -> ConnectionTablePreview:
    """
    Get a data preview for a table using its system's connection.
    The table must have a physical name and belong to a system with an active connection.
    """
    table = _get_table_or_404(table_id, db)
    
    # Get the system and its connection
    system = db.get(System, table.system_id)
    if not system:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="System not found")
    
    # Find an active connection for this system
    connection = (
        db.query(SystemConnection)
        .filter(
            SystemConnection.system_id == table.system_id,
            SystemConnection.status == "active"
        )
        .first()
    )
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No active connection found for this table's system"
        )
    
    try:
        connection_type = SystemConnectionType(connection.connection_type)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported connection type for data preview.",
        ) from exc
    
    try:
        preview = fetch_table_preview(
            connection_type,
            connection.connection_string,
            table.schema_name,
            table.physical_name,
            limit,
        )
    except TablePreviewError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    
    return ConnectionTablePreview(columns=preview.columns, rows=preview.rows)
    db.commit()
