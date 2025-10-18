from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import System, Table
from app.schemas import TableCreate, TableRead, TableUpdate

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
