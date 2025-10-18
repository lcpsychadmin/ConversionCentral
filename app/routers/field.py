from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Field, Table
from app.schemas import FieldCreate, FieldRead, FieldUpdate

router = APIRouter(prefix="/fields", tags=["Fields"])


def _get_field_or_404(field_id: UUID, db: Session) -> Field:
    field = db.get(Field, field_id)
    if not field:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")
    return field


@router.post("", response_model=FieldRead, status_code=status.HTTP_201_CREATED)
def create_field(payload: FieldCreate, db: Session = Depends(get_db)) -> FieldRead:
    if not db.get(Table, payload.table_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")

    field = Field(**payload.dict())
    db.add(field)
    db.commit()
    db.refresh(field)
    return field


@router.get("", response_model=list[FieldRead])
def list_fields(db: Session = Depends(get_db)) -> list[FieldRead]:
    return db.query(Field).all()


@router.get("/{field_id}", response_model=FieldRead)
def get_field(field_id: UUID, db: Session = Depends(get_db)) -> FieldRead:
    return _get_field_or_404(field_id, db)


@router.put("/{field_id}", response_model=FieldRead)
def update_field(
    field_id: UUID, payload: FieldUpdate, db: Session = Depends(get_db)
) -> FieldRead:
    field = _get_field_or_404(field_id, db)

    update_data = payload.dict(exclude_unset=True)
    table_id = update_data.get("table_id")
    if table_id and not db.get(Table, table_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")

    for field_name, value in update_data.items():
        setattr(field, field_name, value)

    db.commit()
    db.refresh(field)
    return field


@router.delete("/{field_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_field(field_id: UUID, db: Session = Depends(get_db)) -> None:
    field = _get_field_or_404(field_id, db)
    db.delete(field)
    db.commit()
