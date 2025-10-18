from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import ConstructedField, ConstructedTable
from app.schemas import ConstructedFieldCreate, ConstructedFieldRead, ConstructedFieldUpdate

router = APIRouter(prefix="/constructed-fields", tags=["Constructed Fields"])


def _get_constructed_field_or_404(constructed_field_id: UUID, db: Session) -> ConstructedField:
    constructed_field = db.get(ConstructedField, constructed_field_id)
    if not constructed_field:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed field not found",
        )
    return constructed_field


def _ensure_constructed_table_exists(constructed_table_id: UUID, db: Session) -> None:
    if not db.get(ConstructedTable, constructed_table_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed table not found",
        )


@router.post("", response_model=ConstructedFieldRead, status_code=status.HTTP_201_CREATED)
def create_constructed_field(
    payload: ConstructedFieldCreate, db: Session = Depends(get_db)
) -> ConstructedFieldRead:
    _ensure_constructed_table_exists(payload.constructed_table_id, db)

    constructed_field = ConstructedField(**payload.dict())
    db.add(constructed_field)
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
        _ensure_constructed_table_exists(constructed_table_id, db)

    for field, value in update_data.items():
        setattr(constructed_field, field, value)

    db.commit()
    db.refresh(constructed_field)
    return constructed_field


@router.delete("/{constructed_field_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_constructed_field(
    constructed_field_id: UUID, db: Session = Depends(get_db)
) -> None:
    constructed_field = _get_constructed_field_or_404(constructed_field_id, db)
    db.delete(constructed_field)
    db.commit()
