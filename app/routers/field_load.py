from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Field, FieldLoad, Release, User
from app.schemas import FieldLoadCreate, FieldLoadRead, FieldLoadUpdate

router = APIRouter(prefix="/field-loads", tags=["Field Loads"])


def _get_field_load_or_404(field_load_id: UUID, db: Session) -> FieldLoad:
    field_load = db.get(FieldLoad, field_load_id)
    if not field_load:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field load not found")
    return field_load


def _validate_fk_presence(
    release_id: UUID | None,
    field_id: UUID | None,
    created_by: UUID | None,
    db: Session,
) -> None:
    if release_id and not db.get(Release, release_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Release not found")
    if field_id and not db.get(Field, field_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Field not found")
    if created_by and not db.get(User, created_by):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")


@router.post("", response_model=FieldLoadRead, status_code=status.HTTP_201_CREATED)
def create_field_load(
    payload: FieldLoadCreate, db: Session = Depends(get_db)
) -> FieldLoadRead:
    _validate_fk_presence(payload.release_id, payload.field_id, payload.created_by, db)

    field_load = FieldLoad(**payload.dict())
    db.add(field_load)
    db.commit()
    db.refresh(field_load)
    return field_load


@router.get("", response_model=list[FieldLoadRead])
def list_field_loads(db: Session = Depends(get_db)) -> list[FieldLoadRead]:
    return db.query(FieldLoad).all()


@router.get("/{field_load_id}", response_model=FieldLoadRead)
def get_field_load(
    field_load_id: UUID, db: Session = Depends(get_db)
) -> FieldLoadRead:
    return _get_field_load_or_404(field_load_id, db)


@router.put("/{field_load_id}", response_model=FieldLoadRead)
def update_field_load(
    field_load_id: UUID, payload: FieldLoadUpdate, db: Session = Depends(get_db)
) -> FieldLoadRead:
    field_load = _get_field_load_or_404(field_load_id, db)

    update_data = payload.dict(exclude_unset=True)
    _validate_fk_presence(
        update_data.get("release_id"),
        update_data.get("field_id"),
        update_data.get("created_by"),
        db,
    )

    for field_name, value in update_data.items():
        setattr(field_load, field_name, value)

    db.commit()
    db.refresh(field_load)
    return field_load


@router.delete("/{field_load_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_field_load(field_load_id: UUID, db: Session = Depends(get_db)) -> None:
    field_load = _get_field_load_or_404(field_load_id, db)
    db.delete(field_load)
    db.commit()
