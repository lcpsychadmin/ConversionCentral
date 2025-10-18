from enum import Enum
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Field, Mapping, MappingSet, User
from app.schemas import MappingCreate, MappingRead, MappingStatus, MappingUpdate

router = APIRouter(prefix="/mappings", tags=["Mappings"])


def _normalize_payload(data: dict) -> dict:
    return {key: value.value if isinstance(value, Enum) else value for key, value in data.items()}


def _get_mapping_or_404(mapping_id: UUID, db: Session) -> Mapping:
    mapping = db.get(Mapping, mapping_id)
    if not mapping:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Mapping not found")
    return mapping


def _ensure_mapping_set_exists(mapping_set_id: UUID, db: Session) -> MappingSet:
    mapping_set = db.get(MappingSet, mapping_set_id)
    if not mapping_set:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Mapping set not found")
    return mapping_set


def _ensure_field_exists(field_id: UUID | None, db: Session, field_name: str) -> None:
    if field_id and not db.get(Field, field_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"{field_name} not found")


def _ensure_user_exists(user_id: UUID | None, db: Session) -> None:
    if user_id and not db.get(User, user_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")


@router.post("", response_model=MappingRead, status_code=status.HTTP_201_CREATED)
def create_mapping(payload: MappingCreate, db: Session = Depends(get_db)) -> MappingRead:
    _ensure_mapping_set_exists(payload.mapping_set_id, db)
    _ensure_field_exists(payload.source_field_id, db, "Source field")
    _ensure_field_exists(payload.target_field_id, db, "Target field")
    _ensure_user_exists(payload.created_by, db)

    mapping = Mapping(**_normalize_payload(payload.dict()))
    db.add(mapping)
    db.commit()
    db.refresh(mapping)
    return mapping


@router.get("", response_model=list[MappingRead])
def list_mappings(db: Session = Depends(get_db)) -> list[MappingRead]:
    return db.query(Mapping).all()


@router.get("/{mapping_id}", response_model=MappingRead)
def get_mapping(mapping_id: UUID, db: Session = Depends(get_db)) -> MappingRead:
    return _get_mapping_or_404(mapping_id, db)


@router.put("/{mapping_id}", response_model=MappingRead)
def update_mapping(
    mapping_id: UUID, payload: MappingUpdate, db: Session = Depends(get_db)
) -> MappingRead:
    mapping = _get_mapping_or_404(mapping_id, db)

    update_data = _normalize_payload(payload.dict(exclude_unset=True))

    source_field_id = update_data.get("source_field_id", mapping.source_field_id)
    target_field_id = update_data.get("target_field_id", mapping.target_field_id)
    created_by = update_data.get("created_by", mapping.created_by)

    _ensure_field_exists(source_field_id, db, "Source field")
    _ensure_field_exists(target_field_id, db, "Target field")
    _ensure_user_exists(created_by, db)

    for field_name, value in update_data.items():
        setattr(mapping, field_name, value)

    db.commit()
    db.refresh(mapping)
    return mapping


@router.delete("/{mapping_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_mapping(mapping_id: UUID, db: Session = Depends(get_db)) -> None:
    mapping = _get_mapping_or_404(mapping_id, db)
    db.delete(mapping)
    db.commit()
