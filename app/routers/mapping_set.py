from enum import Enum
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import MappingSet, Release, User
from app.schemas import (
    MappingSetCreate,
    MappingSetRead,
    MappingSetStatus,
    MappingSetUpdate,
)

router = APIRouter(prefix="/mapping-sets", tags=["Mapping Sets"])


def _normalize_payload(data: dict) -> dict:
    return {key: value.value if isinstance(value, Enum) else value for key, value in data.items()}


def _get_mapping_set_or_404(mapping_set_id: UUID, db: Session) -> MappingSet:
    mapping_set = db.get(MappingSet, mapping_set_id)
    if not mapping_set:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Mapping set not found")
    return mapping_set


def _ensure_release_exists(release_id: UUID, db: Session) -> None:
    if not db.get(Release, release_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Release not found")


def _ensure_user_exists(user_id: UUID | None, db: Session) -> None:
    if user_id and not db.get(User, user_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")


def _ensure_version_increment(release_id: UUID, version: int, db: Session) -> None:
    max_version = (
        db.query(func.max(MappingSet.version))
        .filter(MappingSet.release_id == release_id)
        .scalar()
    )
    expected_version = 1 if max_version is None else max_version + 1
    if version != expected_version:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Version must be {expected_version} for this release",
        )


def _ensure_single_active(release_id: UUID, db: Session, exclude_id: UUID | None = None) -> None:
    query = db.query(MappingSet).filter(
        MappingSet.release_id == release_id,
        MappingSet.status == MappingSetStatus.ACTIVE.value,
    )
    if exclude_id:
        query = query.filter(MappingSet.id != exclude_id)

    if db.query(query.exists()).scalar():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="An active mapping set already exists for this release",
        )


@router.post("", response_model=MappingSetRead, status_code=status.HTTP_201_CREATED)
def create_mapping_set(
    payload: MappingSetCreate, db: Session = Depends(get_db)
) -> MappingSetRead:
    _ensure_release_exists(payload.release_id, db)
    _ensure_user_exists(payload.created_by, db)
    _ensure_version_increment(payload.release_id, payload.version, db)
    if payload.status == MappingSetStatus.ACTIVE:
        _ensure_single_active(payload.release_id, db)

    mapping_set = MappingSet(**_normalize_payload(payload.dict()))
    db.add(mapping_set)
    db.commit()
    db.refresh(mapping_set)
    return mapping_set


@router.get("", response_model=list[MappingSetRead])
def list_mapping_sets(db: Session = Depends(get_db)) -> list[MappingSetRead]:
    return db.query(MappingSet).all()


@router.get("/{mapping_set_id}", response_model=MappingSetRead)
def get_mapping_set(mapping_set_id: UUID, db: Session = Depends(get_db)) -> MappingSetRead:
    return _get_mapping_set_or_404(mapping_set_id, db)


@router.put("/{mapping_set_id}", response_model=MappingSetRead)
def update_mapping_set(
    mapping_set_id: UUID, payload: MappingSetUpdate, db: Session = Depends(get_db)
) -> MappingSetRead:
    mapping_set = _get_mapping_set_or_404(mapping_set_id, db)

    update_data = _normalize_payload(payload.dict(exclude_unset=True))
    _ensure_user_exists(update_data.get("created_by"), db)

    if payload.status == MappingSetStatus.ACTIVE:
        _ensure_single_active(mapping_set.release_id, db, exclude_id=mapping_set.id)

    for field_name, value in update_data.items():
        setattr(mapping_set, field_name, value)

    db.commit()
    db.refresh(mapping_set)
    return mapping_set


@router.delete("/{mapping_set_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_mapping_set(mapping_set_id: UUID, db: Session = Depends(get_db)) -> None:
    mapping_set = _get_mapping_set_or_404(mapping_set_id, db)
    db.delete(mapping_set)
    db.commit()
