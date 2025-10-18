from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import DataObject, DataObjectSystem, System
from app.schemas import (
    DataObjectSystemCreate,
    DataObjectSystemRead,
    DataObjectSystemUpdate,
)

router = APIRouter(prefix="/data-object-systems", tags=["Data Object Systems"])


def _get_data_object_system_or_404(
    link_id: UUID, db: Session
) -> DataObjectSystem:
    link = db.get(DataObjectSystem, link_id)
    if not link:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Link not found")
    return link


def _validate_relationship_payload(
    data_object_id: UUID | None, system_id: UUID | None, db: Session
) -> None:
    if data_object_id and not db.get(DataObject, data_object_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data object not found")
    if system_id and not db.get(System, system_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="System not found")


@router.post(
    "",
    response_model=DataObjectSystemRead,
    status_code=status.HTTP_201_CREATED,
)
def create_data_object_system(
    payload: DataObjectSystemCreate, db: Session = Depends(get_db)
) -> DataObjectSystemRead:
    _validate_relationship_payload(payload.data_object_id, payload.system_id, db)

    link = DataObjectSystem(**payload.dict())
    db.add(link)
    db.commit()
    db.refresh(link)
    return link


@router.get("", response_model=list[DataObjectSystemRead])
def list_data_object_systems(db: Session = Depends(get_db)) -> list[DataObjectSystemRead]:
    return db.query(DataObjectSystem).all()


@router.get("/{link_id}", response_model=DataObjectSystemRead)
def get_data_object_system(
    link_id: UUID, db: Session = Depends(get_db)
) -> DataObjectSystemRead:
    return _get_data_object_system_or_404(link_id, db)


@router.put("/{link_id}", response_model=DataObjectSystemRead)
def update_data_object_system(
    link_id: UUID, payload: DataObjectSystemUpdate, db: Session = Depends(get_db)
) -> DataObjectSystemRead:
    link = _get_data_object_system_or_404(link_id, db)

    update_data = payload.dict(exclude_unset=True)
    _validate_relationship_payload(
        update_data.get("data_object_id"), update_data.get("system_id"), db
    )

    for field, value in update_data.items():
        setattr(link, field, value)

    db.commit()
    db.refresh(link)
    return link


@router.delete("/{link_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_data_object_system(
    link_id: UUID, db: Session = Depends(get_db)
) -> None:
    link = _get_data_object_system_or_404(link_id, db)
    db.delete(link)
    db.commit()
