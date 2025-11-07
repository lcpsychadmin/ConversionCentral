from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import ProcessArea
from app.schemas import ProcessAreaCreate, ProcessAreaRead, ProcessAreaUpdate

router = APIRouter(prefix="/process-areas", tags=["Product Teams"])


def _get_process_area_or_404(process_area_id: UUID, db: Session) -> ProcessArea:
    process_area = db.get(ProcessArea, process_area_id)
    if not process_area:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product team not found")
    return process_area


@router.post("", response_model=ProcessAreaRead, status_code=status.HTTP_201_CREATED)
def create_process_area(
    payload: ProcessAreaCreate, db: Session = Depends(get_db)
) -> ProcessAreaRead:
    process_area = ProcessArea(**payload.dict())
    db.add(process_area)
    db.commit()
    db.refresh(process_area)
    return process_area


@router.get("", response_model=list[ProcessAreaRead])
def list_process_areas(db: Session = Depends(get_db)) -> list[ProcessAreaRead]:
    return db.query(ProcessArea).all()


@router.get("/{process_area_id}", response_model=ProcessAreaRead)
def get_process_area(
    process_area_id: UUID, db: Session = Depends(get_db)
) -> ProcessAreaRead:
    return _get_process_area_or_404(process_area_id, db)


@router.put("/{process_area_id}", response_model=ProcessAreaRead)
def update_process_area(
    process_area_id: UUID, payload: ProcessAreaUpdate, db: Session = Depends(get_db)
) -> ProcessAreaRead:
    process_area = _get_process_area_or_404(process_area_id, db)

    update_data = payload.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(process_area, field, value)

    db.commit()
    db.refresh(process_area)
    return process_area


@router.delete("/{process_area_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_process_area(
    process_area_id: UUID, db: Session = Depends(get_db)
) -> None:
    process_area = _get_process_area_or_404(process_area_id, db)
    db.delete(process_area)
    db.commit()
