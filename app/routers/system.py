from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import System
from app.schemas import SystemCreate, SystemRead, SystemUpdate

router = APIRouter(prefix="/systems", tags=["Systems"])


def _get_system_or_404(system_id: UUID, db: Session) -> System:
    system = db.get(System, system_id)
    if not system:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="System not found")
    return system


@router.post("", response_model=SystemRead, status_code=status.HTTP_201_CREATED)
def create_system(payload: SystemCreate, db: Session = Depends(get_db)) -> SystemRead:
    system = System(**payload.dict())
    db.add(system)
    db.commit()
    db.refresh(system)
    return system


@router.get("", response_model=list[SystemRead])
def list_systems(db: Session = Depends(get_db)) -> list[SystemRead]:
    return db.query(System).all()


@router.get("/{system_id}", response_model=SystemRead)
def get_system(system_id: UUID, db: Session = Depends(get_db)) -> SystemRead:
    return _get_system_or_404(system_id, db)


@router.put("/{system_id}", response_model=SystemRead)
def update_system(
    system_id: UUID, payload: SystemUpdate, db: Session = Depends(get_db)
) -> SystemRead:
    system = _get_system_or_404(system_id, db)

    for field, value in payload.dict(exclude_unset=True).items():
        setattr(system, field, value)

    db.commit()
    db.refresh(system)
    return system


@router.delete("/{system_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_system(system_id: UUID, db: Session = Depends(get_db)) -> None:
    system = _get_system_or_404(system_id, db)
    db.delete(system)
    db.commit()
