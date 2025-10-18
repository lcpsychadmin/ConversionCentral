from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import MockCycle, Release
from app.schemas import MockCycleCreate, MockCycleRead, MockCycleUpdate

router = APIRouter(prefix="/mock-cycles", tags=["Mock Cycles"])


def _get_mock_cycle_or_404(mock_cycle_id: UUID, db: Session) -> MockCycle:
    mock_cycle = db.get(MockCycle, mock_cycle_id)
    if not mock_cycle:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Mock cycle not found")
    return mock_cycle


@router.post("", response_model=MockCycleRead, status_code=status.HTTP_201_CREATED)
def create_mock_cycle(
    payload: MockCycleCreate, db: Session = Depends(get_db)
) -> MockCycleRead:
    if not db.get(Release, payload.release_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Release not found")

    mock_cycle = MockCycle(**payload.dict())
    db.add(mock_cycle)
    db.commit()
    db.refresh(mock_cycle)
    return mock_cycle


@router.get("", response_model=list[MockCycleRead])
def list_mock_cycles(db: Session = Depends(get_db)) -> list[MockCycleRead]:
    return db.query(MockCycle).all()


@router.get("/{mock_cycle_id}", response_model=MockCycleRead)
def get_mock_cycle(mock_cycle_id: UUID, db: Session = Depends(get_db)) -> MockCycleRead:
    return _get_mock_cycle_or_404(mock_cycle_id, db)


@router.put("/{mock_cycle_id}", response_model=MockCycleRead)
def update_mock_cycle(
    mock_cycle_id: UUID, payload: MockCycleUpdate, db: Session = Depends(get_db)
) -> MockCycleRead:
    mock_cycle = _get_mock_cycle_or_404(mock_cycle_id, db)

    update_data = payload.dict(exclude_unset=True)
    release_id = update_data.get("release_id")
    if release_id and not db.get(Release, release_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Release not found")

    for field, value in update_data.items():
        setattr(mock_cycle, field, value)

    db.commit()
    db.refresh(mock_cycle)
    return mock_cycle


@router.delete("/{mock_cycle_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_mock_cycle(mock_cycle_id: UUID, db: Session = Depends(get_db)) -> None:
    mock_cycle = _get_mock_cycle_or_404(mock_cycle_id, db)
    db.delete(mock_cycle)
    db.commit()
