from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import PreLoadValidationResult, Release
from app.schemas import (
    PreLoadValidationResultCreate,
    PreLoadValidationResultRead,
    PreLoadValidationResultUpdate,
)

router = APIRouter(prefix="/pre-load-validation-results", tags=["Pre-Load Validation Results"])


def _get_validation_result_or_404(
    validation_result_id: UUID, db: Session
) -> PreLoadValidationResult:
    validation_result = db.get(PreLoadValidationResult, validation_result_id)
    if not validation_result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pre-load validation result not found",
        )
    return validation_result


def _ensure_release_exists(release_id: UUID | None, db: Session) -> None:
    if release_id and not db.get(Release, release_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Release not found")


@router.post("", response_model=PreLoadValidationResultRead, status_code=status.HTTP_201_CREATED)
def create_pre_load_validation_result(
    payload: PreLoadValidationResultCreate, db: Session = Depends(get_db)
) -> PreLoadValidationResultRead:
    _ensure_release_exists(payload.release_id, db)

    validation_result = PreLoadValidationResult(**payload.dict())
    db.add(validation_result)
    db.commit()
    db.refresh(validation_result)
    return validation_result


@router.get("", response_model=list[PreLoadValidationResultRead])
def list_pre_load_validation_results(
    db: Session = Depends(get_db),
) -> list[PreLoadValidationResultRead]:
    return db.query(PreLoadValidationResult).all()


@router.get("/{validation_result_id}", response_model=PreLoadValidationResultRead)
def get_pre_load_validation_result(
    validation_result_id: UUID, db: Session = Depends(get_db)
) -> PreLoadValidationResultRead:
    return _get_validation_result_or_404(validation_result_id, db)


@router.put("/{validation_result_id}", response_model=PreLoadValidationResultRead)
def update_pre_load_validation_result(
    validation_result_id: UUID,
    payload: PreLoadValidationResultUpdate,
    db: Session = Depends(get_db),
) -> PreLoadValidationResultRead:
    validation_result = _get_validation_result_or_404(validation_result_id, db)

    update_data = payload.dict(exclude_unset=True)
    _ensure_release_exists(update_data.get("release_id"), db)

    for field_name, value in update_data.items():
        setattr(validation_result, field_name, value)

    db.commit()
    db.refresh(validation_result)
    return validation_result


@router.delete("/{validation_result_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_pre_load_validation_result(
    validation_result_id: UUID, db: Session = Depends(get_db)
) -> None:
    validation_result = _get_validation_result_or_404(validation_result_id, db)
    db.delete(validation_result)
    db.commit()
