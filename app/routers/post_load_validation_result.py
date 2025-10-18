from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import PostLoadValidationResult, Release
from app.schemas import (
    PostLoadValidationResultCreate,
    PostLoadValidationResultRead,
    PostLoadValidationResultUpdate,
)

router = APIRouter(prefix="/post-load-validation-results", tags=["Post-Load Validation Results"])


def _get_validation_result_or_404(
    validation_result_id: UUID, db: Session
) -> PostLoadValidationResult:
    validation_result = db.get(PostLoadValidationResult, validation_result_id)
    if not validation_result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Post-load validation result not found",
        )
    return validation_result


def _ensure_release_exists(release_id: UUID | None, db: Session) -> None:
    if release_id and not db.get(Release, release_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Release not found")


@router.post("", response_model=PostLoadValidationResultRead, status_code=status.HTTP_201_CREATED)
def create_post_load_validation_result(
    payload: PostLoadValidationResultCreate, db: Session = Depends(get_db)
) -> PostLoadValidationResultRead:
    _ensure_release_exists(payload.release_id, db)

    validation_result = PostLoadValidationResult(**payload.dict())
    db.add(validation_result)
    db.commit()
    db.refresh(validation_result)
    return validation_result


@router.get("", response_model=list[PostLoadValidationResultRead])
def list_post_load_validation_results(
    db: Session = Depends(get_db),
) -> list[PostLoadValidationResultRead]:
    return db.query(PostLoadValidationResult).all()


@router.get("/{validation_result_id}", response_model=PostLoadValidationResultRead)
def get_post_load_validation_result(
    validation_result_id: UUID, db: Session = Depends(get_db)
) -> PostLoadValidationResultRead:
    return _get_validation_result_or_404(validation_result_id, db)


@router.put("/{validation_result_id}", response_model=PostLoadValidationResultRead)
def update_post_load_validation_result(
    validation_result_id: UUID,
    payload: PostLoadValidationResultUpdate,
    db: Session = Depends(get_db),
) -> PostLoadValidationResultRead:
    validation_result = _get_validation_result_or_404(validation_result_id, db)

    update_data = payload.dict(exclude_unset=True)
    _ensure_release_exists(update_data.get("release_id"), db)

    for field_name, value in update_data.items():
        setattr(validation_result, field_name, value)

    db.commit()
    db.refresh(validation_result)
    return validation_result


@router.delete("/{validation_result_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_post_load_validation_result(
    validation_result_id: UUID, db: Session = Depends(get_db)
) -> None:
    validation_result = _get_validation_result_or_404(validation_result_id, db)
    db.delete(validation_result)
    db.commit()
