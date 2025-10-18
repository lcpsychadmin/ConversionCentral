from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import PreLoadValidationIssue, PreLoadValidationResult
from app.schemas import (
    PreLoadValidationIssueCreate,
    PreLoadValidationIssueRead,
    PreLoadValidationIssueUpdate,
)

router = APIRouter(prefix="/pre-load-validation-issues", tags=["Pre-Load Validation Issues"])


def _get_validation_issue_or_404(
    validation_issue_id: UUID, db: Session
) -> PreLoadValidationIssue:
    validation_issue = db.get(PreLoadValidationIssue, validation_issue_id)
    if not validation_issue:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pre-load validation issue not found",
        )
    return validation_issue


def _ensure_result_exists(validation_result_id: UUID | None, db: Session) -> None:
    if validation_result_id and not db.get(PreLoadValidationResult, validation_result_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pre-load validation result not found",
        )


@router.post("", response_model=PreLoadValidationIssueRead, status_code=status.HTTP_201_CREATED)
def create_pre_load_validation_issue(
    payload: PreLoadValidationIssueCreate, db: Session = Depends(get_db)
) -> PreLoadValidationIssueRead:
    _ensure_result_exists(payload.validation_result_id, db)

    validation_issue = PreLoadValidationIssue(**payload.dict())
    db.add(validation_issue)
    db.commit()
    db.refresh(validation_issue)
    return validation_issue


@router.get("", response_model=list[PreLoadValidationIssueRead])
def list_pre_load_validation_issues(
    db: Session = Depends(get_db),
) -> list[PreLoadValidationIssueRead]:
    return db.query(PreLoadValidationIssue).all()


@router.get("/{validation_issue_id}", response_model=PreLoadValidationIssueRead)
def get_pre_load_validation_issue(
    validation_issue_id: UUID, db: Session = Depends(get_db)
) -> PreLoadValidationIssueRead:
    return _get_validation_issue_or_404(validation_issue_id, db)


@router.put("/{validation_issue_id}", response_model=PreLoadValidationIssueRead)
def update_pre_load_validation_issue(
    validation_issue_id: UUID,
    payload: PreLoadValidationIssueUpdate,
    db: Session = Depends(get_db),
) -> PreLoadValidationIssueRead:
    validation_issue = _get_validation_issue_or_404(validation_issue_id, db)

    update_data = payload.dict(exclude_unset=True)
    _ensure_result_exists(update_data.get("validation_result_id"), db)

    for field_name, value in update_data.items():
        setattr(validation_issue, field_name, value)

    db.commit()
    db.refresh(validation_issue)
    return validation_issue


@router.delete("/{validation_issue_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_pre_load_validation_issue(
    validation_issue_id: UUID, db: Session = Depends(get_db)
) -> None:
    validation_issue = _get_validation_issue_or_404(validation_issue_id, db)
    db.delete(validation_issue)
    db.commit()
