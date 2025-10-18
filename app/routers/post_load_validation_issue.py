from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import PostLoadValidationIssue, PostLoadValidationResult
from app.schemas import (
    PostLoadValidationIssueCreate,
    PostLoadValidationIssueRead,
    PostLoadValidationIssueUpdate,
)

router = APIRouter(prefix="/post-load-validation-issues", tags=["Post-Load Validation Issues"])


def _get_validation_issue_or_404(
    validation_issue_id: UUID, db: Session
) -> PostLoadValidationIssue:
    validation_issue = db.get(PostLoadValidationIssue, validation_issue_id)
    if not validation_issue:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Post-load validation issue not found",
        )
    return validation_issue


def _ensure_result_exists(validation_result_id: UUID | None, db: Session) -> None:
    if validation_result_id and not db.get(PostLoadValidationResult, validation_result_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Post-load validation result not found",
        )


@router.post("", response_model=PostLoadValidationIssueRead, status_code=status.HTTP_201_CREATED)
def create_post_load_validation_issue(
    payload: PostLoadValidationIssueCreate, db: Session = Depends(get_db)
) -> PostLoadValidationIssueRead:
    _ensure_result_exists(payload.validation_result_id, db)

    validation_issue = PostLoadValidationIssue(**payload.dict())
    db.add(validation_issue)
    db.commit()
    db.refresh(validation_issue)
    return validation_issue


@router.get("", response_model=list[PostLoadValidationIssueRead])
def list_post_load_validation_issues(
    db: Session = Depends(get_db),
) -> list[PostLoadValidationIssueRead]:
    return db.query(PostLoadValidationIssue).all()


@router.get("/{validation_issue_id}", response_model=PostLoadValidationIssueRead)
def get_post_load_validation_issue(
    validation_issue_id: UUID, db: Session = Depends(get_db)
) -> PostLoadValidationIssueRead:
    return _get_validation_issue_or_404(validation_issue_id, db)


@router.put("/{validation_issue_id}", response_model=PostLoadValidationIssueRead)
def update_post_load_validation_issue(
    validation_issue_id: UUID,
    payload: PostLoadValidationIssueUpdate,
    db: Session = Depends(get_db),
) -> PostLoadValidationIssueRead:
    validation_issue = _get_validation_issue_or_404(validation_issue_id, db)

    update_data = payload.dict(exclude_unset=True)
    _ensure_result_exists(update_data.get("validation_result_id"), db)

    for field_name, value in update_data.items():
        setattr(validation_issue, field_name, value)

    db.commit()
    db.refresh(validation_issue)
    return validation_issue


@router.delete("/{validation_issue_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_post_load_validation_issue(
    validation_issue_id: UUID, db: Session = Depends(get_db)
) -> None:
    validation_issue = _get_validation_issue_or_404(validation_issue_id, db)
    db.delete(validation_issue)
    db.commit()
