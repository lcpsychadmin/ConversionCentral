from enum import Enum
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import PostLoadValidationApproval, PostLoadValidationResult, User
from app.schemas import (
    PostLoadValidationApprovalCreate,
    PostLoadValidationApprovalRead,
    PostLoadValidationApprovalUpdate,
)

router = APIRouter(prefix="/post-load-validation-approvals", tags=["Post-Load Validation Approvals"])


def _normalize_payload(data: dict) -> dict:
    return {key: value.value if isinstance(value, Enum) else value for key, value in data.items()}


def _get_post_load_validation_approval_or_404(
    approval_id: UUID, db: Session
) -> PostLoadValidationApproval:
    approval = db.get(PostLoadValidationApproval, approval_id)
    if not approval:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Post-load validation approval not found",
        )
    return approval


def _ensure_result_exists(result_id: UUID | None, db: Session) -> None:
    if result_id and not db.get(PostLoadValidationResult, result_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Post-load validation result not found")


def _ensure_user_exists(user_id: UUID | None, db: Session) -> None:
    if user_id and not db.get(User, user_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")


@router.post("", response_model=PostLoadValidationApprovalRead, status_code=status.HTTP_201_CREATED)
def create_post_load_validation_approval(
    payload: PostLoadValidationApprovalCreate, db: Session = Depends(get_db)
) -> PostLoadValidationApprovalRead:
    _ensure_result_exists(payload.post_load_validation_result_id, db)
    _ensure_user_exists(payload.approver_id, db)

    approval = PostLoadValidationApproval(**_normalize_payload(payload.dict()))
    db.add(approval)
    db.commit()
    db.refresh(approval)
    return approval


@router.get("", response_model=list[PostLoadValidationApprovalRead])
def list_post_load_validation_approvals(
    db: Session = Depends(get_db),
) -> list[PostLoadValidationApprovalRead]:
    return db.query(PostLoadValidationApproval).all()


@router.get("/{approval_id}", response_model=PostLoadValidationApprovalRead)
def get_post_load_validation_approval(
    approval_id: UUID, db: Session = Depends(get_db)
) -> PostLoadValidationApprovalRead:
    return _get_post_load_validation_approval_or_404(approval_id, db)


@router.put("/{approval_id}", response_model=PostLoadValidationApprovalRead)
def update_post_load_validation_approval(
    approval_id: UUID,
    payload: PostLoadValidationApprovalUpdate,
    db: Session = Depends(get_db),
) -> PostLoadValidationApprovalRead:
    approval = _get_post_load_validation_approval_or_404(approval_id, db)

    update_data = _normalize_payload(payload.dict(exclude_unset=True))
    _ensure_result_exists(update_data.get("post_load_validation_result_id"), db)
    _ensure_user_exists(update_data.get("approver_id"), db)

    for field_name, value in update_data.items():
        setattr(approval, field_name, value)

    db.commit()
    db.refresh(approval)
    return approval


@router.delete("/{approval_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_post_load_validation_approval(
    approval_id: UUID, db: Session = Depends(get_db)
) -> None:
    approval = _get_post_load_validation_approval_or_404(approval_id, db)
    db.delete(approval)
    db.commit()
