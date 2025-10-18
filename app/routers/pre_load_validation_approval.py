from enum import Enum
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import PreLoadValidationApproval, PreLoadValidationResult, User
from app.schemas import (
    PreLoadValidationApprovalCreate,
    PreLoadValidationApprovalRead,
    PreLoadValidationApprovalUpdate,
)

router = APIRouter(prefix="/pre-load-validation-approvals", tags=["Pre-Load Validation Approvals"])


def _normalize_payload(data: dict) -> dict:
    return {key: value.value if isinstance(value, Enum) else value for key, value in data.items()}


def _get_pre_load_validation_approval_or_404(
    approval_id: UUID, db: Session
) -> PreLoadValidationApproval:
    approval = db.get(PreLoadValidationApproval, approval_id)
    if not approval:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pre-load validation approval not found",
        )
    return approval


def _ensure_result_exists(result_id: UUID | None, db: Session) -> None:
    if result_id and not db.get(PreLoadValidationResult, result_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pre-load validation result not found")


def _ensure_user_exists(user_id: UUID | None, db: Session) -> None:
    if user_id and not db.get(User, user_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")


@router.post("", response_model=PreLoadValidationApprovalRead, status_code=status.HTTP_201_CREATED)
def create_pre_load_validation_approval(
    payload: PreLoadValidationApprovalCreate, db: Session = Depends(get_db)
) -> PreLoadValidationApprovalRead:
    _ensure_result_exists(payload.pre_load_validation_result_id, db)
    _ensure_user_exists(payload.approver_id, db)

    approval = PreLoadValidationApproval(**_normalize_payload(payload.dict()))
    db.add(approval)
    db.commit()
    db.refresh(approval)
    return approval


@router.get("", response_model=list[PreLoadValidationApprovalRead])
def list_pre_load_validation_approvals(
    db: Session = Depends(get_db),
) -> list[PreLoadValidationApprovalRead]:
    return db.query(PreLoadValidationApproval).all()


@router.get("/{approval_id}", response_model=PreLoadValidationApprovalRead)
def get_pre_load_validation_approval(
    approval_id: UUID, db: Session = Depends(get_db)
) -> PreLoadValidationApprovalRead:
    return _get_pre_load_validation_approval_or_404(approval_id, db)


@router.put("/{approval_id}", response_model=PreLoadValidationApprovalRead)
def update_pre_load_validation_approval(
    approval_id: UUID,
    payload: PreLoadValidationApprovalUpdate,
    db: Session = Depends(get_db),
) -> PreLoadValidationApprovalRead:
    approval = _get_pre_load_validation_approval_or_404(approval_id, db)

    update_data = _normalize_payload(payload.dict(exclude_unset=True))
    _ensure_result_exists(update_data.get("pre_load_validation_result_id"), db)
    _ensure_user_exists(update_data.get("approver_id"), db)

    for field_name, value in update_data.items():
        setattr(approval, field_name, value)

    db.commit()
    db.refresh(approval)
    return approval


@router.delete("/{approval_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_pre_load_validation_approval(
    approval_id: UUID, db: Session = Depends(get_db)
) -> None:
    approval = _get_pre_load_validation_approval_or_404(approval_id, db)
    db.delete(approval)
    db.commit()
