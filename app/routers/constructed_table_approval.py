from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import ConstructedTable, ConstructedTableApproval, User
from app.schemas import (
    ConstructedTableApprovalCreate,
    ConstructedTableApprovalDecision,
    ConstructedTableApprovalRead,
    ConstructedTableApprovalRole,
    ConstructedTableApprovalUpdate,
)

router = APIRouter(prefix="/constructed-table-approvals", tags=["Constructed Table Approvals"])


def _get_approval_or_404(approval_id: UUID, db: Session) -> ConstructedTableApproval:
    approval = db.get(ConstructedTableApproval, approval_id)
    if not approval:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed table approval not found",
        )
    return approval


def _ensure_constructed_table_exists(constructed_table_id: UUID, db: Session) -> None:
    if not db.get(ConstructedTable, constructed_table_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed table not found",
        )


def _ensure_user_exists(user_id: UUID, db: Session) -> None:
    if not db.get(User, user_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )


def _maybe_update_timestamp(
    approval: ConstructedTableApproval,
    new_decision: ConstructedTableApprovalDecision | None,
    previous_decision: str,
) -> None:
    if new_decision is None:
        return
    decision_value = new_decision
    if isinstance(decision_value, ConstructedTableApprovalDecision):
        decision_value = decision_value.value
    if decision_value != previous_decision:
        approval.approved_at = datetime.utcnow()


@router.post("", response_model=ConstructedTableApprovalRead, status_code=status.HTTP_201_CREATED)
def create_constructed_table_approval(
    payload: ConstructedTableApprovalCreate, db: Session = Depends(get_db)
) -> ConstructedTableApprovalRead:
    _ensure_constructed_table_exists(payload.constructed_table_id, db)
    _ensure_user_exists(payload.approver_id, db)

    approval = ConstructedTableApproval(**payload.dict())
    db.add(approval)
    db.commit()
    db.refresh(approval)
    return approval


@router.get("", response_model=list[ConstructedTableApprovalRead])
def list_constructed_table_approvals(
    db: Session = Depends(get_db),
) -> list[ConstructedTableApprovalRead]:
    return db.query(ConstructedTableApproval).all()


@router.get("/{approval_id}", response_model=ConstructedTableApprovalRead)
def get_constructed_table_approval(
    approval_id: UUID, db: Session = Depends(get_db)
) -> ConstructedTableApprovalRead:
    return _get_approval_or_404(approval_id, db)


@router.put("/{approval_id}", response_model=ConstructedTableApprovalRead)
def update_constructed_table_approval(
    approval_id: UUID,
    payload: ConstructedTableApprovalUpdate,
    db: Session = Depends(get_db),
) -> ConstructedTableApprovalRead:
    approval = _get_approval_or_404(approval_id, db)
    update_data = payload.dict(exclude_unset=True)

    constructed_table_id = update_data.get("constructed_table_id")
    if constructed_table_id:
        _ensure_constructed_table_exists(constructed_table_id, db)

    approver_id = update_data.get("approver_id")
    if approver_id:
        _ensure_user_exists(approver_id, db)

    new_decision = update_data.get("decision")
    _maybe_update_timestamp(approval, new_decision, approval.decision)

    for field, value in update_data.items():
        if isinstance(value, (ConstructedTableApprovalRole, ConstructedTableApprovalDecision)):
            value = value.value
        setattr(approval, field, value)

    db.commit()
    db.refresh(approval)
    return approval


@router.delete("/{approval_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_constructed_table_approval(
    approval_id: UUID, db: Session = Depends(get_db)
) -> None:
    approval = _get_approval_or_404(approval_id, db)
    db.delete(approval)
    db.commit()
