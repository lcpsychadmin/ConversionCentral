from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import TableLoadOrder, TableLoadOrderApproval, User
from app.schemas import (
    TableLoadOrderApprovalCreate,
    TableLoadOrderApprovalDecision,
    TableLoadOrderApprovalRead,
    TableLoadOrderApprovalRole,
    TableLoadOrderApprovalUpdate,
)

router = APIRouter(
    prefix="/table-load-order-approvals",
    tags=["Table Load Order Approvals"],
)


def _get_approval_or_404(approval_id: UUID, db: Session) -> TableLoadOrderApproval:
    approval = db.get(TableLoadOrderApproval, approval_id)
    if not approval:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Approval not found")
    return approval


def _ensure_order_exists(order_id: UUID, db: Session) -> None:
    if not db.get(TableLoadOrder, order_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table load order not found")


def _ensure_user_exists(user_id: UUID, db: Session) -> None:
    if not db.get(User, user_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")


def _ensure_unique_signature(
    table_load_order_id: UUID,
    approver_id: UUID,
    role: str,
    db: Session,
    current_id: UUID | None = None,
) -> None:
    query = db.query(TableLoadOrderApproval).filter(
        TableLoadOrderApproval.table_load_order_id == table_load_order_id,
        TableLoadOrderApproval.approver_id == approver_id,
        TableLoadOrderApproval.role == role,
    )
    if current_id:
        query = query.filter(TableLoadOrderApproval.id != current_id)
    if db.query(query.exists()).scalar():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Approval already captured for this approver and role",
        )


@router.post("", response_model=TableLoadOrderApprovalRead, status_code=status.HTTP_201_CREATED)
def create_table_load_order_approval(
    payload: TableLoadOrderApprovalCreate, db: Session = Depends(get_db)
) -> TableLoadOrderApprovalRead:
    _ensure_order_exists(payload.table_load_order_id, db)
    _ensure_user_exists(payload.approver_id, db)

    role_value = (
        payload.role.value if isinstance(payload.role, TableLoadOrderApprovalRole) else payload.role
    )
    _ensure_unique_signature(payload.table_load_order_id, payload.approver_id, role_value, db)

    approval = TableLoadOrderApproval(
        table_load_order_id=payload.table_load_order_id,
        approver_id=payload.approver_id,
        role=role_value,
        decision=(
            payload.decision.value
            if isinstance(payload.decision, TableLoadOrderApprovalDecision)
            else payload.decision
        ),
        comments=payload.comments,
    )
    db.add(approval)
    db.commit()
    db.refresh(approval)
    return approval


@router.get("", response_model=list[TableLoadOrderApprovalRead])
def list_table_load_order_approvals(
    db: Session = Depends(get_db),
) -> list[TableLoadOrderApprovalRead]:
    return (
        db.query(TableLoadOrderApproval)
        .order_by(TableLoadOrderApproval.approved_at.desc())
        .all()
    )


@router.get("/{approval_id}", response_model=TableLoadOrderApprovalRead)
def get_table_load_order_approval(
    approval_id: UUID, db: Session = Depends(get_db)
) -> TableLoadOrderApprovalRead:
    return _get_approval_or_404(approval_id, db)


@router.put("/{approval_id}", response_model=TableLoadOrderApprovalRead)
def update_table_load_order_approval(
    approval_id: UUID,
    payload: TableLoadOrderApprovalUpdate,
    db: Session = Depends(get_db),
) -> TableLoadOrderApprovalRead:
    approval = _get_approval_or_404(approval_id, db)
    update_data = payload.dict(exclude_unset=True)

    table_load_order_id = update_data.get("table_load_order_id", approval.table_load_order_id)
    approver_id = update_data.get("approver_id", approval.approver_id)
    role = update_data.get("role", approval.role)

    if isinstance(role, TableLoadOrderApprovalRole):
        role = role.value

    _ensure_order_exists(table_load_order_id, db)
    _ensure_user_exists(approver_id, db)
    _ensure_unique_signature(table_load_order_id, approver_id, role, db, current_id=approval_id)

    if "decision" in update_data:
        decision = update_data["decision"]
        if isinstance(decision, TableLoadOrderApprovalDecision):
            decision = decision.value
        update_data["decision"] = decision

    update_data["role"] = role
    update_data["table_load_order_id"] = table_load_order_id
    update_data["approver_id"] = approver_id

    for field, value in update_data.items():
        setattr(approval, field, value)

    db.commit()
    db.refresh(approval)
    return approval


@router.delete("/{approval_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_table_load_order_approval(
    approval_id: UUID, db: Session = Depends(get_db)
) -> None:
    approval = _get_approval_or_404(approval_id, db)
    db.delete(approval)
    db.commit()