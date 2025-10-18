from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import DataObjectDependency, DependencyApproval, TableDependency, User
from app.schemas import (
    DependencyApprovalCreate,
    DependencyApprovalDecision,
    DependencyApprovalRead,
    DependencyApprovalUpdate,
)

router = APIRouter(prefix="/dependency-approvals", tags=["Dependency Approvals"])


def _get_approval_or_404(approval_id: UUID, db: Session) -> DependencyApproval:
    approval = db.get(DependencyApproval, approval_id)
    if not approval:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Approval not found")
    return approval


def _ensure_dependency_exists(
    data_object_dependency_id: UUID | None,
    table_dependency_id: UUID | None,
    db: Session,
) -> None:
    if data_object_dependency_id:
        if not db.get(DataObjectDependency, data_object_dependency_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data object dependency not found")
    if table_dependency_id:
        if not db.get(TableDependency, table_dependency_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table dependency not found")


def _ensure_user_exists(user_id: UUID, db: Session) -> None:
    if not db.get(User, user_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")


@router.post("", response_model=DependencyApprovalRead, status_code=status.HTTP_201_CREATED)
def create_approval(
    payload: DependencyApprovalCreate, db: Session = Depends(get_db)
) -> DependencyApprovalRead:
    _ensure_dependency_exists(payload.data_object_dependency_id, payload.table_dependency_id, db)
    _ensure_user_exists(payload.approver_id, db)

    approval = DependencyApproval(**payload.dict())
    db.add(approval)
    db.commit()
    db.refresh(approval)
    return approval


@router.get("", response_model=list[DependencyApprovalRead])
def list_approvals(db: Session = Depends(get_db)) -> list[DependencyApprovalRead]:
    return db.query(DependencyApproval).all()


@router.get("/{approval_id}", response_model=DependencyApprovalRead)
def get_approval(
    approval_id: UUID, db: Session = Depends(get_db)
) -> DependencyApprovalRead:
    return _get_approval_or_404(approval_id, db)


@router.put("/{approval_id}", response_model=DependencyApprovalRead)
def update_approval(
    approval_id: UUID,
    payload: DependencyApprovalUpdate,
    db: Session = Depends(get_db),
) -> DependencyApprovalRead:
    approval = _get_approval_or_404(approval_id, db)
    update_data = payload.dict(exclude_unset=True)

    data_object_dependency_id = update_data.get("data_object_dependency_id", approval.data_object_dependency_id)
    table_dependency_id = update_data.get("table_dependency_id", approval.table_dependency_id)

    if data_object_dependency_id and table_dependency_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Approval can only target one dependency",
        )

    _ensure_dependency_exists(data_object_dependency_id, table_dependency_id, db)

    approver_id = update_data.get("approver_id")
    if approver_id:
        _ensure_user_exists(approver_id, db)

    for field, value in update_data.items():
        if field == "decision" and isinstance(value, DependencyApprovalDecision):
            value = value.value
        setattr(approval, field, value)

    db.commit()
    db.refresh(approval)
    return approval


@router.delete("/{approval_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_approval(
    approval_id: UUID, db: Session = Depends(get_db)
) -> None:
    approval = _get_approval_or_404(approval_id, db)
    db.delete(approval)
    db.commit()
