from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import ConstructedTable, ConstructedTableApproval, ExecutionContext
from app.schemas import (
    ConstructedTableApprovalDecision,
    ConstructedTableCreate,
    ConstructedTableRead,
    ConstructedTableStatus,
    ConstructedTableUpdate,
)

router = APIRouter(prefix="/constructed-tables", tags=["Constructed Tables"])


def _get_constructed_table_or_404(constructed_table_id: UUID, db: Session) -> ConstructedTable:
    constructed_table = db.get(ConstructedTable, constructed_table_id)
    if not constructed_table:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed table not found",
        )
    return constructed_table


def _ensure_execution_context_exists(execution_context_id: UUID, db: Session) -> None:
    if not db.get(ExecutionContext, execution_context_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution context not found",
        )


def _has_approved_decisions(constructed_table_id: UUID, db: Session) -> bool:
    return (
        db.query(ConstructedTableApproval)
        .filter(
            ConstructedTableApproval.constructed_table_id == constructed_table_id,
            ConstructedTableApproval.decision
            == ConstructedTableApprovalDecision.APPROVED.value,
        )
        .count()
        > 0
    )


@router.post("", response_model=ConstructedTableRead, status_code=status.HTTP_201_CREATED)
def create_constructed_table(
    payload: ConstructedTableCreate, db: Session = Depends(get_db)
) -> ConstructedTableRead:
    _ensure_execution_context_exists(payload.execution_context_id, db)

    constructed_table = ConstructedTable(**payload.dict())
    db.add(constructed_table)
    db.commit()
    db.refresh(constructed_table)
    return constructed_table


@router.get("", response_model=list[ConstructedTableRead])
def list_constructed_tables(db: Session = Depends(get_db)) -> list[ConstructedTableRead]:
    return db.query(ConstructedTable).all()


@router.get("/{constructed_table_id}", response_model=ConstructedTableRead)
def get_constructed_table(
    constructed_table_id: UUID, db: Session = Depends(get_db)
) -> ConstructedTableRead:
    return _get_constructed_table_or_404(constructed_table_id, db)


@router.put("/{constructed_table_id}", response_model=ConstructedTableRead)
def update_constructed_table(
    constructed_table_id: UUID,
    payload: ConstructedTableUpdate,
    db: Session = Depends(get_db),
) -> ConstructedTableRead:
    constructed_table = _get_constructed_table_or_404(constructed_table_id, db)

    update_data = payload.dict(exclude_unset=True)

    execution_context_id = update_data.get("execution_context_id")
    if execution_context_id:
        _ensure_execution_context_exists(execution_context_id, db)

    status_value = update_data.get("status")
    if status_value and isinstance(status_value, ConstructedTableStatus):
        status_value = status_value.value
    if status_value == ConstructedTableStatus.APPROVED.value and not _has_approved_decisions(
        constructed_table_id, db
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Constructed table must have at least one approved decision before activation",
        )

    for field, value in update_data.items():
        if isinstance(value, ConstructedTableStatus):
            value = value.value
        setattr(constructed_table, field, value)

    db.commit()
    db.refresh(constructed_table)
    return constructed_table


@router.delete("/{constructed_table_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_constructed_table(
    constructed_table_id: UUID, db: Session = Depends(get_db)
) -> None:
    constructed_table = _get_constructed_table_or_404(constructed_table_id, db)
    db.delete(constructed_table)
    db.commit()
