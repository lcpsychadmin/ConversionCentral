from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session, selectinload

from app.database import get_db
from app.models import (
    ExecutionContext,
    MockCycle,
    PreLoadValidationResult,
)
from app.schemas import (
    ExecutionContextCreate,
    ExecutionContextRead,
    ExecutionContextUpdate,
)

router = APIRouter(prefix="/execution-contexts", tags=["Execution Contexts"])


def _get_execution_context_or_404(
    execution_context_id: UUID, db: Session
) -> ExecutionContext:
    execution_context = db.get(ExecutionContext, execution_context_id)
    if not execution_context:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution context not found",
        )
    return execution_context


@router.post(
    "",
    response_model=ExecutionContextRead,
    status_code=status.HTTP_201_CREATED,
)
def create_execution_context(
    payload: ExecutionContextCreate, db: Session = Depends(get_db)
) -> ExecutionContextRead:
    if not db.get(MockCycle, payload.mock_cycle_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Mock cycle not found")

    execution_context = ExecutionContext(**payload.dict())
    db.add(execution_context)
    db.commit()
    db.refresh(execution_context)
    return execution_context


@router.get("", response_model=list[ExecutionContextRead])
def list_execution_contexts(db: Session = Depends(get_db)) -> list[ExecutionContextRead]:
    return db.query(ExecutionContext).all()


@router.get("/{execution_context_id}", response_model=ExecutionContextRead)
def get_execution_context(
    execution_context_id: UUID, db: Session = Depends(get_db)
) -> ExecutionContextRead:
    return _get_execution_context_or_404(execution_context_id, db)


@router.put("/{execution_context_id}", response_model=ExecutionContextRead)
def update_execution_context(
    execution_context_id: UUID,
    payload: ExecutionContextUpdate,
    db: Session = Depends(get_db),
) -> ExecutionContextRead:
    execution_context = _get_execution_context_or_404(execution_context_id, db)

    update_data = payload.dict(exclude_unset=True)
    mock_cycle_id = update_data.get("mock_cycle_id")
    target_mock_cycle = execution_context.mock_cycle
    if mock_cycle_id:
        target_mock_cycle = db.get(MockCycle, mock_cycle_id)
        if not target_mock_cycle:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Mock cycle not found")

    new_status = update_data.get("status")
    if new_status and new_status.replace("_", "").lower() == "readyforload":
        release_id = target_mock_cycle.release_id
        results = (
            db.query(PreLoadValidationResult)
            .options(selectinload(PreLoadValidationResult.approvals))
            .filter(PreLoadValidationResult.release_id == release_id)
            .all()
        )
        if not results:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Pre-load validation results with approvals are required before marking ReadyForLoad",
            )
        for result in results:
            if not result.approvals:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="All pre-load validation approvals must be submitted before ReadyForLoad",
                )
            if any(approval.decision != "approved" for approval in result.approvals):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="All pre-load validation approvals must be approved before ReadyForLoad",
                )

    for field, value in update_data.items():
        setattr(execution_context, field, value)

    db.commit()
    db.refresh(execution_context)
    return execution_context


@router.delete("/{execution_context_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_execution_context(
    execution_context_id: UUID, db: Session = Depends(get_db)
) -> None:
    execution_context = _get_execution_context_or_404(execution_context_id, db)
    db.delete(execution_context)
    db.commit()
