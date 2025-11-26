from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import ProcessArea, ProcessAreaRoleAssignment, Role, User
from app.schemas import (
    ProcessAreaRoleAssignmentCreate,
    ProcessAreaRoleAssignmentRead,
    ProcessAreaRoleAssignmentUpdate,
)

router = APIRouter(
    prefix="/process-area-role-assignments",
    tags=["Process Area Role Assignments"],
)


def _get_assignment_or_404(assignment_id: UUID, db: Session) -> ProcessAreaRoleAssignment:
    assignment = db.get(ProcessAreaRoleAssignment, assignment_id)
    if not assignment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Process area role assignment not found",
        )
    return assignment


def _ensure_foreign_keys(
    process_area_id: UUID | None,
    user_id: UUID | None,
    role_id: UUID | None,
    granted_by: UUID | None,
    db: Session,
) -> None:
    if process_area_id and not db.get(ProcessArea, process_area_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Process area not found")
    if user_id and not db.get(User, user_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    if role_id and not db.get(Role, role_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Role not found")
    if granted_by and not db.get(User, granted_by):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Granting user not found")


def _ensure_unique_combination(
    process_area_id: UUID,
    user_id: UUID,
    role_id: UUID,
    db: Session,
    current_id: UUID | None = None,
) -> None:
    query = db.query(ProcessAreaRoleAssignment).filter(
        ProcessAreaRoleAssignment.process_area_id == process_area_id,
        ProcessAreaRoleAssignment.user_id == user_id,
        ProcessAreaRoleAssignment.role_id == role_id,
    )
    if current_id:
        query = query.filter(ProcessAreaRoleAssignment.id != current_id)
    if db.query(query.exists()).scalar():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Assignment already exists for this user, role, and process area",
        )


@router.post("", response_model=ProcessAreaRoleAssignmentRead, status_code=status.HTTP_201_CREATED)
def create_assignment(
    payload: ProcessAreaRoleAssignmentCreate, db: Session = Depends(get_db)
) -> ProcessAreaRoleAssignmentRead:
    _ensure_foreign_keys(
        payload.process_area_id,
        payload.user_id,
        payload.role_id,
        payload.granted_by,
        db,
    )
    _ensure_unique_combination(payload.process_area_id, payload.user_id, payload.role_id, db)

    assignment = ProcessAreaRoleAssignment(**payload.dict())
    db.add(assignment)
    db.commit()
    db.refresh(assignment)
    return assignment


@router.get("", response_model=list[ProcessAreaRoleAssignmentRead])
def list_assignments(db: Session = Depends(get_db)) -> list[ProcessAreaRoleAssignmentRead]:
    return db.query(ProcessAreaRoleAssignment).all()


@router.get("/{assignment_id}", response_model=ProcessAreaRoleAssignmentRead)
def get_assignment(
    assignment_id: UUID, db: Session = Depends(get_db)
) -> ProcessAreaRoleAssignmentRead:
    return _get_assignment_or_404(assignment_id, db)


@router.put("/{assignment_id}", response_model=ProcessAreaRoleAssignmentRead)
def update_assignment(
    assignment_id: UUID,
    payload: ProcessAreaRoleAssignmentUpdate,
    db: Session = Depends(get_db),
) -> ProcessAreaRoleAssignmentRead:
    assignment = _get_assignment_or_404(assignment_id, db)

    update_data = payload.dict(exclude_unset=True)

    process_area_id = update_data.get("process_area_id", assignment.process_area_id)
    user_id = update_data.get("user_id", assignment.user_id)
    role_id = update_data.get("role_id", assignment.role_id)
    granted_by = update_data.get("granted_by")

    _ensure_foreign_keys(process_area_id, user_id, role_id, granted_by, db)
    _ensure_unique_combination(process_area_id, user_id, role_id, db, current_id=assignment_id)

    for field, value in update_data.items():
        setattr(assignment, field, value)

    db.commit()
    db.refresh(assignment)
    return assignment


@router.delete("/{assignment_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_assignment(
    assignment_id: UUID, db: Session = Depends(get_db)
) -> None:
    assignment = _get_assignment_or_404(assignment_id, db)
    db.delete(assignment)
    db.commit()
