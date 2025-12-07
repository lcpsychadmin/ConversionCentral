from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Workspace
from app.schemas import WorkspaceCreate, WorkspaceRead, WorkspaceUpdate

router = APIRouter(prefix="/workspaces", tags=["Workspaces"])


@router.get("", response_model=list[WorkspaceRead])
def list_workspaces(db: Session = Depends(get_db)) -> list[WorkspaceRead]:
    return (
        db.query(Workspace)
        .order_by(Workspace.is_default.desc(), Workspace.name.asc())
        .all()
    )


@router.post("", response_model=WorkspaceRead, status_code=status.HTTP_201_CREATED)
def create_workspace(
    request: WorkspaceCreate, db: Session = Depends(get_db)
) -> WorkspaceRead:
    name = request.name.strip()
    if not name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Workspace name is required.")

    if request.is_default:
        _clear_default_workspace(db)

    workspace = Workspace(
        name=name,
        description=request.description,
        is_active=request.is_active,
        is_default=request.is_default,
    )

    if not workspace.is_default and not _has_default_workspace(db):
        workspace.is_default = True

    db.add(workspace)

    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Workspace name must be unique.",
        ) from exc

    db.refresh(workspace)
    return workspace


@router.patch("/{workspace_id}", response_model=WorkspaceRead)
def update_workspace(
    workspace_id: UUID,
    request: WorkspaceUpdate,
    db: Session = Depends(get_db),
) -> WorkspaceRead:
    workspace = db.get(Workspace, workspace_id)
    if workspace is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workspace not found.")

    if request.name is not None:
        name = request.name.strip()
        if not name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Workspace name is required.")
        workspace.name = name

    if request.description is not None:
        workspace.description = request.description

    if request.is_active is not None:
        workspace.is_active = request.is_active

    if request.is_default is True:
        _clear_default_workspace(db, exclude_id=workspace_id)
        workspace.is_default = True
    elif request.is_default is False:
        workspace.is_default = False
        if not _has_default_workspace(db, exclude_id=workspace_id):
            # Prevent leaving the system without a default workspace.
            workspace.is_default = True

    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Workspace name must be unique.",
        ) from exc

    db.refresh(workspace)
    return workspace


@router.delete("/{workspace_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_workspace(
    workspace_id: UUID,
    db: Session = Depends(get_db),
) -> None:
    workspace = db.get(Workspace, workspace_id)
    if workspace is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workspace not found.")

    was_default = workspace.is_default
    db.delete(workspace)
    db.flush()

    if was_default or not _has_default_workspace(db):
        _assign_default_workspace(db, exclude_id=workspace_id)

    db.commit()


def _clear_default_workspace(db: Session, *, exclude_id: UUID | None = None) -> None:
    query = db.query(Workspace).filter(Workspace.is_default.is_(True))
    if exclude_id is not None:
        query = query.filter(Workspace.id != exclude_id)
    query.update({Workspace.is_default: False}, synchronize_session=False)


def _has_default_workspace(db: Session, *, exclude_id: UUID | None = None) -> bool:
    query = db.query(Workspace).filter(Workspace.is_default.is_(True))
    if exclude_id is not None:
        query = query.filter(Workspace.id != exclude_id)
    return db.query(query.exists()).scalar() or False


def _assign_default_workspace(db: Session, *, exclude_id: UUID | None = None) -> None:
    if _has_default_workspace(db, exclude_id=exclude_id):
        return

    replacement = (
        db.query(Workspace)
        .filter(Workspace.id != exclude_id)
        .order_by(Workspace.created_at.asc())
        .first()
    )

    if replacement is not None:
        replacement.is_default = True
