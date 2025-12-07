from __future__ import annotations

from typing import Optional
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from app.models import Workspace


def resolve_workspace_id(db: Session, workspace_id: Optional[UUID]) -> UUID:
    """Resolve the workspace to scope a request to.

    If a workspace ID is provided, ensure it exists and is active. Otherwise
    return the first active workspace, preferring the default workspace when
    available.
    """

    if workspace_id:
        workspace = db.get(Workspace, workspace_id)
        if not workspace:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workspace not found")
        if not workspace.is_active:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Workspace is inactive")
        return workspace.id

    workspace = (
        db.query(Workspace)
        .filter(Workspace.is_active.is_(True))
        .order_by(Workspace.is_default.desc(), Workspace.created_at.asc())
        .first()
    )
    if not workspace:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No workspaces have been configured",
        )
    return workspace.id
