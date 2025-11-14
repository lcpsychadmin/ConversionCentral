from __future__ import annotations

from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import LegalRequirement
from app.schemas import (
    LegalRequirementCreate,
    LegalRequirementRead,
    LegalRequirementUpdate,
)

router = APIRouter(prefix="/legal-requirements", tags=["Application Settings"])


def _normalize_name(value: str) -> str:
    return value.strip()


def _get_requirement_or_404(requirement_id: UUID, db: Session) -> LegalRequirement:
    requirement = db.get(LegalRequirement, requirement_id)
    if not requirement:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Legal or regulatory requirement not found")
    return requirement


@router.post("", response_model=LegalRequirementRead, status_code=status.HTTP_201_CREATED)
def create_requirement(payload: LegalRequirementCreate, db: Session = Depends(get_db)) -> LegalRequirementRead:
    name = _normalize_name(payload.name)
    if not name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name is required")

    exists_stmt = select(LegalRequirement).where(LegalRequirement.name == name)
    if db.execute(exists_stmt).scalars().first():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="A requirement with this name already exists")

    requirement = LegalRequirement(
        name=name,
        description=payload.description,
        status=payload.status,
        display_order=payload.display_order,
    )
    db.add(requirement)
    try:
        db.commit()
    except IntegrityError as exc:  # pragma: no cover - safeguard for concurrent writes
        db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unable to create requirement") from exc

    db.refresh(requirement)
    return requirement


@router.get("", response_model=List[LegalRequirementRead])
def list_requirements(db: Session = Depends(get_db)) -> List[LegalRequirementRead]:
    stmt = select(LegalRequirement).order_by(LegalRequirement.display_order.nulls_last(), LegalRequirement.name.asc())
    return list(db.execute(stmt).scalars().all())


@router.get("/{requirement_id}", response_model=LegalRequirementRead)
def get_requirement(requirement_id: UUID, db: Session = Depends(get_db)) -> LegalRequirementRead:
    return _get_requirement_or_404(requirement_id, db)


@router.put("/{requirement_id}", response_model=LegalRequirementRead)
def update_requirement(requirement_id: UUID, payload: LegalRequirementUpdate, db: Session = Depends(get_db)) -> LegalRequirementRead:
    requirement = _get_requirement_or_404(requirement_id, db)

    update_data = payload.dict(exclude_unset=True)
    if "name" in update_data:
        name = _normalize_name(update_data["name"] or "")
        if not name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name is required")
        exists_stmt = (
            select(LegalRequirement)
            .where(LegalRequirement.name == name)
            .where(LegalRequirement.id != requirement.id)
        )
        if db.execute(exists_stmt).scalars().first():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="A requirement with this name already exists")
        requirement.name = name

    if "description" in update_data:
        requirement.description = update_data["description"]
    if "status" in update_data:
        requirement.status = update_data["status"] or "active"
    if "display_order" in update_data:
        requirement.display_order = update_data["display_order"]

    db.commit()
    db.refresh(requirement)
    return requirement


@router.delete(
	"/{requirement_id}",
	status_code=status.HTTP_204_NO_CONTENT,
	response_class=Response
)
def delete_requirement(requirement_id: UUID, db: Session = Depends(get_db)) -> Response:
    requirement = _get_requirement_or_404(requirement_id, db)
    db.delete(requirement)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
