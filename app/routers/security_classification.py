from __future__ import annotations

from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import SecurityClassification
from app.schemas import (
    SecurityClassificationCreate,
    SecurityClassificationRead,
    SecurityClassificationUpdate,
)

router = APIRouter(prefix="/security-classifications", tags=["Application Settings"])


def _normalize_name(value: str) -> str:
    return value.strip()


def _get_classification_or_404(classification_id: UUID, db: Session) -> SecurityClassification:
    classification = db.get(SecurityClassification, classification_id)
    if not classification:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Security classification not found")
    return classification


@router.post("", response_model=SecurityClassificationRead, status_code=status.HTTP_201_CREATED)
def create_classification(payload: SecurityClassificationCreate, db: Session = Depends(get_db)) -> SecurityClassificationRead:
    name = _normalize_name(payload.name)
    if not name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name is required")

    exists_stmt = select(SecurityClassification).where(SecurityClassification.name == name)
    if db.execute(exists_stmt).scalars().first():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="A classification with this name already exists")

    classification = SecurityClassification(
        name=name,
        description=payload.description,
        status=payload.status,
        display_order=payload.display_order,
    )
    db.add(classification)
    try:
        db.commit()
    except IntegrityError as exc:  # pragma: no cover - safeguard for concurrent writes
        db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unable to create classification") from exc

    db.refresh(classification)
    return classification


@router.get("", response_model=List[SecurityClassificationRead])
def list_classifications(db: Session = Depends(get_db)) -> List[SecurityClassificationRead]:
    stmt = select(SecurityClassification).order_by(
        SecurityClassification.display_order.nulls_last(),
        SecurityClassification.name.asc(),
    )
    return list(db.execute(stmt).scalars().all())


@router.get("/{classification_id}", response_model=SecurityClassificationRead)
def get_classification(classification_id: UUID, db: Session = Depends(get_db)) -> SecurityClassificationRead:
    return _get_classification_or_404(classification_id, db)


@router.put("/{classification_id}", response_model=SecurityClassificationRead)
def update_classification(classification_id: UUID, payload: SecurityClassificationUpdate, db: Session = Depends(get_db)) -> SecurityClassificationRead:
    classification = _get_classification_or_404(classification_id, db)

    update_data = payload.dict(exclude_unset=True)
    if "name" in update_data:
        name = _normalize_name(update_data["name"] or "")
        if not name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Name is required")
        exists_stmt = (
            select(SecurityClassification)
            .where(SecurityClassification.name == name)
            .where(SecurityClassification.id != classification.id)
        )
        if db.execute(exists_stmt).scalars().first():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="A classification with this name already exists")
        classification.name = name

    if "description" in update_data:
        classification.description = update_data["description"]
    if "status" in update_data:
        classification.status = update_data["status"] or "active"
    if "display_order" in update_data:
        classification.display_order = update_data["display_order"]

    db.commit()
    db.refresh(classification)
    return classification


@router.delete(
	"/{classification_id}",
	status_code=status.HTTP_204_NO_CONTENT,
	response_class=Response
)
def delete_classification(classification_id: UUID, db: Session = Depends(get_db)) -> Response:
    classification = _get_classification_or_404(classification_id, db)
    db.delete(classification)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
