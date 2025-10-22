"""Router for managing constructed data validation rules."""
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import ConstructedDataValidationRule, ConstructedTable
from app.schemas import (
    ConstructedDataValidationRuleCreate,
    ConstructedDataValidationRuleRead,
    ConstructedDataValidationRuleUpdate,
)

router = APIRouter(
    prefix="/constructed-data-validation-rules",
    tags=["Constructed Data Validation Rules"]
)


def _get_validation_rule_or_404(rule_id: UUID, db: Session) -> ConstructedDataValidationRule:
    rule = db.get(ConstructedDataValidationRule, rule_id)
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Validation rule not found",
        )
    return rule


def _ensure_constructed_table_exists(constructed_table_id: UUID, db: Session) -> None:
    if not db.get(ConstructedTable, constructed_table_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed table not found",
        )


@router.post("", response_model=ConstructedDataValidationRuleRead, status_code=status.HTTP_201_CREATED)
def create_validation_rule(
    payload: ConstructedDataValidationRuleCreate, db: Session = Depends(get_db)
) -> ConstructedDataValidationRuleRead:
    """Create a new validation rule for a constructed table."""
    _ensure_constructed_table_exists(payload.constructed_table_id, db)

    rule = ConstructedDataValidationRule(**payload.dict())
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return rule


@router.get("", response_model=list[ConstructedDataValidationRuleRead])
def list_validation_rules(
    constructed_table_id: UUID | None = None, db: Session = Depends(get_db)
) -> list[ConstructedDataValidationRuleRead]:
    """List validation rules, optionally filtered by constructed table."""
    query = db.query(ConstructedDataValidationRule)
    
    if constructed_table_id:
        query = query.filter(
            ConstructedDataValidationRule.constructed_table_id == constructed_table_id
        )
    
    return query.all()


@router.get("/{rule_id}", response_model=ConstructedDataValidationRuleRead)
def get_validation_rule(
    rule_id: UUID, db: Session = Depends(get_db)
) -> ConstructedDataValidationRuleRead:
    """Get a specific validation rule."""
    return _get_validation_rule_or_404(rule_id, db)


@router.put("/{rule_id}", response_model=ConstructedDataValidationRuleRead)
def update_validation_rule(
    rule_id: UUID,
    payload: ConstructedDataValidationRuleUpdate,
    db: Session = Depends(get_db),
) -> ConstructedDataValidationRuleRead:
    """Update a validation rule."""
    rule = _get_validation_rule_or_404(rule_id, db)
    update_data = payload.dict(exclude_unset=True)

    for field, value in update_data.items():
        setattr(rule, field, value)

    db.commit()
    db.refresh(rule)
    return rule


@router.delete("/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_validation_rule(
    rule_id: UUID, db: Session = Depends(get_db)
) -> None:
    """Delete a validation rule."""
    rule = _get_validation_rule_or_404(rule_id, db)
    db.delete(rule)
    db.commit()
