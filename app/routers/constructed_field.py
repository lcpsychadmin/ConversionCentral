from logging import getLogger
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import case, func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import ConstructedField, ConstructedTable
from app.schemas import (
    ConstructedFieldCreate,
    ConstructedFieldRead,
    ConstructedFieldUpdate,
)
from app.constants.audit_fields import AUDIT_FIELD_NAME_SET

logger = getLogger(__name__)

router = APIRouter(prefix="/constructed-fields", tags=["Constructed Fields"])


def _get_constructed_field_or_404(constructed_field_id: UUID, db: Session) -> ConstructedField:
    constructed_field = db.get(ConstructedField, constructed_field_id)
    if not constructed_field:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed field not found",
        )
    return constructed_field


def _ensure_constructed_table_exists(constructed_table_id: UUID, db: Session) -> ConstructedTable:
    constructed_table = db.get(ConstructedTable, constructed_table_id)
    if not constructed_table:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed table not found",
        )
    return constructed_table


def _sync_constructed_table_to_sql_server(
    constructed_table: ConstructedTable, _db: Session
) -> None:
    """Legacy helper retained for compatibility.

    SQL Server synchronization relied on per-system connection metadata that has
    been removed from the platform. Rather than attempting a partial sync, log
    a message so operators understand why nothing happens.
    """
    logger.info(
        "SQL Server sync skipped for constructed table %s because system connections are no longer tracked.",
        getattr(constructed_table, "id", constructed_table),
    )


@router.post("", response_model=ConstructedFieldRead, status_code=status.HTTP_201_CREATED)
def create_constructed_field(
    payload: ConstructedFieldCreate, db: Session = Depends(get_db)
) -> ConstructedFieldRead:
    constructed_table = _ensure_constructed_table_exists(payload.constructed_table_id, db)

    constructed_field = ConstructedField(**payload.dict())
    db.add(constructed_field)
    db.flush()

    # Sync the entire table to SQL Server with all fields
    _sync_constructed_table_to_sql_server(constructed_table, db)

    db.commit()
    db.refresh(constructed_field)
    return constructed_field


@router.get("", response_model=list[ConstructedFieldRead])
def list_constructed_fields(
    constructed_table_id: UUID | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[ConstructedFieldRead]:
    query = db.query(ConstructedField)

    if constructed_table_id:
        query = query.filter(ConstructedField.constructed_table_id == constructed_table_id)

    audit_priority = case(
        (func.lower(ConstructedField.name).in_(tuple(AUDIT_FIELD_NAME_SET)), 1),
        else_=0,
    )

    return (
        query.order_by(
            audit_priority.asc(),
            ConstructedField.display_order.asc(),
            ConstructedField.created_at.asc(),
            ConstructedField.id.asc(),
        )
        .all()
    )


@router.get("/{constructed_field_id}", response_model=ConstructedFieldRead)
def get_constructed_field(
    constructed_field_id: UUID, db: Session = Depends(get_db)
) -> ConstructedFieldRead:
    return _get_constructed_field_or_404(constructed_field_id, db)


@router.put("/{constructed_field_id}", response_model=ConstructedFieldRead)
def update_constructed_field(
    constructed_field_id: UUID,
    payload: ConstructedFieldUpdate,
    db: Session = Depends(get_db),
) -> ConstructedFieldRead:
    constructed_field = _get_constructed_field_or_404(constructed_field_id, db)
    update_data = payload.dict(exclude_unset=True)

    constructed_table_id = update_data.get("constructed_table_id")
    if constructed_table_id:
        constructed_table = _ensure_constructed_table_exists(constructed_table_id, db)
    else:
        constructed_table = db.get(ConstructedTable, constructed_field.constructed_table_id)

    for field, value in update_data.items():
        setattr(constructed_field, field, value)

    db.flush()

    _sync_constructed_table_to_sql_server(constructed_table, db)

    db.commit()
    db.refresh(constructed_field)
    return constructed_field


@router.delete("/{constructed_field_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_constructed_field(
    constructed_field_id: UUID, db: Session = Depends(get_db)
) -> None:
    constructed_field = _get_constructed_field_or_404(constructed_field_id, db)
    constructed_table = db.get(ConstructedTable, constructed_field.constructed_table_id)

    db.delete(constructed_field)
    db.flush()

    _sync_constructed_table_to_sql_server(constructed_table, db)

    db.commit()
