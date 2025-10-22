from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import ConstructedData, ConstructedTable
from app.schemas import (
    ConstructedDataBatchSaveRequest,
    ConstructedDataBatchSaveResponse,
    ConstructedDataCreate,
    ConstructedDataRead,
    ConstructedDataUpdate,
    ConstructedTableStatus,
)
from app.services.validation_engine import ValidationEngine

router = APIRouter(prefix="/constructed-data", tags=["Constructed Data"])


def _get_constructed_data_or_404(constructed_data_id: UUID, db: Session) -> ConstructedData:
    constructed_data = db.get(ConstructedData, constructed_data_id)
    if not constructed_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed data not found",
        )
    return constructed_data


def _get_constructed_table_or_error(constructed_table_id: UUID, db: Session) -> ConstructedTable:
    constructed_table = db.get(ConstructedTable, constructed_table_id)
    if not constructed_table:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed table not found",
        )
    return constructed_table


def _ensure_table_is_approved(constructed_table_id: UUID, db: Session) -> None:
    constructed_table = _get_constructed_table_or_error(constructed_table_id, db)
    if constructed_table.status != ConstructedTableStatus.APPROVED.value:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Constructed table must be approved before data can be managed",
        )


@router.post("", response_model=ConstructedDataRead, status_code=status.HTTP_201_CREATED)
def create_constructed_data(
    payload: ConstructedDataCreate, db: Session = Depends(get_db)
) -> ConstructedDataRead:
    _ensure_table_is_approved(payload.constructed_table_id, db)

    constructed_data = ConstructedData(**payload.dict())
    db.add(constructed_data)
    db.commit()
    db.refresh(constructed_data)
    return constructed_data


@router.get("", response_model=list[ConstructedDataRead])
def list_constructed_data(db: Session = Depends(get_db)) -> list[ConstructedDataRead]:
    return db.query(ConstructedData).all()


@router.get("/{constructed_data_id}", response_model=ConstructedDataRead)
def get_constructed_data(
    constructed_data_id: UUID, db: Session = Depends(get_db)
) -> ConstructedDataRead:
    return _get_constructed_data_or_404(constructed_data_id, db)


@router.put("/{constructed_data_id}", response_model=ConstructedDataRead)
def update_constructed_data(
    constructed_data_id: UUID,
    payload: ConstructedDataUpdate,
    db: Session = Depends(get_db),
) -> ConstructedDataRead:
    constructed_data = _get_constructed_data_or_404(constructed_data_id, db)
    update_data = payload.dict(exclude_unset=True)

    constructed_table_id = update_data.get("constructed_table_id")
    if constructed_table_id:
        _ensure_table_is_approved(constructed_table_id, db)

    for field, value in update_data.items():
        setattr(constructed_data, field, value)

    db.commit()
    db.refresh(constructed_data)
    return constructed_data


@router.delete("/{constructed_data_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_constructed_data(
    constructed_data_id: UUID, db: Session = Depends(get_db)
) -> None:
    constructed_data = _get_constructed_data_or_404(constructed_data_id, db)
    db.delete(constructed_data)
    db.commit()


@router.post("/{constructed_table_id}/batch-save", response_model=ConstructedDataBatchSaveResponse)
def batch_save_constructed_data(
    constructed_table_id: UUID,
    request: ConstructedDataBatchSaveRequest,
    db: Session = Depends(get_db),
) -> ConstructedDataBatchSaveResponse:
    """
    Batch save multiple rows of constructed data with validation.

    Args:
        constructed_table_id: ID of the constructed table
        request: Batch save request with rows and optional validateOnly flag
        db: Database session

    Returns:
        Response with success status, rows saved count, and any validation errors

    The validateOnly flag allows testing validation without saving data.
    If validation fails, no data is saved (transaction rolls back).
    """
    # Ensure table exists and is approved
    _ensure_table_is_approved(constructed_table_id, db)

    # Initialize validation engine
    engine = ValidationEngine(db)

    # Validate all rows
    validation_errors = engine.validate_batch(request.rows, constructed_table_id, is_new_rows=True)

    # If there are validation errors, return them without saving
    if validation_errors:
        return ConstructedDataBatchSaveResponse(
            success=False,
            rowsSaved=0,
            errors=[error.dict() for error in validation_errors],
        )

    # If validateOnly is true, return success without saving
    if request.validateOnly:
        return ConstructedDataBatchSaveResponse(
            success=True,
            rowsSaved=0,
            errors=[],
        )

    # Save all validated rows
    try:
        for row_data in request.rows:
            constructed_data = ConstructedData(
                constructed_table_id=constructed_table_id,
                payload=row_data,
            )
            db.add(constructed_data)

        db.commit()

        return ConstructedDataBatchSaveResponse(
            success=True,
            rowsSaved=len(request.rows),
            errors=[],
        )
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error saving constructed data: {str(e)}",
        )


@router.get(
    "/{constructed_table_id}/by-table", response_model=list[ConstructedDataRead]
)
def get_constructed_data_by_table(
    constructed_table_id: UUID, db: Session = Depends(get_db)
) -> list[ConstructedDataRead]:
    """
    Get all constructed data rows for a specific table.

    Args:
        constructed_table_id: ID of the constructed table
        db: Database session

    Returns:
        List of all data rows for the table
    """
    # Ensure table exists
    _get_constructed_table_or_error(constructed_table_id, db)

    return (
        db.query(ConstructedData)
        .filter(ConstructedData.constructed_table_id == constructed_table_id)
        .all()
    )