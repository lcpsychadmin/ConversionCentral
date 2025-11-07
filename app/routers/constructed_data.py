import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session, selectinload

from app.database import get_db
from app.models import ConstructedTable
from app.schemas import (
    ConstructedDataBatchSaveRequest,
    ConstructedDataBatchSaveResponse,
    ConstructedDataCreate,
    ConstructedDataRead,
    ConstructedDataUpdate,
    ConstructedTableStatus,
)
from app.services.constructed_data_store import ConstructedDataRecord, ConstructedDataStore
from app.services.constructed_data_warehouse import (
    ConstructedDataWarehouse,
    ConstructedDataWarehouseError,
)
from app.services.validation_engine import ValidationEngine

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/constructed-data", tags=["Constructed Data"])


def get_constructed_data_store() -> ConstructedDataStore:
    return ConstructedDataStore()


def get_constructed_data_warehouse() -> ConstructedDataWarehouse:
    return ConstructedDataWarehouse()


def _record_to_schema(record: ConstructedDataRecord) -> ConstructedDataRead:
    return ConstructedDataRead(
        id=record.id,
        constructed_table_id=record.constructed_table_id,
        row_identifier=record.row_identifier,
        payload=record.payload,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def _get_constructed_table(
    constructed_table_id: UUID,
    db: Session,
    *,
    require_approved: bool = False,
    include_fields: bool = False,
) -> ConstructedTable:
    query = db.query(ConstructedTable)
    if include_fields:
        query = query.options(selectinload(ConstructedTable.fields))
    constructed_table = (
        query.filter(ConstructedTable.id == constructed_table_id).first()
    )
    if not constructed_table:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed table not found",
        )
    if require_approved and constructed_table.status != ConstructedTableStatus.APPROVED.value:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Constructed table must be approved before data can be managed",
        )
    return constructed_table


@router.post("", response_model=ConstructedDataRead, status_code=status.HTTP_201_CREATED)
def create_constructed_data(
    payload: ConstructedDataCreate,
    db: Session = Depends(get_db),
    store: ConstructedDataStore = Depends(get_constructed_data_store),
    warehouse: ConstructedDataWarehouse = Depends(get_constructed_data_warehouse),
) -> ConstructedDataRead:
    constructed_table = _get_constructed_table(
        payload.constructed_table_id,
        db,
        require_approved=True,
        include_fields=True,
    )

    try:
        record = store.insert_row(
            constructed_table_id=payload.constructed_table_id,
            payload=payload.payload,
            row_identifier=payload.row_identifier,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    try:
        warehouse.upsert_records(constructed_table, [record])
    except ConstructedDataWarehouseError as exc:
        try:
            store.delete_row(record.id)
        except Exception as cleanup_error:  # pragma: no cover - defensive logging
            logger.error(
                "Failed to roll back constructed data row %s after warehouse error: %s",
                record.id,
                cleanup_error,
            )
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    return _record_to_schema(record)


@router.get("", response_model=list[ConstructedDataRead])
def list_constructed_data(
    store: ConstructedDataStore = Depends(get_constructed_data_store),
) -> list[ConstructedDataRead]:
    try:
        records = store.list_all_rows()
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    return [_record_to_schema(record) for record in records]


@router.get("/{constructed_data_id}", response_model=ConstructedDataRead)
def get_constructed_data(
    constructed_data_id: UUID,
    store: ConstructedDataStore = Depends(get_constructed_data_store),
) -> ConstructedDataRead:
    try:
        record = store.get_row(constructed_data_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed data not found",
        )
    return _record_to_schema(record)


@router.put("/{constructed_data_id}", response_model=ConstructedDataRead)
def update_constructed_data(
    constructed_data_id: UUID,
    payload: ConstructedDataUpdate,
    db: Session = Depends(get_db),
    store: ConstructedDataStore = Depends(get_constructed_data_store),
    warehouse: ConstructedDataWarehouse = Depends(get_constructed_data_warehouse),
) -> ConstructedDataRead:
    try:
        existing_record = store.get_row(constructed_data_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    if existing_record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed data not found",
        )
    update_data = payload.dict(exclude_unset=True)

    original_table = _get_constructed_table(
        existing_record.constructed_table_id,
        db,
        require_approved=True,
        include_fields=True,
    )

    target_table_id = update_data.get("constructed_table_id", existing_record.constructed_table_id)
    target_table = original_table
    if target_table_id != existing_record.constructed_table_id:
        target_table = _get_constructed_table(
            target_table_id,
            db,
            require_approved=True,
            include_fields=True,
        )

    update_kwargs: dict[str, object] = {}
    if "constructed_table_id" in update_data:
        update_kwargs["constructed_table_id"] = update_data["constructed_table_id"]
    if "payload" in update_data:
        update_kwargs["payload"] = update_data["payload"]
    if "row_identifier" in update_data:
        update_kwargs["row_identifier"] = update_data["row_identifier"]

    try:
        updated = store.update_row(constructed_data_id, **update_kwargs)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    try:
        if target_table.id != original_table.id:
            warehouse.upsert_records(target_table, [updated])
            try:
                warehouse.delete_rows(original_table, [existing_record.id])
            except ConstructedDataWarehouseError as delete_exc:
                try:
                    warehouse.delete_rows(target_table, [updated.id])
                except ConstructedDataWarehouseError as cleanup_exc:  # pragma: no cover - defensive logging
                    logger.error(
                        "Failed to roll back new warehouse row %s after delete failure: %s",
                        updated.id,
                        cleanup_exc,
                    )
                raise delete_exc
        else:
            warehouse.upsert_records(target_table, [updated])
    except ConstructedDataWarehouseError as exc:
        try:
            store.update_row(
                constructed_data_id,
                constructed_table_id=existing_record.constructed_table_id,
                payload=existing_record.payload,
                row_identifier=existing_record.row_identifier,
            )
        except Exception as rollback_error:  # pragma: no cover - defensive logging
            logger.error(
                "Failed to revert constructed data row %s after warehouse error: %s",
                constructed_data_id,
                rollback_error,
            )
        try:
            warehouse.upsert_records(original_table, [existing_record])
        except ConstructedDataWarehouseError as warehouse_rollback_error:  # pragma: no cover - defensive logging
            logger.error(
                "Failed to restore warehouse state for row %s: %s",
                constructed_data_id,
                warehouse_rollback_error,
            )
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    return _record_to_schema(updated)


@router.delete("/{constructed_data_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_constructed_data(
    constructed_data_id: UUID,
    db: Session = Depends(get_db),
    store: ConstructedDataStore = Depends(get_constructed_data_store),
    warehouse: ConstructedDataWarehouse = Depends(get_constructed_data_warehouse),
) -> None:
    try:
        record = store.get_row(constructed_data_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Constructed data not found",
        )

    constructed_table = _get_constructed_table(
        record.constructed_table_id,
        db,
        include_fields=True,
    )

    try:
        warehouse.delete_rows(constructed_table, [record.id])
    except ConstructedDataWarehouseError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    try:
        store.delete_row(constructed_data_id)
    except RuntimeError as exc:
        try:
            warehouse.upsert_records(constructed_table, [record])
        except ConstructedDataWarehouseError as restore_error:  # pragma: no cover - defensive logging
            logger.error(
                "Failed to restore warehouse row %s after delete rollback: %s",
                record.id,
                restore_error,
            )
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@router.post("/{constructed_table_id}/batch-save", response_model=ConstructedDataBatchSaveResponse)
def batch_save_constructed_data(
    constructed_table_id: UUID,
    request: ConstructedDataBatchSaveRequest,
    db: Session = Depends(get_db),
    store: ConstructedDataStore = Depends(get_constructed_data_store),
    warehouse: ConstructedDataWarehouse = Depends(get_constructed_data_warehouse),
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
    constructed_table = _get_constructed_table(
        constructed_table_id,
        db,
        require_approved=True,
        include_fields=True,
    )

    # Initialize validation engine
    engine = ValidationEngine(db, store)

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
        records = store.insert_rows(constructed_table_id, request.rows)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - surface storage failure
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error saving constructed data: {exc}",
        )

    try:
        warehouse.upsert_records(constructed_table, records)
    except ConstructedDataWarehouseError as exc:
        for record in records:
            try:
                store.delete_row(record.id)
            except Exception as cleanup_error:  # pragma: no cover - defensive logging
                logger.error(
                    "Failed to roll back constructed data row %s after warehouse batch error: %s",
                    record.id,
                    cleanup_error,
                )
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return ConstructedDataBatchSaveResponse(
        success=True,
        rowsSaved=len(request.rows),
        errors=[],
    )


@router.get(
    "/{constructed_table_id}/by-table", response_model=list[ConstructedDataRead]
)
def get_constructed_data_by_table(
    constructed_table_id: UUID,
    db: Session = Depends(get_db),
    store: ConstructedDataStore = Depends(get_constructed_data_store),
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
    _get_constructed_table(constructed_table_id, db)

    try:
        records = store.list_rows(constructed_table_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    return [_record_to_schema(record) for record in records]