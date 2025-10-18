from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import DataObject, Table, TableLoadOrder
from app.schemas import (
    TableLoadOrderCreate,
    TableLoadOrderRead,
    TableLoadOrderUpdate,
)

router = APIRouter(prefix="/table-load-orders", tags=["Table Load Orders"])


def _get_order_or_404(order_id: UUID, db: Session) -> TableLoadOrder:
    order = db.get(TableLoadOrder, order_id)
    if not order:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Load order not found")
    return order


def _ensure_data_object_exists(data_object_id: UUID, db: Session) -> None:
    if not db.get(DataObject, data_object_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data object not found")


def _ensure_table_exists(table_id: UUID, db: Session) -> None:
    if not db.get(Table, table_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")


def _ensure_unique_table(
    data_object_id: UUID,
    table_id: UUID,
    db: Session,
    current_id: UUID | None = None,
) -> None:
    query = db.query(TableLoadOrder).filter(
        TableLoadOrder.data_object_id == data_object_id,
        TableLoadOrder.table_id == table_id,
    )
    if current_id:
        query = query.filter(TableLoadOrder.id != current_id)
    if db.query(query.exists()).scalar():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Table already has a load order for this data object",
        )


def _fetch_orders(db: Session, data_object_id: UUID) -> list[TableLoadOrder]:
    return (
        db.query(TableLoadOrder)
        .filter(TableLoadOrder.data_object_id == data_object_id)
        .order_by(TableLoadOrder.sequence, TableLoadOrder.created_at, TableLoadOrder.id)
        .all()
    )


def _validate_sequence(sequence: int, existing_count: int) -> None:
    max_position = existing_count + 1
    if sequence < 1 or sequence > max_position:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Sequence must be between 1 and {max_position}",
        )


def _shift_sequences(
    db: Session,
    data_object_id: UUID,
    start: int,
    end: int,
    delta: int,
    exclude_id: UUID | None = None,
) -> None:
    if start > end:
        return

    query = db.query(TableLoadOrder).filter(TableLoadOrder.data_object_id == data_object_id)
    if exclude_id:
        query = query.filter(TableLoadOrder.id != exclude_id)

    query = query.filter(TableLoadOrder.sequence >= start, TableLoadOrder.sequence <= end)
    order_by = TableLoadOrder.sequence.desc() if delta > 0 else TableLoadOrder.sequence.asc()

    for order in query.order_by(order_by).all():
        order.sequence += delta


@router.post("", response_model=TableLoadOrderRead, status_code=status.HTTP_201_CREATED)
def create_table_load_order(
    payload: TableLoadOrderCreate, db: Session = Depends(get_db)
) -> TableLoadOrderRead:
    _ensure_data_object_exists(payload.data_object_id, db)
    _ensure_table_exists(payload.table_id, db)
    _ensure_unique_table(payload.data_object_id, payload.table_id, db)

    existing_orders = _fetch_orders(db, payload.data_object_id)
    _validate_sequence(payload.sequence, len(existing_orders))

    max_sequence = len(existing_orders)
    _shift_sequences(
        db,
        payload.data_object_id,
        payload.sequence,
        max_sequence,
        delta=1,
    )

    new_order = TableLoadOrder(
        data_object_id=payload.data_object_id,
        table_id=payload.table_id,
        sequence=payload.sequence,
        notes=payload.notes,
    )

    db.add(new_order)

    db.commit()
    db.refresh(new_order)
    return new_order


@router.get("", response_model=list[TableLoadOrderRead])
def list_table_load_orders(db: Session = Depends(get_db)) -> list[TableLoadOrderRead]:
    return (
        db.query(TableLoadOrder)
        .order_by(TableLoadOrder.data_object_id, TableLoadOrder.sequence)
        .all()
    )


@router.get("/{order_id}", response_model=TableLoadOrderRead)
def get_table_load_order(order_id: UUID, db: Session = Depends(get_db)) -> TableLoadOrderRead:
    return _get_order_or_404(order_id, db)


@router.put("/{order_id}", response_model=TableLoadOrderRead)
def update_table_load_order(
    order_id: UUID,
    payload: TableLoadOrderUpdate,
    db: Session = Depends(get_db),
) -> TableLoadOrderRead:
    order = _get_order_or_404(order_id, db)
    update_data = payload.dict(exclude_unset=True)

    if "table_id" in update_data:
        new_table_id = update_data["table_id"]
        _ensure_table_exists(new_table_id, db)
        _ensure_unique_table(order.data_object_id, new_table_id, db, current_id=order_id)
        order.table_id = new_table_id

    if "notes" in update_data:
        order.notes = update_data["notes"]

    all_orders = _fetch_orders(db, order.data_object_id)
    other_orders = [o for o in all_orders if o.id != order_id]
    original_sequence = order.sequence
    target_sequence = update_data.get("sequence", original_sequence)
    _validate_sequence(target_sequence, len(other_orders))

    if target_sequence != original_sequence:
        sentinel = len(all_orders) + 1
        order.sequence = sentinel
        db.flush()

        if target_sequence < original_sequence:
            _shift_sequences(
                db,
                order.data_object_id,
                target_sequence,
                original_sequence - 1,
                delta=1,
                exclude_id=order_id,
            )
        else:
            _shift_sequences(
                db,
                order.data_object_id,
                original_sequence + 1,
                target_sequence,
                delta=-1,
                exclude_id=order_id,
            )

        db.flush()
        order.sequence = target_sequence

    db.commit()
    db.refresh(order)
    return order


@router.delete("/{order_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_table_load_order(order_id: UUID, db: Session = Depends(get_db)) -> None:
    order = _get_order_or_404(order_id, db)
    db.delete(order)
    max_sequence = len(_fetch_orders(db, order.data_object_id))
    _shift_sequences(
        db,
        order.data_object_id,
        order.sequence + 1,
        max_sequence,
        delta=-1,
        exclude_id=order_id,
    )
    db.commit()